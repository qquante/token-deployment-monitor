"""
Tweet Processor Worker

Processes tweets and classifies them using LLM Agent.

Responsibilities:
- Subscribe to twitter:tweet channel
- Classify tweets (PRE-BANKR request or @bankrbot DEPLOYED)
- Parse token/fee info
- Publish to appropriate channels (telegram_progressive handles sending)

Input Channels:
- twitter:tweet - Raw tweets from listener

Output Channels:
- twitter:prebankr - PRE-BANKR request detected (telegram_progressive subscribes)
- twitter:deployed - DEPLOYED notification detected (telegram_progressive subscribes)
- enrich:request - User enrichment requests (optional, for caching)
"""

import os
import sys
import json
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime

from workers.base import BaseWorker, project_root
from shared.pubsub import Channels, Events
from shared.models import TokenInfo
# NOTE: telegram_progressive worker handles message sending now
# from shared.formatters import build_deployed_message, build_prebankr_message

# Import parser functions from src
sys.path.insert(0, str(project_root))
from src.database.parser import classify_bankr_tweet


class TweetProcessorWorker(BaseWorker):
    """
    Processes tweets and classifies using LLM.
    """
    
    WORKER_NAME = "tweet-processor"
    
    def __init__(self):
        super().__init__()
        
        # LLM config
        self.google_key = os.getenv("GOOGLE_API_KEY", "")
        self.openrouter_key = os.getenv("OPENROUTER_API_KEY", "")
        self.model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
        
        if not self.google_key and not self.openrouter_key:
            self.logger.warning("No LLM API key set - classification will be limited")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Processed tweets (dedupe)
        self._processed_tweets = set()
        
        # Stats
        self.stats.update({
            "tweets_processed": 0,
            "prebankr_detected": 0,
            "deployed_detected": 0,
            "agent_calls": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session for LLM calls."""
        await super().setup()
        self._session = aiohttp.ClientSession()
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Subscribe to tweets and process them."""
        self.logger.info("Subscribing to twitter:tweet...")
        
        async for channel, message in self.redis.subscribe(Channels.TWITTER_TWEET):
            if not self._running:
                break
            
            try:
                await self._process_tweet(message)
            except Exception as e:
                self.logger.error(f"Process error: {e}")
                self.stats["errors"] += 1
    
    async def _process_tweet(self, event: Dict[str, Any]):
        """Process a single tweet event."""
        tweet_id = event.get("tweet_id", "")
        author = event.get("author_username", "")
        text = event.get("text", "")
        raw_tweet = event.get("raw_tweet", {})
        
        # GLOBAL DEDUPE using Redis SETNX (works across all replicas!)
        # This ensures only ONE replica processes each tweet
        dedupe_key = f"tweet_processed:{tweet_id}"
        is_new = await self.redis.setnx(dedupe_key, "1")
        
        if not is_new:
            # Another replica already claimed this tweet
            self.logger.debug(f"[DEDUPE] Tweet {tweet_id[:8]}... already being processed")
            return
        
        # Set TTL to auto-cleanup (1 hour)
        await self.redis.expire(dedupe_key, 3600)
        
        # Also keep local cache for fast path (avoid Redis call for recent tweets)
        self._processed_tweets.add(tweet_id)
        if len(self._processed_tweets) > 10000:
            self._processed_tweets = set(list(self._processed_tweets)[-5000:])
        
        self.stats["tweets_processed"] += 1
        
        author_lower = author.lower()
        
        # =====================================================================
        # CASE 1: From @bankrbot (DEPLOYED notification)
        # =====================================================================
        if author_lower == "bankrbot":
            await self._handle_bankrbot_tweet(event, raw_tweet)
        
        # =====================================================================
        # CASE 2: Mentions @bankrbot (potential PRE-BANKR)
        # =====================================================================
        elif "@bankrbot" in text.lower():
            await self._handle_user_tweet(event, raw_tweet)
        
        self.stats["messages_processed"] += 1
    
    async def _handle_bankrbot_tweet(self, event: Dict[str, Any], raw_tweet: Dict[str, Any]):
        """Handle tweet from @bankrbot."""
        # Parse using existing parser
        parsed = classify_bankr_tweet(raw_tweet)
        action_type = parsed.get("type", "unknown")
        
        if action_type != "deployment":
            self.logger.debug(f"Bankrbot tweet type: {action_type} - skipping")
            return
        
        self.logger.info(f"[DEPLOYED] {parsed.get('token_name')} (${parsed.get('token_symbol')})")
        self.stats["deployed_detected"] += 1
        
        # Extract data
        from_user = parsed.get("from_user", {})
        to_users = parsed.get("to_users", [])
        original_tweet_id = raw_tweet.get("inReplyToId", "")
        
        # Create deployed event
        deployed_event = Events.deployed_detected(
            tweet_id=event.get("tweet_id", ""),
            tweet_url=event.get("url", ""),
            original_tweet_id=original_tweet_id,
            token_name=parsed.get("token_name", ""),
            token_symbol=parsed.get("token_symbol", ""),
            contract_address=parsed.get("contract_address", ""),
            from_username=from_user.get("username", ""),
            to_usernames=[u.get("username", "") for u in to_users],
            raw_tweet=raw_tweet,
        )
        
        # Add extra parsed data
        deployed_event["fee_recipient"] = parsed.get("fee_recipient_username", "")
        deployed_event["fee_percentage"] = parsed.get("fee_percentage", "")
        
        # Publish event
        await self.publish(Channels.TWITTER_DEPLOYED, deployed_event)
        
        # Request enrichment for FROM and TO users
        from_username = from_user.get("username")
        if from_username:
            await self._request_enrichment(from_username, priority=2)
        
        for to_user in to_users:
            to_username = to_user.get("username")
            if to_username:
                await self._request_enrichment(to_username, priority=2)
        
        # NOTE: telegram_progressive worker handles sending the message
        # It subscribes to TWITTER_DEPLOYED channel and sends progressive messages
    
    async def _handle_user_tweet(self, event: Dict[str, Any], raw_tweet: Dict[str, Any]):
        """Handle user tweet mentioning @bankrbot."""
        author = event.get("author_username", "")
        text = event.get("text", "")
        
        # Use LLM to classify
        result = await self._classify_with_agent(author, text)
        
        if not result:
            return
        
        is_deploy = result.get("is_deploy", False)
        confidence = result.get("confidence", 0)
        
        # Log
        self.logger.info(f"[AGENT] @{author}: deploy={is_deploy} conf={confidence}%")
        
        # Require high confidence
        if not is_deploy or confidence < 85:
            self.logger.debug(f"Low confidence ({confidence}%) - skipping")
            return
        
        self.logger.info(f"[PRE-BANKR] Detected from @{author}")
        self.stats["prebankr_detected"] += 1
        
        # Extract token/fee info
        token = result.get("token", {}) or {}
        fee = result.get("fee", {}) or {}
        
        # Fee recipients
        fee_recipients = fee.get("recipients", []) or []
        if not fee_recipients and fee.get("recipient"):
            fee_recipients = [fee.get("recipient")]
        fee_recipients = [
            r.lstrip("@") for r in fee_recipients
            if r and r.lower().replace("@", "") != "bankrbot"
        ]
        
        # Create PRE-BANKR event
        prebankr_event = Events.prebankr_detected(
            tweet_id=event.get("tweet_id", ""),
            tweet_url=event.get("url", ""),
            from_username=author,
            to_usernames=fee_recipients,
            token_name=token.get("name"),
            token_symbol=token.get("symbol"),
            confidence=confidence,
            raw_tweet=raw_tweet,
        )
        
        # Publish
        await self.publish(Channels.TWITTER_PREBANKR, prebankr_event)
        
        # Save token info to Redis
        token_info = TokenInfo(
            tweet_id=event.get("tweet_id", ""),
            token_symbol=token.get("symbol", ""),
            token_name=token.get("name", ""),
            tweet_url=event.get("url", ""),
            from_username=author,
            to_usernames=fee_recipients,
            prebankr_at=datetime.now().isoformat(),
        )
        await self.storage.save_token_info(token_info)
        
        # Request enrichment for FROM and TO users (optional, for caching)
        await self._request_enrichment(author, priority=1)
        for recipient in fee_recipients:
            await self._request_enrichment(recipient, priority=1)
        
        # NOTE: telegram_progressive worker handles sending the message
        # It subscribes to TWITTER_PREBANKR channel and sends progressive messages
    
    async def _classify_with_agent(
        self,
        author: str,
        text: str,
    ) -> Optional[Dict[str, Any]]:
        """Classify tweet using LLM agent."""
        self.stats["agent_calls"] += 1
        
        # Build prompt
        prompt = f"""Analyze this tweet from @{author}:

"{text}"

Is this a request to deploy a new token via @bankrbot?

Return JSON with:
- is_deploy: boolean
- confidence: 0-100
- token: {{name, symbol}} if detected
- fee: {{recipients: [], split: boolean}} if fee recipients mentioned

Rules:
- Must mention @bankrbot
- Must show intent to create/deploy token
- Fee recipient is usually tagged user
- High confidence = clear deploy intent"""

        try:
            if self.google_key:
                return await self._call_google_ai(prompt)
            elif self.openrouter_key:
                return await self._call_openrouter(prompt)
            else:
                return None
        except Exception as e:
            self.logger.error(f"Agent error: {e}")
            return None
    
    async def _call_google_ai(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call Google AI directly."""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        
        async with self._session.post(
            url,
            params={"key": self.google_key},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "topP": 0.8,
                    "maxOutputTokens": 512,
                }
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
            
            # Extract text from response
            candidates = data.get("candidates", [])
            if not candidates:
                return None
            
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if not parts:
                return None
            
            text = parts[0].get("text", "")
            
            # Parse JSON from response
            return self._extract_json(text)
    
    async def _call_openrouter(self, prompt: str) -> Optional[Dict[str, Any]]:
        """Call OpenRouter API."""
        model = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
        
        async with self._session.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {self.openrouter_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 512,
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
            
            choices = data.get("choices", [])
            if not choices:
                return None
            
            content = choices[0].get("message", {}).get("content", "")
            return self._extract_json(content)
    
    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from LLM response text."""
        # Try to find JSON block
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            text = text[start:end]
        elif "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            text = text[start:end]
        
        # Try to parse
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            # Try to find JSON object in text
            for i, char in enumerate(text):
                if char == "{":
                    for j in range(len(text) - 1, i, -1):
                        if text[j] == "}":
                            try:
                                return json.loads(text[i:j+1])
                            except:
                                pass
            return None
    
    async def _request_enrichment(self, username: str, priority: int = 0):
        """Request user enrichment via Pub/Sub."""
        event = Events.enrich_request(
            username=username,
            fetch_smart_followers=True,
            priority=priority,
        )
        await self.publish(Channels.ENRICH_REQUEST, event)


# Entry point
if __name__ == "__main__":
    TweetProcessorWorker.main()
