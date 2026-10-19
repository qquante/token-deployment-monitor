"""
Twitter Listener Worker

Connects to Twitter WebSocket API and publishes tweets to Redis.

Responsibilities:
- Maintain WebSocket connection to Twitter
- Receive real-time tweets
- Publish raw tweets to Redis Pub/Sub
- Auto-reconnect on disconnection

Output Channels:
- twitter:tweet - Raw tweet data
"""

import os
import json
import asyncio
import websockets
from typing import Dict, Any

from workers.base import BaseWorker
from shared.pubsub import Channels, Events


class TwitterListenerWorker(BaseWorker):
    """
    Listens to Twitter WebSocket and publishes tweets.
    """
    
    WORKER_NAME = "twitter-listener"
    
    def __init__(self):
        super().__init__()
        
        self.api_key = os.getenv("TWITTER_API_KEY", "")
        self.ws_url = "wss://ws.twitterapi.io/twitter/tweet/websocket"
        
        if not self.api_key:
            raise ValueError("TWITTER_API_KEY not set!")
        
        # Stats specific to this worker
        self.stats.update({
            "tweets_received": 0,
            "reconnects": 0,
            "last_tweet_at": None,
        })
    
    async def run(self):
        """Main loop - connect to WebSocket and process messages."""
        headers = {"x-api-key": self.api_key}
        
        while self._running:
            try:
                self.logger.info("Connecting to Twitter WebSocket...")
                
                async with websockets.connect(
                    self.ws_url,
                    additional_headers=headers,
                    ping_interval=40,
                    ping_timeout=30,
                    close_timeout=10,
                ) as ws:
                    self.logger.info("Connected!")
                    
                    async for message in ws:
                        if not self._running:
                            break
                        
                        await self._handle_message(message)
            
            except websockets.ConnectionClosed as e:
                self.logger.warning(f"Connection closed: {e}")
                self.stats["reconnects"] += 1
                if self._running:
                    self.logger.info("Reconnecting in 5s...")
                    await asyncio.sleep(5)
            
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")
                self.stats["errors"] += 1
                if self._running:
                    self.logger.info("Reconnecting in 10s...")
                    await asyncio.sleep(10)
    
    async def _handle_message(self, message: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            event_type = data.get("event_type", "unknown")
            
            if event_type == "connected":
                self.logger.info("WebSocket ready - waiting for tweets")
            
            elif event_type == "ping":
                pass  # Ignore pings
            
            elif event_type == "tweet":
                tweets = data.get("tweets", [])
                rule_tag = data.get("rule_tag", "")
                rule_value = data.get("rule_value", "")
                
                for tweet in tweets:
                    await self._process_tweet(tweet, rule_tag, rule_value)
            
            elif event_type == "error":
                self.logger.error(f"API error: {data.get('message', 'Unknown')}")
                self.stats["errors"] += 1
        
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON: {e}")
            self.stats["errors"] += 1
        except Exception as e:
            self.logger.error(f"Message handling error: {e}")
            self.stats["errors"] += 1
    
    async def _process_tweet(self, tweet: Dict[str, Any], rule_tag: str, rule_value: str):
        """Process a single tweet and publish to Redis."""
        tweet_id = tweet.get("id", "")
        author = tweet.get("author", {})
        author_username = author.get("userName", "")
        tweet_text = tweet.get("text", "")
        tweet_url = tweet.get("url", "")
        in_reply_to = tweet.get("inReplyToId", "")
        
        # Update stats
        self.stats["tweets_received"] += 1
        self.stats["last_tweet_at"] = tweet.get("createdAt", "")
        
        # Log
        text_preview = tweet_text[:80].replace("\n", " ")
        self.logger.info(f"@{author_username}: {text_preview}...")
        
        # Create event
        event = Events.twitter_tweet(
            tweet_id=tweet_id,
            author_username=author_username,
            text=tweet_text,
            url=tweet_url,
            in_reply_to_id=in_reply_to,
            raw_tweet=tweet,
        )
        
        # Add metadata
        event["rule_tag"] = rule_tag
        event["rule_value"] = rule_value
        
        # Publish to Redis
        await self.publish(Channels.TWITTER_TWEET, event)
        
        self.stats["messages_processed"] += 1


# Entry point
if __name__ == "__main__":
    TwitterListenerWorker.main()
