"""
Moltbook Pre-Clawnch Sync Worker

Monitors token launches on Moltbook /submolts/clawnch endpoint.

Responsibilities:
- Periodically fetch posts from Moltbook submolt clawnch API
- Parse !clawnch commands to extract token data
- Fetch agent profile if not in cache
- Fetch R/C from AlphaGate
- Send enriched notifications to Telegram
- Dedupe processed posts

Output Channels:
- telegram:send - Pre-Clawnch notifications
"""

import os
import re
import json
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime
import html

from workers.base import BaseWorker
from shared.pubsub import Channels
from src.async_clients.alphagate import AsyncAlphaGateClient
from src.async_clients.twitter import AsyncTwitterClient


class MoltbookSyncWorker(BaseWorker):
    """
    Monitors Pre-Clawnch token launches via Moltbook API.
    
    Features:
    - Poll Moltbook /submolts/clawnch for !clawnch posts
    - Fetch agent profile from /agents/profile if not cached
    - AlphaGate enrichment (R, C, Smart Followers)
    - Send formatted Telegram notifications
    """
    
    WORKER_NAME = "moltbook-sync"
    
    # APIs - Moltbook submolt clawnch
    SUBMOLT_API = "https://www.moltbook.com/api/v1/submolts/clawnch"
    SUBMOLT_GENERAL_API = "https://www.moltbook.com/api/v1/submolts/general"
    AGENT_PROFILE_API = "https://www.moltbook.com/api/v1/agents/profile"
    
    # Polling interval (seconds)
    POLL_INTERVAL = 5
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 10
    
    # Regex to extract !clawnch JSON data
    CLAWNCH_JSON_PATTERN = re.compile(
        r'!clawnch\s*```json\s*(\{.*?\})\s*```',
        re.DOTALL | re.IGNORECASE
    )
    
    # Regex for key:value format
    CLAWNCH_KV_PATTERN = re.compile(
        r'!clawnch\s+name:\s*(.+?)\s+symbol:\s*(\w+)\s+(?:description:\s*(.+?)\s+)?wallet:\s*(0x[a-fA-F0-9]+)',
        re.DOTALL | re.IGNORECASE
    )
    
    def __init__(self):
        super().__init__()
        
        # Telegram channel
        self.telegram_chat_preclawnch = os.getenv("TELEGRAM_CHAT_ID_PRECLAWNCH", "")
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._alphagate: Optional[AsyncAlphaGateClient] = None
        
        # Stats
        self.stats.update({
            "posts_processed": 0,
            "tokens_found": 0,
            "agent_lookups": 0,
            "alphagate_lookups": 0,
            "api_calls": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session and AlphaGate client."""
        await super().setup()
        
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
                "Accept": "application/json",
            }
        )
        
        # Initialize AlphaGate client
        self._alphagate = AsyncAlphaGateClient()
        await self._alphagate._ensure_session()
        
        self.logger.info(f"[PRECLAWNCH] Initialized")
        self.logger.info(f"[PRECLAWNCH] Telegram Channel: {self.telegram_chat_preclawnch}")
        self.logger.info(f"[PRECLAWNCH] Poll interval: {self.POLL_INTERVAL}s")
    
    async def teardown(self):
        """Close HTTP session and AlphaGate client."""
        if self._session:
            await self._session.close()
        if self._alphagate:
            await self._alphagate.close()
        await super().teardown()
    
    async def run(self):
        """Main loop - poll Moltbook submolt."""
        self.logger.info("Starting Pre-Clawnch sync loop...")
        
        while self._running:
            try:
                await self._sync_submolt()
            except Exception as e:
                self.logger.error(f"[PRECLAWNCH] Sync error: {e}")
                self.stats["errors"] += 1
            
            await asyncio.sleep(self.POLL_INTERVAL)
    
    # POSTS SYNC - DUAL ENDPOINT
    
    async def _sync_submolt(self):
        """Fetch from both /submolts/clawnch AND /posts, merge and deduplicate."""
        self.logger.debug("[PRECLAWNCH] Fetching from dual endpoints...")
        
        all_posts = {}  # Dict for deduplication by post_id
        
        # 1. Fetch from /submolts/clawnch (primary source - guaranteed !clawnch)
        try:
            self.stats["api_calls"] += 1
            async with self._session.get(
                self.SUBMOLT_API,
                params={"sort": "new"},
                headers={
                    "accept": "*/*",
                    "referer": "https://www.moltbook.com/m/clawnch",
                }
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        for post in data.get("posts", []):
                            post_id = post.get("id", "")
                            if post_id:
                                all_posts[post_id] = post
                        self.logger.info(f"[PRECLAWNCH] /clawnch: {len(data.get('posts', []))} posts")
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] submolts API error: {e}")
        
        # 2. Fetch from /submolts/general (for !clawnch posts in general)
        try:
            self.stats["api_calls"] += 1
            async with self._session.get(
                self.SUBMOLT_GENERAL_API,
                params={"sort": "new", "limit": 50},
                headers={
                    "accept": "*/*",
                    "referer": "https://www.moltbook.com/m/general",
                }
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        general_count = 0
                        for post in data.get("posts", []):
                            content = post.get("content", "") or ""
                            if "!clawnch" in content.lower():
                                post_id = post.get("id", "")
                                if post_id and post_id not in all_posts:
                                    all_posts[post_id] = post
                                    general_count += 1
                        self.logger.info(f"[PRECLAWNCH] /general: {len(data.get('posts', []))} posts ({general_count} !clawnch)")
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] general API error: {e}")
        
        # 3. Fetch from /posts (secondary source - all submolts, filter for !clawnch)
        try:
            self.stats["api_calls"] += 1
            async with self._session.get(
                "https://www.moltbook.com/api/v1/posts",
                params={"limit": 100, "sort": "new"},
                headers={
                    "accept": "*/*",
                    "referer": "https://www.moltbook.com/",
                }
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        posts_count = 0
                        for post in data.get("posts", []):
                            content = post.get("content", "") or ""
                            if "!clawnch" in content.lower():
                                post_id = post.get("id", "")
                                if post_id and post_id not in all_posts:
                                    all_posts[post_id] = post
                                    posts_count += 1
                        self.logger.info(f"[PRECLAWNCH] /posts: {len(data.get('posts', []))} posts ({posts_count} !clawnch)")
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] posts API error: {e}")
        
        if not all_posts:
            self.logger.warning("[PRECLAWNCH] No posts received from either endpoint")
            return
        
        # Convert to list and sort by created_at (newest first)
        posts = list(all_posts.values())
        
        self.logger.info(f"[PRECLAWNCH] Received {len(posts)} unique posts from Moltbook")
        
        # Log each post
        for idx, post in enumerate(posts, 1):
            post_id = post.get("id", "")[:8]
            author = post.get("author") or {}
            author_name = author.get("name", "Unknown")
            karma = author.get("karma", 0)
            self.logger.debug(f"[PRECLAWNCH] Post {idx}/{len(posts)}: {post_id}... | {author_name} (karma:{karma})")
        
        new_count = 0
        
        for post in posts:
            post_id = post.get("id")
            if not post_id:
                continue
            
            # Check if already processed (dedupe)
            if await self._is_post_processed(post_id):
                continue
            
            # Extract token data from !clawnch command
            token_data = self._extract_token_data(post)
            if not token_data:
                # Not a valid token launch post
                await self._mark_post_processed(post_id)
                continue
            
            new_count += 1
            self.stats["posts_processed"] += 1
            self.stats["tokens_found"] += 1
            
            author = post.get("author") or {}
            author_name = author.get("name", "Unknown")
            
            self.logger.info(
                f"[PRECLAWNCH] ✅ New token: {token_data.get('name')} (${token_data.get('symbol')}) "
                f"by {author_name}"
            )
            
            # Process and send notification
            await self._process_token_launch(post, token_data)
            
            # Mark as processed
            await self._mark_post_processed(post_id)
            
            # Rate limiting
            await asyncio.sleep(2)
        
        if new_count > 0:
            self.logger.info(f"[PRECLAWNCH] Processed {new_count} new token launches")
        
        self.stats["messages_processed"] += 1
    
    # TOKEN EXTRACTION
    
    def _extract_token_data(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract token data from !clawnch command in post content."""
        content = post.get("content", "")
        if not content:
            return None
        
        # Check if it's a clawnch post
        if "!clawnch" not in content.lower():
            return None
        
        # Try JSON format first (```json {...} ```)
        match = self.CLAWNCH_JSON_PATTERN.search(content)
        if match:
            try:
                json_str = match.group(1)
                data = json.loads(json_str)
                return {
                    "name": data.get("name", ""),
                    "symbol": data.get("symbol", ""),
                    "wallet": data.get("wallet", ""),
                    "description": data.get("description", ""),
                    "image": data.get("image", ""),
                    "website": data.get("website", ""),
                    "twitter": data.get("twitter", ""),
                }
            except json.JSONDecodeError:
                pass
        
        # Try key:value format
        match = self.CLAWNCH_KV_PATTERN.search(content)
        if match:
            return {
                "name": match.group(1).strip(),
                "symbol": match.group(2).strip(),
                "description": match.group(3).strip() if match.group(3) else "",
                "wallet": match.group(4).strip(),
                "image": "",
                "website": "",
                "twitter": "",
            }
        
        # Manual parsing as fallback
        token_data = {}
        
        name_match = re.search(r'name:\s*(.+?)(?=\s*symbol:|$)', content, re.IGNORECASE)
        if name_match:
            token_data["name"] = name_match.group(1).strip()
        
        symbol_match = re.search(r'symbol:\s*(\w+)', content, re.IGNORECASE)
        if symbol_match:
            token_data["symbol"] = symbol_match.group(1).strip()
        
        wallet_match = re.search(r'wallet:\s*(0x[a-fA-F0-9]+)', content, re.IGNORECASE)
        if wallet_match:
            token_data["wallet"] = wallet_match.group(1).strip()
        
        desc_match = re.search(r'description:\s*(.+?)(?=\s*wallet:|image:|website:|twitter:|$)', content, re.DOTALL | re.IGNORECASE)
        token_data["description"] = desc_match.group(1).strip() if desc_match else ""
        
        img_match = re.search(r'image:\s*([^\n]+)', content, re.IGNORECASE)
        token_data["image"] = img_match.group(1).strip() if img_match else ""
        
        if all(k in token_data and token_data[k] for k in ["name", "symbol", "wallet"]):
            return token_data
        
        self.logger.warning(f"[PRECLAWNCH] Missing required fields: {token_data}")
        return None
    
    # AGENT DATA
    
    async def _get_agent_profile(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch agent profile from cache or Moltbook API.
        
        Returns agent data with x_handle for AlphaGate lookup.
        """
        if not agent_name:
            return None
        
        agent_name_lower = agent_name.lower()
        
        # Check cache first (molt_agent from molt_sync worker)
        try:
            cached = await self.storage.get_molt_agent(agent_name_lower)
            if cached:
                self.logger.debug(f"[PRECLAWNCH] Agent CACHE HIT: {agent_name}")
                return cached
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] Cache lookup error: {e}")
        
        # Fetch from Moltbook API
        try:
            self.stats["agent_lookups"] += 1
            
            async with self._session.get(
                self.AGENT_PROFILE_API,
                params={"name": agent_name},
                headers={
                    "accept": "*/*",
                    "referer": f"https://www.moltbook.com/u/{agent_name}",
                }
            ) as resp:
                if resp.status != 200:
                    self.logger.debug(f"[PRECLAWNCH] Agent API returned {resp.status} for {agent_name}")
                    return None
                
                data = await resp.json()
                
                if not data.get("success"):
                    return None
                
                agent = data.get("agent", {})
                owner = agent.get("owner", {})
                
                # Extract data
                x_handle = owner.get("x_handle", "")
                x_follower_count = owner.get("x_follower_count", 0)
                x_verified = owner.get("x_verified", False)  # Owner Twitter verified status
                karma = agent.get("karma", 0)
                description = agent.get("description", "")
                follower_count = agent.get("follower_count", 0)
                
                self.logger.info(
                    f"[PRECLAWNCH] Agent API {agent_name}: x_handle=@{x_handle}, "
                    f"karma={karma}, followers={follower_count}, x_verified={x_verified}"
                )
                
                # Save to cache
                try:
                    await self.storage.save_molt_agent(
                        x_handle=x_handle.lower().lstrip("@") if x_handle else "",
                        agent_name=agent_name,
                        is_verified=x_verified,
                        karma=karma,
                        description=description,
                        agent_id=agent.get("id", ""),
                        followers=x_follower_count,
                    )
                except Exception as e:
                    self.logger.debug(f"[PRECLAWNCH] Cache save error: {e}")
                
                return {
                    "x_handle": x_handle.lower().lstrip("@") if x_handle else "",
                    "x_follower_count": x_follower_count,
                    "x_verified": x_verified,
                    "karma": karma,
                    "description": description,
                    "follower_count": follower_count,
                }
                
        except Exception as e:
            self.logger.error(f"[PRECLAWNCH] Agent API error {agent_name}: {e}")
            return None
    
    async def _get_alphagate_data(self, x_handle: str) -> Dict[str, Any]:
        """Get AlphaGate data (R, C) for a Twitter handle."""
        if not x_handle:
            return {"prev_usernames_count": 0, "contracts_count": 0, "smart_followers": 0}
        
        x_handle = x_handle.lower().lstrip("@")
        
        # Try cache first
        try:
            cached = await self.storage.get_alphagate_data(x_handle)
            if cached:
                self.logger.debug(f"[PRECLAWNCH] AlphaGate CACHE HIT @{x_handle}")
                return cached
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] AlphaGate cache error: {e}")
        
        # Fetch from API
        try:
            self.stats["alphagate_lookups"] += 1
            result = await self._alphagate.get_child_data(x_handle)
            if result:
                r_count = result.get("prev_usernames_count", 0)
                c_count = result.get("contracts_count", 0)
                sf_count = result.get("smart_followers", 0)
                self.logger.info(f"[PRECLAWNCH] AlphaGate API @{x_handle}: R={r_count} C={c_count} SF={sf_count}")
                
                # Cache for future use
                try:
                    await self.storage.save_alphagate_data(x_handle, r_count, c_count, sf_count)
                except:
                    pass
                
                return {"prev_usernames_count": r_count, "contracts_count": c_count, "smart_followers": sf_count}
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] AlphaGate API error @{x_handle}: {e}")
        
        return {"prev_usernames_count": 0, "contracts_count": 0, "smart_followers": 0}
    
    async def _get_twitter_data(self, x_handle: str) -> Dict[str, Any]:
        """Get Twitter profile data (bio, followers, created_at, verified, affiliate_label)."""
        if not x_handle:
            return {}
        
        x_handle = x_handle.lower().lstrip("@")
        
        # CHECK CACHE FIRST (24h TTL) - GLOBAL CACHE SHARED WITH ALL WORKERS
        try:
            cached = await self.storage.get_twitter_user(x_handle)
            if cached:
                self.logger.debug(f"[PRECLAWNCH] Twitter CACHE HIT @{x_handle}")
                return {
                    "followers_count": cached.get("followers", 0),
                    "description": cached.get("bio", ""),
                    "created_at": cached.get("created_at", ""),
                    "is_blue_verified": cached.get("is_blue_verified", False),
                    "name": cached.get("name", ""),
                    "affiliate_label": cached.get("affiliate_label", ""),
                }
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] Twitter cache error: {e}")
        
        # CALL API ONLY IF NOT IN CACHE
        try:
            async with AsyncTwitterClient() as client:
                # Get user info (followers, bio, verified)
                user_info = await client.get_user_info(x_handle)
                
                # Get user about (affiliate label - company badge)
                user_about = await client.get_user_about(x_handle)
                affiliate_label = None
                if user_about:
                    affiliate_label = user_about.get("affiliate_label")
                
                if user_info:
                    self.logger.info(f"[PRECLAWNCH] Twitter API @{x_handle}: followers={user_info.get('followers', 0)}, verified={user_info.get('isBlueVerified', False)}, affiliate={affiliate_label}")
                    return {
                        "followers_count": user_info.get("followers", 0),
                        "description": user_info.get("description", ""),
                        "created_at": user_info.get("createdAt", ""),
                        "is_blue_verified": user_info.get("isBlueVerified", False),
                        "name": user_info.get("name", ""),
                        "affiliate_label": affiliate_label,  # Company badge (X, xAI, APPLE, etc)
                    }
        except Exception as e:
            self.logger.debug(f"[PRECLAWNCH] Twitter API error @{x_handle}: {e}")
        
        return {}

    # NOTIFICATION
    
    async def _process_token_launch(self, post: Dict[str, Any], token: Dict[str, Any]):
        """Process token launch and send notification."""
        post_id = post.get("id", "")
        author = post.get("author", {})
        author_name = author.get("name", "")
        
        try:
            # Fetch agent profile (cache or API)
            agent = await self._get_agent_profile(author_name)
            
            # Get AlphaGate data if we have x_handle
            alphagate_data = {"prev_usernames_count": 0, "contracts_count": 0, "smart_followers": 0}
            twitter_data = {}
            if agent and agent.get("x_handle"):
                alphagate_data = await self._get_alphagate_data(agent["x_handle"])
                twitter_data = await self._get_twitter_data(agent["x_handle"])
            
            # *** SAVE PRE-CLAWNCH TOKEN FOR LINKING ***
            # This saves the post_id which matches on-chain context_parsed.messageId
            token_wallet = token.get("wallet", "")
            x_handle = agent.get("x_handle", "") if agent else ""
            
            await self.storage.save_preclawnch_token(
                post_id=post_id,  # This is the messageId used on-chain
                platform="moltbook",
                token_name=token.get("name", ""),
                token_symbol=token.get("symbol", ""),
                token_wallet=token_wallet,
                agent_name=author_name,
                x_handle=x_handle,
            )
            
            # Get author karma from post for trigger check
            author_karma = author.get("karma", 0) if author else 0
            
            # Check MoltBook triggers and create auto-buys
            await self._check_moltbook_triggers(
                post_id=post_id,
                token_symbol=token.get("symbol", ""),
                x_handle=x_handle,
                followers=twitter_data.get("followers_count", 0) if twitter_data else 0,
                smart_followers=alphagate_data.get("smart_followers", 0),
                karma=author_karma,
                is_verified=twitter_data.get("is_blue_verified", False) if twitter_data else False,
            )
            
            # Save twitter_data to cache for DEPLOYED CLAWNCH
            if twitter_data and x_handle:
                try:
                    await self.storage.save_twitter_user(
                        username=x_handle,
                        user_id="",
                        name=twitter_data.get("name", ""),
                        followers=twitter_data.get("followers_count", 0),
                        following=0,
                        is_blue_verified=twitter_data.get("is_blue_verified", False),
                        verified_type="",
                        created_at=twitter_data.get("created_at", ""),
                        bio=twitter_data.get("description", ""),
                        affiliate_label=twitter_data.get("affiliate_label", ""),
                    )
                except Exception as e:
                    self.logger.debug(f"[PRECLAWNCH] Twitter cache save error: {e}")
            
            # Format message
            message = self._format_message(post, token, agent, alphagate_data, twitter_data)
            
            # Create inline buttons
            buttons = self._create_inline_buttons(post, agent, token)
            
            # Send to Telegram
            await self._send_telegram_notification(message, buttons)
            
            # Publish structured data for tracker worker
            await self.publish(Channels.PRECLAWNCH_MOLTBOOK, {
                "post_id": post_id,
                "agent_name": author_name,
                "x_handle": x_handle,
                "token_symbol": token.get("symbol", ""),
                "token_name": token.get("name", ""),
                "message": message,
                "buttons": buttons,
            })
            
            self.logger.info(f"[PRECLAWNCH] Sent Telegram notification for {token.get('symbol')} (post_id={post_id[:16]}...)")
        
        except Exception as e:
            self.logger.error(f"[PRECLAWNCH] Error processing post {post_id}: {e}", exc_info=True)
    
    def _format_message(
        self,
        post: Dict[str, Any],
        token: Dict[str, Any],
        agent: Optional[Dict[str, Any]],
        alphagate_data: Dict[str, Any],
        twitter_data: Dict[str, Any] = None
    ) -> str:
        """Format Pre-Clawnch message for Telegram (HTML)."""
        twitter_data = twitter_data or {}
        
        def esc(text):
            return html.escape(str(text)) if text else ""
        
        def fmt_date(date_str):
            if not date_str:
                return ""
            try:
                # Twitter format: "Tue Mar 15 00:00:00 +0000 2022" or ISO
                if "T" in str(date_str):
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
                return dt.strftime("%d %b %Y")
            except:
                try:
                    dt = datetime.fromisoformat(date_str.replace("+00:00", "").replace("Z", ""))
                    return dt.strftime("%d %b %Y")
                except:
                    return date_str[:10] if len(date_str) > 10 else date_str
        
        def fmt_number(n):
            try:
                n = int(n)
                if n >= 1_000_000:
                    return f"{n/1_000_000:.1f}M"
                elif n >= 1_000:
                    return f"{n/1_000:.1f}K"
                return str(n)
            except:
                return str(n)
        
        lines = []
        
        # Get data
        author = post.get("author") or {}
        author_name = author.get("name", "Unknown")
        author_karma = author.get("karma", 0)
        author_followers = author.get("follower_count", 0)
        
        token_name = token.get('name', 'Unknown')
        token_symbol = token.get('symbol', '???')
        token_wallet = token.get('wallet', '')
        token_desc = token.get('description', '')
        token_website = token.get('website', '')
        token_image = token.get('image', '')
        
        x_handle = agent.get("x_handle", "") if agent else token.get("twitter", "")
        # Clean x_handle (remove URL prefix if present)
        if x_handle and ("x.com/" in x_handle or "twitter.com/" in x_handle):
            x_handle = x_handle.split("/")[-1].lstrip("@")
        
        # AlphaGate data
        r_count = alphagate_data.get("prev_usernames_count", 0)
        c_count = alphagate_data.get("contracts_count", 0)
        sf_count = alphagate_data.get("smart_followers", 0)
        
        # Twitter API data (bio, followers, created_at, verified, affiliate)
        twitter_followers = twitter_data.get("followers_count", 0)
        twitter_bio = twitter_data.get("description", "")
        twitter_created = twitter_data.get("created_at", "")
        is_verified = twitter_data.get("is_blue_verified", False)
        affiliate_label = twitter_data.get("affiliate_label", "")  # Company badge (X, xAI, etc)
        
        # Header with monospace token name
        lines.append("🟢 <b>NEW PRE CLAWNCH</b>")
        lines.append("")
        lines.append(f"<code>{esc(token_name)} (${esc(token_symbol)})</code> by 📖 <b>MOLTBOOK</b>")
        lines.append("")
        
        # ==================== AGENT SECTION ====================
        lines.append("━━━━━━━🤖 <b>AGENT</b>━━━━━━━")
        lines.append("")
        
        # Agent name with profile link
        moltbook_url = f"https://www.moltbook.com/u/{author_name}"
        lines.append(f'🤖 <b>Agent:</b> <a href="{moltbook_url}">{esc(author_name)}</a>')
        
        # Karma
        lines.append(f"⭐️ <b>Karma:</b> {author_karma}")
        
        # Moltbook Followers with alert (HIGH = positive ⛔️⚠️)
        followers_alert = " ⛔️⚠️⛔️⚠️⛔️⚠️" if author_followers >= 5 else ""
        lines.append(f"👥 <b>Moltbook Followers:</b> {fmt_number(author_followers)}{followers_alert}")
        lines.append("")
        
        # ==================== OWNER SECTION ====================
        if x_handle:
            lines.append("━━━━━━━👨🏻 <b>OWNER</b>━━━━━━━")
            lines.append("")
            
            # Twitter handle with R/C and verification + company badge
            verified_icon = ""
            if is_verified:
                verified_icon = " ✅"
            if affiliate_label:
                verified_icon += f" 🏢{affiliate_label}"
            
            lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{x_handle}">@{esc(x_handle)}</a> (R:{r_count} | C:{c_count}){verified_icon}')
            
            # Twitter followers
            if twitter_followers:
                lines.append(f"👤 <b>Followers:</b> {fmt_number(twitter_followers)}")
            
            # Smart followers with alert (HIGH = positive ⛔️⚠️)
            sf_alert = " ⛔️⚠️⛔️⚠️⛔️⚠️" if sf_count >= 15 else ""
            lines.append(f"⭐️ <b>Smart Followers:</b> {sf_count}{sf_alert}")
            
            # Created at
            if twitter_created:
                lines.append(f"📅 <b>Criado em:</b> {fmt_date(twitter_created)}")
            
            # Bio
            if twitter_bio:
                bio_display = twitter_bio[:100] + "..." if len(twitter_bio) > 100 else twitter_bio
                lines.append(f"📕 <b>Bio:</b> {esc(bio_display)}")
            
            lines.append("")
        
        # ==================== TOKEN SECTION ====================
        lines.append("━━━━━━━🪙 <b>TOKEN</b>━━━━━━━")
        lines.append("")
        
        lines.append(f"📛 <b>Name:</b> {esc(token_name)} (${esc(token_symbol)})")
        
        if token_wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{token_wallet}</code>")
        
        if token_website:
            lines.append(f'🌐 <b>Web:</b> <a href="{token_website}">{esc(token_website[:40])}...</a>' if len(token_website) > 40 else f'🌐 <b>Web:</b> <a href="{token_website}">{esc(token_website)}</a>')
        
        if token_image:
            lines.append(f'🖼 <b>Image:</b> <a href="{token_image}">Link</a>')
        
        if token_desc:
            desc_display = token_desc[:200] + "..." if len(token_desc) > 200 else token_desc
            lines.append(f"📕 <b>Description:</b> {esc(desc_display)}")
        
        # Footer with monospace token name
        lines.append("")
        lines.append(f"<code>{esc(token_name)} (${esc(token_symbol)})</code> by 📖 <b>MOLTBOOK</b>")
        
        return "\n".join(lines)
    
    def _create_inline_buttons(
        self,
        post: Dict[str, Any],
        agent: Optional[Dict[str, Any]],
        token: Dict[str, Any]
    ) -> List[List[Dict[str, str]]]:
        """Create inline keyboard buttons.
        
        Layout:
        Row 1: [Agent Profile] [Post] [Owner] (external links)
        Row 2: [Like] [Buy] (actions with post_id callbacks)
        """
        keyboard = []
        
        author = post.get("author") or {}
        author_name = author.get("name", "")
        post_id = post.get("id", "")
        x_handle = agent.get("x_handle", "") if agent else token.get("twitter", "")
        
        # Row 1: External links [Agent Profile] [Post] [Owner]
        row1 = []
        
        # Agent Profile (Moltbook)
        if author_name:
            row1.append({
                "text": "👤 Agent",
                "url": f"https://www.moltbook.com/u/{author_name}"
            })
        
        # Moltbook Post
        if post_id:
            row1.append({
                "text": "📄 Post",
                "url": f"https://www.moltbook.com/m/clawnch/post/{post_id}"
            })
        
        # Owner Twitter
        if x_handle:
            row1.append({
                "text": "🐦 Owner",
                "url": f"https://x.com/{x_handle}"
            })
        
        if row1:
            keyboard.append(row1)
        
        # Row 2: Actions [Like] [Buy] with post_id
        if post_id:
            keyboard.append([
                {
                    "text": "❤️ Like",
                    "callback_data": f"like:{post_id}"
                },
                {
                    "text": "💰 Buy",
                    "callback_data": f"buy:{post_id}"
                }
            ])
        
        return keyboard
    
    async def _send_telegram_notification(
        self,
        message: str,
        buttons: List[List[Dict[str, str]]]
    ):
        """Send notification to Telegram via pub/sub."""
        try:
            chat_id = self.telegram_chat_preclawnch
            
            if not chat_id:
                self.logger.warning("[PRECLAWNCH] No Telegram chat ID configured (TELEGRAM_CHAT_ID_PRECLAWNCH)!")
                return
            
            event = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "reply_markup": {
                    "inline_keyboard": buttons
                },
                "disable_web_page_preview": True,
                "channel": "preclawnch",
            }
            
            await self.publish(Channels.TELEGRAM_SEND, event)
        except Exception as e:
            self.logger.error(f"[PRECLAWNCH] Telegram send error: {e}")
    
    # TRIGGER AUTO-BUY
    
    async def _check_moltbook_triggers(
        self,
        post_id: str,
        token_symbol: str,
        x_handle: str,
        followers: int,
        smart_followers: int,
        karma: int,
        is_verified: bool,
    ):
        """
        Check MoltBook triggers for all users and create auto-buys.
        
        For each user with MoltBook trigger enabled:
        1. Check if their conditions are met
        2. If yes, create PendingBuy with status APPROVED
        3. Notify user via Telegram
        """
        try:
            # Get all users with MoltBook trigger enabled
            user_ids = await self.storage.get_users_with_moltbook_trigger()
            
            if not user_ids:
                self.logger.debug(f"[MOLTBOOK TRIGGER] No users with MoltBook trigger enabled")
                return
            
            self.logger.info(f"[MOLTBOOK TRIGGER] Checking {len(user_ids)} users for ${token_symbol}")
            
            triggered_count = 0
            
            for user_id in user_ids:
                try:
                    # Check if trigger conditions are met
                    if await self.storage.check_moltbook_trigger(
                        user_id=user_id,
                        x_handle=x_handle,
                        followers=followers,
                        smart_followers=smart_followers,
                        karma=karma,
                        is_verified=is_verified,
                    ):
                        # Try to acquire lock to prevent duplicates
                        if not await self.storage.try_acquire_buy_creation_lock(user_id, post_id):
                            self.logger.debug(f"[MOLTBOOK TRIGGER] Lock not acquired for user {user_id} post {post_id[:16]}...")
                            continue
                        
                        # Check if user already has a pending buy for this post
                        existing = await self.storage.get_pending_buy(user_id, post_id)
                        if existing:
                            self.logger.debug(f"[MOLTBOOK TRIGGER] User {user_id} already has buy for {post_id[:16]}...")
                            continue
                        
                        # Get user config for trading params
                        config = await self.storage.get_user_config(user_id)
                        
                        # Check if user is properly configured
                        if not config.is_configured():
                            self.logger.warning(f"[MOLTBOOK TRIGGER] User {user_id} not configured, skipping")
                            continue
                        
                        # Create pending buy with status APPROVED
                        buy = await self.storage.approve_buy(
                            user_id=user_id,
                            tweet_id=post_id,  # Using post_id as identifier
                            username=config.username,
                            token_symbol=token_symbol,
                        )
                        
                        triggered_count += 1
                        
                        self.logger.info(
                            f"[MOLTBOOK TRIGGER] ✅ Auto-buy created for user {user_id}: "
                            f"${token_symbol} ETH={config.eth_amount}"
                        )
                        
                        # Notify user via Telegram
                        await self._notify_trigger_buy(
                            user_id=user_id,
                            token_symbol=token_symbol,
                            x_handle=x_handle,
                            eth_amount=config.eth_amount,
                            post_id=post_id,
                        )
                        
                except Exception as e:
                    self.logger.error(f"[MOLTBOOK TRIGGER] Error processing user {user_id}: {e}")
                    continue
            
            if triggered_count > 0:
                self.logger.info(f"[MOLTBOOK TRIGGER] Created {triggered_count} auto-buys for ${token_symbol}")
                
        except Exception as e:
            self.logger.error(f"[MOLTBOOK TRIGGER] Error: {e}", exc_info=True)
    
    async def _notify_trigger_buy(
        self,
        user_id: int,
        token_symbol: str,
        x_handle: str,
        eth_amount: float,
        post_id: str,
    ):
        """Send notification to user about trigger auto-buy."""
        try:
            message = (
                f"🎯 <b>MOLTBOOK TRIGGER ATIVADO!</b>\n\n"
                f"Token: <code>${token_symbol}</code>\n"
                f"Creator: @{x_handle}\n"
                f"ETH: {eth_amount}\n\n"
                f"✅ Compra APROVADA automaticamente!\n"
                f"Aguardando token ir on-chain...\n\n"
                f"<i>Post ID: {post_id[:16]}...</i>"
            )
            
            # Send via pubsub to telegram sender (private message)
            await self.publish(Channels.TELEGRAM_SEND, {
                "chat_id": user_id,  # Send to user's private chat
                "text": message,
                "parse_mode": "HTML",
                "channel": "private",
            })
            
        except Exception as e:
            self.logger.error(f"[MOLTBOOK TRIGGER] Notification error for user {user_id}: {e}")
    
    # DEDUPE
    
    async def _is_post_processed(self, post_id: str) -> bool:
        """Check if post was already processed."""
        return await self.redis.sismember("preclawnch_posts_processed", post_id)
    
    async def _mark_post_processed(self, post_id: str):
        """Mark post as processed."""
        await self.redis.sadd("preclawnch_posts_processed", post_id)
        # Keep for 30 days
        await self.redis.expire("preclawnch_posts_processed", 86400 * 30)


# Entry point
if __name__ == "__main__":
    MoltbookSyncWorker.main()
