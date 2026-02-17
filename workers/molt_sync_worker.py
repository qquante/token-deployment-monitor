"""
MOLT Agent Sync Worker

Synchronizes MOLT agents from Moltbook API to Redis cache.

Responsibilities:
- Periodically fetch agents from Moltbook API
- Fetch verification tweets from Twitter
- Fetch Twitter user info (followers)
- Fetch smart followers from AlphaGate
- Filter VIP smart followers
- Send notifications to Telegram (main + KOL channels)
- Cache agent data in Redis for fast lookup

Output Channels:
- molt:agent_new - New agent registered
- molt:agent_verified - Agent completed verification
- telegram:send - Telegram notifications
"""

import os
import asyncio
import aiohttp
import urllib.parse
import html
from typing import Dict, Any, Optional, List, Set
from datetime import datetime

from workers.base import BaseWorker
from shared.pubsub import Channels


# =============================================================================
# VIP ACCOUNTS - Smart followers that qualify for KOL filter
# =============================================================================

VIP_ACCOUNTS = {
    "elonmusk", "base", "antoniogm", "baseapp", "dimabuterin",
    "VitalikButerin", "benparr", "brian_armstrong", "MattPRD",
    "coinbase", "OctaneAI", "Aoife_Teague", "austingriffith",
    "0xDeployer", "bankrbot", "clanker_world", "jessepollak",
    "sama", "openclaw"
}
VIP_ACCOUNTS_LOWER = {v.lower() for v in VIP_ACCOUNTS}

# KOL Filter thresholds
MIN_FOLLOWERS_KOL = 500
MIN_SMART_FOLLOWERS_KOL = 2


class MoltSyncWorker(BaseWorker):
    """
    Synchronizes MOLT agents from Moltbook to Redis.
    
    Features:
    - Periodic sync with Moltbook API
    - Twitter search for verification tweets
    - Twitter user info (followers count)
    - AlphaGate smart followers
    - VIP filtering and KOL channel
    - Redis caching for instant lookups
    """
    
    WORKER_NAME = "molt-sync"
    
    # APIs
    MOLTBOOK_API = "https://www.moltbook.com/api/v1/agents/recent"
    TWITTER_SEARCH_API = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    TWITTER_USER_INFO_API = "https://api.twitterapi.io/twitter/user/info"
    ALPHAGATE_API = "https://api.alphagate.io/api/v1/child/parents"
    
    # Intervals (seconds)
    MOLTBOOK_INTERVAL = 300  # 5 minutes
    TWITTER_INTERVAL = 120   # 2 minutes (verification tweets)
    
    def __init__(self):
        super().__init__()
        
        self.twitter_api_key = os.getenv("TWITTER_API_KEY", "")
        self.alphagate_user_id = os.getenv("ALPHAGATE_USER_ID", "")
        self.alphagate_session_id = os.getenv("ALPHAGATE_SESSION_ID", "")
        
        # Telegram channels for MOLT notifications
        self.telegram_chat_molt = os.getenv("TELEGRAM_CHAT_ID_MOLT", "")
        self.telegram_chat_molt_kol = os.getenv("TELEGRAM_CHAT_ID_MOLT_KOL", "")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Track verified handles (to avoid re-checking)
        self._verified_handles: Set[str] = set()
        
        # Stats
        self.stats.update({
            "agents_synced": 0,
            "agents_verified": 0,
            "agents_kol": 0,
            "api_calls": 0,
            "telegram_sent": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )
        
        # Load existing verified handles from Redis
        existing_agents = await self.storage.get_all_molt_agents()
        for handle in existing_agents:
            agent = await self.storage.get_molt_agent(handle)
            if agent and agent.get("is_verified"):
                self._verified_handles.add(handle.lower())
        
        # Log configuration
        self.logger.info(f"[MOLT] Initialized with {len(self._verified_handles)} verified agents")
        self.logger.info(f"[MOLT] Twitter API: {'✅' if self.twitter_api_key else '❌'}")
        self.logger.info(f"[MOLT] AlphaGate: {'✅' if self.alphagate_user_id else '❌'}")
        self.logger.info(f"[MOLT] Telegram MOLT: {'✅' if self.telegram_chat_molt else '❌'}")
        self.logger.info(f"[MOLT] Telegram KOL: {'✅' if self.telegram_chat_molt_kol else '❌'}")
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Run sync loops."""
        self.logger.info("Starting MOLT sync loops...")
        
        # Run both sync loops in parallel
        await asyncio.gather(
            self._moltbook_loop(),
            self._verification_loop(),
            return_exceptions=True,
        )
    
    # =========================================================================
    # MOLTBOOK SYNC
    # =========================================================================
    
    async def _moltbook_loop(self):
        """Periodically sync agents from Moltbook."""
        while self._running:
            try:
                await self._sync_moltbook()
            except Exception as e:
                self.logger.error(f"[MOLT] Sync error: {e}")
                self.stats["errors"] += 1
            
            await asyncio.sleep(self.MOLTBOOK_INTERVAL)
    
    async def _sync_moltbook(self):
        """Fetch and sync agents from Moltbook API."""
        self.logger.info("[MOLT] Syncing from Moltbook...")
        
        self.stats["api_calls"] += 1
        
        try:
            async with self._session.get(
                self.MOLTBOOK_API,
                headers={
                    "accept": "application/json",
                    "referer": "https://www.moltbook.com/",
                }
            ) as resp:
                if resp.status != 200:
                    self.logger.warning(f"[MOLT] Moltbook API returned {resp.status}")
                    return
                
                data = await resp.json()
        except Exception as e:
            self.logger.error(f"[MOLT] Moltbook API error: {e}")
            return
        
        agents = data.get("agents", [])
        if not agents:
            self.logger.debug("[MOLT] No agents returned")
            return
        
        self.logger.info(f"[MOLT] Received {len(agents)} agents from Moltbook")
        
        # VERBOSE: Log each agent received
        for idx, agent in enumerate(agents, 1):
            owner = agent.get("owner", {})
            agent_name = agent.get("name", "N/A")
            x_handle = owner.get("x_handle", "N/A")
            follower_count = owner.get("x_follower_count", 0)
            self.logger.info(f"[MOLT] Agent {idx}/{len(agents)}: {agent_name} | @{x_handle} | {follower_count:,} followers")
        
        new_count = 0
        kol_count = 0
        
        for agent in agents:
            owner = agent.get("owner", {})
            x_handle = owner.get("x_handle", "")
            
            if not x_handle:
                continue
            
            x_handle_clean = x_handle.lower().lstrip("@")
            
            # Check if already exists
            existing = await self.storage.get_molt_agent(x_handle_clean)
            is_new = existing is None
            
            if is_new:
                new_count += 1
                self.stats["agents_synced"] += 1
                self.logger.info(f"[MOLT] New agent: @{x_handle_clean} ({agent.get('name')})")
                
                # Fetch Twitter user info
                twitter_data = await self._fetch_twitter_user(x_handle_clean) or {}
                twitter_id = twitter_data.get("id", "")
                followers = twitter_data.get("followers", owner.get("x_follower_count", 0))
                
                await asyncio.sleep(0.3)  # Rate limiting
                
                # Fetch smart followers from AlphaGate
                smart_count, all_sf, vip_followers = await self._fetch_smart_followers(
                    x_handle_clean, twitter_id
                )
                
                self.logger.info(
                    f"[MOLT] @{x_handle_clean}: followers={followers}, "
                    f"smart={smart_count}, vips={len(vip_followers)}"
                )
                
                # Check if passes KOL filter
                passes_kol = followers >= MIN_FOLLOWERS_KOL or len(vip_followers) >= MIN_SMART_FOLLOWERS_KOL
                
                if passes_kol:
                    kol_count += 1
                    self.stats["agents_kol"] += 1
                
                # Save agent with extra data
                await self.storage.save_molt_agent(
                    x_handle=x_handle_clean,
                    agent_name=agent.get("name", "Unknown"),
                    is_verified=self._is_handle_verified(x_handle_clean),
                    karma=agent.get("karma", 0),
                    description=agent.get("description", ""),
                    agent_id=agent.get("id", ""),
                    followers=followers,
                    smart_followers_count=smart_count,
                    vip_followers=vip_followers,
                    passes_kol_filter=passes_kol,
                )
                
                await asyncio.sleep(1)  # Rate limiting between agents
            else:
                # Just update basic info for existing agents
                await self.storage.save_molt_agent(
                    x_handle=x_handle_clean,
                    agent_name=agent.get("name", "Unknown"),
                    is_verified=self._is_handle_verified(x_handle_clean),
                    karma=agent.get("karma", 0),
                    description=agent.get("description", ""),
                    agent_id=agent.get("id", ""),
                )
        
        total = await self.storage.get_molt_agents_count()
        self.logger.info(f"[MOLT] Sync complete: {new_count} new ({kol_count} KOL), {total} total cached")
        
        self.stats["messages_processed"] += 1
    
    def _is_handle_verified(self, x_handle: str) -> bool:
        """Check if handle is in verified set."""
        return x_handle.lower() in self._verified_handles
    
    # =========================================================================
    # VERIFICATION SYNC
    # =========================================================================
    
    async def _verification_loop(self):
        """Periodically check for verification tweets."""
        # Wait a bit before starting
        await asyncio.sleep(30)
        
        while self._running:
            try:
                await self._sync_verifications()
            except Exception as e:
                self.logger.error(f"[MOLT] Verification sync error: {e}")
                self.stats["errors"] += 1
            
            await asyncio.sleep(self.TWITTER_INTERVAL)
    
    async def _sync_verifications(self):
        """Search for verification tweets and update agents."""
        if not self.twitter_api_key:
            return
        
        self.logger.debug("[MOLT] Checking verification tweets...")
        
        self.stats["api_calls"] += 1
        
        # Search for verification tweets
        query = '"on @moltbook" "Verification"'
        encoded = urllib.parse.quote(query)
        url = f"{self.TWITTER_SEARCH_API}?query={encoded}"
        
        try:
            async with self._session.get(
                url,
                headers={"X-API-Key": self.twitter_api_key}
            ) as resp:
                if resp.status != 200:
                    return
                
                data = await resp.json()
        except Exception as e:
            self.logger.debug(f"[MOLT] Twitter search error: {e}")
            return
        
        tweets = data.get("tweets", [])
        if not tweets:
            return
        
        new_verified = 0
        for tweet in tweets:
            author = tweet.get("author", {})
            username = author.get("userName", "").lower()
            
            if not username:
                continue
            
            # Skip if already verified
            if username in self._verified_handles:
                continue
            
            # Check if this handle is a MOLT agent
            if await self.storage.is_molt_agent(username):
                # Mark as verified
                await self.storage.set_molt_agent_verified(username, True)
                self._verified_handles.add(username)
                
                new_verified += 1
                self.stats["agents_verified"] += 1
                self.logger.info(f"[MOLT] Agent verified: @{username}")
        
        if new_verified > 0:
            self.logger.info(f"[MOLT] {new_verified} new verifications")
    
    # =========================================================================
    # TWITTER API
    # =========================================================================
    
    async def _fetch_twitter_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch Twitter user info (followers, etc.)."""
        if not self.twitter_api_key:
            return None
        
        try:
            self.stats["api_calls"] += 1
            async with self._session.get(
                f"{self.TWITTER_USER_INFO_API}?userName={username}",
                headers={"X-API-Key": self.twitter_api_key}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
        except Exception as e:
            self.logger.debug(f"[MOLT] Twitter user info error @{username}: {e}")
        
        return None
    
    # =========================================================================
    # ALPHAGATE API
    # =========================================================================
    
    async def _fetch_smart_followers(
        self,
        username: str,
        twitter_id: str,
        max_pages: int = 20
    ) -> tuple[int, List[str], List[str]]:
        """
        Fetch smart followers from AlphaGate.
        
        Returns: (count, all_usernames, vip_usernames)
        """
        if not self.alphagate_user_id or not self.alphagate_session_id:
            return 0, [], []
        
        if not twitter_id:
            return 0, [], []
        
        all_usernames = []
        page = 1
        
        try:
            while page <= max_pages:
                self.stats["api_calls"] += 1
                
                params = {
                    'page': page,
                    'username': username.lstrip('@'),
                    'twitterId': str(twitter_id),
                    'sortBy': 'timestamp',
                    'order': 1,
                }
                
                async with self._session.get(
                    self.ALPHAGATE_API,
                    params=params,
                    headers={
                        "Cookie": f"userId={self.alphagate_user_id}; extensionSessionId={self.alphagate_session_id}",
                        "Origin": "https://alphagate.io",
                        "Referer": "https://alphagate.io/",
                    }
                ) as resp:
                    if resp.status != 200:
                        break
                    
                    data = await resp.json()
                    followings = data.get('data', {}).get('followings', [])
                    has_next = data.get('data', {}).get('hasNext', False)
                    
                    for item in followings:
                        parent = item.get('parent_id', {})
                        uname = parent.get('username')
                        if uname:
                            all_usernames.append(uname)
                    
                    if not has_next:
                        break
                    page += 1
                    await asyncio.sleep(0.2)
                    
        except Exception as e:
            self.logger.debug(f"[MOLT] AlphaGate error @{username}: {e}")
        
        # Filter VIPs
        vips_found = [u for u in all_usernames if u.lower() in VIP_ACCOUNTS_LOWER]
        
        return len(all_usernames), all_usernames, vips_found
    
    # =========================================================================
    # TELEGRAM NOTIFICATIONS
    # =========================================================================
    
    def _format_agent_message(
        self,
        agent: Dict[str, Any],
        twitter_data: Dict[str, Any],
        smart_followers_count: int,
        vip_followers: List[str],
        passes_kol_filter: bool,
    ) -> str:
        """Format MOLT agent message for Telegram."""
        
        def esc(text):
            return html.escape(str(text)) if text else ""
        
        def format_number(n):
            try:
                return f"{int(n):,}".replace(",", ".")
            except:
                return str(n)
        
        lines = []
        
        # KOL Filter banner (if passes)
        if passes_kol_filter:
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("   <b>MOLT AGENT FILTER</b> ✅")
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("")
        
        # Agent info
        name = esc(agent.get("name", "???"))
        description = esc(agent.get("description", "Sem descrição"))
        karma = agent.get("karma", 0)
        
        owner = agent.get("owner", {})
        x_handle = owner.get("x_handle", "???")
        x_name = esc(owner.get("x_name", x_handle))
        
        # Twitter data
        followers = twitter_data.get("followers", owner.get("x_follower_count", 0))
        following = twitter_data.get("following", "?")
        is_verified = twitter_data.get("isBlueVerified", False)
        
        # Created date
        created_at = agent.get("created_at", "")
        try:
            agent_dt = datetime.fromisoformat(created_at.replace("+00:00", "").replace("Z", ""))
            agent_date = agent_dt.strftime("%d/%m/%Y às %H:%M")
        except:
            agent_date = "N/A"
        
        # Verification badge
        badge = " ✅" if is_verified else ""
        
        # MOLT Agent banner
        is_molt_verified = self._is_handle_verified(x_handle.lower().lstrip("@"))
        lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
        if is_molt_verified:
            lines.append("   <b>MOLT AGENT VERIFIED</b> ✅")
        else:
            lines.append("   <b>MOLT AGENT</b>")
        lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
        lines.append("")
        
        # Main info
        lines.append(f"🤖 <b>Nome:</b> <code>{name}</code>")
        lines.append(f"📝 <i>{description[:150]}{'...' if len(description) > 150 else ''}</i>")
        lines.append(f"⭐ <b>Karma:</b> <code>{karma}</code>")
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        
        # Owner info
        handle_link = f'<a href="https://twitter.com/{x_handle}">@{esc(x_handle)}</a>'
        lines.append(f"👤 <b>Owner:</b> <b>{x_name}</b>{badge} ({handle_link})")
        lines.append("")
        lines.append(f"┌ 👥 <b>Seguidores:</b> <code>{format_number(followers)}</code>")
        lines.append(f"├ 👁️ <b>Seguindo:</b> <code>{format_number(following)}</code>")
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if smart_followers_count >= 5 else ""
        lines.append(f"└ 💎 <b>Smart Followers:</b> <code>{smart_followers_count}</code>{sf_emoji}")
        
        # VIP followers
        if vip_followers:
            for vip in vip_followers[:5]:  # Limit to 5
                lines.append(f"   └ @{esc(vip)} 🔑")
        
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("")
        lines.append(f"⏰ <b>Registrado em:</b> <code>{agent_date}</code>")
        lines.append(f"🌐 <a href=\"https://www.moltbook.com/\">Ver no Moltbook</a>")
        lines.append("")
        lines.append(f"👤 https://twitter.com/{x_handle}")
        
        return "\n".join(lines)
    
    async def _send_telegram_notification(
        self,
        message: str,
        chat_id: str,
    ) -> bool:
        """Send message to Telegram via pub/sub."""
        if not chat_id:
            return False
        
        try:
            event = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "channel": "molt",
            }
            await self.publish(Channels.TELEGRAM_SEND, event)
            self.stats["telegram_sent"] += 1
            return True
        except Exception as e:
            self.logger.error(f"[MOLT] Telegram send error: {e}")
            return False
    
    # =========================================================================
    # UTILITIES
    # =========================================================================
    
    async def check_molt_agent(self, x_handle: str) -> Optional[Dict[str, Any]]:
        """
        Check if a Twitter handle is a MOLT agent.
        
        Returns agent info or None.
        """
        return await self.storage.get_molt_agent(x_handle)


# Entry point
if __name__ == "__main__":
    MoltSyncWorker.main()
