"""
User Enricher Worker

Enriches user data from multiple APIs.

Responsibilities:
- Subscribe to enrich:request channel
- Fetch user data from Twitter API
- Fetch R/C counts from AlphaGate
- Fetch smart followers from AlphaGate
- Resolve ENS names
- Cache results in Redis
- Publish results

Input Channels:
- enrich:request - User enrichment requests

Output Channels:
- enrich:result - Enrichment results
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from datetime import datetime

from workers.base import BaseWorker
from shared.pubsub import Channels, Events
from shared.models import EnrichedUser


class UserEnricherWorker(BaseWorker):
    """
    Enriches user data from multiple APIs in parallel.
    """
    
    WORKER_NAME = "user-enricher"
    
    def __init__(self):
        super().__init__()
        
        # API configs
        self.twitter_api_key = os.getenv("TWITTER_API_KEY", "")
        self.alphagate_user_id = os.getenv("ALPHAGATE_USER_ID", "")
        self.alphagate_session_id = os.getenv("ALPHAGATE_SESSION_ID", "")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Stats
        self.stats.update({
            "users_enriched": 0,
            "cache_hits": 0,
            "api_calls": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Subscribe to enrichment requests."""
        self.logger.info("Subscribing to enrich:request...")
        
        async for channel, message in self.redis.subscribe(Channels.ENRICH_REQUEST):
            if not self._running:
                break
            
            try:
                await self._handle_request(message)
            except Exception as e:
                self.logger.error(f"Enrich error: {e}")
                self.stats["errors"] += 1
    
    async def _handle_request(self, event: Dict[str, Any]):
        """Handle enrichment request."""
        username = event.get("username", "")
        wallet = event.get("wallet", "")
        fetch_smart_followers = event.get("fetch_smart_followers", True)
        correlation_id = event.get("correlation_id", "")
        priority = event.get("priority", 0)
        
        if not username:
            return
        
        username = username.lstrip("@").lower()
        
        # Check cache first
        cached = await self.storage.get_enriched_user(username)
        if cached:
            self.stats["cache_hits"] += 1
            self.logger.debug(f"Cache hit: @{username}")
            
            # Publish result
            await self._publish_result(cached, correlation_id, from_cache=True)
            self.stats["messages_processed"] += 1
            return
        
        self.logger.info(f"Enriching @{username} (priority={priority})")
        
        # Enrich from APIs in parallel
        user = await self._enrich_user(username, wallet, fetch_smart_followers)
        
        if user:
            # Cache result
            await self.storage.save_enriched_user(user)
            self.stats["users_enriched"] += 1
            
            # Publish result
            await self._publish_result(user, correlation_id)
        else:
            # Publish error
            error_event = Events.enrich_result(
                username=username,
                success=False,
                error="Failed to enrich user",
                correlation_id=correlation_id,
            )
            await self.publish(Channels.ENRICH_RESULT, error_event)
        
        self.stats["messages_processed"] += 1
    
    async def _enrich_user(
        self,
        username: str,
        wallet: str = "",
        fetch_smart_followers: bool = True,
    ) -> Optional[EnrichedUser]:
        """Enrich user from multiple APIs (with 24h cache)."""
        user = EnrichedUser(username=username, wallet=wallet)
        
        # CHECK CACHES FIRST (24h TTL each)
        twitter_cached = False
        alphagate_cached = False
        
        # Check Twitter cache
        try:
            twitter_cache = await self.storage.get_twitter_user(username)
            if twitter_cache:
                self.logger.debug(f"[CACHE] Twitter HIT @{username}")
                user.user_id = twitter_cache.get("user_id", "")
                user.name = twitter_cache.get("name", username)
                user.followers = twitter_cache.get("followers", 0)
                user.following = twitter_cache.get("following", 0)
                user.is_blue_verified = twitter_cache.get("is_blue_verified", False)
                user.verified_type = twitter_cache.get("verified_type", "")
                user.created_at = twitter_cache.get("created_at", "")
                user.bio = twitter_cache.get("bio", "")
                user.affiliate_label = twitter_cache.get("affiliate_label", "")
                twitter_cached = True
        except Exception as e:
            self.logger.debug(f"[CACHE] Twitter check error: {e}")
        
        # Check AlphaGate cache
        try:
            alphagate_cache = await self.storage.get_alphagate_data(username)
            if alphagate_cache:
                self.logger.debug(f"[CACHE] AlphaGate HIT @{username}")
                user.prev_usernames_count = alphagate_cache.get("prev_usernames_count", 0)
                user.contracts_count = alphagate_cache.get("contracts_count", 0)
                user.smart_followers = alphagate_cache.get("smart_followers", 0)
                alphagate_cached = True
        except Exception as e:
            self.logger.debug(f"[CACHE] AlphaGate check error: {e}")
        
        # CALL APIs ONLY FOR MISSING DATA
        tasks = []
        
        if not twitter_cached:
            tasks.append(("twitter", self._fetch_twitter_user(username)))
            tasks.append(("twitter_about", self._fetch_twitter_about(username)))
        
        if not alphagate_cached:
            tasks.append(("alphagate_child", self._fetch_alphagate_child(username)))
        
        if wallet:
            tasks.append(("ens", self._resolve_ens(wallet)))
        
        # Execute in parallel
        if tasks:
            results = await asyncio.gather(
                *[t[1] for t in tasks],
                return_exceptions=True,
            )
            
            # Process results
            for i, (name, _) in enumerate(tasks):
                result = results[i]
                
                if isinstance(result, Exception):
                    self.logger.debug(f"API error ({name}): {result}")
                    continue
                
                if not result:
                    continue
                
                if name == "twitter":
                    user.user_id = str(result.get("id", ""))
                    user.name = result.get("name", username)
                    user.followers = result.get("followers", 0)
                    user.following = result.get("following", 0)
                    user.is_blue_verified = result.get("isBlueVerified", False)
                    user.verified_type = result.get("verifiedType", "")
                    user.created_at = result.get("createdAt", "")
                    user.bio = result.get("description", "")
                    user.profile_image_url = result.get("profilePicture", "")
                
                elif name == "twitter_about":
                    user.affiliate_label = result.get("affiliate_label", "")
                
                elif name == "alphagate_child":
                    user.prev_usernames_count = result.get("prev_usernames_count", 0)
                    user.contracts_count = result.get("contracts_count", 0)
                    user.smart_followers = result.get("smart_followers", 0)
                
                elif name == "ens":
                    user.ens = result
            
            # Save Twitter data to cache (if we called the API)
            if not twitter_cached and user.user_id:
                try:
                    await self.storage.save_twitter_user(
                        username=username,
                        user_id=user.user_id,
                        name=user.name,
                        followers=user.followers,
                        following=user.following,
                        is_blue_verified=user.is_blue_verified,
                        verified_type=user.verified_type or "",
                        created_at=user.created_at,
                        bio=user.bio,
                        affiliate_label=user.affiliate_label,
                    )
                    self.logger.debug(f"[CACHE] Twitter SAVED @{username}")
                except Exception as e:
                    self.logger.debug(f"[CACHE] Twitter save error: {e}")
        
        user.cached_at = datetime.now().isoformat()
        return user
    
    async def _fetch_twitter_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch user info from Twitter API."""
        if not self.twitter_api_key:
            return None
        
        self.stats["api_calls"] += 1
        
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/info?userName={username}",
                headers={"X-API-Key": self.twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
        except:
            pass
        
        return None
    
    async def _fetch_twitter_about(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch user about (affiliate label) from Twitter API."""
        if not self.twitter_api_key:
            return None
        
        self.stats["api_calls"] += 1
        
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/about?userName={username}",
                headers={"X-API-Key": self.twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {}
                    
                    # Extract affiliate label
                    highlights = data.get("data", {}).get("highlightedLabel", {})
                    if highlights.get("label"):
                        result["affiliate_label"] = highlights["label"]
                    
                    return result
        except:
            pass
        
        return None
    
    async def _fetch_alphagate_child(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch R and C counts from AlphaGate (with 24h cache)."""
        # Check Redis cache first (24 hours TTL)
        try:
            cached = await self.storage.get_alphagate_data(username)
            if cached:
                self.logger.debug(f"[ALPHAGATE] CACHE HIT @{username}: R={cached['prev_usernames_count']} C={cached['contracts_count']} SF={cached['smart_followers']}")
                return cached
        except Exception as e:
            self.logger.debug(f"[ALPHAGATE] Cache check error: {e}")
        
        if not self.alphagate_user_id or not self.alphagate_session_id:
            self.logger.warning(f"[ALPHAGATE] Skipping child @{username} - no credentials configured")
            return None
        
        self.stats["api_calls"] += 1
        
        try:
            url = f"https://api.alphagate.io/api/v1/child?username={username}"
            async with self._session.get(
                url,
                headers={
                    "Cookie": f"userId={self.alphagate_user_id}; extensionSessionId={self.alphagate_session_id}",
                    "Origin": "https://alphagate.io",
                    "Referer": "https://alphagate.io/",
                },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Data is nested: data.data.child
                    child = data.get("data", {}).get("child", {}) or {}
                    
                    # Field is prev_usernames (with underscore), not prevUsernames
                    prev_usernames = child.get("prev_usernames", []) or []
                    contracts = child.get("contracts", []) or []
                    key_followers = child.get("key_followers_count", 0) or 0
                    
                    result = {
                        "prev_usernames_count": len(prev_usernames),
                        "contracts_count": len(contracts),
                        "smart_followers": key_followers,  # From child response
                    }
                    # Log the actual values for debugging
                    self.logger.info(f"[ALPHAGATE] API @{username}: R={result['prev_usernames_count']} C={result['contracts_count']} SF={result['smart_followers']}")
                    
                    # Save to Redis cache (24 hours)
                    try:
                        await self.storage.save_alphagate_data(
                            username,
                            result["prev_usernames_count"],
                            result["contracts_count"],
                            result["smart_followers"],
                        )
                    except Exception as e:
                        self.logger.debug(f"[ALPHAGATE] Cache save error: {e}")
                    
                    return result
                elif resp.status == 401 or resp.status == 403:
                    body = await resp.text()
                    self.logger.error(f"[ALPHAGATE] AUTH ERROR @{username}: status={resp.status} body={body[:200]}")
                else:
                    body = await resp.text()
                    self.logger.warning(f"[ALPHAGATE] child @{username} status={resp.status} body={body[:100]}")
        except Exception as e:
            self.logger.error(f"[ALPHAGATE] child @{username} exception: {e}")
        
        return None
    
    async def _fetch_smart_followers(
        self,
        username: str,
        user_id: str,
    ) -> Optional[list]:
        """Fetch smart followers from AlphaGate."""
        if not self.alphagate_user_id or not self.alphagate_session_id:
            self.logger.debug(f"[ALPHAGATE] Skipping smartfollowers - no credentials")
            return None
        
        self.stats["api_calls"] += 1
        
        try:
            async with self._session.get(
                f"https://api.alphagate.io/api/v1/user/smartfollowers?username={username}&userId={user_id}",
                headers={
                    "Cookie": f"userId={self.alphagate_user_id}; extensionSessionId={self.alphagate_session_id}",
                    "Origin": "https://alphagate.io",
                    "Referer": "https://alphagate.io/",
                },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    sf = data.get("smartFollowers", [])
                    self.logger.debug(f"[ALPHAGATE] smartfollowers @{username}: {len(sf)}")
                    return sf
                else:
                    self.logger.warning(f"[ALPHAGATE] smartfollowers @{username} status={resp.status}")
        except Exception as e:
            self.logger.warning(f"[ALPHAGATE] smartfollowers @{username} error: {e}")
        
        return None
    
    async def _resolve_ens(self, wallet: str) -> Optional[str]:
        """Resolve ENS name for wallet address."""
        self.stats["api_calls"] += 1
        
        try:
            # Use public ENS resolver
            async with self._session.get(
                f"https://api.ensideas.com/ens/resolve/{wallet}",
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("name", "")
        except:
            pass
        
        return None
    
    async def _publish_result(
        self,
        user: EnrichedUser,
        correlation_id: str,
        from_cache: bool = False,
    ):
        """Publish enrichment result."""
        event = Events.enrich_result(
            username=user.username,
            success=True,
            data=user.to_redis(),
            correlation_id=correlation_id,
        )
        event["from_cache"] = from_cache
        
        await self.publish(Channels.ENRICH_RESULT, event)


# Entry point
if __name__ == "__main__":
    UserEnricherWorker.main()
