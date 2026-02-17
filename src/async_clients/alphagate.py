"""
Async AlphaGate API client using aiohttp.

Fetches "smart followers" data with parallel pagination.
"""

import os
import logging
import asyncio
from typing import Optional, Dict, Any, List

import aiohttp

logger = logging.getLogger(__name__)


class AsyncAlphaGateClient:
    """
    Async client for AlphaGate API.
    
    Fetches smart followers with PARALLEL pagination -
    all pages are fetched simultaneously for maximum speed.
    
    Example:
        async with AsyncAlphaGateClient() as client:
            followers = await client.get_smart_followers("jessepollak", "18876842")
            print(f"Found {len(followers)} smart followers")
    """
    
    BASE_URL = "https://api.alphagate.io/api/v1"
    
    def __init__(
        self,
        user_id_cookie: Optional[str] = None,
        extension_session_cookie: Optional[str] = None
    ):
        """
        Initialize async AlphaGate client.
        
        Args:
            user_id_cookie: The 'userId' cookie value
            extension_session_cookie: The 'extensionSessionId' cookie value
        """
        self.user_id_cookie = user_id_cookie or os.getenv("ALPHAGATE_USER_ID", "")
        self.extension_session_cookie = extension_session_cookie or os.getenv("ALPHAGATE_SESSION_ID", "")
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Create session on context enter."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session on context exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists with cookies."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            
            # Build cookies
            cookies = {}
            if self.user_id_cookie:
                cookies["userId"] = self.user_id_cookie
            if self.extension_session_cookie:
                cookies["extensionSessionId"] = self.extension_session_cookie
            
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                cookies=cookies,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Origin": "https://alphagate.io",
                    "Referer": "https://alphagate.io/",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                }
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_smart_followers(
        self,
        username: str,
        twitter_id: str,
        max_pages: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get all smart followers for a user with PARALLEL pagination.
        
        Strategy:
        1. Fetch page 1 to get total count
        2. Calculate total pages needed
        3. Fetch all remaining pages IN PARALLEL
        
        Args:
            username: Twitter username (without @)
            twitter_id: Twitter numeric user ID
            max_pages: Maximum pages to fetch (safety limit)
            
        Returns:
            List of smart follower dicts sorted by followers_count
        """
        username = username.lstrip("@")
        await self._ensure_session()
        
        # First, fetch page 1 to understand the data
        first_page = await self._fetch_page(username, twitter_id, page=1)
        
        if not first_page or "data" not in first_page:
            return []
        
        data = first_page["data"]
        followings = data.get("followings", [])
        all_followers = self._extract_followers(followings)
        
        # Check if there are more pages
        has_next = data.get("hasNext", False)
        
        if has_next and max_pages > 1:
            # Fetch remaining pages in PARALLEL
            # We'll speculatively fetch up to max_pages
            page_tasks = [
                self._fetch_page(username, twitter_id, page=p)
                for p in range(2, max_pages + 1)
            ]
            
            page_results = await asyncio.gather(*page_tasks, return_exceptions=True)
            
            for result in page_results:
                if isinstance(result, dict) and "data" in result:
                    page_data = result["data"]
                    page_followings = page_data.get("followings", [])
                    all_followers.extend(self._extract_followers(page_followings))
                    
                    # If this page has no next, stop processing
                    if not page_data.get("hasNext", False):
                        break
        
        # Sort by followers_count descending and deduplicate
        seen = set()
        unique_followers = []
        for f in all_followers:
            key = f.get("username", "").lower()
            if key and key not in seen:
                seen.add(key)
                unique_followers.append(f)
        
        unique_followers.sort(key=lambda x: x.get("followers_count", 0), reverse=True)
        
        logger.debug(f"Found {len(unique_followers)} smart followers for @{username}")
        return unique_followers
    
    def _extract_followers(self, followings: List[Dict]) -> List[Dict[str, Any]]:
        """Extract follower data from API response."""
        followers = []
        for item in followings:
            parent = item.get("parent_id", {})
            if parent and parent.get("username"):
                followers.append({
                    "username": parent.get("username", ""),
                    "followers_count": parent.get("followers_count", 0),
                    "description": parent.get("description", ""),
                    "tag": parent.get("tag", []),
                    "profile_image_url": parent.get("profile_image_url", ""),
                    "timestamp": item.get("timestamp"),
                    "followers_when_found": item.get("followers_when_found", 0),
                })
        return followers
    
    async def _fetch_page(
        self,
        username: str,
        twitter_id: str,
        page: int = 1
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single page of smart followers.
        
        Args:
            username: Twitter username
            twitter_id: Twitter user ID
            page: Page number
            
        Returns:
            API response dict or None on error
        """
        url = f"{self.BASE_URL}/child/parents"
        params = {
            "page": page,
            "username": username,
            "twitterId": twitter_id,
            "sortBy": "timestamp",
            "order": 1,
        }
        
        try:
            async with self._session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"AlphaGate API returned {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.error(f"AlphaGate timeout for @{username} page {page}")
            return None
        except Exception as e:
            logger.error(f"AlphaGate error: {e}")
            return None
    
    async def get_child_data(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get child data for a user (R: prev_usernames, C: contracts).
        
        Args:
            username: Twitter username
            
        Returns:
            Dict with prev_usernames_count (R) and contracts_count (C)
        """
        await self._ensure_session()
        
        url = f"{self.BASE_URL}/child"
        params = {"username": username}
        
        try:
            async with self._session.get(url, params=params) as response:
                data = await response.json()
                logger.info(f"AlphaGate /child @{username}: status={response.status} msg={data.get('message', 'N/A')[:50]}")
                
                if response.status == 200:
                    if data.get("message") == "Fetched data successfully":
                        child = data.get("data", {}).get("child", {})
                        if child:
                            prev_usernames = child.get("prev_usernames", []) or []
                            contracts = child.get("contracts", []) or []
                            smart_followers = child.get("key_followers_count", 0)
                            return {
                                "prev_usernames_count": len(prev_usernames),
                                "contracts_count": len(contracts),
                                "smart_followers": smart_followers,
                                "prev_usernames": prev_usernames,
                            }
                return None
        except Exception as e:
            logger.info(f"AlphaGate child error for @{username}: {e}")
            return None


# =============================================================================
# Module-level convenience
# =============================================================================

_client: Optional[AsyncAlphaGateClient] = None


async def get_client() -> AsyncAlphaGateClient:
    """Get or create singleton async client."""
    global _client
    if _client is None:
        _client = AsyncAlphaGateClient()
    await _client._ensure_session()
    return _client


async def get_smart_followers(username: str, twitter_id: str) -> List[Dict[str, Any]]:
    """Convenience function to get smart followers."""
    client = await get_client()
    return await client.get_smart_followers(username, twitter_id)
