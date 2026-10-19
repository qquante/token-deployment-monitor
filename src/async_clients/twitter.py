"""
Async Twitter API client using aiohttp.

High-performance async version of TwitterAPIClient.
"""

import os
import logging
import asyncio
from typing import Optional, Dict, Any, List

import aiohttp

logger = logging.getLogger(__name__)


class AsyncTwitterClient:
    """
    Async client for TwitterAPI.io.
    
    Uses aiohttp for non-blocking HTTP requests.
    Supports concurrent requests for multiple users.
    
    Example:
        async with AsyncTwitterClient() as client:
            # Single user
            user = await client.get_user_info("vitalik")
            
            # Multiple users in parallel
            users = await client.get_users_info(["vitalik", "jessepollak"])
    """
    
    BASE_URL = "https://api.twitterapi.io"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize async client.
        
        Args:
            api_key: TwitterAPI.io API key (or set TWITTER_API_KEY env var)
        """
        self.api_key = api_key or os.getenv("TWITTER_API_KEY", "")
        self._session: Optional[aiohttp.ClientSession] = None
        self._headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    async def __aenter__(self):
        """Create session on context enter."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session on context exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(
                headers=self._headers,
                timeout=timeout
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed user profile information.
        
        Args:
            username: Twitter username (without @)
            
        Returns:
            User profile dict or None if not found
        """
        await self._ensure_session()
        username = username.lstrip("@")
        
        url = f"{self.BASE_URL}/twitter/user/info"
        
        try:
            async with self._session.get(url, params={"userName": username}) as response:
                if response.status != 200:
                    logger.warning(f"Twitter API error for @{username}: {response.status}")
                    return None
                
                result = await response.json()
                
                if result.get("status") == "success" and result.get("data"):
                    user_data = result["data"]
                    logger.debug(f"Got user info for @{username}: {user_data.get('followers', 0)} followers")
                    return user_data
                else:
                    logger.warning(f"No user data for @{username}: {result.get('msg', 'Unknown')}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching user info for @{username}")
            return None
        except Exception as e:
            logger.error(f"Failed to get user info for @{username}: {e}")
            return None
    
    async def get_users_info(self, usernames: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get profile info for multiple users IN PARALLEL.
        
        Args:
            usernames: List of usernames to look up
            
        Returns:
            Dict mapping username -> user profile data
        """
        if not usernames:
            return {}
        
        # Fetch all users in parallel
        tasks = [self.get_user_info(username) for username in usernames]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for username, result in zip(usernames, results):
            username = username.lstrip("@").lower()
            if isinstance(result, dict):
                output[username] = result
            elif isinstance(result, Exception):
                logger.error(f"Error fetching @{username}: {result}")
        
        return output
    
    async def get_user_about(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get user about data including affiliate label (Xai, APPLE, X, etc.).
        
        Args:
            username: Twitter username (without @)
            
        Returns:
            Dict with affiliate_label or None
        """
        await self._ensure_session()
        username = username.lstrip("@")
        
        url = f"{self.BASE_URL}/twitter/user_about"
        
        try:
            async with self._session.get(url, params={"userName": username}) as response:
                if response.status != 200:
                    return None
                
                result = await response.json()
                
                if result.get("status") == "success" and result.get("data"):
                    user_data = result["data"]
                    
                    # Extract affiliate label
                    affiliate_label = None
                    affiliates = user_data.get("affiliates_highlighted_label", {})
                    if affiliates:
                        label = affiliates.get("label", {})
                        affiliate_label = label.get("description")
                    
                    # Also check identity_profile_labels
                    if not affiliate_label:
                        identity = user_data.get("identity_profile_labels_highlighted_label", {})
                        if identity:
                            label = identity.get("label", {})
                            affiliate_label = label.get("description")
                    
                    return {
                        "affiliate_label": affiliate_label,
                        "is_blue_verified": user_data.get("isBlueVerified", False),
                    }
                return None
                    
        except Exception as e:
            logger.debug(f"Failed to get user_about for @{username}: {e}")
            return None


# =============================================================================
# Module-level convenience
# =============================================================================

_client: Optional[AsyncTwitterClient] = None


async def get_client() -> AsyncTwitterClient:
    """Get or create singleton async client."""
    global _client
    if _client is None:
        _client = AsyncTwitterClient()
    await _client._ensure_session()
    return _client


async def get_user_info(username: str) -> Optional[Dict[str, Any]]:
    """Convenience function to get user info."""
    client = await get_client()
    return await client.get_user_info(username)
