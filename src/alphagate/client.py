"""
AlphaGate API client for smart followers data.

AlphaGate tracks which influential accounts ("smart money") follow a user.
This is useful for identifying high-quality accounts early.

Usage:
    client = AlphaGateClient()
    
    # Get smart followers for a user
    followers = client.get_smart_followers("jessepollak", "18876842")
    print(f"Smart followers: {len(followers)}")
    
    for f in followers:
        print(f"  @{f['username']} ({f['followers_count']})")
"""

import os
import logging
from typing import List, Dict, Any, Optional

import requests

logger = logging.getLogger(__name__)


class AlphaGateClient:
    """
    Client for AlphaGate API.
    
    AlphaGate tracks "smart followers" - influential crypto accounts
    that follow a given user. This helps identify promising accounts.
    
    Attributes:
        base_url: AlphaGate API base URL
        session_cookies: Authentication cookies (userId, extensionSessionId)
    """
    
    BASE_URL = "https://api.alphagate.io/api/v1"
    
    def __init__(
        self,
        user_id_cookie: Optional[str] = None,
        extension_session_cookie: Optional[str] = None
    ):
        """
        Initialize AlphaGate client.
        
        Args:
            user_id_cookie: The 'userId' cookie value from AlphaGate session
            extension_session_cookie: The 'extensionSessionId' cookie value
        """
        self.user_id_cookie = user_id_cookie or os.getenv("ALPHAGATE_USER_ID")
        self.extension_session_cookie = extension_session_cookie or os.getenv("ALPHAGATE_SESSION_ID")
        
        self._session = requests.Session()
        self._session.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://alphagate.io',
            'Referer': 'https://alphagate.io/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        })
        
        # Set cookies if provided
        if self.user_id_cookie:
            self._session.cookies.set('userId', self.user_id_cookie, domain='api.alphagate.io')
        if self.extension_session_cookie:
            self._session.cookies.set('extensionSessionId', self.extension_session_cookie, domain='api.alphagate.io')
    
    def get_smart_followers(
        self,
        username: str,
        twitter_id: str,
        max_pages: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get all smart followers for a user.
        
        Paginates through all results to get complete list.
        
        Args:
            username: Twitter username (without @)
            twitter_id: Twitter numeric user ID
            max_pages: Maximum pages to fetch (safety limit)
            
        Returns:
            List of smart follower dicts with:
            - username: Smart follower's username
            - followers_count: Their follower count
            - description: Their bio
            - tag: List of tags (e.g., ["Alpha Hunter"])
            - profile_image_url: Avatar URL
            - timestamp: When they followed
            - followers_when_found: Target's followers at time of follow
        """
        username = username.lstrip('@')
        all_followers = []
        page = 1
        
        while page <= max_pages:
            try:
                response = self._fetch_page(username, twitter_id, page)
                
                if not response or 'data' not in response:
                    logger.warning(f"Invalid response from AlphaGate for @{username}")
                    break
                
                data = response['data']
                followings = data.get('followings', [])
                
                # Extract parent_id (the smart follower) data
                for item in followings:
                    parent = item.get('parent_id', {})
                    if parent and parent.get('username'):
                        all_followers.append({
                            'username': parent.get('username', ''),
                            'followers_count': parent.get('followers_count', 0),
                            'description': parent.get('description', ''),
                            'tag': parent.get('tag', []),
                            'profile_image_url': parent.get('profile_image_url', ''),
                            'timestamp': item.get('timestamp'),
                            'followers_when_found': item.get('followers_when_found', 0),
                            'performance': parent.get('performance'),
                        })
                
                # Check if there are more pages
                if not data.get('hasNext', False):
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching smart followers for @{username}: {e}")
                break
        
        # Sort by followers_count descending
        all_followers.sort(key=lambda x: x.get('followers_count', 0), reverse=True)
        
        logger.debug(f"Found {len(all_followers)} smart followers for @{username}")
        return all_followers
    
    def _fetch_page(
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
            'page': page,
            'username': username,
            'twitterId': twitter_id,
            'sortBy': 'timestamp',
            'order': 1,
        }
        
        try:
            response = self._session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"AlphaGate API request failed: {e}")
            return None
    
    def get_smart_followers_count(
        self,
        username: str,
        twitter_id: str
    ) -> int:
        """
        Get just the count of smart followers.
        
        Args:
            username: Twitter username
            twitter_id: Twitter user ID
            
        Returns:
            Number of smart followers
        """
        followers = self.get_smart_followers(username, twitter_id)
        return len(followers)
    
    def format_smart_followers_list(
        self,
        username: str,
        twitter_id: str
    ) -> str:
        """
        Get formatted list of smart followers.
        
        Args:
            username: Twitter username
            twitter_id: Twitter user ID
            
        Returns:
            Formatted string with numbered list of smart followers
        """
        followers = self.get_smart_followers(username, twitter_id)
        
        if not followers:
            return "⭐️Smart Followers: 0 💎"
        
        lines = [f"⭐️Smart Followers: {len(followers)} 💎"]
        
        for i, f in enumerate(followers, 1):
            handle = f['username']
            count = f['followers_count']
            
            # Format follower count
            if count >= 1_000_000:
                count_str = f"{count/1_000_000:.1f}M"
            elif count >= 1_000:
                count_str = f"{count/1_000:.1f}K"
            else:
                count_str = str(count)
            
            url = f"https://x.com/{handle}"
            lines.append(f"   {i}. @{handle} ({count_str}) - {url}")
        
        return "\n".join(lines)


# =============================================================================
# Module-level convenience functions
# =============================================================================

_client: Optional[AlphaGateClient] = None


def _get_client() -> AlphaGateClient:
    """Get or create singleton client."""
    global _client
    if _client is None:
        _client = AlphaGateClient()
    return _client


def get_smart_followers(username: str, twitter_id: str) -> List[Dict[str, Any]]:
    """
    Get smart followers for a user.
    
    Args:
        username: Twitter username
        twitter_id: Twitter user ID
        
    Returns:
        List of smart follower dicts
    """
    return _get_client().get_smart_followers(username, twitter_id)


def get_smart_followers_count(username: str, twitter_id: str) -> int:
    """
    Get count of smart followers.
    
    Args:
        username: Twitter username
        twitter_id: Twitter user ID
        
    Returns:
        Number of smart followers
    """
    return _get_client().get_smart_followers_count(username, twitter_id)


def format_smart_followers(username: str, twitter_id: str) -> str:
    """
    Get formatted list of smart followers.
    
    Args:
        username: Twitter username
        twitter_id: Twitter user ID
        
    Returns:
        Formatted string
    """
    return _get_client().format_smart_followers_list(username, twitter_id)
