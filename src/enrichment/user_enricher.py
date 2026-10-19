"""
User data enricher.

Enriches user data by combining:
1. Basic info from tweet mentions (username, user_id, name)
2. Full profile from Twitter API (followers, bio, verified, createdAt, etc.)
3. ENS/Basename from wallet address (if available)

Usage:
    enricher = UserEnricher(twitter_client)
    
    # Enrich a single user
    enriched = enricher.enrich_user({
        "username": "jessepollak",
        "user_id": "18876842",
        "wallet": "0x..."
    })
    
    # Enrich multiple users
    enriched_users = enricher.enrich_users([user1, user2])
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class UserEnricher:
    """
    Enriches user data from multiple sources.
    
    Attributes:
        twitter_client: TwitterAPIClient instance for user lookups
        ens_resolver: ENSResolver instance for name resolution
    """
    
    def __init__(self, twitter_client=None, ens_resolver=None):
        """
        Initialize enricher.
        
        Args:
            twitter_client: Optional TwitterAPIClient (will be lazy-loaded if needed)
            ens_resolver: Optional ENSResolver (will be lazy-loaded if needed)
        """
        self._twitter_client = twitter_client
        self._ens_resolver = ens_resolver
    
    @property
    def twitter_client(self):
        """Get or create Twitter client."""
        if self._twitter_client is None:
            from ..twitter.client import TwitterAPIClient
            self._twitter_client = TwitterAPIClient()
        return self._twitter_client
    
    @property
    def ens_resolver(self):
        """Get or create ENS resolver."""
        if self._ens_resolver is None:
            from ..ens.resolver import ENSResolver
            self._ens_resolver = ENSResolver()
        return self._ens_resolver
    
    def enrich_user(
        self,
        user: Dict[str, Any],
        fetch_twitter: bool = True,
        resolve_ens: bool = True
    ) -> Dict[str, Any]:
        """
        Enrich a user dict with additional data.
        
        Args:
            user: Base user dict (must have 'username')
            fetch_twitter: Whether to fetch Twitter profile data
            resolve_ens: Whether to resolve ENS/Basename
            
        Returns:
            Enriched user dict with all available data
        """
        if not user:
            return user
        
        username = user.get("username", "")
        if not username:
            return user
        
        enriched = dict(user)  # Copy original
        
        # Fetch Twitter profile data
        if fetch_twitter:
            try:
                profile = self.twitter_client.get_user_info(username)
                if profile:
                    enriched = self._merge_twitter_profile(enriched, profile)
                    logger.debug(f"Enriched @{username} with Twitter data")
            except Exception as e:
                logger.warning(f"Failed to fetch Twitter data for @{username}: {e}")
        
        # Resolve ENS/Basename if wallet is available
        if resolve_ens and enriched.get("wallet"):
            try:
                ens_name = self.ens_resolver.reverse_lookup(enriched["wallet"])
                if ens_name:
                    enriched["ens"] = ens_name
                    logger.debug(f"Resolved ENS for @{username}: {ens_name}")
            except Exception as e:
                logger.warning(f"Failed to resolve ENS for @{username}: {e}")
        
        return enriched
    
    def _merge_twitter_profile(
        self,
        base: Dict[str, Any],
        profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge Twitter profile data into base user dict.
        
        Maps TwitterAPI.io response fields to our format.
        """
        # Map API response fields
        field_mapping = {
            # API field -> our field
            "id": "user_id",
            "name": "name",
            "userName": "username",
            "description": "bio",
            "followers": "followers",
            "following": "following",
            "favouritesCount": "likes_count",
            "statusesCount": "tweets_count",
            "mediaCount": "media_count",
            "createdAt": "created_at",
            "profilePicture": "profile_image_url",
            "coverPicture": "cover_image_url",
            "isVerified": "is_verified",
            "isBlueVerified": "is_blue_verified",
            "verifiedType": "verified_type",
            "isAutomated": "is_automated",
            "automatedBy": "automated_by",
            "location": "location",
            "url": "website",
            "protected": "is_protected",
            "canDm": "can_dm",
        }
        
        result = dict(base)
        
        for api_field, our_field in field_mapping.items():
            if api_field in profile and profile[api_field] is not None:
                result[our_field] = profile[api_field]
        
        # Also keep original fields from API for compatibility
        result["_raw_profile"] = profile
        
        return result
    
    def enrich_users(
        self,
        users: List[Dict[str, Any]],
        fetch_twitter: bool = True,
        resolve_ens: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Enrich multiple users.
        
        Args:
            users: List of user dicts
            fetch_twitter: Whether to fetch Twitter data
            resolve_ens: Whether to resolve ENS
            
        Returns:
            List of enriched user dicts
        """
        return [
            self.enrich_user(user, fetch_twitter, resolve_ens)
            for user in users
        ]
    
    def enrich_from_to(
        self,
        from_user: Dict[str, Any],
        to_users: List[Dict[str, Any]],
        fetch_twitter: bool = True,
        resolve_ens: bool = True
    ) -> tuple:
        """
        Enrich FROM and TO users together.
        
        Convenience method for enriching deployment participants.
        
        Args:
            from_user: The requester
            to_users: List of recipients
            fetch_twitter: Whether to fetch Twitter data
            resolve_ens: Whether to resolve ENS
            
        Returns:
            Tuple of (enriched_from, enriched_to_list)
        """
        enriched_from = self.enrich_user(from_user, fetch_twitter, resolve_ens)
        enriched_to = self.enrich_users(to_users, fetch_twitter, resolve_ens)
        
        return enriched_from, enriched_to


# Module-level convenience functions
_enricher: Optional[UserEnricher] = None


def _get_enricher() -> UserEnricher:
    """Get or create singleton enricher."""
    global _enricher
    if _enricher is None:
        _enricher = UserEnricher()
    return _enricher


def enrich_user(
    user: Dict[str, Any],
    fetch_twitter: bool = True,
    resolve_ens: bool = True
) -> Dict[str, Any]:
    """
    Enrich a single user.
    
    Args:
        user: User dict with at least 'username'
        fetch_twitter: Whether to fetch Twitter profile
        resolve_ens: Whether to resolve ENS name
        
    Returns:
        Enriched user dict
    """
    return _get_enricher().enrich_user(user, fetch_twitter, resolve_ens)


def enrich_users(
    users: List[Dict[str, Any]],
    fetch_twitter: bool = True,
    resolve_ens: bool = True
) -> List[Dict[str, Any]]:
    """
    Enrich multiple users.
    
    Args:
        users: List of user dicts
        fetch_twitter: Whether to fetch Twitter profile
        resolve_ens: Whether to resolve ENS name
        
    Returns:
        List of enriched user dicts
    """
    return _get_enricher().enrich_users(users, fetch_twitter, resolve_ens)
