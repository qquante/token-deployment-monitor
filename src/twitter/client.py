"""
TwitterAPI.io client for managing filter rules.

This module provides a client to interact with TwitterAPI.io's
filter rule management endpoints. It handles authentication,
rule CRUD operations, and webhook configuration.
"""

import os
import logging
from typing import List, Optional, Dict, Any

import requests

from .models import FilterRule

logger = logging.getLogger(__name__)


class TwitterAPIError(Exception):
    """Exception raised for TwitterAPI.io errors."""
    
    def __init__(self, message: str, status_code: int = None, response: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class TwitterAPIClient:
    """
    Client for TwitterAPI.io filter rule management.
    
    Provides methods to create, read, update, and delete
    tweet filter rules that power the webhook system.
    
    Example:
        client = TwitterAPIClient(api_key="your_key")
        
        # Create a rule
        rule = client.add_rule(
            tag="bankr_mentions",
            value="@bankrbot",
            interval_seconds=10
        )
        
        # Activate it
        client.activate_rule(rule.rule_id)
        
        # List all rules
        rules = client.get_rules()
    """
    
    BASE_URL = "https://api.twitterapi.io"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize client.
        
        Args:
            api_key: TwitterAPI.io API key (or set TWITTER_API_KEY env var)
        """
        self.api_key = api_key or os.getenv("TWITTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key required. Pass api_key or set TWITTER_API_KEY env var"
            )
        
        self._session = requests.Session()
        self._session.headers.update({
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
        })
    
    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make authenticated API request."""
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            if method == "GET":
                response = self._session.get(url)
            elif method == "POST":
                response = self._session.post(url, json=data)
            elif method == "DELETE":
                response = self._session.delete(url, json=data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            result = response.json()
            
            if result.get('status') == 'error':
                raise TwitterAPIError(
                    result.get('msg', 'Unknown error'),
                    response.status_code,
                    result
                )
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise TwitterAPIError(str(e))
    
    def get_rules(self) -> List[FilterRule]:
        """
        Get all filter rules.
        
        Returns:
            List of FilterRule objects
        """
        result = self._request("GET", "/oapi/tweet_filter/get_rules")
        rules_data = result.get('rules', [])
        return [FilterRule.from_dict(r) for r in rules_data]
    
    def add_rule(
        self,
        tag: str,
        value: str,
        interval_seconds: int = 10
    ) -> FilterRule:
        """
        Add a new filter rule.
        
        Note: New rules are created as inactive. Use activate_rule()
        to enable them after setting up your webhook.
        
        Args:
            tag: Label to identify the rule
            value: Filter expression (e.g., "@bankrbot" or "from:user")
            interval_seconds: Check interval (min 100ms, max 86400s)
            
        Returns:
            Created FilterRule with rule_id
        """
        rule = FilterRule(
            tag=tag,
            value=value,
            interval_seconds=interval_seconds,
        )
        
        result = self._request(
            "POST",
            "/oapi/tweet_filter/add_rule",
            rule.to_create_payload()
        )
        
        rule.rule_id = result.get('rule_id')
        logger.info(f"Created rule '{tag}' with ID: {rule.rule_id}")
        
        return rule
    
    def update_rule(
        self,
        rule_id: str,
        tag: str,
        value: str,
        interval_seconds: float = 10.0,
        is_effect: int = 0
    ) -> bool:
        """
        Update an existing rule.
        
        Args:
            rule_id: ID of rule to update
            tag: New tag label
            value: New filter expression
            interval_seconds: New check interval
            is_effect: 1 to activate, 0 to deactivate
            
        Returns:
            True if successful
        """
        rule = FilterRule(
            rule_id=rule_id,
            tag=tag,
            value=value,
            interval_seconds=interval_seconds,
            is_effect=is_effect,
        )
        
        self._request(
            "POST",
            "/oapi/tweet_filter/update_rule",
            rule.to_update_payload()
        )
        
        logger.info(f"Updated rule {rule_id}: effect={is_effect}")
        return True
    
    def activate_rule(self, rule_id: str, tag: str, value: str, interval: float = 10.0) -> bool:
        """
        Activate a filter rule.
        
        Args:
            rule_id: Rule ID to activate
            tag: Rule tag
            value: Rule filter value
            interval: Check interval
            
        Returns:
            True if successful
        """
        return self.update_rule(rule_id, tag, value, interval, is_effect=1)
    
    def deactivate_rule(self, rule_id: str, tag: str, value: str, interval: float = 10.0) -> bool:
        """
        Deactivate a filter rule.
        
        Args:
            rule_id: Rule ID to deactivate
            tag: Rule tag
            value: Rule filter value
            interval: Check interval
            
        Returns:
            True if successful
        """
        return self.update_rule(rule_id, tag, value, interval, is_effect=0)
    
    def delete_rule(self, rule_id: str) -> bool:
        """
        Delete a filter rule.
        
        Args:
            rule_id: Rule ID to delete
            
        Returns:
            True if successful
        """
        self._request(
            "DELETE",
            "/oapi/tweet_filter/delete_rule",
            {'rule_id': rule_id}
        )
        
        logger.info(f"Deleted rule {rule_id}")
        return True
    
    # =========================================================================
    # USER INFO METHODS
    # =========================================================================
    
    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed user profile information.
        
        Calls the /twitter/user/info endpoint to get full profile data including:
        - id, name, userName
        - followers, following
        - description (bio)
        - isBlueVerified, verifiedType
        - createdAt
        - profilePicture, coverPicture
        - isAutomated, automatedBy
        
        Args:
            username: Twitter username (without @)
            
        Returns:
            User profile dict or None if not found
        """
        try:
            # Remove @ if present
            username = username.lstrip('@')
            
            url = f"{self.BASE_URL}/twitter/user/info"
            response = self._session.get(url, params={"userName": username})
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('status') == 'success' and result.get('data'):
                user_data = result['data']
                logger.debug(f"Got user info for @{username}: {user_data.get('followers', 0)} followers")
                return user_data
            else:
                logger.warning(f"No user data for @{username}: {result.get('msg', 'Unknown')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get user info for @{username}: {e}")
            return None
    
    def get_users_info(self, usernames: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get profile info for multiple users.
        
        Args:
            usernames: List of usernames to look up
            
        Returns:
            Dict mapping username -> user profile data
        """
        results = {}
        for username in usernames:
            username = username.lstrip('@')
            user_info = self.get_user_info(username)
            if user_info:
                results[username.lower()] = user_info
        return results
    
    def get_rule_by_tag(self, tag: str) -> Optional[FilterRule]:
        """
        Find a rule by its tag.
        
        Args:
            tag: Rule tag to find
            
        Returns:
            FilterRule if found, None otherwise
        """
        rules = self.get_rules()
        for rule in rules:
            if rule.tag == tag:
                return rule
        return None
    
    def setup_bankr_monitor(
        self,
        interval_seconds: float = 5.0,
        activate: bool = False
    ) -> FilterRule:
        """
        Set up monitoring for Bankr deployment commands.
        
        Creates a filter rule to capture tweets mentioning @bankrbot.
        
        Args:
            interval_seconds: Check frequency
            activate: Whether to activate immediately
            
        Returns:
            Created/existing FilterRule
        """
        tag = "bankr_deployments"
        value = "@bankrbot"
        
        # Check if rule already exists
        existing = self.get_rule_by_tag(tag)
        if existing:
            logger.info(f"Rule '{tag}' already exists: {existing.rule_id}")
            if activate and existing.is_effect != 1:
                self.activate_rule(
                    existing.rule_id,
                    existing.tag,
                    existing.value,
                    existing.interval_seconds
                )
            return existing
        
        # Create new rule
        rule = self.add_rule(
            tag=tag,
            value=value,
            interval_seconds=int(max(interval_seconds, 1))  # Min 1 second for API
        )
        
        if activate:
            self.activate_rule(rule.rule_id, tag, value, interval_seconds)
        
        return rule
