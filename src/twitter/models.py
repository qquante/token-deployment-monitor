"""
Data models for Twitter API responses.

Defines the structure of tweets, webhook payloads, and filter rules
as received from TwitterAPI.io.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


@dataclass
class TweetAuthor:
    """Twitter user who authored a tweet."""
    
    id: str
    username: str
    name: str
    profile_image_url: Optional[str] = None
    verified: bool = False
    followers_count: int = 0
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TweetAuthor':
        """Create from API response dict."""
        return cls(
            id=str(data.get('id', '')),
            username=data.get('username', ''),
            name=data.get('name', ''),
            profile_image_url=data.get('profile_image_url'),
            verified=data.get('verified', False),
            followers_count=data.get('followers_count', 0),
        )


@dataclass
class Tweet:
    """
    Tweet data from TwitterAPI.io webhook.
    
    Represents a single tweet with all metadata needed
    to detect Bankr deployment commands.
    """
    
    id: str
    text: str
    author: TweetAuthor
    created_at: str
    retweet_count: int = 0
    like_count: int = 0
    reply_count: int = 0
    quote_count: int = 0
    conversation_id: Optional[str] = None
    in_reply_to_user_id: Optional[str] = None
    in_reply_to_tweet_id: Optional[str] = None
    entities: Dict[str, Any] = field(default_factory=dict)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Tweet':
        """Create from webhook payload tweet dict."""
        author_data = data.get('author', {})
        author = TweetAuthor.from_dict(author_data)
        
        return cls(
            id=str(data.get('id', '')),
            text=data.get('text', ''),
            author=author,
            created_at=data.get('created_at', ''),
            retweet_count=data.get('retweet_count', 0),
            like_count=data.get('like_count', 0),
            reply_count=data.get('reply_count', 0),
            quote_count=data.get('quote_count', 0),
            conversation_id=data.get('conversation_id'),
            in_reply_to_user_id=data.get('in_reply_to_user_id'),
            in_reply_to_tweet_id=data.get('in_reply_to_tweet_id'),
            entities=data.get('entities', {}),
            raw_data=data,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'text': self.text,
            'author': {
                'id': self.author.id,
                'username': self.author.username,
                'name': self.author.name,
            },
            'created_at': self.created_at,
            'retweet_count': self.retweet_count,
            'like_count': self.like_count,
            'reply_count': self.reply_count,
        }
    
    @property
    def tweet_url(self) -> str:
        """Get the Twitter URL for this tweet."""
        return f"https://twitter.com/{self.author.username}/status/{self.id}"
    
    def has_mention(self, username: str) -> bool:
        """Check if tweet mentions a specific user."""
        mentions = self.entities.get('mentions', [])
        return any(m.get('username', '').lower() == username.lower() for m in mentions)
    
    def is_reply_to(self, username: str) -> bool:
        """Check if this is a reply to a specific user."""
        if not self.in_reply_to_user_id:
            return False
        # Check mentions for the username
        return self.has_mention(username)


@dataclass
class WebhookPayload:
    """
    Complete webhook payload from TwitterAPI.io.
    
    Contains the event metadata and list of matching tweets.
    """
    
    event_type: str
    rule_id: str
    rule_tag: str
    tweets: List[Tweet]
    timestamp: int
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WebhookPayload':
        """Parse webhook POST body."""
        tweets_data = data.get('tweets', [])
        tweets = [Tweet.from_dict(t) for t in tweets_data]
        
        return cls(
            event_type=data.get('event_type', 'tweet'),
            rule_id=data.get('rule_id', ''),
            rule_tag=data.get('rule_tag', ''),
            tweets=tweets,
            timestamp=data.get('timestamp', 0),
            raw_data=data,
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WebhookPayload':
        """Parse from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


@dataclass
class FilterRule:
    """
    Tweet filter rule configuration.
    
    Represents a rule in TwitterAPI.io's filter system.
    """
    
    rule_id: Optional[str] = None
    tag: str = ""
    value: str = ""
    interval_seconds: float = 10.0
    is_effect: int = 0  # 0 = disabled, 1 = enabled
    webhook_url: Optional[str] = None
    
    def to_create_payload(self) -> Dict[str, Any]:
        """Generate payload for add_rule API."""
        return {
            'tag': self.tag,
            'value': self.value,
            'interval_seconds': int(self.interval_seconds),
        }
    
    def to_update_payload(self) -> Dict[str, Any]:
        """Generate payload for update_rule API."""
        return {
            'rule_id': self.rule_id,
            'tag': self.tag,
            'value': self.value,
            'interval_seconds': self.interval_seconds,
            'is_effect': self.is_effect,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FilterRule':
        """Create from API response."""
        return cls(
            rule_id=data.get('rule_id'),
            tag=data.get('tag', ''),
            value=data.get('value', ''),
            interval_seconds=data.get('interval_seconds', 10.0),
            is_effect=data.get('is_effect', 0),
        )


# Bankr-specific patterns
BANKR_MENTIONS = [
    "@bankrbot",
    "@bankr",
]

BANKR_DEPLOY_KEYWORDS = [
    "deploy",
    "launch",
    "create",
    "mint",
]
