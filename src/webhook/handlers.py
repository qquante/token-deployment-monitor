"""
Webhook handlers for processing incoming tweets.

This module provides handler classes that process incoming
webhook payloads and detect Bankr deployment commands.
"""

import re
import logging
from typing import List, Optional, Callable, Dict, Any
from dataclasses import dataclass
from datetime import datetime

try:
    from src.twitter.models import Tweet, WebhookPayload
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.twitter.models import Tweet, WebhookPayload

logger = logging.getLogger(__name__)


@dataclass
class BankrDeploymentSignal:
    """
    Detected Bankr deployment signal from a tweet.
    
    Contains extracted information about a potential
    token deployment via Bankr.
    """
    
    tweet: Tweet
    detected_at: datetime
    token_name: Optional[str] = None
    token_symbol: Optional[str] = None
    token_image: Optional[str] = None
    confidence: float = 0.0
    raw_command: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'tweet_id': self.tweet.id,
            'tweet_url': self.tweet.tweet_url,
            'author': self.tweet.author.username,
            'text': self.tweet.text,
            'detected_at': self.detected_at.isoformat(),
            'token_name': self.token_name,
            'token_symbol': self.token_symbol,
            'token_image': self.token_image,
            'confidence': self.confidence,
        }


class BankrDeploymentHandler:
    """
    Handler for detecting Bankr deployment commands in tweets.
    
    Analyzes incoming tweets to identify token deployment
    requests and extracts relevant metadata.
    
    Example:
        handler = BankrDeploymentHandler()
        handler.on_deployment(my_callback)
        
        # Process webhook payload
        signals = handler.process(payload)
    """
    
    # Bankr bot usernames to detect
    BANKR_USERNAMES = ['bankrbot', 'bankr']
    
    # Keywords that indicate deployment intent
    DEPLOY_KEYWORDS = [
        'deploy', 'launch', 'create', 'mint', 'make',
        'token', 'coin', 'memecoin'
    ]
    
    # Patterns to extract token name/symbol
    TOKEN_NAME_PATTERNS = [
        r'(?:name|called?|named?)[:\s]+["\']?([A-Za-z0-9\s]+)["\']?',
        r'\$([A-Z]{2,10})',  # $SYMBOL format
        r'ticker[:\s]+["\']?([A-Z]{2,10})["\']?',
    ]
    
    def __init__(self):
        """Initialize handler."""
        self._callbacks: List[Callable[[BankrDeploymentSignal], None]] = []
        self._processed_tweets: set = set()
    
    def on_deployment(self, callback: Callable[[BankrDeploymentSignal], None]) -> None:
        """
        Register callback for deployment detections.
        
        Args:
            callback: Function to call with BankrDeploymentSignal
        """
        self._callbacks.append(callback)
    
    def process(self, payload: WebhookPayload) -> List[BankrDeploymentSignal]:
        """
        Process webhook payload and detect deployments.
        
        Args:
            payload: WebhookPayload from TwitterAPI.io
            
        Returns:
            List of detected deployment signals
        """
        signals = []
        
        for tweet in payload.tweets:
            # Skip already processed tweets
            if tweet.id in self._processed_tweets:
                continue
            
            self._processed_tweets.add(tweet.id)
            
            signal = self._analyze_tweet(tweet)
            if signal:
                signals.append(signal)
                self._notify_callbacks(signal)
        
        return signals
    
    def _analyze_tweet(self, tweet: Tweet) -> Optional[BankrDeploymentSignal]:
        """
        Analyze a single tweet for deployment signals.
        
        Args:
            tweet: Tweet to analyze
            
        Returns:
            BankrDeploymentSignal if deployment detected, None otherwise
        """
        text_lower = tweet.text.lower()
        
        # Check if it mentions Bankr
        mentions_bankr = any(
            f"@{username}" in text_lower 
            for username in self.BANKR_USERNAMES
        )
        
        if not mentions_bankr:
            return None
        
        # Calculate confidence based on keywords
        confidence = 0.3  # Base confidence for mentioning Bankr
        
        keyword_matches = sum(
            1 for kw in self.DEPLOY_KEYWORDS if kw in text_lower
        )
        confidence += min(keyword_matches * 0.15, 0.45)
        
        # Check for reply pattern (common for Bankr commands)
        if tweet.in_reply_to_tweet_id:
            confidence += 0.1
        
        # Extract token info
        token_name, token_symbol = self._extract_token_info(tweet.text)
        if token_name or token_symbol:
            confidence += 0.15
        
        # Extract image URL from entities
        token_image = self._extract_image(tweet)
        if token_image:
            confidence += 0.1
        
        # Only return signal if confidence is high enough
        if confidence < 0.4:
            return None
        
        logger.info(
            f"Detected potential Bankr deployment: {tweet.tweet_url} "
            f"(confidence: {confidence:.2f})"
        )
        
        return BankrDeploymentSignal(
            tweet=tweet,
            detected_at=datetime.utcnow(),
            token_name=token_name,
            token_symbol=token_symbol,
            token_image=token_image,
            confidence=min(confidence, 1.0),
            raw_command=tweet.text,
        )
    
    def _extract_token_info(self, text: str) -> tuple:
        """Extract token name and symbol from text."""
        token_name = None
        token_symbol = None
        
        for pattern in self.TOKEN_NAME_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                extracted = match.group(1).strip()
                if extracted.isupper() and len(extracted) <= 10:
                    token_symbol = extracted
                else:
                    token_name = extracted
                break
        
        return token_name, token_symbol
    
    def _extract_image(self, tweet: Tweet) -> Optional[str]:
        """Extract image URL from tweet entities."""
        media = tweet.entities.get('media', [])
        for item in media:
            if item.get('type') == 'photo':
                return item.get('url') or item.get('media_url_https')
        return None
    
    def _notify_callbacks(self, signal: BankrDeploymentSignal) -> None:
        """Notify all registered callbacks."""
        for callback in self._callbacks:
            try:
                callback(signal)
            except Exception as e:
                logger.error(f"Callback error: {e}")
