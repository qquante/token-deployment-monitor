"""
Redis Pub/Sub channels and event types.

All inter-worker communication goes through these channels.
"""

from dataclasses import dataclass
from typing import Dict, Any, List
from datetime import datetime
import json


class Channels:
    """Redis Pub/Sub channel names."""
    
    # Twitter events
    TWITTER_TWEET = "twitter:tweet"              # New tweet received
    TWITTER_PREBANKR = "twitter:prebankr"        # PRE-BANKR request detected
    TWITTER_DEPLOYED = "twitter:deployed"        # @bankrbot DEPLOYED tweet
    
    # On-chain events
    ONCHAIN_TOKEN = "onchain:token"              # TokenCreated event
    ONCHAIN_BLOCK = "onchain:block"              # New block
    ONCHAIN_AUCTION = "onchain:auction"          # Auction events
    
    # Telegram events
    TELEGRAM_SEND = "telegram:send"              # Send message request
    TELEGRAM_EDIT = "telegram:edit"              # Edit message request
    TELEGRAM_CALLBACK = "telegram:callback"      # Callback query received
    TELEGRAM_MESSAGE = "telegram:message"        # Text message received
    
    # Trading events
    TRADING_BUY_APPROVED = "trading:buy_approved"    # User approved buy
    TRADING_TOKEN_LINKED = "trading:token_linked"    # Token linked to buy
    TRADING_EXECUTE = "trading:execute"              # Execute trade now
    TRADING_RESULT = "trading:result"                # Trade result
    
    # Enrichment events
    ENRICH_REQUEST = "enrich:request"            # Request user enrichment
    ENRICH_RESULT = "enrich:result"              # Enrichment completed
    
    # Pre-Clawnch events (Moltbook tracking)
    PRECLAWNCH_MOLTBOOK = "preclawnch:moltbook"  # Moltbook PRE-CLAWNCH token for tracking
    
    # System events
    SYSTEM_HEARTBEAT = "system:heartbeat"        # Worker heartbeat
    SYSTEM_ERROR = "system:error"                # Error notification


class Events:
    """Event builders for Pub/Sub messages."""
    
    # =========================================================================
    # TWITTER EVENTS
    # =========================================================================
    
    @staticmethod
    def twitter_tweet(
        tweet_id: str,
        author_username: str,
        text: str,
        url: str,
        in_reply_to_id: str = "",
        raw_tweet: Dict = None,
    ) -> Dict:
        """Twitter tweet event."""
        return {
            "event": "twitter_tweet",
            "timestamp": datetime.now().isoformat(),
            "tweet_id": tweet_id,
            "author_username": author_username,
            "text": text,
            "url": url,
            "in_reply_to_id": in_reply_to_id,
            "raw_tweet": raw_tweet or {},
        }
    
    @staticmethod
    def prebankr_detected(
        tweet_id: str,
        tweet_url: str,
        from_username: str,
        to_usernames: List[str],
        token_name: str = None,
        token_symbol: str = None,
        confidence: int = 0,
        raw_tweet: Dict = None,
    ) -> Dict:
        """PRE-BANKR request detected event."""
        return {
            "event": "prebankr_detected",
            "timestamp": datetime.now().isoformat(),
            "tweet_id": tweet_id,
            "tweet_url": tweet_url,
            "from_username": from_username,
            "to_usernames": to_usernames,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "confidence": confidence,
            "raw_tweet": raw_tweet or {},
        }
    
    @staticmethod
    def deployed_detected(
        tweet_id: str,
        tweet_url: str,
        original_tweet_id: str,
        token_name: str,
        token_symbol: str,
        contract_address: str,
        from_username: str,
        to_usernames: List[str],
        raw_tweet: Dict = None,
    ) -> Dict:
        """DEPLOYED notification detected event."""
        return {
            "event": "deployed_detected",
            "timestamp": datetime.now().isoformat(),
            "tweet_id": tweet_id,
            "tweet_url": tweet_url,
            "original_tweet_id": original_tweet_id,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "contract_address": contract_address,
            "from_username": from_username,
            "to_usernames": to_usernames,
            "raw_tweet": raw_tweet or {},
        }
    
    # =========================================================================
    # ON-CHAIN EVENTS
    # =========================================================================
    
    @staticmethod
    def token_created(
        token_address: str,
        token_name: str,
        token_symbol: str,
        creator_wallet: str,
        pool_id: str,
        platform: str,
        tweet_id: str = "",
        tweet_url: str = "",
        block_number: int = 0,
        tx_hash: str = "",
    ) -> Dict:
        """Token created on-chain event."""
        return {
            "event": "token_created",
            "timestamp": datetime.now().isoformat(),
            "token_address": token_address,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "creator_wallet": creator_wallet,
            "pool_id": pool_id,
            "platform": platform,
            "tweet_id": tweet_id,
            "tweet_url": tweet_url,
            "block_number": block_number,
            "tx_hash": tx_hash,
        }
    
    @staticmethod
    def new_block(
        block_number: int,
        block_hash: str = "",
        timestamp: int = 0,
    ) -> Dict:
        """New block event."""
        return {
            "event": "new_block",
            "timestamp": datetime.now().isoformat(),
            "block_number": block_number,
            "block_hash": block_hash,
            "block_timestamp": timestamp,
        }
    
    # =========================================================================
    # TELEGRAM EVENTS
    # =========================================================================
    
    @staticmethod
    def telegram_send(
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Dict = None,
        channel: str = "deployed",  # deployed, prebankr, trading
        correlation_id: str = "",
    ) -> Dict:
        """Telegram send message request."""
        return {
            "event": "telegram_send",
            "timestamp": datetime.now().isoformat(),
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "reply_markup": reply_markup,
            "channel": channel,
            "correlation_id": correlation_id,
        }
    
    @staticmethod
    def telegram_edit(
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Dict = None,
        correlation_id: str = "",
    ) -> Dict:
        """Telegram edit message request."""
        return {
            "event": "telegram_edit",
            "timestamp": datetime.now().isoformat(),
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "reply_markup": reply_markup,
            "correlation_id": correlation_id,
        }
    
    @staticmethod
    def telegram_callback(
        callback_id: str,
        user_id: int,
        username: str,
        chat_id: int,
        message_id: int,
        data: str,
    ) -> Dict:
        """Telegram callback query event."""
        return {
            "event": "telegram_callback",
            "timestamp": datetime.now().isoformat(),
            "callback_id": callback_id,
            "user_id": user_id,
            "username": username,
            "chat_id": chat_id,
            "message_id": message_id,
            "data": data,
        }
    
    # =========================================================================
    # TRADING EVENTS
    # =========================================================================
    
    @staticmethod
    def buy_approved(
        user_id: int,
        username: str,
        tweet_id: str,
        token_symbol: str,
        eth_amount: float,
        block_offset: int,
        fee_expected: float,
    ) -> Dict:
        """User approved buy event."""
        return {
            "event": "buy_approved",
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "username": username,
            "tweet_id": tweet_id,
            "token_symbol": token_symbol,
            "eth_amount": eth_amount,
            "block_offset": block_offset,
            "fee_expected": fee_expected,
        }
    
    @staticmethod
    def token_linked(
        user_id: int,
        tweet_id: str,
        token_address: str,
        token_symbol: str,
        pool_id: str,
        auction_expire_block: int,
        target_block: int,
    ) -> Dict:
        """Token linked to pending buy event."""
        return {
            "event": "token_linked",
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "tweet_id": tweet_id,
            "token_address": token_address,
            "token_symbol": token_symbol,
            "pool_id": pool_id,
            "auction_expire_block": auction_expire_block,
            "target_block": target_block,
        }
    
    @staticmethod
    def trade_result(
        user_id: int,
        tweet_id: str,
        token_symbol: str,
        status: str,  # DONE, FAILED
        tx_hash: str = "",
        error: str = "",
        eth_spent: float = 0,
        gas_used: int = 0,
    ) -> Dict:
        """Trade execution result event."""
        return {
            "event": "trade_result",
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "tweet_id": tweet_id,
            "token_symbol": token_symbol,
            "status": status,
            "tx_hash": tx_hash,
            "error": error,
            "eth_spent": eth_spent,
            "gas_used": gas_used,
        }
    
    # =========================================================================
    # ENRICHMENT EVENTS
    # =========================================================================
    
    @staticmethod
    def enrich_request(
        username: str,
        wallet: str = "",
        fetch_smart_followers: bool = True,
        correlation_id: str = "",
        priority: int = 0,  # 0=normal, 1=high (prebankr), 2=urgent (deployed)
    ) -> Dict:
        """User enrichment request event."""
        return {
            "event": "enrich_request",
            "timestamp": datetime.now().isoformat(),
            "username": username,
            "wallet": wallet,
            "fetch_smart_followers": fetch_smart_followers,
            "correlation_id": correlation_id,
            "priority": priority,
        }
    
    @staticmethod
    def enrich_result(
        username: str,
        success: bool,
        data: Dict = None,
        error: str = "",
        correlation_id: str = "",
    ) -> Dict:
        """User enrichment result event."""
        return {
            "event": "enrich_result",
            "timestamp": datetime.now().isoformat(),
            "username": username,
            "success": success,
            "data": data or {},
            "error": error,
            "correlation_id": correlation_id,
        }
    
    # =========================================================================
    # SYSTEM EVENTS
    # =========================================================================
    
    @staticmethod
    def heartbeat(
        worker_name: str,
        worker_id: str,
        status: str = "healthy",
        stats: Dict = None,
    ) -> Dict:
        """Worker heartbeat event."""
        return {
            "event": "heartbeat",
            "timestamp": datetime.now().isoformat(),
            "worker_name": worker_name,
            "worker_id": worker_id,
            "status": status,
            "stats": stats or {},
        }
    
    @staticmethod
    def error(
        worker_name: str,
        error_type: str,
        message: str,
        details: Dict = None,
    ) -> Dict:
        """Error notification event."""
        return {
            "event": "error",
            "timestamp": datetime.now().isoformat(),
            "worker_name": worker_name,
            "error_type": error_type,
            "message": message,
            "details": details or {},
        }
