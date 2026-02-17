"""
On-chain WebSocket listener for real-time TokenCreated events.

This module provides real-time monitoring of Clanker contract
events via WebSocket subscription.
"""

import os
import json
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Callable, Awaitable, List
from datetime import datetime

import aiohttp
from eth_abi import decode

logger = logging.getLogger(__name__)

# Clanker V4 contract address
CLANKER_V4 = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"

# TokenCreated event topic
TOKEN_CREATED_TOPIC = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"

# ABI types for decoding non-indexed parameters
TOKEN_CREATED_ABI_TYPES = [
    'address',      # msgSender
    'string',       # tokenImage
    'string',       # tokenName
    'string',       # tokenSymbol
    'string',       # tokenMetadata
    'string',       # tokenContext
    'int24',        # startingTick
    'address',      # poolHook
    'bytes32',      # poolId
    'address',      # pairedToken
    'address',      # locker
    'address',      # mevModule
    'uint256',      # extensionsSupply
    'address[]',    # extensions
]

# Known Bankr deployer addresses
BANKR_DEPLOYERS = [
    "0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9",
    "0x10904ad5228820a81ece2661f40dbd987469fccb",
    "0x88075f206d78434c1b8c6b694581a09895f58ca9",
]


@dataclass
class OnchainDeployment:
    """
    Parsed on-chain deployment data.
    
    Contains all relevant information from a TokenCreated event,
    ready to be displayed in Telegram.
    """
    # Token info
    token_address: str
    token_name: str
    token_symbol: str
    token_image: str
    
    # Creator info
    creator_wallet: str  # token_admin
    deployer_wallet: str  # msg_sender
    
    # Context (for linking to Twitter)
    platform: str  # "X", "farcaster", etc.
    message_id: str  # URL of original tweet or farcaster post
    user_id: str  # Twitter/Farcaster user ID
    
    # Block info
    block_number: int
    tx_hash: str
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Raw data
    context_parsed: Dict[str, Any] = field(default_factory=dict)
    metadata_parsed: Dict[str, Any] = field(default_factory=dict)
    
    def is_twitter(self) -> bool:
        """Check if this deployment is from Twitter/X."""
        return self.platform.lower() == "x"
    
    def get_tweet_id(self) -> Optional[str]:
        """Extract tweet ID from message_id URL."""
        if not self.is_twitter() or not self.message_id:
            return None
        # URL format: https://twitter.com/user/status/123456789
        if "/status/" in self.message_id:
            return self.message_id.split("/status/")[-1].split("?")[0]
        return None
    
    def get_tweet_username(self) -> Optional[str]:
        """Extract username from message_id URL."""
        if not self.is_twitter() or not self.message_id:
            return None
        # URL format: https://twitter.com/USERNAME/status/123456789
        try:
            # Remove protocol and domain
            path = self.message_id.split("twitter.com/")[-1]
            # Get username (first path segment before /status/)
            if "/status/" in path:
                username = path.split("/status/")[0]
                return username if username else None
        except:
            pass
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_address": self.token_address,
            "token_name": self.token_name,
            "token_symbol": self.token_symbol,
            "token_image": self.token_image,
            "creator_wallet": self.creator_wallet,
            "deployer_wallet": self.deployer_wallet,
            "platform": self.platform,
            "message_id": self.message_id,
            "user_id": self.user_id,
            "block_number": self.block_number,
            "tx_hash": self.tx_hash,
            "timestamp": self.timestamp.isoformat(),
        }


class OnchainListener:
    """
    WebSocket listener for real-time on-chain events.
    
    Connects to Ankr WSS and subscribes to TokenCreated events
    from the Clanker contract. Only Bankr deployments are processed.
    
    Usage:
        listener = OnchainListener()
        await listener.start(callback=handle_deployment)
    """
    
    def __init__(
        self,
        wss_url: Optional[str] = None,
        http_url: Optional[str] = None,
    ):
        """
        Initialize the listener.
        
        Args:
            wss_url: WebSocket RPC URL (defaults to ANKR_RPC_WSS env var)
            http_url: HTTP RPC URL for additional queries (defaults to ANKR_RPC_HTTP)
        """
        self.wss_url = wss_url or os.getenv("ANKR_RPC_WSS", "")
        self.http_url = http_url or os.getenv("ANKR_RPC_HTTP", "")
        
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._subscription_id: Optional[str] = None
        self._reconnect_delay = 1
        self._max_reconnect_delay = 60
    
    async def start(
        self,
        callback: Callable[[OnchainDeployment], Awaitable[None]],
    ):
        """
        Start listening for on-chain events.
        
        Args:
            callback: Async function to call when a Bankr deployment is detected
        """
        if not self.wss_url:
            logger.error("ANKR_RPC_WSS not configured - on-chain listener disabled")
            return
        
        self._running = True
        logger.info(f"[ONCHAIN] Starting listener: {self.wss_url[:50]}...")
        
        while self._running:
            try:
                await self._connect_and_listen(callback)
            except asyncio.CancelledError:
                logger.info("[ONCHAIN] Listener cancelled")
                break
            except Exception as e:
                logger.error(f"[ONCHAIN] Connection error: {e}")
                if self._running:
                    logger.info(f"[ONCHAIN] Reconnecting in {self._reconnect_delay}s...")
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        self._max_reconnect_delay
                    )
    
    async def stop(self):
        """Stop the listener."""
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("[ONCHAIN] Listener stopped")
    
    async def _connect_and_listen(
        self,
        callback: Callable[[OnchainDeployment], Awaitable[None]],
    ):
        """Connect to WebSocket and listen for events."""
        self._session = aiohttp.ClientSession()
        
        async with self._session.ws_connect(self.wss_url) as ws:
            self._ws = ws
            self._reconnect_delay = 1  # Reset on successful connection
            
            # Subscribe to logs
            await self._subscribe(ws)
            logger.info("[ONCHAIN] Connected and subscribed to TokenCreated events")
            
            # Listen for messages
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_message(msg.data, callback)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"[ONCHAIN] WebSocket error: {ws.exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("[ONCHAIN] WebSocket closed")
                    break
    
    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse):
        """Subscribe to TokenCreated logs."""
        subscribe_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                {
                    "address": CLANKER_V4,
                    "topics": [TOKEN_CREATED_TOPIC]
                }
            ]
        }
        
        await ws.send_json(subscribe_request)
        
        # Wait for subscription confirmation
        response = await ws.receive_json()
        if "result" in response:
            self._subscription_id = response["result"]
            logger.info(f"[ONCHAIN] Subscription ID: {self._subscription_id}")
        else:
            raise ValueError(f"Subscription failed: {response}")
    
    async def _handle_message(
        self,
        data: str,
        callback: Callable[[OnchainDeployment], Awaitable[None]],
    ):
        """Handle incoming WebSocket message."""
        try:
            message = json.loads(data)
            
            # Check if this is a subscription notification
            if message.get("method") == "eth_subscription":
                params = message.get("params", {})
                if params.get("subscription") == self._subscription_id:
                    log = params.get("result", {})
                    await self._process_log(log, callback)
        
        except json.JSONDecodeError as e:
            logger.error(f"[ONCHAIN] JSON decode error: {e}")
        except Exception as e:
            logger.error(f"[ONCHAIN] Message handling error: {e}")
    
    async def _process_log(
        self,
        log: Dict[str, Any],
        callback: Callable[[OnchainDeployment], Awaitable[None]],
    ):
        """Process a TokenCreated log event."""
        try:
            # Decode the event
            deployment = self._decode_log(log)
            
            if not deployment:
                return
            
            # Check if this is a Bankr deployment
            if deployment.deployer_wallet.lower() not in [d.lower() for d in BANKR_DEPLOYERS]:
                logger.debug(f"[ONCHAIN] Non-Bankr deployment, skipping: {deployment.token_address}")
                return
            
            # Only process X (Twitter) deployments for now
            if not deployment.is_twitter():
                logger.info(f"[ONCHAIN] Non-Twitter deployment ({deployment.platform}): {deployment.token_name}")
                return
            
            logger.info(
                f"[ONCHAIN] 🔵 BANKR DEPLOYMENT DETECTED!\n"
                f"  Token: {deployment.token_name} ({deployment.token_symbol})\n"
                f"  Contract: {deployment.token_address}\n"
                f"  Creator: {deployment.creator_wallet}\n"
                f"  Platform: {deployment.platform}\n"
                f"  Tweet: {deployment.message_id}"
            )
            
            # Call the callback
            await callback(deployment)
            
        except Exception as e:
            logger.error(f"[ONCHAIN] Log processing error: {e}")
    
    def _decode_log(self, log: Dict[str, Any]) -> Optional[OnchainDeployment]:
        """Decode a raw log into OnchainDeployment."""
        try:
            topics = log.get("topics", [])
            data = log.get("data", "0x")
            
            if len(topics) < 3:
                logger.warning("[ONCHAIN] Invalid log: not enough topics")
                return None
            
            # Extract indexed parameters from topics
            token_address = "0x" + topics[1][-40:]
            token_admin = "0x" + topics[2][-40:]
            
            # Decode non-indexed parameters from data
            data_bytes = bytes.fromhex(data[2:]) if data.startswith("0x") else bytes.fromhex(data)
            decoded = decode(TOKEN_CREATED_ABI_TYPES, data_bytes)
            
            msg_sender = decoded[0]
            token_image = decoded[1]
            token_name = decoded[2]
            token_symbol = decoded[3]
            token_metadata = decoded[4]
            token_context = decoded[5]
            
            # Parse context JSON
            context_parsed = {}
            try:
                context_parsed = json.loads(token_context) if token_context else {}
            except json.JSONDecodeError:
                pass
            
            # Parse metadata JSON
            metadata_parsed = {}
            try:
                metadata_parsed = json.loads(token_metadata) if token_metadata else {}
            except json.JSONDecodeError:
                pass
            
            # Extract platform and message info from context
            platform = context_parsed.get("platform", "unknown")
            message_id = context_parsed.get("messageId", "")
            user_id = context_parsed.get("id", "")
            
            # Get block info
            block_number = int(log.get("blockNumber", "0x0"), 16)
            tx_hash = log.get("transactionHash", "")
            
            return OnchainDeployment(
                token_address=token_address,
                token_name=token_name,
                token_symbol=token_symbol,
                token_image=token_image,
                creator_wallet=token_admin,
                deployer_wallet=msg_sender,
                platform=platform,
                message_id=message_id,
                user_id=user_id,
                block_number=block_number,
                tx_hash=tx_hash,
                context_parsed=context_parsed,
                metadata_parsed=metadata_parsed,
            )
            
        except Exception as e:
            logger.error(f"[ONCHAIN] Decode error: {e}")
            return None
    
    async def get_creator_info(self, wallet: str) -> Dict[str, Any]:
        """
        Fetch creator wallet info via HTTP RPC.
        
        Args:
            wallet: Creator wallet address
            
        Returns:
            Dict with balance, tx_count
        """
        if not self.http_url:
            return {}
        
        try:
            async with aiohttp.ClientSession() as session:
                # Get balance
                balance_req = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getBalance",
                    "params": [wallet, "latest"]
                }
                
                # Get transaction count
                tx_count_req = {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_getTransactionCount",
                    "params": [wallet, "latest"]
                }
                
                async with session.post(self.http_url, json=balance_req) as resp:
                    balance_data = await resp.json()
                    balance_wei = int(balance_data.get("result", "0x0"), 16)
                    balance_eth = balance_wei / 1e18
                
                async with session.post(self.http_url, json=tx_count_req) as resp:
                    tx_data = await resp.json()
                    tx_count = int(tx_data.get("result", "0x0"), 16)
                
                return {
                    "balance": f"{balance_eth:.4f} ETH",
                    "tx_count": tx_count,
                }
                
        except Exception as e:
            logger.error(f"[ONCHAIN] Creator info error: {e}")
            return {}
