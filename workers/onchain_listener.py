"""
On-Chain Listener Worker

Connects to Ethereum WebSocket and listens for TokenCreated events.

Responsibilities:
- Subscribe to TokenCreated events from Clanker V4
- Subscribe to new block headers
- Decode event data
- Publish to Redis Pub/Sub

Output Channels:
- onchain:token - TokenCreated events
- onchain:block - New block headers
"""

import os
import json
import asyncio
import aiohttp
import time
from typing import Dict, Any, Optional

try:
    import eth_abi
except ImportError:
    eth_abi = None

from workers.base import BaseWorker
from shared.pubsub import Channels, Events


# Clanker V4 contract on Base
CLANKER_V4_ADDRESS = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"

# TokenCreated event signature (from Clanker V4)
TOKEN_CREATED_TOPIC = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"

# Event ABI for decoding (data portion - non-indexed parameters)
# Indexed: topics[1]=tokenAddress, topics[2]=tokenAdmin
TOKEN_CREATED_ABI = [
    'address',    # 0: msgSender
    'string',     # 1: tokenImage
    'string',     # 2: tokenName
    'string',     # 3: tokenSymbol
    'string',     # 4: tokenMetadata (JSON: description, socialMediaUrls, auditUrls)
    'string',     # 5: tokenContext (JSON: interface, platform, messageId, id)
    'int24',      # 6: startingTick
    'address',    # 7: poolHook
    'bytes32',    # 8: poolId
    'address',    # 9: pairedToken (WETH)
    'address',    # 10: locker
    'address',    # 11: mevModule
    'uint256',    # 12: extensionsSupply
    'address[]',  # 13: extensions
]

# Legitimate Bankr deployer wallets (verified via @bankrbot Twitter)
# Only tokens from these wallets will be published as Bankr tokens
BANKR_DEPLOYER_WHITELIST = {
    "0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9",  # Official @bankrbot deployer
}

# Legitimate Clawnch deployer wallets (verified via contract.json analysis)
# Only tokens from these wallets will be published as Clawnch tokens
CLAWNCH_DEPLOYER_WHITELIST = {
    "0xfc426dfeae55dae2f936a592450c9ecea87a5736",  # Official Clawnch deployer (89%)
}

# =============================================================================
# FLASHBLOCKS CONFIGURATION
# =============================================================================
# Enable Ankr Premium Flashblocks for faster token detection (~200ms vs ~2s)
# Uses pendingLogs subscription to get events before block confirmation
FLASHBLOCKS_ENABLED = os.getenv("FLASHBLOCKS_ENABLED", "true").lower() == "true"

# Deduplication cache TTL (seconds) - how long to remember processed tokens
DEDUP_CACHE_TTL = 300  # 5 minutes


class OnchainListenerWorker(BaseWorker):
    """
    Listens to blockchain events via WebSocket.
    """
    
    WORKER_NAME = "onchain-listener"
    
    def __init__(self):
        super().__init__()
        
        self.wss_url = os.getenv("ANKR_RPC_WSS", "")
        self.http_url = os.getenv("ANKR_RPC_HTTP", "")
        
        if not self.wss_url:
            raise ValueError("ANKR_RPC_WSS not set!")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Deduplication cache: token_address -> timestamp
        # Prevents processing same token twice (flashblock + confirmed)
        self._processed_tokens: Dict[str, float] = {}
        
        # Stats
        self.stats.update({
            "tokens_detected": 0,
            "blocks_received": 0,
            "current_block": 0,
            "reconnects": 0,
            # Flashblocks stats
            "flashblock_tokens": 0,    # Tokens detected via pendingLogs (fast)
            "confirmed_tokens": 0,     # Tokens detected via logs (confirmed)
            "duplicate_tokens": 0,     # Tokens received in both channels
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession()
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Main loop - single WebSocket with both subscriptions."""
        while self._running:
            try:
                # heartbeat=30 sends ping every 30s to keep connection alive
                async with self._session.ws_connect(
                    self.wss_url,
                    heartbeat=30, 
                    receive_timeout=60,
                ) as ws:
                    self.logger.info(f"Connecting to Base WebSocket: {self.wss_url[:50]}...")
                    
                    # Subscribe to TokenCreated events
                    await ws.send_json({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": CLANKER_V4_ADDRESS,
                                "topics": [TOKEN_CREATED_TOPIC],
                            }
                        ]
                    })
                    resp = await ws.receive_json()
                    token_sub_id = resp.get("result")
                    self.logger.info(f"Token subscription: {token_sub_id}")
                    
                    # Subscribe to newHeads (blocks)
                    await ws.send_json({
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "eth_subscribe",
                        "params": ["newHeads"]
                    })
                    resp = await ws.receive_json()
                    block_sub_id = resp.get("result")
                    self.logger.info(f"Block subscription: {block_sub_id}")
                    
                    # =========================================================
                    # FLASHBLOCKS: Subscribe to pendingLogs for faster detection
                    # Gets events ~200ms after sequencer vs ~2s after block
                    # =========================================================
                    flashblock_sub_id = None
                    if FLASHBLOCKS_ENABLED:
                        await ws.send_json({
                            "jsonrpc": "2.0",
                            "id": 3,
                            "method": "eth_subscribe",
                            "params": [
                                "pendingLogs",
                                {
                                    "address": CLANKER_V4_ADDRESS,
                                    "topics": [TOKEN_CREATED_TOPIC],
                                }
                            ]
                        })
                        resp = await ws.receive_json()
                        flashblock_sub_id = resp.get("result")
                        if flashblock_sub_id:
                            self.logger.info(f"[FLASHBLOCK] Subscription active: {flashblock_sub_id}")
                        else:
                            error = resp.get("error", {})
                            self.logger.warning(f"[FLASHBLOCK] Subscription failed: {error} - using confirmed logs only")
                    else:
                        self.logger.info("[FLASHBLOCK] Disabled via FLASHBLOCKS_ENABLED=false")
                    
                    active_subs = ["logs", "newHeads"]
                    if flashblock_sub_id:
                        active_subs.append("pendingLogs")
                    self.logger.info(f"Active subscriptions: {', '.join(active_subs)} - listening...")
                    
                    # Track message count for debugging
                    msg_count = 0
                    
                    # Listen for both events
                    async for msg in ws:
                        if not self._running:
                            break
                        
                        msg_count += 1
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            
                            # Debug: log every 10th message or subscription events
                            if msg_count <= 5 or msg_count % 100 == 0:
                                self.logger.debug(f"[WS MSG #{msg_count}] {str(data)[:100]}...")
                            
                            if data.get("method") == "eth_subscription":
                                sub_id = data.get("params", {}).get("subscription")
                                
                                if sub_id == token_sub_id:
                                    # Confirmed logs (block already mined)
                                    await self._handle_token_event(data, source="confirmed")
                                elif sub_id == flashblock_sub_id:
                                    # Flashblock pendingLogs (preconfirmed, ~200ms)
                                    await self._handle_token_event(data, source="flashblock")
                                elif sub_id == block_sub_id:
                                    await self._handle_block(data)
                        
                        elif msg.type == aiohttp.WSMsgType.PING:
                            self.logger.debug(f"[WS] Received PING")
                        
                        elif msg.type == aiohttp.WSMsgType.PONG:
                            self.logger.debug(f"[WS] Received PONG")
                        
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            self.logger.warning(f"WebSocket closed/error: {msg.type}")
                            break
            
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.logger.warning("[WS] Receive timeout - reconnecting...")
                self.stats["reconnects"] += 1
                await asyncio.sleep(2)
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}", exc_info=True)
                self.stats["errors"] += 1
                self.stats["reconnects"] += 1
                if self._running:
                    self.logger.info("Reconnecting in 5s...")
                    await asyncio.sleep(5)
    
    async def _handle_token_event(self, data: Dict[str, Any], source: str = "confirmed"):
        """
        Process TokenCreated event.
        
        Args:
            data: WebSocket subscription message
            source: "flashblock" (pendingLogs, ~200ms) or "confirmed" (logs, ~2s)
        """
        if data.get("method") != "eth_subscription":
            return
        
        result = data.get("params", {}).get("result", {})
        if not result:
            return
        
        try:
            # Extract basic info
            tx_hash = result.get("transactionHash", "")
            block_hex = result.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if block_hex else 0
            
            # Decode event data (returns full structured data)
            decoded = self._decode_token_created(result)
            if not decoded:
                return
            
            token_address = decoded["token_address"]
            token_symbol = decoded["token_symbol"]
            
            # =================================================================
            # DEDUPLICATION: Prevent processing same token twice
            # Flashblocks arrive first (~200ms), confirmed logs arrive later (~2s)
            # =================================================================
            if token_address in self._processed_tokens:
                if source == "confirmed":
                    # Already processed via flashblock - just update stats
                    self.stats["duplicate_tokens"] += 1
                    self.logger.debug(
                        f"[DEDUP] ${token_symbol} already processed via flashblock, "
                        f"confirmed log ignored"
                    )
                else:
                    # Duplicate flashblock (shouldn't happen, but handle gracefully)
                    self.logger.debug(f"[DEDUP] ${token_symbol} duplicate flashblock ignored")
                return
            
            # Mark as processed with timestamp
            self._processed_tokens[token_address] = time.time()
            
            # Cleanup old entries periodically (every 100 tokens)
            if len(self._processed_tokens) % 100 == 0:
                self._cleanup_processed_tokens()
            
            # Get context from decoded data
            context_parsed = decoded.get("context_parsed", {})
            platform = context_parsed.get("platform", "unknown")
            interface = context_parsed.get("interface", "")
            
            # Extract tweet info
            tweet_id = ""
            tweet_url = context_parsed.get("messageId", "")
            tweet_username = ""
            
            if "/status/" in tweet_url:
                tweet_id = tweet_url.split("/status/")[-1].split("?")[0]
                parts = tweet_url.split("/status/")[0].split("/")
                if parts:
                    tweet_username = parts[-1]
            
            # =================================================================
            # LOG PREFIX based on source and interface
            # =================================================================
            if source == "flashblock":
                source_tag = "[FLASH]"
                self.stats["flashblock_tokens"] += 1
            else:
                source_tag = "[CONFIRMED]"
                self.stats["confirmed_tokens"] += 1
            
            if interface == "Bankr":
                type_tag = "BANKR"
            elif interface == "Clawnch":
                type_tag = "CLAWNCH"
            else:
                type_tag = "TOKEN"
            
            log_prefix = f"{source_tag}[{type_tag}]"
            
            if tweet_id:
                self.logger.info(
                    f"{log_prefix} {decoded['token_name']} (${token_symbol}) "
                    f"block={block_number} tweet={tweet_id} @{tweet_username}"
                )
            else:
                self.logger.info(
                    f"{log_prefix} {decoded['token_name']} (${token_symbol}) "
                    f"block={block_number} platform={platform}"
                )
            
            # Debug: Log messageId for CLAWNCH tokens (helps debug matching)
            if interface == "Clawnch":
                raw_message_id = context_parsed.get("messageId", "")
                self.logger.info(
                    f"[CLAWNCH DEBUG] {decoded['token_symbol']} messageId={raw_message_id} platform={platform}"
                )
            
            # Get creator from tx
            creator_wallet = await self._get_tx_sender(tx_hash)
            
            # Create base event
            event = Events.token_created(
                token_address=decoded["token_address"],
                token_name=decoded["token_name"],
                token_symbol=decoded["token_symbol"],
                creator_wallet=creator_wallet,
                pool_id=decoded["pool_id"],
                platform=platform,
                tweet_id=tweet_id,
                tweet_url=tweet_url,
                block_number=block_number,
                tx_hash=tx_hash,
            )
            
            # === ADD ALL DECODED FIELDS FOR SIGNALS ===
            event["token_admin"] = decoded.get("token_admin", "")
            event["msg_sender"] = decoded.get("msg_sender", "")
            event["token_image"] = decoded.get("token_image", "")
            event["token_metadata"] = decoded.get("token_metadata", "")
            event["token_context"] = decoded.get("token_context", "")
            event["starting_tick"] = decoded.get("starting_tick", 0)
            event["pool_hook"] = decoded.get("pool_hook", "")
            event["paired_token"] = decoded.get("paired_token", "")
            event["locker"] = decoded.get("locker", "")
            event["mev_module"] = decoded.get("mev_module", "")
            event["extensions_supply"] = decoded.get("extensions_supply", 0)
            event["extensions"] = decoded.get("extensions", [])
            event["context_parsed"] = context_parsed
            event["metadata_parsed"] = decoded.get("metadata_parsed", {})
            event["interface"] = interface
            event["tweet_username"] = tweet_username
            
            # Legacy compatibility
            event["image"] = decoded.get("token_image", "")
            event["context"] = decoded.get("token_context", "")
            
            # =================================================================
            # FLASHBLOCK METADATA: Let downstream know the detection source
            # =================================================================
            event["source"] = source
            event["is_preconfirmed"] = (source == "flashblock")
            
            # =================================================================
            # SCAM FILTER: Block fake Bankr tokens from unauthorized deployers
            # =================================================================
            if interface == "Bankr":
                msg_sender = decoded.get("msg_sender", "").lower()
                if msg_sender not in BANKR_DEPLOYER_WHITELIST:
                    self.logger.warning(
                        f"[SCAM] Fake Bankr token blocked: {decoded['token_name']} "
                        f"(${decoded['token_symbol']}) deployer={msg_sender[:16]}..."
                    )
                    # DO NOT PUBLISH - silently drop scam token
                    self.stats["messages_processed"] += 1
                    return
            
            # =================================================================
            # SCAM FILTER: Block fake Clawnch tokens from unauthorized deployers
            # =================================================================
            if interface == "Clawnch":
                msg_sender = decoded.get("msg_sender", "").lower()
                if msg_sender not in CLAWNCH_DEPLOYER_WHITELIST:
                    self.logger.warning(
                        f"[SCAM] Fake Clawnch token blocked: {decoded['token_name']} "
                        f"(${decoded['token_symbol']}) deployer={msg_sender[:16]}..."
                    )
                    # DO NOT PUBLISH - silently drop scam token
                    self.stats["messages_processed"] += 1
                    return
            
            # Publish (only reaches here if legitimate)
            await self.publish(Channels.ONCHAIN_TOKEN, event)
            
            self.stats["tokens_detected"] += 1
            self.stats["messages_processed"] += 1
        
        except Exception as e:
            self.logger.error(f"Token event error: {e}")
            self.stats["errors"] += 1
    
    async def _handle_block(self, data: Dict[str, Any]):
        """Process new block header."""
        if data.get("method") != "eth_subscription":
            return
        
        result = data.get("params", {}).get("result", {})
        if not result:
            return
        
        try:
            block_hex = result.get("number", "0x0")
            block_number = int(block_hex, 16)
            block_hash = result.get("hash", "")
            timestamp = int(result.get("timestamp", "0x0"), 16)
            
            self.stats["blocks_received"] += 1
            self.stats["current_block"] = block_number
            
            # Log every block (Base produces ~2s blocks)
            if self.stats["blocks_received"] % 30 == 1:  # Log every ~1 minute
                self.logger.info(f"[BLOCK] #{block_number} (received {self.stats['blocks_received']} blocks)")
            
            # Create and publish event
            event = Events.new_block(
                block_number=block_number,
                block_hash=block_hash,
                timestamp=timestamp,
            )
            
            await self.publish(Channels.ONCHAIN_BLOCK, event)
        
        except Exception as e:
            self.logger.error(f"Block event error: {e}")
    
    def _cleanup_processed_tokens(self):
        """
        Remove old entries from deduplication cache to prevent memory leak.
        
        Called periodically (every 100 tokens processed).
        Removes entries older than DEDUP_CACHE_TTL seconds.
        """
        now = time.time()
        cutoff = now - DEDUP_CACHE_TTL
        
        old_count = len(self._processed_tokens)
        self._processed_tokens = {
            addr: ts for addr, ts in self._processed_tokens.items()
            if ts > cutoff
        }
        
        cleaned = old_count - len(self._processed_tokens)
        if cleaned > 0:
            self.logger.debug(
                f"[CLEANUP] Removed {cleaned} old tokens from dedup cache "
                f"(remaining: {len(self._processed_tokens)})"
            )
    
    def _decode_token_created(self, log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Decode TokenCreated event data into full structured format."""
        if not eth_abi:
            self.logger.warning("eth_abi not installed - cannot decode events")
            return None
        
        try:
            topics = log.get("topics", [])
            data_hex = log.get("data", "0x")
            
            # Need at least 3 topics: event sig, token address, token admin
            if len(topics) < 3:
                self.logger.warning(f"Not enough topics: {len(topics)}")
                return None
            
            # Indexed params from topics
            token_address = "0x" + topics[1][-40:]
            token_admin = "0x" + topics[2][-40:]
            
            # Decode data portion
            if data_hex.startswith("0x"):
                data_hex = data_hex[2:]
            
            data_bytes = bytes.fromhex(data_hex)
            decoded = eth_abi.decode(TOKEN_CREATED_ABI, data_bytes)
            
            # decoded[0]  = msgSender (address)
            # decoded[1]  = tokenImage (string)
            # decoded[2]  = tokenName (string)
            # decoded[3]  = tokenSymbol (string)
            # decoded[4]  = tokenMetadata (string - JSON)
            # decoded[5]  = tokenContext (string - JSON)
            # decoded[6]  = startingTick (int24)
            # decoded[7]  = poolHook (address)
            # decoded[8]  = poolId (bytes32)
            # decoded[9]  = pairedToken (address)
            # decoded[10] = locker (address)
            # decoded[11] = mevModule (address)
            # decoded[12] = extensionsSupply (uint256)
            # decoded[13] = extensions (address[])
            
            # Parse context JSON
            context_parsed = {}
            try:
                context_parsed = json.loads(decoded[5]) if decoded[5] else {}
            except:
                pass
            
            # Parse metadata JSON
            metadata_parsed = {}
            try:
                metadata_parsed = json.loads(decoded[4]) if decoded[4] else {}
            except:
                pass
            
            # Get block number from log
            block_hex = log.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if isinstance(block_hex, str) else block_hex
            
            # Get tx hash
            tx_hash = log.get("transactionHash", "")
            if tx_hash.startswith("0x"):
                tx_hash = tx_hash[2:]
            
            return {
                # === FULL DATA MATCHING bankr_deployments.json ===
                "token_address": token_address.lower(),
                "token_admin": token_admin.lower(),
                "msg_sender": decoded[0].lower() if decoded[0] else "",
                "token_image": decoded[1],
                "token_name": decoded[2],
                "token_symbol": decoded[3],
                "token_metadata": decoded[4],
                "token_context": decoded[5],
                "starting_tick": decoded[6],
                "pool_hook": decoded[7].lower() if decoded[7] else "",
                "pool_id": decoded[8].hex(),
                "paired_token": decoded[9].lower() if decoded[9] else "",
                "locker": decoded[10].lower() if decoded[10] else "",
                "mev_module": decoded[11].lower() if decoded[11] else "",
                "extensions_supply": decoded[12],
                "extensions": [e.lower() for e in decoded[13]] if decoded[13] else [],
                "block_number": block_number,
                "tx_hash": tx_hash,
                "context_parsed": context_parsed,
                "metadata_parsed": metadata_parsed,
                # === LEGACY FIELDS FOR COMPATIBILITY ===
                "token": token_address,  # Legacy field
                "name": decoded[2],      # Legacy field
                "symbol": decoded[3],    # Legacy field
                "image": decoded[1],     # Legacy field
                "context": decoded[5],   # Legacy field
            }
        
        except Exception as e:
            self.logger.error(f"Decode error: {e}")
            return None
    
    async def _get_tx_sender(self, tx_hash: str) -> str:
        """Get transaction sender address."""
        if not self.http_url or not tx_hash:
            return ""
        
        try:
            async with self._session.post(self.http_url, json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getTransactionByHash",
                "params": [tx_hash]
            }) as resp:
                data = await resp.json()
                result = data.get("result", {})
                return result.get("from", "")
        except:
            return ""


# Entry point
if __name__ == "__main__":
    OnchainListenerWorker.main()
