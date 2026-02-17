"""On-Chain Listener Worker.

Connects to Base L2 via WebSocket and listens for token deployment events:
  - **Bankr Launcher** ``Create`` events on ``0x660EaA...8D12`` (new, own stack)
  - **Clanker V4** ``TokenCreated`` events on ``0xE85A59...83a9`` (legacy)

Publishes decoded events to Redis Streams for downstream consumers
(Token Watcher, Trading Executor, Telegram Progressive).

Key improvements over v1:
  - Publishes to Redis **Streams** (at-least-once delivery) instead of Pub/Sub.
  - Saves ``last_processed_block`` to Redis for WebSocket reconnection catch-up.
  - After reconnection, does ``eth_getLogs`` from last_processed_block to latest
    to recover any events missed during downtime.
  - Dedup cache moved to Redis Set (TTL 10 min) for crash-safe dedup.
  - **Flashblocks v2**: Consumes the raw Base sequencer Flashblocks stream
    (``wss://mainnet.flashblocks.base.org/ws``) which sends Brotli-compressed
    JSON every ~200ms with transaction receipts+logs.  Token events are detected
    **~1.8 seconds faster** than waiting for confirmed blocks.

Output Streams:
  - ``stream:onchain:token`` -- TokenCreated / Create events
  - ``stream:onchain:block`` -- New block headers
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp
import brotli
import websockets
import websockets.exceptions

from workers.base import BaseWorker
from shared.pubsub import Channels, Events

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ---- BANKR LAUNCHER (new, own stack) ----
BANKR_LAUNCHER_ADDRESS = "0x660EaAeDEbc968F8F3694354FA8ec0b4c5BA8D12"
# event Create(address indexed pairedToken, address tokenAddress,
#              address lockerAddress, address lpNftAddress)
BANKR_CREATE_TOPIC = "0x68ff1cfcdcf76864161555fc0de1878d8f83ec6949bf351df74d8a4a1a2679ab"
# Bankr's own Uniswap V4 hook
BANKR_HOOK = "0x3e342a06f9592459d75721d6956b570f02ef2dc0"
# Uniswap V4 PoolManager on Base (for reading Initialize events)
UNISWAP_V4_POOL_MANAGER = "0x498581ff718922c3f8e6a244956af099b2652b2b"
# Uniswap V4 Initialize event topic
POOL_INITIALIZE_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"

# ---- CLANKER V4 (legacy) ----
CLANKER_V4_ADDRESS = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"
# TokenCreated event signature (keccak256)
TOKEN_CREATED_TOPIC = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"

# MEV Module V2 -- used for nextAuctionBlock queries
MEV_MODULE_V2 = "0xebb25bb797d82cb78e1bc70406b13233c0854413"

# Event ABI types (non-indexed params in data portion)
# Indexed: topics[1] = tokenAddress, topics[2] = tokenAdmin
TOKEN_CREATED_ABI = [
    "address",    # 0: msgSender
    "string",     # 1: tokenImage
    "string",     # 2: tokenName
    "string",     # 3: tokenSymbol
    "string",     # 4: tokenMetadata (JSON)
    "string",     # 5: tokenContext (JSON: interface, platform, messageId, id)
    "int24",      # 6: startingTick
    "address",    # 7: poolHook
    "bytes32",    # 8: poolId
    "address",    # 9: pairedToken (WETH)
    "address",    # 10: locker
    "address",    # 11: mevModule
    "uint256",    # 12: extensionsSupply
    "address[]",  # 13: extensions
]

# ERC20 name() / symbol() / tokenURI() selectors for Bankr Launcher tokens
ERC20_NAME_SELECTOR = "0x06fdde03"
ERC20_SYMBOL_SELECTOR = "0x95d89b41"
ERC20_TOKENURI_SELECTOR = "0x3c130d90"  # tokenURI() -- no args, custom function

# Filebase private IPFS RPC (authenticated -- set key via env or here)
# Generate at: Filebase Dashboard > Access Keys > IPFS RPC API Keys
FILEBASE_RPC_URL = "https://rpc.filebase.io/api/v0/cat"
FILEBASE_API_KEY = ""  # TODO: set when IPFS RPC token is generated

# Public IPFS gateways (all tried in PARALLEL; fastest wins)
IPFS_PUBLIC_GATEWAYS = [
    "https://ipfs.filebase.io/ipfs/",   # ~0.03s
    "https://ipfs.io/ipfs/",            # ~0.08s
    "https://dweb.link/ipfs/",          # ~0.18s
    "https://nftstorage.link/ipfs/",    # ~0.28s
]
# Alias for _ipfs_to_gateway (image URLs sent to frontend)
IPFS_GATEWAYS = IPFS_PUBLIC_GATEWAYS
IPFS_FETCH_TIMEOUT = 2          # seconds per gateway (first parallel pass)
IPFS_RETRY_INTERVAL = 0.25      # seconds between background retries
IPFS_RETRY_MAX_SECONDS = 300    # 5 minutes max background retry
URI_RETRY_INTERVAL = 0.25       # seconds between tokenURI() RPC retries
URI_RETRY_MAX_SECONDS = 30      # max time to wait for tokenURI() to appear

# ---- Lightspeed: Parse metadata directly from TX calldata ----
BANKR_CREATE_SELECTOR = "765e827f"  # ERC-4337 handleOps wrapping Bankr create()
BANKR_DIRECT_CREATE_SELECTOR = "882db707"  # Direct create() on Bankr Launcher
# Byte offsets in args (calldata[4:]) for ABI-encoded strings
# (32-byte length prefix at offset, then UTF-8 content at offset+32)
TX_NAME_OFFSET = 1340               # ABI string header for token name
TX_SYMBOL_OFFSET = 1404             # ABI string header for token symbol
TX_URI_OFFSET = 1532                # ABI string header for token URI
PRECONF_RPC_TIMEOUT = 2             # seconds for preconf endpoint calls

# ---- Deployer whitelists (for legacy Clanker events only) ----
BANKR_DEPLOYER_WHITELIST = frozenset({
    "0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9",  # Original deployer
    "0xd9acd656a5f1b519c9e76a2a6092265a74186e58",  # Current active deployer
})

CLAWNCH_DEPLOYER_WHITELIST = frozenset({
    "0xfc426dfeae55dae2f936a592450c9ecea87a5736",
})

# Legitimate Clanker deployer wallets (verified via forensic TX analysis)
# This wallet deploys ALL real Clanker tokens (both Twitter and Farcaster)
CLANKER_DEPLOYER_WHITELIST = frozenset({
    "0xd9acd656a5f1b519c9e76a2a6092265a74186e58",  # Official Clanker deployer
})

# Dedup cache TTL (seconds) -- how long to remember processed tokens in Redis
DEDUP_TTL = 600  # 10 minutes

# Redis key for last processed block
LAST_BLOCK_KEY = "onchain:last_processed_block"


class OnchainListenerWorker(BaseWorker):
    """Listens for token deployment events on Base via WebSocket.

    Dual subscription:
      - Bankr Launcher ``Create`` events (new tokens)
      - Clanker V4 ``TokenCreated`` events (legacy tokens)

    Publishes decoded events to Redis Streams and tracks block progress
    for reconnection catch-up.
    """

    WORKER_NAME = "onchain-listener"

    def __init__(self) -> None:
        super().__init__()
        self._session: aiohttp.ClientSession | None = None
        self._ipfs_session: aiohttp.ClientSession | None = None
        self._eth_abi = None  # Lazy import
        self._fb_calldata_cache: dict[str, bytes] = {}
        self._fb_cache_block: int = 0

    async def setup(self) -> None:
        await super().setup()
        self._session = aiohttp.ClientSession()

        # Shared IPFS session with connection pooling + DNS cache
        ipfs_connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            ttl_dns_cache=300,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        self._ipfs_session = aiohttp.ClientSession(
            connector=ipfs_connector,
            timeout=aiohttp.ClientTimeout(total=IPFS_FETCH_TIMEOUT),
        )

        # Lazy import eth_abi (optional C dependency)
        try:
            import eth_abi
            self._eth_abi = eth_abi
            self.logger.info("eth_abi loaded for event decoding")
        except ImportError:
            self.logger.error("eth_abi not installed -- cannot decode TokenCreated events")
            raise

        self.stats.update({
            "tokens_detected": 0,
            "blocks_received": 0,
            "current_block": 0,
            "reconnects": 0,
            "flashblock_tokens": 0,
            "confirmed_tokens": 0,
            "duplicate_tokens": 0,
            "catchup_tokens": 0,
            "bankr_launcher_tokens": 0,
            "clanker_legacy_tokens": 0,
            "flashblock_msgs_processed": 0,
            "flashblock_cache_hits": 0,
            "flashblock_stream_connected": False,
        })

    async def teardown(self) -> None:
        if self._ipfs_session:
            await self._ipfs_session.close()
        if self._session:
            await self._session.close()
        await super().teardown()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect to WebSocket and listen. Reconnect on failure.

        Launches two parallel streams:
          1. **Flashblocks raw stream** (``wss://mainnet.flashblocks.base.org/ws``)
             — detects token events ~200ms after inclusion (preconfirmation).
          2. **Standard RPC WebSocket** (Alchemy/Chainstack/Ankr)
             — confirmed ``logs`` + ``newHeads`` subscriptions as fallback.

        The dedup mechanism ensures only the first detection (flashblock) is
        processed; the later confirmed event is silently dropped.
        """
        wss_url = self.settings.primary_wss
        if not wss_url:
            self.logger.error("No WebSocket RPC configured -- cannot start listener")
            return

        provider = "alchemy" if self.settings.alchemy_wss else (
            "chainstack" if self.settings.chainstack_wss else "ankr"
        )
        self.logger.info("Using %s WebSocket endpoint for confirmed blocks", provider)

        # --- Launch Flashblocks raw stream in parallel ---
        flashblocks_enabled = self.settings.flashblocks_enabled
        if flashblocks_enabled and self.settings.flashblocks_wss:
            self.logger.info(
                "Launching Flashblocks raw stream (%s)",
                self.settings.flashblocks_wss[:60],
            )
            self.create_background_task(self._flashblocks_stream_loop())
        else:
            self.logger.warning(
                "Flashblocks raw stream DISABLED (flashblocks_enabled=%s, url=%s)",
                flashblocks_enabled, bool(self.settings.flashblocks_wss),
            )

        while self._running:
            try:
                async with self._session.ws_connect(
                    wss_url, heartbeat=30, receive_timeout=60
                ) as ws:
                    self.logger.info("WebSocket connected to %s", wss_url[:60])

                    # --- Catch-up from last_processed_block ---
                    await self._catchup(wss_url)

                    # --- Subscribe to Bankr Launcher Create events ---
                    bankr_sub_id = await self._ws_subscribe(ws, "logs", {
                        "address": BANKR_LAUNCHER_ADDRESS,
                        "topics": [BANKR_CREATE_TOPIC],
                    }, sub_id=1)
                    self.logger.info("Bankr Launcher subscription: %s", bankr_sub_id)

                    # --- Subscribe to legacy Clanker TokenCreated logs ---
                    clanker_sub_id = await self._ws_subscribe(ws, "logs", {
                        "address": CLANKER_V4_ADDRESS,
                        "topics": [TOKEN_CREATED_TOPIC],
                    }, sub_id=2)
                    self.logger.info("Clanker legacy subscription: %s", clanker_sub_id)

                    # --- Subscribe to newHeads (blocks) ---
                    block_sub_id = await self._ws_subscribe(
                        ws, "newHeads", None, sub_id=3
                    )
                    self.logger.info("Block subscription: %s", block_sub_id)

                    # Note: pendingLogs via eth_subscribe is NOT supported by
                    # any third-party provider (Alchemy, Chainstack, QuickNode).
                    # Flashblocks detection is handled by the parallel raw stream
                    # consumer (_flashblocks_stream_loop) which connects directly
                    # to wss://mainnet.flashblocks.base.org/ws.

                    self.logger.info("Listening for confirmed events (Bankr Launcher + Clanker legacy)...")

                    # --- Message loop ---
                    async for msg in ws:
                        if not self._running:
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("method") != "eth_subscription":
                                continue

                            sub_id = data.get("params", {}).get("subscription")

                            if sub_id == bankr_sub_id:
                                await self._handle_bankr_create(data, source="confirmed")
                            elif sub_id == clanker_sub_id:
                                await self._handle_token_event(data, source="confirmed")
                            elif sub_id == block_sub_id:
                                await self._handle_block(data)

                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            self.logger.warning("WebSocket closed/error: %s", msg.type)
                            break

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.logger.warning("WebSocket receive timeout, reconnecting...")
                self.stats["reconnects"] += 1
                await asyncio.sleep(2)
            except Exception:
                self.logger.exception("WebSocket error")
                self.stats["errors"] += 1
                self.stats["reconnects"] += 1
                if self._running:
                    self.logger.info("Reconnecting in 5s...")
                    await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # WebSocket helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _ws_subscribe(
        ws: aiohttp.ClientWebSocketResponse,
        method: str,
        params: dict | None,
        sub_id: int = 1,
    ) -> str | None:
        """Send an eth_subscribe and return the subscription ID."""
        payload: list[Any] = [method]
        if params:
            payload.append(params)
        await ws.send_json({
            "jsonrpc": "2.0", "id": sub_id,
            "method": "eth_subscribe", "params": payload,
        })
        resp = await ws.receive_json()
        return resp.get("result")

    # ------------------------------------------------------------------
    # Token event handling
    # ------------------------------------------------------------------

    async def _handle_token_event(self, data: dict[str, Any], source: str) -> None:
        """Decode a TokenCreated log and publish to the stream."""
        result = data.get("params", {}).get("result", {})
        if not result:
            return

        try:
            tx_hash = result.get("transactionHash", "")
            block_hex = result.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if block_hex else 0

            decoded = self._decode_token_created(result)
            if not decoded:
                return

            token_address = decoded["token_address"]
            token_symbol = decoded["token_symbol"]

            # --- Dedup (Redis Set with TTL) ---
            dedup_key = f"onchain:dedup:{token_address}"
            already = await self.redis.setnx(dedup_key, source)
            if not already:
                # Key already existed = duplicate
                self.stats["duplicate_tokens"] += 1
                self.logger.debug(
                    "Dedup: $%s already processed, %s ignored", token_symbol, source
                )
                return
            await self.redis.expire(dedup_key, DEDUP_TTL)

            # --- Context parsing ---
            context_parsed = decoded.get("context_parsed", {})
            interface = context_parsed.get("interface", "")
            platform = context_parsed.get("platform", "unknown")

            # Extract tweet_id from messageId (Bankr URLs contain /status/ID)
            tweet_url = context_parsed.get("messageId", "")
            tweet_id = ""
            tweet_username = ""
            if "/status/" in tweet_url:
                tweet_id = tweet_url.split("/status/")[-1].split("?")[0]
                parts = tweet_url.split("/status/")[0].split("/")
                if parts:
                    tweet_username = parts[-1]
            elif tweet_url.isdigit() and len(tweet_url) > 10:
                # Plain numeric tweet ID (bankr legacy format)
                tweet_id = tweet_url

            # --- Scam filter ---
            msg_sender = decoded.get("msg_sender", "").lower()
            if interface == "Bankr" and msg_sender not in BANKR_DEPLOYER_WHITELIST:
                self.logger.warning(
                    "Scam Bankr token blocked: %s ($%s) deployer=%s",
                    decoded["token_name"], token_symbol, msg_sender[:16],
                )
                return
            if interface == "Clawnch" and msg_sender not in CLAWNCH_DEPLOYER_WHITELIST:
                self.logger.warning(
                    "Scam Clawnch token blocked: %s ($%s) deployer=%s",
                    decoded["token_name"], token_symbol, msg_sender[:16],
                )
                return
            if interface.lower() in ("clanker", "clanker.world") and msg_sender not in CLANKER_DEPLOYER_WHITELIST:
                self.logger.warning(
                    "Scam Clanker token blocked: %s ($%s) deployer=%s",
                    decoded["token_name"], token_symbol, msg_sender[:16],
                )
                return

            # --- Build event ---
            event = Events.token_created(
                token_address=token_address,
                token_name=decoded["token_name"],
                token_symbol=token_symbol,
                creator_wallet=msg_sender,
                pool_id=decoded["pool_id"],
                platform=platform,
                tweet_id=tweet_id,
                tweet_url=tweet_url,
                block_number=block_number,
                tx_hash=tx_hash,
            )

            # Add full decoded fields for downstream consumers
            event["token_admin"] = decoded.get("token_admin", "")
            event["msg_sender"] = msg_sender
            event["context_parsed"] = context_parsed
            event["metadata_parsed"] = decoded.get("metadata_parsed", {})
            event["interface"] = interface
            event["tweet_username"] = tweet_username
            event["mev_module"] = decoded.get("mev_module", "")
            event["pool_hook"] = decoded.get("pool_hook", "")
            event["paired_token"] = decoded.get("paired_token", "")
            event["locker"] = decoded.get("locker", "")
            event["starting_tick"] = decoded.get("starting_tick", 0)
            event["extensions_supply"] = decoded.get("extensions_supply", 0)
            event["extensions"] = decoded.get("extensions", [])
            event["image"] = decoded.get("token_image", "")
            event["source"] = source
            event["is_preconfirmed"] = (source == "flashblock")

            # --- Publish to Redis Stream ---
            await self.redis.xadd(Channels.ONCHAIN_TOKEN, event)

            # Update stats
            if source == "flashblock":
                self.stats["flashblock_tokens"] += 1
                tag = "FLASH"
            else:
                self.stats["confirmed_tokens"] += 1
                tag = "CONFIRMED"

            self.stats["tokens_detected"] += 1
            self.stats["messages_processed"] += 1

            self.logger.info(
                "[%s][%s] %s ($%s) block=%d tweet=%s",
                tag,
                "BANKR" if interface == "Bankr" else "CLAWNCH" if interface == "Clawnch" else "TOKEN",
                decoded["token_name"],
                token_symbol,
                block_number,
                tweet_id or context_parsed.get("messageId", "none"),
            )

        except Exception:
            self.logger.exception("Token event processing error")
            self.stats["errors"] += 1

    # ------------------------------------------------------------------
    # Bankr Launcher Create event handling (new stack)
    # ------------------------------------------------------------------

    async def _handle_bankr_create(
        self,
        data: dict[str, Any],
        source: str,
        tx_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Decode a Bankr Launcher ``Create`` event and publish INSTANTLY.

        The Create event is emitted by 0x660EaA...8D12 and only contains:
          - topic[1] = pairedToken (always WETH)
          - data[0:32] = tokenAddress
          - data[32:64] = lockerAddress
          - data[64:96] = lpNftAddress

        **Lightspeed path** (flashblock only): ``tx_metadata`` provides
        pre-parsed name, symbol, URI, pool_id, pool_fee extracted directly
        from the flashblock TX calldata + receipt logs.  When present, the
        event is published with REAL data and background enrichment skips
        all RPC calls, going straight to IPFS fetch.

        Strategy (zero-blocking):
          1. Decode raw event data + dedup  → ~0 ms
          2. Publish event to Redis Stream  → ~0 ms
          3. Fire-and-forget background task for IPFS only (if lightspeed)
        """
        result = data.get("params", {}).get("result", {})
        if not result:
            return

        try:
            tx_hash = result.get("transactionHash", "")
            block_hex = result.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if block_hex else 0

            # ---- Decode Create event (instant) ----
            log_data = result.get("data", "0x")
            if log_data.startswith("0x"):
                log_data = log_data[2:]
            data_bytes = bytes.fromhex(log_data)

            if len(data_bytes) < 96:
                self.logger.warning("[BANKR-CREATE] Data too short: %d bytes", len(data_bytes))
                return

            token_address = ("0x" + data_bytes[12:32].hex()).lower()
            locker_address = ("0x" + data_bytes[44:64].hex()).lower()

            # ---- Dedup (instant) ----
            dedup_key = f"onchain:dedup:{token_address}"
            already = await self.redis.setnx(dedup_key, source)
            if not already:
                self.stats["duplicate_tokens"] += 1
                return
            await self.redis.expire(dedup_key, DEDUP_TTL)

            # ---- Extract pre-parsed metadata (lightspeed path) ----
            pre_name = (tx_metadata or {}).get("name", "")
            pre_symbol = (tx_metadata or {}).get("symbol", "")
            pre_uri = (tx_metadata or {}).get("uri", "")
            pre_pool_id = (tx_metadata or {}).get("pool_id", "")
            pre_pool_fee = int((tx_metadata or {}).get("pool_fee", 0))
            pre_pool_hook = (tx_metadata or {}).get("pool_hook", "")

            # ---- CAMADA 2: Fetch TX via preconf RPC, parse calldata ----
            if not pre_name and token_address and tx_hash:
                try:
                    tx_result = await self._rpc_get_transaction(tx_hash)
                    if tx_result and tx_result.get("input"):
                        rpc_calldata = bytes.fromhex(tx_result["input"][2:])
                        parsed = self._parse_tx_calldata(rpc_calldata)
                        if parsed.get("name"):
                            pre_name = parsed["name"]
                        if parsed.get("symbol"):
                            pre_symbol = parsed["symbol"]
                        if parsed.get("uri") and not pre_uri:
                            pre_uri = parsed["uri"]
                        if pre_name:
                            self.logger.info(
                                "[BANKR-RPC-TX] Resolved via preconf TX: %s ($%s) for %s",
                                pre_name, pre_symbol, token_address[:12],
                            )
                except Exception as exc:
                    self.logger.debug(
                        "[BANKR-RPC-TX] Failed for %s: %s", token_address[:12], exc,
                    )

            # ---- CAMADA 3: RPC name()/symbol() on the contract ----
            if not pre_name and token_address:
                try:
                    rpc_url = getattr(self.settings, "flashblocks_preconf_http", "") or self.settings.primary_http
                    rpc_name, rpc_symbol = await asyncio.wait_for(
                        self._fetch_erc20_metadata(rpc_url, token_address),
                        timeout=2.0,
                    )
                    if rpc_name and rpc_name not in ("Unknown", "???"):
                        pre_name = rpc_name
                    if rpc_symbol and rpc_symbol not in ("???",):
                        pre_symbol = rpc_symbol
                    if pre_name:
                        self.logger.info(
                            "[BANKR-RPC-FALLBACK] Resolved: %s ($%s) for %s",
                            pre_name, pre_symbol, token_address[:12],
                        )
                except asyncio.TimeoutError:
                    self.logger.debug(
                        "[BANKR-RPC-FALLBACK] Timeout for %s", token_address[:12],
                    )
                except Exception as exc:
                    self.logger.debug(
                        "[BANKR-RPC-FALLBACK] Failed for %s: %s",
                        token_address[:12], exc,
                    )

            # ---- Build event and publish INSTANTLY ----
            event = Events.token_created(
                token_address=token_address,
                token_name=pre_name,        # REAL name from TX calldata
                token_symbol=pre_symbol,    # REAL symbol from TX calldata
                creator_wallet="",
                pool_id=pre_pool_id,        # REAL pool_id from receipt
                platform="Bankr",
                tweet_id="",
                tweet_url="",
                block_number=block_number,
                tx_hash=tx_hash,
            )
            event["token_admin"] = ""
            event["msg_sender"] = BANKR_LAUNCHER_ADDRESS.lower()
            event["context_parsed"] = {"interface": "Bankr"}
            event["metadata_parsed"] = {"description": "Deployed using @bankrbot on X"}
            event["interface"] = "Bankr"
            event["tweet_username"] = ""
            event["mev_module"] = ""
            pre_pool_hook = (tx_metadata or {}).get("pool_hook", "")
            event["pool_hook"] = pre_pool_hook.lower() if pre_pool_hook else BANKR_HOOK.lower()
            event["paired_token"] = "0x4200000000000000000000000000000000000006"
            event["locker"] = locker_address
            event["starting_tick"] = 0
            event["extensions_supply"] = 0
            event["extensions"] = []
            event["source"] = source
            event["is_preconfirmed"] = (source == "flashblock")
            event["launcher"] = "bankr"
            event["token_image"] = ""
            event["token_uri"] = pre_uri
            event["pool_fee"] = pre_pool_fee

            await self.redis.xadd(Channels.ONCHAIN_TOKEN, event)

            self.logger.info(
                "[%s][BANKR-INSTANT] %s ($%s) block=%d addr=%s uri=%s",
                "FLASH" if source == "flashblock" else "CONFIRMED",
                pre_name or token_address[:12],
                pre_symbol or "?",
                block_number, token_address[:12],
                "yes" if pre_uri else "no",
            )

            # ---- Fire-and-forget: background enrichment ----
            # If lightspeed data is available, enrichment skips RPC
            # and goes directly to IPFS fetch.
            self.create_background_task(
                self._enrich_bankr_token(
                    token_address, locker_address, tx_hash,
                    block_number, source,
                    pre_name=pre_name,
                    pre_symbol=pre_symbol,
                    pre_uri=pre_uri,
                    pre_pool_id=pre_pool_id,
                    pre_pool_fee=pre_pool_fee,
                    pre_pool_hook=pre_pool_hook,
                )
            )

            # Update stats
            if source == "flashblock":
                self.stats["flashblock_tokens"] += 1
            else:
                self.stats["confirmed_tokens"] += 1
            self.stats["tokens_detected"] += 1
            self.stats["bankr_launcher_tokens"] += 1
            self.stats["messages_processed"] += 1

        except Exception:
            self.logger.exception("[BANKR-CREATE] Processing error")
            self.stats["errors"] += 1

    # ------------------------------------------------------------------
    # Background metadata enrichment (RPC + IPFS, non-blocking)
    # ------------------------------------------------------------------

    async def _enrich_bankr_token(
        self,
        token_address: str,
        locker_address: str,
        tx_hash: str,
        block_number: int,
        source: str,
        *,
        pre_name: str = "",
        pre_symbol: str = "",
        pre_uri: str = "",
        pre_pool_id: str = "",
        pre_pool_fee: int = 0,
        pre_pool_hook: str = "",
    ) -> None:
        """Fetch metadata and publish updates.

        **Lightspeed path** (when pre-parsed data is available):
          - Skips ALL RPC calls (name, symbol, tokenURI, getReceipt).
          - Goes directly to IPFS fetch with the pre-parsed URI.
          - Publishes ONE complete ``IPFS_METADATA_UPDATE`` with real data.

        **Legacy path** (confirmed blocks / no pre-parsed data):
          - Falls back to parallel RPC calls, then IPFS.
          - Uses preconf RPC as first choice, regular RPC as fallback.
        """
        try:
            rpc_url = self.settings.primary_http
            preconf_url = getattr(self.settings, "flashblocks_preconf_http", "") or ""

            # ---- Resolve metadata: lightspeed or RPC fallback ----
            token_name = pre_name
            token_symbol = pre_symbol
            token_uri = pre_uri
            pool_id = pre_pool_id
            pool_fee = pre_pool_fee
            pool_hook = pre_pool_hook

            if token_uri:
                # LIGHTSPEED: URI already known from TX calldata.
                # Still need pool_id if not parsed from receipt.
                if not pool_id:
                    pool_id, pool_fee, pool_hook_rpc = await self._fetch_pool_id_from_receipt(
                        rpc_url, tx_hash
                    )
                    if pool_hook_rpc and not pool_hook:
                        pool_hook = pool_hook_rpc
                if not token_name:
                    token_name, token_symbol = await self._fetch_erc20_metadata(
                        rpc_url, token_address
                    )

                self.logger.info(
                    "[LIGHTSPEED-ENRICH] %s ($%s) addr=%s → IPFS direct",
                    token_name, token_symbol, token_address[:12],
                )
            else:
                # NO URI from TX calldata — try preconf RPC first, then regular
                effective_rpc = preconf_url if preconf_url else rpc_url

                meta_task = asyncio.create_task(
                    self._fetch_erc20_metadata(effective_rpc, token_address)
                )
                pool_task = asyncio.create_task(
                    self._fetch_pool_id_from_receipt(rpc_url, tx_hash)
                ) if not pool_id else None
                uri_task = asyncio.create_task(
                    self._fetch_token_uri(effective_rpc, token_address)
                )

                tasks_to_gather: list = [meta_task, uri_task]
                results = await asyncio.gather(*tasks_to_gather)
                (token_name, token_symbol) = results[0]
                token_uri = results[1]

                if pool_task:
                    pool_id, pool_fee, pool_hook_rpc = await pool_task
                    if pool_hook_rpc and not pool_hook:
                        pool_hook = pool_hook_rpc

                # If preconf RPC returned empty URI, try regular RPC once
                if not token_uri and preconf_url and preconf_url != rpc_url:
                    token_uri = await self._fetch_token_uri(rpc_url, token_address)

                self.logger.info(
                    "[BANKR-ENRICH] RPC done: %s ($%s) addr=%s pool=%s fee=%d uri=%s",
                    token_name, token_symbol,
                    token_address[:12], pool_id[:16] if pool_id else "none",
                    pool_fee, "yes" if token_uri else "no",
                )

            # ---- Retry name()/symbol() if still Unknown (contract not indexed yet) ----
            if (not token_name or token_name == "Unknown") and token_address:
                retry_rpc = preconf_url or rpc_url
                for attempt in range(1, 11):  # 10 retries × 0.5s = 5s max
                    await asyncio.sleep(0.5)
                    try:
                        rn, rs = await asyncio.wait_for(
                            self._fetch_erc20_metadata(retry_rpc, token_address),
                            timeout=2.0,
                        )
                        if rn and rn not in ("Unknown", "???"):
                            token_name = rn
                            if rs and rs not in ("???",):
                                token_symbol = rs
                            self.logger.info(
                                "[BANKR-RETRY] Resolved after %d×0.5s: %s ($%s) for %s",
                                attempt, token_name, token_symbol, token_address[:12],
                            )
                            break
                    except Exception:
                        pass

            # ---- Publish RPC metadata ONLY if we have real data ----
            # Skip publishing Unknown/$??? to avoid writing sentinel values
            if token_name and token_name != "Unknown":
                rpc_update: dict[str, Any] = {
                    "token_address": token_address,
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "pool_id": pool_id,
                    "pool_fee": pool_fee,
                    "block_number": block_number,
                }
                if pool_hook:
                    rpc_update["pool_hook"] = pool_hook
                await self.redis.publish(Channels.IPFS_METADATA_UPDATE, rpc_update)

            # ---- IPFS metadata fetch ----
            ipfs_meta: dict[str, Any] = {}
            tweet_id = ""
            tweet_url = ""
            tweet_username = ""
            token_image = ""
            ipfs_success = False

            if token_uri:
                try:
                    ipfs_meta = await self._fetch_ipfs_metadata_parallel(token_uri)
                    if ipfs_meta:
                        ipfs_success = True
                        tweet_id, tweet_url, tweet_username, token_image = (
                            self._extract_ipfs_fields(ipfs_meta)
                        )
                except Exception:
                    pass

            if ipfs_success:
                ipfs_update: dict[str, Any] = {
                    "token_address": token_address,
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "tweet_id": tweet_id,
                    "tweet_url": tweet_url,
                    "tweet_username": tweet_username,
                    "token_image": token_image,
                    "block_number": block_number,
                }
                await self.redis.publish(Channels.IPFS_METADATA_UPDATE, ipfs_update)

            self.logger.info(
                "[BANKR-ENRICH] %s ($%s) addr=%s tweet=%s img=%s",
                token_name, token_symbol, token_address[:12],
                tweet_id[:12] if tweet_id else "none",
                "yes" if token_image else "no",
            )

            # ---- Background retry if needed ----
            if token_uri and not ipfs_success:
                # URI exists but IPFS fetch failed — retry IPFS only
                await self._ipfs_background_retry(
                    token_address, token_uri, token_name, token_symbol,
                )
            elif not token_uri:
                # URI not set yet — retry tokenURI() RPC
                await self._token_uri_retry(
                    rpc_url, token_address, token_name, token_symbol,
                )

        except Exception:
            self.logger.exception("[BANKR-ENRICH] Error enriching %s", token_address[:12])

    async def _fetch_erc20_metadata(
        self, rpc_url: str, token_address: str
    ) -> tuple[str, str]:
        """Fetch name() and symbol() from an ERC20 token contract."""
        name = "Unknown"
        symbol = "???"
        if not rpc_url:
            return name, symbol

        try:
            # name()
            name_result = await self._rpc_call(rpc_url, "eth_call", [
                {"to": token_address, "data": ERC20_NAME_SELECTOR}, "latest"
            ])
            if name_result and len(name_result) > 2:
                name = self._decode_string_return(name_result) or name

            # symbol()
            symbol_result = await self._rpc_call(rpc_url, "eth_call", [
                {"to": token_address, "data": ERC20_SYMBOL_SELECTOR}, "latest"
            ])
            if symbol_result and len(symbol_result) > 2:
                symbol = self._decode_string_return(symbol_result) or symbol

        except Exception:
            self.logger.warning("[BANKR-CREATE] Failed to fetch ERC20 metadata for %s", token_address[:12])

        return name, symbol

    @staticmethod
    def _decode_string_return(hex_data: str) -> str | None:
        """Decode an ABI-encoded string return value."""
        try:
            if hex_data.startswith("0x"):
                hex_data = hex_data[2:]
            data = bytes.fromhex(hex_data)
            if len(data) < 64:
                return None
            # Standard ABI: offset at [0:32], length at [offset:offset+32], then string
            offset = int.from_bytes(data[0:32], "big")
            length = int.from_bytes(data[offset:offset + 32], "big")
            return data[offset + 32:offset + 32 + length].decode("utf-8", errors="replace")
        except Exception:
            return None

    async def _fetch_pool_id_from_receipt(
        self, rpc_url: str, tx_hash: str
    ) -> tuple[str, int, str]:
        """Fetch the Uniswap V4 pool_id, fee, AND hooks from the Initialize event in the TX receipt.

        Returns:
            (pool_id_hex, fee, hooks_address) — pool_id without 0x prefix;
            fee as int (e.g. 12000); hooks as checksummed 0x-prefixed address.
            On failure returns ("", 0, "").
        """
        if not rpc_url or not tx_hash:
            return ("", 0, "")

        try:
            receipt = await self._rpc_call(
                rpc_url, "eth_getTransactionReceipt", [tx_hash]
            )
            if not receipt or "logs" not in receipt:
                return ("", 0, "")

            for log in receipt["logs"]:
                topics = log.get("topics", [])
                log_addr = log.get("address", "").lower()
                # Initialize event from Uniswap V4 PoolManager
                if (
                    topics
                    and topics[0] == POOL_INITIALIZE_TOPIC
                    and log_addr == UNISWAP_V4_POOL_MANAGER.lower()
                    and len(topics) >= 2
                ):
                    pool_id = topics[1][2:]  # strip 0x prefix

                    # Initialize event data layout (hex chars):
                    #   [0:64]    fee          (uint24)
                    #   [64:128]  tickSpacing  (int24)
                    #   [128:192] hooks        (address — last 40 hex chars)
                    #   [192:256] sqrtPriceX96 (uint160)
                    #   [256:320] tick         (int24)
                    fee = 0
                    hooks = ""
                    raw = log.get("data", "0x")
                    if raw.startswith("0x"):
                        raw = raw[2:]
                    if len(raw) >= 64:
                        fee = int(raw[0:64], 16)
                    if len(raw) >= 192:
                        hooks = "0x" + raw[152:192]

                    self.logger.info(
                        "[BANKR-CREATE] Initialize: pool=%s fee=%d hooks=%s",
                        pool_id[:16], fee, hooks[:16] if hooks else "none",
                    )
                    return (pool_id, fee, hooks)

            self.logger.warning("[BANKR-CREATE] No Initialize event in TX %s", tx_hash[:16])
            return ("", 0, "")

        except Exception:
            self.logger.warning("[BANKR-CREATE] Failed to fetch receipt for %s", tx_hash[:16])
            return ("", 0, "")

    # ------------------------------------------------------------------
    # IPFS metadata fetching (Bankr Launcher tokens)
    # ------------------------------------------------------------------

    async def _fetch_token_uri(
        self, rpc_url: str, token_address: str
    ) -> str:
        """Call tokenURI() on a Bankr Launcher token contract.

        Returns the raw URI string (typically ``ipfs://CID`` or an
        ``https://`` URL), or ``""`` on failure.
        """
        if not rpc_url:
            return ""
        try:
            result = await self._rpc_call(rpc_url, "eth_call", [
                {"to": token_address, "data": ERC20_TOKENURI_SELECTOR}, "latest"
            ])
            if result and len(result) > 66:
                return self._decode_string_return(result) or ""
        except Exception:
            self.logger.debug("[BANKR-CREATE] tokenURI() call failed for %s", token_address[:12])
        return ""

    async def _fetch_ipfs_metadata_parallel(self, uri: str) -> dict[str, Any]:
        """Fetch JSON metadata from IPFS.

        Strategy:
        1. Filebase private RPC (POST /api/v0/cat, authenticated) -- fastest
        2. All public gateways in parallel as fallback

        Returns the parsed JSON dict from the fastest source, or ``{}``
        if all fail.
        """
        cid = self._extract_cid(uri)
        if not cid:
            return {}

        sess = self._ipfs_session

        # --- Helper: try Filebase private RPC ---
        async def _try_filebase() -> dict[str, Any] | None:
            try:
                headers = {"Authorization": f"Bearer {FILEBASE_API_KEY}"}
                url = f"{FILEBASE_RPC_URL}?arg={cid}"
                async with sess.post(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict):
                            return data
            except Exception:
                pass
            return None

        # --- Helper: try a public gateway ---
        async def _try_public(gw_url: str) -> dict[str, Any] | None:
            try:
                async with sess.get(gw_url) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict):
                            return data
            except Exception:
                pass
            return None

        # Launch ALL in parallel: Filebase RPC (if key set) + public gateways
        public_urls = [gw + cid for gw in IPFS_PUBLIC_GATEWAYS]
        tasks: list[asyncio.Task] = []
        if FILEBASE_API_KEY:
            tasks.append(asyncio.create_task(_try_filebase()))
        tasks += [asyncio.create_task(_try_public(u)) for u in public_urls]

        result: dict[str, Any] = {}
        for coro in asyncio.as_completed(tasks):
            r = await coro
            if r:
                result = r
                break

        # Cancel remaining tasks
        for t in tasks:
            if not t.done():
                t.cancel()

        if not result:
            self.logger.debug("[IPFS] All gateways failed for %s", uri[:40])
        return result

    # ------------------------------------------------------------------
    # tokenURI RPC retry (0.25s interval, up to 30s)
    # ------------------------------------------------------------------

    async def _token_uri_retry(
        self,
        rpc_url: str,
        token_address: str,
        token_name: str,
        token_symbol: str,
    ) -> None:
        """Retry tokenURI() RPC call every 0.25s until it returns a value.

        Bankr Launcher sets tokenURI in the same TX as the Create event,
        but the RPC node queried via ``eth_call`` with ``"latest"`` may
        lag 1-2 blocks behind the flashblock tip.  This loop polls until
        the node catches up, then fetches IPFS metadata and publishes
        the update for instant PRE-BANKR linking.
        """
        deadline = asyncio.get_event_loop().time() + URI_RETRY_MAX_SECONDS
        attempt = 0

        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(URI_RETRY_INTERVAL)
            attempt += 1
            try:
                token_uri = await self._fetch_token_uri(rpc_url, token_address)
                if not token_uri:
                    continue

                self.logger.info(
                    "[URI-RETRY] tokenURI found after %d attempts (%.1fs) for %s ($%s): %s",
                    attempt, attempt * URI_RETRY_INTERVAL,
                    token_name, token_symbol, token_uri[:60],
                )

                # Also re-fetch name/symbol if they were Unknown
                if token_name == "Unknown" or token_symbol == "???":
                    try:
                        token_name, token_symbol = await self._fetch_erc20_metadata(
                            rpc_url, token_address
                        )
                    except Exception:
                        pass

                # Fetch IPFS metadata
                ipfs_meta = await self._fetch_ipfs_metadata_parallel(token_uri)
                if ipfs_meta:
                    tweet_id, tweet_url, tweet_username, token_image = (
                        self._extract_ipfs_fields(ipfs_meta)
                    )
                    self.logger.info(
                        "[URI-RETRY] IPFS resolved: %s ($%s) tweet=%s img=%s",
                        token_name, token_symbol,
                        tweet_id[:15] if tweet_id else "none",
                        "yes" if token_image else "no",
                    )
                    await self.redis.publish(
                        Channels.IPFS_METADATA_UPDATE,
                        {
                            "token_address": token_address.lower(),
                            "token_name": token_name,
                            "token_symbol": token_symbol,
                            "tweet_id": tweet_id,
                            "tweet_url": tweet_url,
                            "tweet_username": tweet_username,
                            "token_image": token_image,
                            "token_uri": token_uri,
                            "metadata": ipfs_meta,
                        },
                    )
                    return

                # URI found but IPFS failed — hand off to IPFS retry loop
                self.logger.info(
                    "[URI-RETRY] URI found but IPFS failed, handing to IPFS retry for %s",
                    token_address[:12],
                )
                await self._ipfs_background_retry(
                    token_address, token_uri, token_name, token_symbol,
                )
                return

            except Exception:
                pass  # retry on next iteration

        self.logger.warning(
            "[URI-RETRY] Gave up after %d attempts (%.0fs) for %s ($%s)",
            attempt, URI_RETRY_MAX_SECONDS, token_name, token_symbol,
        )

    # ------------------------------------------------------------------
    # IPFS background retry (0.25s interval, up to 5 min)
    # ------------------------------------------------------------------

    async def _ipfs_background_retry(
        self,
        token_address: str,
        token_uri: str,
        token_name: str,
        token_symbol: str,
    ) -> None:
        """Retry IPFS metadata fetch every 0.5s for up to 5 minutes.

        When successful, publishes an ``ipfs:metadata:update`` Pub/Sub
        event so that ``telegram_progressive`` can update the Supabase
        record with the image and tweet_id.
        """
        deadline = asyncio.get_event_loop().time() + IPFS_RETRY_MAX_SECONDS
        attempt = 0

        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(IPFS_RETRY_INTERVAL)
            attempt += 1
            try:
                ipfs_meta = await self._fetch_ipfs_metadata_parallel(token_uri)
                if ipfs_meta:
                    tweet_id, tweet_url, tweet_username, token_image = (
                        self._extract_ipfs_fields(ipfs_meta)
                    )
                    self.logger.info(
                        "[IPFS-RETRY] Success after %d attempts for %s ($%s) tweet=%s img=%s",
                        attempt, token_name, token_symbol,
                        tweet_id[:12] if tweet_id else "none",
                        "yes" if token_image else "no",
                    )
                    # Publish update so telegram_progressive can update Supabase
                    await self.redis.publish(
                        Channels.IPFS_METADATA_UPDATE,
                        {
                            "token_address": token_address.lower(),
                            "token_name": token_name,
                            "token_symbol": token_symbol,
                            "tweet_id": tweet_id,
                            "tweet_url": tweet_url,
                            "tweet_username": tweet_username,
                            "token_image": token_image,
                            "token_uri": token_uri,
                            "metadata": ipfs_meta,
                        },
                    )
                    return
            except Exception:
                pass  # retry on next iteration

        self.logger.warning(
            "[IPFS-RETRY] Gave up after %d attempts (%.0fs) for %s ($%s)",
            attempt, IPFS_RETRY_MAX_SECONDS, token_name, token_symbol,
        )

    # ------------------------------------------------------------------
    # IPFS helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_cid(uri: str) -> str:
        """Extract the CID from an IPFS URI or gateway URL."""
        if not uri:
            return ""
        if uri.startswith("ipfs://"):
            return uri[7:]
        # Try to extract CID from gateway URLs like https://...ipfs/<CID>
        for gw in IPFS_PUBLIC_GATEWAYS:
            if uri.startswith(gw):
                return uri[len(gw):]
        # Plain HTTPS URL -- not an IPFS CID
        return ""

    @staticmethod
    def _ipfs_uri_to_urls(uri: str) -> list[str]:
        """Convert an IPFS URI or HTTPS URL to a list of gateway URLs."""
        if not uri:
            return []
        if uri.startswith("ipfs://"):
            cid = uri[7:]
            return [gw + cid for gw in IPFS_PUBLIC_GATEWAYS]
        if uri.startswith("https://") or uri.startswith("http://"):
            return [uri]
        return []

    @staticmethod
    def _extract_ipfs_fields(
        meta: dict[str, Any],
    ) -> tuple[str, str, str, str]:
        """Extract tweet_id, tweet_url, tweet_username, token_image from metadata."""
        tweet_id = ""
        tweet_url = meta.get("tweet_url", "")
        tweet_username = ""
        token_image = ""

        if tweet_url and "/status/" in tweet_url:
            tweet_id = tweet_url.split("/status/")[-1].split("?")[0]
            parts = tweet_url.split("/status/")[0].split("/")
            if parts:
                tweet_username = parts[-1]

        raw_image = meta.get("image", "")
        if raw_image:
            token_image = OnchainListenerWorker._ipfs_to_gateway(raw_image)

        return tweet_id, tweet_url, tweet_username, token_image

    @staticmethod
    def _ipfs_to_gateway(uri: str) -> str:
        """Convert an ``ipfs://CID`` URI to an HTTPS gateway URL.

        Returns the original string for non-IPFS URIs.
        """
        if uri and uri.startswith("ipfs://"):
            return IPFS_GATEWAYS[0] + uri[7:]
        return uri or ""

    # ------------------------------------------------------------------
    # Lightspeed: RLP decode + TX input parsing (zero-RPC metadata)
    # ------------------------------------------------------------------

    @staticmethod
    def _rlp_decode(data: bytes) -> list:
        """Minimal RLP decoder sufficient for EIP-1559 transaction fields.

        Returns a list of decoded items (bytes or nested lists).
        """
        def _decode(d: bytes, pos: int) -> tuple[Any, int]:
            if pos >= len(d):
                return b"", pos
            b = d[pos]
            if b < 0x80:
                return bytes([b]), pos + 1
            elif b <= 0xb7:
                length = b - 0x80
                return d[pos + 1:pos + 1 + length], pos + 1 + length
            elif b <= 0xbf:
                len_of_len = b - 0xb7
                length = int.from_bytes(d[pos + 1:pos + 1 + len_of_len], "big")
                start = pos + 1 + len_of_len
                return d[start:start + length], start + length
            elif b <= 0xf7:
                length = b - 0xc0
                items = []
                end = pos + 1 + length
                p = pos + 1
                while p < end:
                    item, p = _decode(d, p)
                    items.append(item)
                return items, end
            else:
                len_of_len = b - 0xf7
                length = int.from_bytes(d[pos + 1:pos + 1 + len_of_len], "big")
                start = pos + 1 + len_of_len
                end = start + length
                items = []
                p = start
                while p < end:
                    item, p = _decode(d, p)
                    items.append(item)
                return items, end

        result, _ = _decode(data, 0)
        return result if isinstance(result, list) else [result]

    @staticmethod
    def _decode_abi_string(calldata: bytes, byte_offset: int) -> str:
        """Decode an ABI-encoded dynamic string at a given byte offset.

        The string layout is:  [offset:offset+32] = length, then UTF-8 bytes.
        Returns empty string if the decoded value contains non-printable chars
        (indicates we're reading at a wrong offset).
        """
        try:
            if byte_offset + 32 > len(calldata):
                return ""
            length = int.from_bytes(calldata[byte_offset:byte_offset + 32], "big")
            if length == 0 or length > 1000:
                return ""
            start = byte_offset + 32
            if start + length > len(calldata):
                return ""
            raw = calldata[start:start + length]
            text = raw.decode("utf-8", errors="strict")
            # Reject strings with non-printable characters (wrong offset)
            if any(c < ' ' and c not in ('\t', '\n', '\r') for c in text):
                return ""
            return text
        except (UnicodeDecodeError, Exception):
            return ""

    @staticmethod
    def _parse_tx_calldata(calldata: bytes) -> dict[str, str]:
        """Extract token name, symbol, and URI from Bankr Launcher calldata.

        The calldata goes through ERC-4337 ``handleOps`` so the actual
        ``create(CreateParams)`` call is nested inside the UserOperation.
        The name/symbol/URI strings live inside ``tokenFactoryData`` at
        positions that shift depending on the total calldata size.

        Strategy (cascading):
          1. **IPFS-anchor heuristic**: find ``ipfs://`` in the calldata,
             then use the consistent ABI encoding pattern to locate
             name (uri_offset - 192) and symbol (uri_offset - 128).
          2. **Fixed offsets** (legacy): try the known offsets 1340/1404/1532
             that work for the most common 2784-byte calldata layout.
          3. **Fallback**: raw ``ipfs://`` substring search for URI only.

        Returns dict with keys: ``name``, ``symbol``, ``uri``.
        """
        result: dict[str, str] = {"name": "", "symbol": "", "uri": ""}

        if len(calldata) < 4:
            return result

        # ---- Attempt 1: IPFS-anchor on FULL calldata (works for ALL selectors
        #      including ERC-4337 handleOps where Bankr params are nested) ----
        ipfs_full = OnchainListenerWorker._parse_via_ipfs_anchor(calldata)
        if ipfs_full.get("name"):
            return ipfs_full

        # ---- Attempt 2: For known Bankr selectors, try with args (calldata[4:]) ----
        selector = calldata[:4].hex()
        if selector in (BANKR_CREATE_SELECTOR, BANKR_DIRECT_CREATE_SELECTOR):
            args = calldata[4:]

            # IPFS-anchor on args (standard layout)
            ipfs_result = OnchainListenerWorker._parse_via_ipfs_anchor(args)
            if ipfs_result.get("name"):
                return ipfs_result

            # Fixed offsets (works for standard 2784-byte layout)
            result["name"] = OnchainListenerWorker._decode_abi_string(args, TX_NAME_OFFSET)
            result["symbol"] = OnchainListenerWorker._decode_abi_string(args, TX_SYMBOL_OFFSET)
            result["uri"] = OnchainListenerWorker._decode_abi_string(args, TX_URI_OFFSET)

            if result["name"]:
                return result

        # ---- Attempt 3: Fallback (URI only from raw ipfs:// search) ----
        fb = OnchainListenerWorker._fallback_parse_calldata(calldata)
        if fb.get("uri"):
            result["uri"] = fb["uri"]

        return result

    @staticmethod
    def _parse_via_ipfs_anchor(args: bytes) -> dict[str, str]:
        """Locate name/symbol/URI using the ``ipfs://`` marker as an anchor.

        Scans backward from the URI length word in 32-byte steps looking
        for valid ABI-encoded strings.  The first valid string found is
        the **symbol**, the second is the **name**.

        This universal approach works for both ERC-4337 ``handleOps``
        calldata (gap of 128 bytes between symbol and URI) and direct
        ``create()`` calldata (gap of 192 bytes), without needing to
        know which layout is in use.
        """
        result: dict[str, str] = {"name": "", "symbol": "", "uri": ""}
        try:
            ipfs_pos = args.find(b"ipfs://")
            if ipfs_pos < 0:
                return result

            uri_len_offset = ipfs_pos - 32
            if uri_len_offset < 64:
                return result

            uri = OnchainListenerWorker._decode_abi_string(args, uri_len_offset)
            if not uri or not uri.startswith("ipfs://"):
                return result
            result["uri"] = uri

            # Scan backward from URI to find symbol and name
            candidates: list[str] = []
            for gap in range(32, 513, 32):
                offset = uri_len_offset - gap
                if offset < 0:
                    break
                s = OnchainListenerWorker._decode_abi_string(args, offset)
                if s and len(s) >= 2 and not s.isspace():
                    candidates.append(s)
                    if len(candidates) >= 2:
                        break

            if len(candidates) >= 2:
                result["symbol"] = candidates[0]
                result["name"] = candidates[1]
            elif len(candidates) == 1:
                result["name"] = candidates[0]

        except Exception:
            pass
        return result

    @staticmethod
    def _fallback_parse_calldata(calldata: bytes) -> dict[str, str]:
        """Search for ``ipfs://`` substring in raw calldata bytes."""
        result: dict[str, str] = {"name": "", "symbol": "", "uri": ""}
        try:
            idx = calldata.find(b"ipfs://")
            if idx >= 0:
                # Read until null byte or non-printable char
                end = idx
                while end < len(calldata) and 32 <= calldata[end] <= 126:
                    end += 1
                result["uri"] = calldata[idx:end].decode("ascii", errors="ignore")
        except Exception:
            pass
        return result

    @staticmethod
    def _parse_pool_from_receipt_logs(logs: list[dict]) -> tuple[str, int, str]:
        """Extract pool_id, pool_fee, and hooks from Initialize event in receipt logs.

        Eliminates the need for a separate ``eth_getTransactionReceipt`` RPC.

        Initialize event data layout (hex chars, not bytes):
            [0:64]    fee          (uint24, padded to 32 bytes)
            [64:128]  tickSpacing  (int24)
            [128:192] hooks        (address — last 40 hex chars)
            [192:256] sqrtPriceX96 (uint160)
            [256:320] tick         (int24)

        Returns (pool_id_hex_no_prefix, fee_int, hooks_address) or ("", 0, "").
        """
        for log in logs:
            topics = log.get("topics") or []
            log_addr = (log.get("address") or "").lower()
            if (
                topics
                and topics[0] == POOL_INITIALIZE_TOPIC
                and log_addr == UNISWAP_V4_POOL_MANAGER.lower()
                and len(topics) >= 2
            ):
                pool_id = topics[1]
                if pool_id.startswith("0x"):
                    pool_id = pool_id[2:]
                fee = 0
                hooks = ""
                raw = log.get("data", "0x")
                if isinstance(raw, str) and raw.startswith("0x"):
                    raw = raw[2:]
                if isinstance(raw, str) and len(raw) >= 64:
                    fee = int(raw[0:64], 16)
                if isinstance(raw, str) and len(raw) >= 192:
                    # hooks address is in chars [128:192], last 40 hex = 20 bytes
                    hooks = "0x" + raw[152:192]
                return (pool_id, fee, hooks)
        return ("", 0, "")

    @staticmethod
    def _keccak256(data: bytes) -> bytes:
        """Compute Keccak-256 hash (same as Ethereum uses)."""
        from hashlib import sha3_256 as _keccak  # noqa: N813
        # Python's hashlib sha3_256 IS Keccak-256 in newer versions,
        # but Ethereum uses the pre-NIST Keccak. Use pysha3 or
        # eth_hash if available, otherwise fall back.
        try:
            from eth_hash.auto import keccak  # type: ignore[import]
            return keccak(data)
        except ImportError:
            pass
        try:
            import sha3  # type: ignore[import]  # pysha3
            k = sha3.keccak_256()
            k.update(data)
            return k.digest()
        except ImportError:
            pass
        # Last resort: Python's sha3_256 (NOTE: this is NIST SHA-3, NOT
        # Ethereum Keccak-256. Will NOT produce correct TX hashes.)
        h = _keccak(data)
        return h.digest()

    def _match_flashblock_tx(
        self,
        tx_hash: str,
        raw_txs: list[str],
    ) -> bytes | None:
        """Find the raw transaction matching a given tx_hash in flashblock data.

        Returns the decoded calldata (input data) or None.
        """
        target = tx_hash.lower()
        if target.startswith("0x"):
            target = target[2:]

        for raw_tx_hex in raw_txs:
            try:
                raw_hex = raw_tx_hex
                if raw_hex.startswith("0x"):
                    raw_hex = raw_hex[2:]
                raw_bytes = bytes.fromhex(raw_hex)

                # Compute tx hash: keccak256 of the full raw bytes
                tx_h = self._keccak256(raw_bytes).hex()
                if tx_h != target:
                    continue

                # EIP-1559: first byte is 0x02, followed by RLP list
                if raw_bytes[0] == 0x02:
                    decoded = self._rlp_decode(raw_bytes[1:])
                    # EIP-1559: [chainId, nonce, maxPriorityFee, maxFee,
                    #            gasLimit, to, value, data, accessList,
                    #            v, r, s]
                    if isinstance(decoded, list) and len(decoded) >= 8:
                        calldata = decoded[7]
                        if isinstance(calldata, bytes):
                            return calldata
                # EIP-2930 (type 0x01)
                elif raw_bytes[0] == 0x01:
                    decoded = self._rlp_decode(raw_bytes[1:])
                    if isinstance(decoded, list) and len(decoded) >= 8:
                        calldata = decoded[7]
                        if isinstance(calldata, bytes):
                            return calldata
                # Legacy
                else:
                    decoded = self._rlp_decode(raw_bytes)
                    if isinstance(decoded, list) and len(decoded) >= 6:
                        calldata = decoded[5]
                        if isinstance(calldata, bytes):
                            return calldata

            except Exception:
                continue

        return None

    def _index_flashblock_txs(self, raw_txs: list[str]) -> None:
        """Pre-index raw transactions from a Flashblock diff into the cache.

        Computes keccak256 of each raw TX, decodes RLP to extract calldata,
        and caches ALL transactions so that Bankr Create events (including
        ERC-4337 wrapped ones) can be matched to their calldata.
        """
        for raw_tx_hex in raw_txs:
            try:
                raw_hex = raw_tx_hex
                if raw_hex.startswith("0x"):
                    raw_hex = raw_hex[2:]
                raw_bytes = bytes.fromhex(raw_hex)

                tx_hash = self._keccak256(raw_bytes).hex()

                # Decode calldata from RLP
                calldata: bytes | None = None
                if raw_bytes[0] == 0x02:  # EIP-1559
                    decoded = self._rlp_decode(raw_bytes[1:])
                    if isinstance(decoded, list) and len(decoded) >= 8:
                        cd = decoded[7]
                        if isinstance(cd, bytes):
                            calldata = cd
                elif raw_bytes[0] == 0x01:  # EIP-2930
                    decoded = self._rlp_decode(raw_bytes[1:])
                    if isinstance(decoded, list) and len(decoded) >= 8:
                        cd = decoded[7]
                        if isinstance(cd, bytes):
                            calldata = cd
                else:  # Legacy
                    decoded = self._rlp_decode(raw_bytes)
                    if isinstance(decoded, list) and len(decoded) >= 6:
                        cd = decoded[5]
                        if isinstance(cd, bytes):
                            calldata = cd

                if calldata and len(calldata) >= 4:
                    self._fb_calldata_cache[tx_hash] = calldata

            except Exception:
                continue

    # ------------------------------------------------------------------
    # Flashblocks raw stream (200ms preconfirmations)
    # ------------------------------------------------------------------

    async def _flashblocks_stream_loop(self) -> None:
        """Consume the raw Base sequencer Flashblocks WebSocket stream.

        The stream at ``wss://mainnet.flashblocks.base.org/ws`` sends
        Brotli-compressed JSON every ~200ms.  Each message represents a
        single flashblock (sub-block) and contains:

          - ``index``: flashblock index within the 2s block (0=system, 1-10=user)
          - ``diff.transactions``: RLP-encoded transactions
          - ``metadata.block_number``: the block being built
          - ``metadata.receipts``: tx receipts **with logs**

        We scan receipts for Bankr Create / Clanker TokenCreated events
        and feed them to the existing handlers with ``source="flashblock"``.
        The dedup mechanism ensures confirmed events are ignored later.
        """
        url = self.settings.flashblocks_wss
        if not url:
            self.logger.info("Flashblocks WSS URL not configured, skipping stream")
            return

        reconnect_delay = 1  # seconds, with exponential backoff
        max_reconnect_delay = 30

        while self._running:
            try:
                self.logger.info(
                    "[FLASHBLOCKS] Connecting to raw stream: %s", url[:60]
                )
                async with websockets.connect(
                    url,
                    max_size=10 * 1024 * 1024,  # 10MB max message
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self.logger.info("[FLASHBLOCKS] Connected! Scanning for token events every ~200ms")
                    self.stats["flashblock_stream_connected"] = True
                    reconnect_delay = 1  # reset on success

                    async for raw in ws:
                        if not self._running:
                            break

                        try:
                            await self._process_flashblock_message(raw)
                        except Exception:
                            self.logger.exception("[FLASHBLOCKS] Error processing message")
                            self.stats["errors"] += 1

            except asyncio.CancelledError:
                self.logger.info("[FLASHBLOCKS] Stream loop cancelled")
                break
            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError,
                ConnectionError,
            ) as exc:
                self.stats["flashblock_stream_connected"] = False
                self.logger.warning(
                    "[FLASHBLOCKS] Connection lost (%s), reconnecting in %ds...",
                    type(exc).__name__, reconnect_delay,
                )
                self.stats["reconnects"] += 1
                if self._running:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception:
                self.logger.exception("[FLASHBLOCKS] Unexpected error")
                self.stats["errors"] += 1
                if self._running:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    async def _process_flashblock_message(self, raw: bytes) -> None:
        """Decompress and scan a single flashblock message for token events.

        **Lightspeed path**: For Bankr Create events, also extracts:
          - pool_id + pool_fee from the Initialize event in the SAME receipt
          - token name, symbol, URI from the raw TX calldata (diff.transactions)
        This eliminates ALL background RPC calls (name, symbol, tokenURI,
        getTransactionReceipt) and delivers complete metadata instantly.
        """
        if not isinstance(raw, bytes):
            return

        # Decompress Brotli → JSON
        try:
            decompressed = brotli.decompress(raw)
            data = json.loads(decompressed)
        except (brotli.error, json.JSONDecodeError) as exc:
            self.logger.debug("[FLASHBLOCKS] Decode error: %s", exc)
            return

        block_number = data.get("metadata", {}).get("block_number", 0)
        fb_index = data.get("index", -1)
        receipts = data.get("metadata", {}).get("receipts", {})
        raw_txs = data.get("diff", {}).get("transactions", [])

        # Manage per-block calldata cache
        if block_number != self._fb_cache_block:
            self._fb_calldata_cache.clear()
            self._fb_cache_block = block_number
        if raw_txs:
            self._index_flashblock_txs(raw_txs)

        self.stats["flashblock_msgs_processed"] += 1

        # Log progress every ~100 messages (~20 seconds)
        if self.stats["flashblock_msgs_processed"] % 100 == 1:
            self.logger.info(
                "[FLASHBLOCKS] block=%d idx=%d msgs=%d tokens=%d",
                block_number, fb_index,
                self.stats["flashblock_msgs_processed"],
                self.stats["flashblock_tokens"],
            )

        if not receipts:
            return

        # Scan every receipt for target events
        for tx_hash, receipt_obj in receipts.items():
            if not isinstance(receipt_obj, dict):
                continue

            # Receipt can be wrapped in tx type key: Eip1559, Eip4844, Legacy, Eip2930, Deposit
            logs: list[dict] = []
            for _fmt_key, inner in receipt_obj.items():
                if isinstance(inner, dict) and "logs" in inner:
                    logs = inner.get("logs", [])
                    break

            has_bankr_create = False
            for log in logs:
                addr = (log.get("address") or "").lower()
                topics = log.get("topics") or []
                if not topics:
                    continue

                topic0 = topics[0]

                # --- Bankr Launcher Create event ---
                if (
                    addr == BANKR_LAUNCHER_ADDRESS.lower()
                    and topic0 == BANKR_CREATE_TOPIC
                ):
                    has_bankr_create = True

                    # ---- LIGHTSPEED: Extract metadata from flashblock ----
                    tx_metadata: dict[str, Any] = {}

                    # 1. Parse pool_id + pool_fee from Initialize event
                    #    in the SAME receipt (eliminates eth_getTransactionReceipt)
                    pool_id, pool_fee, pool_hook = self._parse_pool_from_receipt_logs(logs)
                    if pool_id:
                        tx_metadata["pool_id"] = pool_id
                        tx_metadata["pool_fee"] = pool_fee
                    if pool_hook:
                        tx_metadata["pool_hook"] = pool_hook

                    # 2. Parse name/symbol/URI from cached TX calldata
                    target_hash = tx_hash.lower().removeprefix("0x")
                    calldata = self._fb_calldata_cache.get(target_hash)
                    if calldata:
                        self.stats["flashblock_cache_hits"] += 1
                    else:
                        self.logger.warning(
                            "[LIGHTSPEED] Cache miss: tx=%s cache_size=%d block=%d",
                            target_hash[:16], len(self._fb_calldata_cache), block_number,
                        )
                    if calldata:
                        parsed = self._parse_tx_calldata(calldata)
                        if parsed.get("name"):
                            tx_metadata["name"] = parsed["name"]
                        if parsed.get("symbol"):
                            tx_metadata["symbol"] = parsed["symbol"]
                        if parsed.get("uri"):
                            tx_metadata["uri"] = parsed["uri"]

                    if tx_metadata:
                        self.logger.info(
                            "[LIGHTSPEED] TX parsed (cache): name=%s symbol=%s uri=%s pool=%s fee=%d",
                            tx_metadata.get("name", "?")[:20],
                            tx_metadata.get("symbol", "?"),
                            tx_metadata.get("uri", "?")[:40],
                            tx_metadata.get("pool_id", "?")[:16],
                            tx_metadata.get("pool_fee", 0),
                        )

                    wrapped = self._wrap_flashblock_log(
                        log, tx_hash, block_number, topics
                    )
                    await self._handle_bankr_create(
                        wrapped, source="flashblock", tx_metadata=tx_metadata or None,
                    )
                    self.logger.info(
                        "[FLASHBLOCKS] Bankr Create detected! block=%d fb_idx=%d tx=%s",
                        block_number, fb_index, tx_hash[:20],
                    )

                # --- Clanker V4 TokenCreated event ---
                elif (
                    addr == CLANKER_V4_ADDRESS.lower()
                    and topic0 == TOKEN_CREATED_TOPIC
                ):
                    wrapped = self._wrap_flashblock_log(
                        log, tx_hash, block_number, topics
                    )
                    await self._handle_token_event(wrapped, source="flashblock")
                    self.logger.info(
                        "[FLASHBLOCKS] Clanker TokenCreated detected! block=%d fb_idx=%d tx=%s",
                        block_number, fb_index, tx_hash[:20],
                    )

    @staticmethod
    def _wrap_flashblock_log(
        log: dict[str, Any],
        tx_hash: str,
        block_number: int,
        topics: list[str],
    ) -> dict[str, Any]:
        """Wrap a flashblock receipt log into the format expected by handlers.

        The handlers expect: ``data["params"]["result"]`` to contain a log
        with ``transactionHash``, ``blockNumber``, ``data``, ``topics``, etc.
        """
        return {
            "params": {
                "result": {
                    "transactionHash": tx_hash,
                    "blockNumber": hex(block_number),
                    "address": log.get("address", ""),
                    "data": log.get("data", "0x"),
                    "topics": topics,
                    "logIndex": "0x0",
                    "transactionIndex": "0x0",
                    "removed": False,
                }
            }
        }

    # ------------------------------------------------------------------
    # Block handling
    # ------------------------------------------------------------------

    async def _handle_block(self, data: dict[str, Any]) -> None:
        """Process a new block header and publish to stream."""
        result = data.get("params", {}).get("result", {})
        if not result:
            return

        try:
            block_number = int(result.get("number", "0x0"), 16)
            block_hash = result.get("hash", "")
            timestamp = int(result.get("timestamp", "0x0"), 16)

            self.stats["blocks_received"] += 1
            self.stats["current_block"] = block_number

            # Publish to stream
            event = Events.new_block(
                block_number=block_number,
                block_hash=block_hash,
                timestamp=timestamp,
            )
            await self.redis.xadd(Channels.ONCHAIN_BLOCK, event)

            # Save last processed block for reconnection catch-up
            await self.redis.set(LAST_BLOCK_KEY, str(block_number))

            # Log every ~60s (30 blocks at 2s/block)
            if self.stats["blocks_received"] % 30 == 1:
                self.logger.info(
                    "Block #%d (received %d blocks, %d tokens)",
                    block_number,
                    self.stats["blocks_received"],
                    self.stats["tokens_detected"],
                )

        except Exception:
            self.logger.exception("Block event error")

    # ------------------------------------------------------------------
    # Reconnection catch-up via eth_getLogs
    # ------------------------------------------------------------------

    async def _catchup(self, wss_url: str) -> None:
        """Replay missed events between last_processed_block and latest.

        Fetches both Bankr Launcher Create and legacy Clanker TokenCreated
        events via HTTP RPC ``eth_getLogs``.
        """
        last_block_str = await self.redis.get(LAST_BLOCK_KEY)
        if not last_block_str:
            self.logger.info("No last_processed_block saved, skipping catch-up")
            return

        last_block = int(last_block_str)

        # Get latest block via HTTP RPC
        rpc_url = self.settings.primary_http
        if not rpc_url:
            self.logger.warning("No HTTP RPC configured, skipping catch-up")
            return

        try:
            latest_block = await self._rpc_call(
                rpc_url, "eth_blockNumber", []
            )
            if not latest_block:
                return
            latest = int(latest_block, 16)

            gap = latest - last_block
            if gap <= 0:
                self.logger.info("No block gap, catch-up not needed")
                return
            if gap > 1000:
                # Too large a gap, limit to last 1000 blocks
                last_block = latest - 1000
                gap = 1000

            self.logger.info(
                "Catch-up: fetching eth_getLogs from block %d to %d (%d blocks)",
                last_block + 1, latest, gap,
            )

            # Fetch logs in chunks of 500 blocks
            chunk_size = 500
            total_found = 0
            from_block = last_block + 1

            while from_block <= latest:
                to_block = min(from_block + chunk_size - 1, latest)

                # ---- Bankr Launcher Create events ----
                bankr_logs = await self._rpc_call(rpc_url, "eth_getLogs", [{
                    "address": BANKR_LAUNCHER_ADDRESS,
                    "topics": [BANKR_CREATE_TOPIC],
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                }])
                if bankr_logs:
                    for log_entry in bankr_logs:
                        wrapped = {
                            "method": "eth_subscription",
                            "params": {"result": log_entry},
                        }
                        await self._handle_bankr_create(wrapped, source="catchup")
                        total_found += 1

                # ---- Legacy Clanker TokenCreated events ----
                clanker_logs = await self._rpc_call(rpc_url, "eth_getLogs", [{
                    "address": CLANKER_V4_ADDRESS,
                    "topics": [TOKEN_CREATED_TOPIC],
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                }])
                if clanker_logs:
                    for log_entry in clanker_logs:
                        wrapped = {
                            "method": "eth_subscription",
                            "params": {"result": log_entry},
                        }
                        await self._handle_token_event(wrapped, source="catchup")
                        total_found += 1

                from_block = to_block + 1

            self.stats["catchup_tokens"] += total_found
            self.logger.info(
                "Catch-up complete: found %d tokens in %d blocks", total_found, gap
            )

        except Exception:
            self.logger.exception("Catch-up failed")

    async def _rpc_get_transaction(self, tx_hash: str) -> dict[str, Any] | None:
        """Fetch full transaction by hash, preferring the preconf RPC."""
        url = getattr(self.settings, "flashblocks_preconf_http", "") or self.settings.primary_http
        if not url:
            return None
        return await self._rpc_call(url, "eth_getTransactionByHash", [tx_hash])

    async def _rpc_call(self, url: str, method: str, params: list) -> Any:
        """Make a JSON-RPC call via HTTP."""
        try:
            async with self._session.post(url, json={
                "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
            }) as resp:
                data = await resp.json()
                if "error" in data:
                    self.logger.error("RPC error: %s", data["error"])
                    return None
                return data.get("result")
        except Exception:
            self.logger.exception("RPC call failed: %s", method)
            return None

    # ------------------------------------------------------------------
    # Event decoding
    # ------------------------------------------------------------------

    def _decode_token_created(self, log: dict[str, Any]) -> dict[str, Any] | None:
        """Decode TokenCreated event from raw log data."""
        if not self._eth_abi:
            return None

        try:
            topics = log.get("topics", [])
            data_hex = log.get("data", "0x")

            if len(topics) < 3:
                self.logger.warning("Not enough topics: %d", len(topics))
                return None

            # Indexed params
            token_address = "0x" + topics[1][-40:]
            token_admin = "0x" + topics[2][-40:]

            # Decode data
            if data_hex.startswith("0x"):
                data_hex = data_hex[2:]

            data_bytes = bytes.fromhex(data_hex)
            decoded = self._eth_abi.decode(TOKEN_CREATED_ABI, data_bytes)

            # Parse context JSON
            context_parsed = {}
            try:
                context_parsed = json.loads(decoded[5]) if decoded[5] else {}
            except (json.JSONDecodeError, ValueError):
                pass

            # Parse metadata JSON
            metadata_parsed = {}
            try:
                metadata_parsed = json.loads(decoded[4]) if decoded[4] else {}
            except (json.JSONDecodeError, ValueError):
                pass

            block_hex = log.get("blockNumber", "0x0")
            block_number = int(block_hex, 16) if isinstance(block_hex, str) else block_hex

            return {
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
                "context_parsed": context_parsed,
                "metadata_parsed": metadata_parsed,
            }

        except Exception:
            self.logger.exception("Decode error")
            return None


# Entry point
if __name__ == "__main__":
    OnchainListenerWorker.main()
