"""Telegram Progressive Worker.

Sends progressive Telegram messages with live editing as user data is
enriched. This is the primary notification engine for the system.

Subscribes to three channels/streams:
  1. ``twitter:prebankr`` (Pub/Sub) -- PRE-BANKR detected by tweet_processor
  2. ``twitter:deployed`` (Pub/Sub) -- DEPLOYED detected by tweet_processor
  3. ``stream:onchain:token`` (Stream) -- TokenCreated from onchain_listener

For each event:
  - Send an initial message immediately (with loading placeholders)
  - Enrich users in parallel (Twitter API, AlphaGate, MOLT agent data)
  - Edit the message progressively as data arrives
  - Cache enriched data in Redis for instant display on DEPLOYED

Also handles:
  - On-chain-first flow (token on-chain before Twitter confirms)
  - Clawnch token routing (moltbook, 4claw, moltx to separate channel)
  - New interface detection
  - New platform detection
  - Admin blacklist commands (``/blacklist``, ``/listblacklist``, etc.)

Message types:
  - PRE-BANKR: Sent to prebankr channel with FROM/TO user enrichment
  - DEPLOYED BANKR: Sent to deployed channel (or updates existing on-chain msg)
  - On-chain BANKR: Sent immediately with creator wallet info
  - CLAWNCH: Sent to clawnch channel with agent/platform info
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp

from config.settings import get_settings
from shared.pubsub import Channels, ConsumerGroups, Events
from workers.base import BaseWorker
from workers.telegram.api import TelegramAPI

logger = logging.getLogger(__name__)

# ---- Bankr deployer identification ----
# Legacy Clanker V4: single wallet that deployed bankr tokens
BANKR_DEPLOYER = "0xd9acd656a5f1b519c9e76a2a6092265a74186e58"
# New Bankr Launcher: contract address (msg_sender in event)
BANKR_LAUNCHER_ADDRESS = "0x660eaaedebc968f8f3694354fa8ec0b4c5ba8d12"


class TelegramProgressiveWorker(BaseWorker):
    """Progressive Telegram notification worker with live message editing."""

    WORKER_NAME = "telegram-progressive"

    # Consume on-chain token stream
    STREAMS = [
        (Channels.ONCHAIN_TOKEN, ConsumerGroups.TELEGRAM_PROGRESSIVE),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._api = TelegramAPI()
        self._session: aiohttp.ClientSession | None = None

        # API keys
        self._twitter_api_key: str = ""
        self._alphagate_user_id: str = ""
        self._alphagate_session_id: str = ""

        # Telegram config
        self._deployed_channel: str = ""
        self._prebankr_channel: str = ""
        self._clawnch_channel: str = ""
        self._newinterface_channel: str = ""
        self._clanker_channel: str = ""
        self._preclanker_channel: str = ""
        self._admin_group: str = ""

        # Bot tokens
        self._bot_token: str = ""
        self._pool_tokens: list[str] = []
        self._clawnch_token: str = ""
        self._admin_token: str = ""

        # Token pool round-robin
        self._pool_index: int = 0

        # Pending on-chain messages (for on-chain-first flow)
        self._pending_onchain: dict[str, dict[str, Any]] = {}

        # Dedup: tweet_ids already sent as deployed (Bankr or Clanker) to avoid
        # the same token appearing in both channels when Bankr Launcher and
        # Clanker V4 emit events for the same deployment.
        self._deployed_tweet_ids: set[str] = set()

        # Seen interfaces/platforms
        self._seen_interfaces: set[str] = set()
        self._seen_platforms: set[str] = set()
        self._known_clawnch_platforms = frozenset({
            "moltbook", "4claw", "moltx", "clawstr",
        })

        # Memory cache for enriched users
        self._user_cache: dict[str, dict[str, Any]] = {}

        # Blacklist polling offset
        self._last_update_id: int | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        await super().setup()
        settings = get_settings()

        self._twitter_api_key = settings.twitter_api_key
        self._alphagate_user_id = settings.alphagate_user_id
        self._alphagate_session_id = settings.alphagate_session_id

        # Channels
        self._deployed_channel = settings.telegram_channel_id
        self._prebankr_channel = settings.telegram_prebankr_channel_id
        self._clawnch_channel = settings.telegram_clawnch_channel_id or "-1003726736391"
        self._newinterface_channel = settings.telegram_newinterface_channel_id or "-1003670396729"
        self._clanker_channel = settings.telegram_clanker_channel_id or "-1003646039549"
        self._preclanker_channel = settings.telegram_preclanker_channel_id or "-1003758811790"
        self._admin_group = settings.telegram_admin_group_id

        # Bot tokens
        self._bot_token = settings.telegram_bot_token
        pool_str = settings.telegram_bot_pool_tokens
        self._pool_tokens = [
            t.strip() for t in pool_str.split(",") if t.strip()
        ] if pool_str else ([self._bot_token] if self._bot_token else [])
        self._clawnch_token = settings.telegram_bot_token_clawnch
        self._admin_token = getattr(settings, "telegram_admin_bot_token", "")

        # HTTP sessions
        await self._api.connect()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )

        # Load seen interfaces from Redis
        self._seen_interfaces = await self.redis.smembers("seen_interfaces") or set()
        logger.info(
            "[PROGRESSIVE] Channels: deployed=%s prebankr=%s clawnch=%s clanker=%s",
            self._deployed_channel, self._prebankr_channel, self._clawnch_channel,
            self._clanker_channel,
        )
        logger.info("[PROGRESSIVE] Bot pool: %d tokens", len(self._pool_tokens))

    async def teardown(self) -> None:
        await self._api.close()
        if self._session:
            await self._session.close()
        await super().teardown()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Subscribe to Pub/Sub channels and read Stream in parallel."""
        logger.info("[PROGRESSIVE] Starting event loops")

        await asyncio.gather(
            self._pubsub_loop(),
            self._ipfs_pubsub_loop(),
            self._stream_loop(),
            self._blacklist_poll_loop(),
            return_exceptions=True,
        )

    async def _pubsub_loop(self) -> None:
        """Subscribe to Pub/Sub channels for PRE-BANKR, DEPLOYED, PRE-CLANKER, DEPLOYED-CLANKER."""
        async for channel, message in self.redis.subscribe(
            Channels.TWITTER_PREBANKR,
            Channels.TWITTER_DEPLOYED,
            Channels.TWITTER_PRECLANKER,
            Channels.TWITTER_DEPLOYED_CLANKER,
        ):
            if not self._running:
                break
            if channel == Channels.TWITTER_PREBANKR:
                self.create_background_task(self._handle_prebankr_safe(message))
            elif channel == Channels.TWITTER_DEPLOYED:
                self.create_background_task(self._handle_deployed_safe(message))
            elif channel == Channels.TWITTER_PRECLANKER:
                self.create_background_task(self._handle_preclanker_safe(message))
            elif channel == Channels.TWITTER_DEPLOYED_CLANKER:
                self.create_background_task(self._handle_deployed_clanker_safe(message))

    async def _ipfs_pubsub_loop(self) -> None:
        """Dedicated Pub/Sub loop for IPFS metadata updates from background retry.

        Uses a SEPARATE Redis connection (not ``self.redis``) because
        redis-py allows only ONE pubsub per connection, and the main
        ``_pubsub_loop`` already occupies ``self.redis._pubsub``.
        """
        import redis.asyncio as _aioredis

        channel = Channels.IPFS_METADATA_UPDATE
        r = None
        ps = None
        while self._running:
            try:
                r = _aioredis.Redis(
                    host=self.settings.redis_host,
                    port=self.settings.redis_port,
                    db=self.settings.redis_db,
                    password=self.settings.redis_password or None,
                    decode_responses=True,
                )
                ps = r.pubsub()
                await ps.subscribe(channel)
                logger.info("[IPFS-LOOP] Subscribed to %s", channel)

                async for message in ps.listen():
                    if not self._running:
                        break
                    if message.get("type") != "message":
                        continue
                    data = message.get("data", "")
                    try:
                        if isinstance(data, str):
                            data = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if isinstance(data, dict):
                        self.create_background_task(
                            self._handle_ipfs_update_safe(data)
                        )
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[IPFS-LOOP] Error -- reconnecting in 2s")
                await asyncio.sleep(2)
            finally:
                try:
                    if ps:
                        await ps.unsubscribe()
                    if r:
                        await r.aclose()
                except Exception:
                    pass
                ps = None
                r = None

    async def _stream_loop(self) -> None:
        """Read on-chain token stream for TokenCreated events."""
        while self._running:
            entries = await self.read_stream(
                ConsumerGroups.TELEGRAM_PROGRESSIVE,
                count=10,
                block=2000,
            )
            for stream_name, stream_entries in entries:
                for entry_id, fields in stream_entries:
                    self.create_background_task(
                        self._handle_onchain_safe(fields, entry_id)
                    )

    # ------------------------------------------------------------------
    # Safe wrappers
    # ------------------------------------------------------------------

    async def _handle_prebankr_safe(self, msg: dict[str, Any]) -> None:
        try:
            await self._handle_prebankr(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] PRE-BANKR handler error")
            self.stats["errors"] += 1

    async def _handle_deployed_safe(self, msg: dict[str, Any]) -> None:
        try:
            await self._handle_deployed(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] DEPLOYED handler error")
            self.stats["errors"] += 1

    async def _handle_preclanker_safe(self, msg: dict[str, Any]) -> None:
        try:
            await self._handle_preclanker(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] PRE-CLANKER handler error")
            self.stats["errors"] += 1

    async def _handle_deployed_clanker_safe(self, msg: dict[str, Any]) -> None:
        try:
            await self._handle_deployed_clanker(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] DEPLOYED-CLANKER handler error")
            self.stats["errors"] += 1

    async def _handle_ipfs_update_safe(self, msg: dict[str, Any]) -> None:
        try:
            await self._handle_ipfs_update(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] IPFS update handler error")
            self.stats["errors"] += 1

    async def _handle_onchain_safe(
        self, msg: dict[str, Any], entry_id: str
    ) -> None:
        try:
            await self._handle_onchain_token(msg)
        except Exception:
            logger.exception("[PROGRESSIVE] ONCHAIN handler error")
            self.stats["errors"] += 1
        finally:
            await self.ack_stream(
                Channels.ONCHAIN_TOKEN,
                ConsumerGroups.TELEGRAM_PROGRESSIVE,
                entry_id,
            )

    # ==================================================================
    # PRE-BANKR HANDLER
    # ==================================================================

    async def _handle_prebankr(self, event: dict[str, Any]) -> None:
        """Handle PRE-BANKR: send initial message, enrich in parallel, edit."""
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        from_username = event.get("from_username", "")
        to_usernames: list[str] = event.get("to_usernames", []) or []
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        raw_tweet: dict = event.get("raw_tweet", {})

        # Global dedupe
        if not await self.redis.setnx(f"prebankr_processed:{tweet_id}", "1"):
            return
        await self.redis.expire(f"prebankr_processed:{tweet_id}", 3600)

        # Blacklist check
        if from_username and await self.storage.is_prebankr_blacklisted(from_username):
            logger.info("[PRE-BANKR] Blacklisted @%s -- skipping", from_username)
            return

        # Word blacklist check
        text_to_check = f"{token_name} {token_symbol} {raw_tweet.get('text', '')}"
        matched = await self.storage.check_prebankr_word_blacklist(text_to_check)
        if matched:
            logger.info("[PRE-BANKR] Word blacklisted '%s' -- skipping", matched)
            return

        # Thread inheritance (for reply chains)
        in_reply_to = raw_tweet.get("inReplyToId", "")
        parent_cache = None
        if in_reply_to:
            parent_cache = await self.storage.get_prebankr_data(in_reply_to)
            if parent_cache:
                token_name = token_name or parent_cache.get("token_name", "")
                token_symbol = token_symbol or parent_cache.get("token_symbol", "")
                from_username = from_username or parent_cache.get("from_username", "")
                to_usernames = to_usernames or parent_cache.get("to_usernames", [])
                await self.storage.link_pending_buys_to_thread(in_reply_to, tweet_id)

        logger.info("[PRE-BANKR] %s ($%s) from @%s", token_name, token_symbol, from_username)

        # Save PRE-BANKR data cache (initial, before enrichment)
        await self.storage.save_prebankr_data(
            tweet_id=tweet_id,
            from_username=from_username,
            to_usernames=to_usernames,
            token_name=token_name,
            token_symbol=token_symbol,
            enriched_from=parent_cache.get("enriched_from") if parent_cache else None,
            enriched_to=parent_cache.get("enriched_to") if parent_cache else None,
        )

        # ------------------------------------------------------------------
        # STEP 1: Enrich users FIRST (always, regardless of Telegram)
        # ------------------------------------------------------------------
        enriched_from: dict[str, Any] | None = (
            parent_cache.get("enriched_from") if parent_cache else None
        )
        enriched_to: list[dict[str, Any] | None] = [None] * len(to_usernames)
        if parent_cache and parent_cache.get("enriched_to"):
            cached_et = parent_cache["enriched_to"]
            for i in range(min(len(cached_et), len(enriched_to))):
                enriched_to[i] = cached_et[i]

        async def enrich_from_user() -> None:
            nonlocal enriched_from
            if enriched_from:
                return  # Already have from parent cache
            enriched_from = await self._enrich_user(from_username)
            if enriched_from:
                molt = await self.storage.get_molt_agent(from_username)
                if molt:
                    enriched_from.update({
                        "is_molt_agent": True,
                        "molt_agent_name": molt.get("agent_name", ""),
                        "molt_karma": molt.get("karma", 0),
                    })

        async def enrich_to_user(idx: int, username: str) -> None:
            if enriched_to[idx]:
                return  # Already have from parent cache
            data = await self._enrich_user(username)
            if data:
                molt = await self.storage.get_molt_agent(username)
                if molt:
                    data.update({
                        "is_molt_agent": True,
                        "molt_agent_name": molt.get("agent_name", ""),
                    })
                enriched_to[idx] = data

        enrich_tasks = [enrich_from_user()]
        for i, uname in enumerate(to_usernames):
            enrich_tasks.append(enrich_to_user(i, uname))
        await asyncio.gather(*enrich_tasks, return_exceptions=True)

        logger.info(
            "[PRE-BANKR] Enrichment done: from=%s to=%d/%d",
            "yes" if enriched_from else "no",
            sum(1 for e in enriched_to if e),
            len(to_usernames),
        )

        # Update cache with enriched data
        await self.storage.save_prebankr_data(
            tweet_id=tweet_id,
            from_username=from_username,
            to_usernames=to_usernames,
            token_name=token_name,
            token_symbol=token_symbol,
            enriched_from=enriched_from,
            enriched_to=enriched_to,
        )

        # ------------------------------------------------------------------
        # STEP 1.5: Extract parent tweet data (Task A)
        # ------------------------------------------------------------------
        parent_tweet: dict[str, Any] | None = None
        try:
            parent_tweet = await self._extract_parent_tweet(raw_tweet)
        except Exception:
            logger.debug("[PRE-BANKR] Parent tweet extraction failed")

        prebankr_at = datetime.now(timezone.utc).isoformat()

        # ------------------------------------------------------------------
        # STEP 2: Save log to Supabase (ALWAYS, with enriched data)
        # ------------------------------------------------------------------
        final_text = self._build_prebankr_enriched(
            enriched_from, enriched_to, tweet_url, token_name,
            token_symbol, raw_tweet.get("text", ""), tweet_id,
        )
        log_content: dict[str, Any] = {
            "from_username": from_username,
            "to_usernames": to_usernames,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "tweet_url": tweet_url,
            "tweet_text": raw_tweet.get("text", "")[:500],
            "confidence": event.get("confidence", 0),
            "enriched_from": enriched_from,
            "enriched_to": enriched_to,
            # Task A: parent tweet data
            "parent_tweet": parent_tweet,
            # Task D: detection timestamp
            "prebankr_at": prebankr_at,
            # LP fees (null for pre-bankr, populated on deployed_bankr)
            "buy_fee_pct": None,
            "sell_fee_pct": None,
        }
        await self.log_writer.prebankr(
            source_id=tweet_id,
            content=log_content,
            html_content=final_text,
        )

        # ------------------------------------------------------------------
        # STEP 3: Send to Telegram (best-effort, failure does NOT block)
        # ------------------------------------------------------------------
        buttons = self._build_prebankr_buttons(tweet_url, tweet_id)
        msg_id = await self._send_message(
            self._prebankr_channel, final_text, buttons, pool=True,
        )
        if not msg_id:
            logger.warning("[PRE-BANKR] Telegram send failed (best-effort) -- log saved to Supabase")
        else:
            logger.info("[PRE-BANKR] Sent Telegram message %d", msg_id)

        self.stats["messages_processed"] += 1

    # ==================================================================
    # IPFS METADATA UPDATE HANDLER (background retry result)
    # ==================================================================

    async def _handle_ipfs_update(self, event: dict[str, Any]) -> None:
        """Handle progressive metadata updates from background enrichment.

        Called for BOTH:
        - RPC metadata (name, symbol, pool_id) — arrives first (~0.5s)
        - IPFS metadata (image, tweet_id, tweet_url) — arrives second (~1s)

        When ``tweet_id`` is present, performs an INSTANT lookup of the
        PRE-BANKR cache to merge enriched user data (from_username,
        to_usernames, enriched_from, enriched_to, detected_at) into the
        Supabase log — eliminating the ~20s wait for the bankrbot tweet.
        """
        token_address = event.get("token_address", "")
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        tweet_username = event.get("tweet_username", "")
        token_image = event.get("token_image", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        pool_id = event.get("pool_id", "")

        if not token_address:
            return

        logger.info(
            "[META-UPDATE] %s ($%s) contract=%s pool=%s tweet=%s img=%s",
            token_name or "?", token_symbol or "?",
            token_address[:16],
            pool_id[:12] if pool_id else "-",
            tweet_id[:12] if tweet_id else "-",
            "yes" if token_image else "-",
        )

        # Determine new source_id: use tweet_id for PRE-BANKR linking
        new_source_id = tweet_id if tweet_id else None

        # Build partial content update (only non-empty fields).
        # Skip sentinel values "Unknown" / "???" to avoid overwriting
        # empty fields with misleading placeholders.
        content_update: dict[str, Any] = {}
        if token_name and token_name != "Unknown":
            content_update["token_name"] = token_name
        if token_symbol and token_symbol != "???":
            content_update["token_symbol"] = token_symbol
        if token_image:
            content_update["image"] = token_image
        if tweet_id:
            content_update["original_tweet_id"] = tweet_id
        if tweet_url:
            content_update["tweet_url"] = tweet_url

        # ---- INSTANT PRE-BANKR linking ----
        # If we have a tweet_id, look up the cached PRE-BANKR data
        # (from_username, to_usernames, enriched_from, enriched_to, detected_at).
        # This merges the full card data ~1s after on-chain instead of
        # waiting ~20s for the bankrbot DEPLOYED tweet.
        if tweet_id:
            cached = await self.storage.get_prebankr_data(tweet_id)
            if cached:
                logger.info(
                    "[META-UPDATE] PRE-BANKR cache HIT for tweet=%s → instant link",
                    tweet_id[:15],
                )
                if cached.get("from_username"):
                    content_update["from_username"] = cached["from_username"]
                if cached.get("to_usernames"):
                    content_update["to_usernames"] = cached["to_usernames"]
                if cached.get("enriched_from"):
                    content_update["enriched_from"] = cached["enriched_from"]
                if cached.get("enriched_to"):
                    content_update["enriched_to"] = cached["enriched_to"]
                if cached.get("detected_at"):
                    content_update["detected_at"] = cached["detected_at"]
                if cached.get("token_name") and not token_name:
                    content_update["token_name"] = cached["token_name"]
                if cached.get("token_symbol") and not token_symbol:
                    content_update["token_symbol"] = cached["token_symbol"]

        # ---- Update pool_data fee if provided ----
        # Background enrichment publishes the ACTUAL fee from the Initialize event.
        # This corrects the initial fallback (0x800000) stored by token_watcher.
        pool_fee = int(event.get("pool_fee", 0))
        if pool_fee > 0 and token_address:
            try:
                existing = await self.storage.get_pool_data(token_address)
                if existing:
                    old_fee = int(existing.get("fee", 0))
                    if old_fee != pool_fee:
                        existing["fee"] = str(pool_fee)
                        await self.storage.save_pool_data(token_address, existing)
                        logger.info(
                            "[META-UPDATE] Pool fee corrected: %s %d → %d",
                            token_address[:12], old_fee, pool_fee,
                        )
            except Exception:
                logger.exception("[META-UPDATE] Failed to update pool fee for %s", token_address[:12])

        if not content_update:
            return

        # Update via merge SQL (content || $4::jsonb)
        # Retry up to 10 times because the initial INSERT (from _handle_onchain_token)
        # may not have committed yet when this handler runs.
        try:
            updated = False
            for _attempt in range(10):
                ok = await self.log_writer.deployed_bankr_update(
                    contract_address=token_address,
                    new_source_id=new_source_id,
                    content=content_update,
                )
                if ok:
                    updated = True
                    break
                await asyncio.sleep(0.5)

            if updated:
                logger.info(
                    "[META-UPDATE] Supabase updated for %s ($%s)",
                    token_name or token_address[:12], token_symbol or "?",
                )
            else:
                logger.warning(
                    "[META-UPDATE] No log found after retries for %s",
                    token_address[:16],
                )
        except Exception:
            logger.exception("[META-UPDATE] Failed to update Supabase for %s", token_address[:16])

    # ==================================================================
    # DEPLOYED HANDLER
    # ==================================================================

    async def _handle_deployed(self, event: dict[str, Any]) -> None:
        """Handle DEPLOYED: update existing on-chain msg or send new."""
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        original_tweet_id = event.get("original_tweet_id", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        contract_address = event.get("contract_address", "")
        from_username = event.get("from_username", "")
        to_usernames: list[str] = event.get("to_usernames", []) or []

        # Global dedupe
        if not await self.redis.setnx(f"deployed_processed:{tweet_id}", "1"):
            return
        await self.redis.expire(f"deployed_processed:{tweet_id}", 3600)

        # Check PRE-BANKR cache
        cached_data = None
        if original_tweet_id:
            cached_data = await self.storage.get_prebankr_data(original_tweet_id)
            if cached_data:
                from_username = cached_data.get("from_username") or from_username
                to_usernames = cached_data.get("to_usernames") or to_usernames
                token_name = token_name or cached_data.get("token_name", "")
                token_symbol = token_symbol or cached_data.get("token_symbol", "")

        # Fallback: get token info from Redis by contract address
        if (not token_name or not token_symbol) and contract_address:
            token_info = await self.storage.get_token_by_address(contract_address)
            if token_info:
                token_name = token_name or token_info.get("token_name", "")
                token_symbol = token_symbol or token_info.get("token_symbol", "")

        # Fallback: get token info from Redis by original tweet id
        if (not token_name or not token_symbol) and original_tweet_id:
            token_info = await self.storage.get_token_info(original_tweet_id)
            if token_info:
                token_name = token_name or token_info.get("token_name", "")
                token_symbol = token_symbol or token_info.get("token_symbol", "")

        # Check for pending on-chain message (on-chain-first flow)
        onchain_msg = self._pending_onchain.pop(original_tweet_id, None)
        if not onchain_msg and contract_address:
            # Bankr Launcher tokens are keyed by token_address (no tweet_id at on-chain time)
            onchain_msg = self._pending_onchain.pop(contract_address.lower(), None)
        if not onchain_msg and original_tweet_id:
            # Fallback search by URL
            for key in list(self._pending_onchain.keys()):
                if original_tweet_id in key:
                    onchain_msg = self._pending_onchain.pop(key, None)
                    break

        if onchain_msg:
            # Update existing on-chain message with full Twitter data
            logger.info(
                "[DEPLOYED] Updating on-chain message for %s ($%s) with tweet data",
                token_name, token_symbol,
            )
            msg_id = onchain_msg.get("message_id", 0)
            channel = onchain_msg.get("channel", self._deployed_channel)
            token = onchain_msg.get("token", self._bot_token)

            # Enrich users
            enriched_from = cached_data.get("enriched_from") if cached_data else None
            enriched_to = cached_data.get("enriched_to", []) if cached_data else []

            if not enriched_from and from_username:
                enriched_from = await self._enrich_user(from_username)
            for i, uname in enumerate(to_usernames):
                if i >= len(enriched_to) or not enriched_to[i]:
                    data = await self._enrich_user(uname)
                    if i < len(enriched_to):
                        enriched_to[i] = data
                    else:
                        enriched_to.append(data)

            # Fetch liked_by usernames for display
            liked_by: list[str] = []
            if original_tweet_id:
                liked_by = await self.storage.get_liked_usernames(original_tweet_id)

            # Build updated message with all tweet info (v1 style)
            new_text = self._build_deployed_message(
                token_name, token_symbol, contract_address,
                from_username, to_usernames, tweet_url,
                enriched_from, enriched_to,
                onchain_msg.get("creator_wallet", ""),
                onchain_msg.get("creator_info", {}),
                liked_by=liked_by,
            )
            buttons = self._build_deployed_buttons(
                contract_address, tweet_url, original_tweet_id,
            )

            # Edit Telegram message
            try:
                await self._api.edit_message(
                    token, channel, msg_id, new_text,
                    reply_markup={"inline_keyboard": buttons},
                )
            except Exception:
                logger.warning("[DEPLOYED] edit_message failed (non-fatal)")

            # --- Link PRE-BANKR to on-chain token ---
            if contract_address and original_tweet_id:
                try:
                    await self.storage.update_token_address(original_tweet_id, contract_address)
                except Exception:
                    logger.warning("[DEPLOYED] update_token_address failed (non-fatal)")

            # --- Read fee data ---
            _fee_buy = None
            _fee_sell = None
            _fee_info = None
            if contract_address:
                _fee_info = await self.storage.get_token_by_address(contract_address)
            if not _fee_info and original_tweet_id:
                _fee_info = await self.storage.get_token_info(original_tweet_id)
            if _fee_info:
                _fb = _fee_info.get("buy_fee_pct")
                _fs = _fee_info.get("sell_fee_pct")
                if _fb is not None:
                    try:
                        _fee_buy = float(_fb)
                    except (ValueError, TypeError):
                        pass
                if _fs is not None:
                    try:
                        _fee_sell = float(_fs)
                    except (ValueError, TypeError):
                        pass

            # --- Update the existing Supabase log (created by on-chain instant) ---
            # Use UPDATE by contract_address so the frontend card updates in-place
            # instead of creating a duplicate.
            onchain_at_val = onchain_msg.get("onchain_at")
            full_content = {
                "original_tweet_id": original_tweet_id,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "contract_address": contract_address.lower(),
                "from_username": from_username,
                "to_usernames": to_usernames,
                "tweet_url": tweet_url,
                "enriched_from": enriched_from,
                "enriched_to": enriched_to,
                "on_chain": True,
                "onchain_at": onchain_at_val,
                # 3 timestamps
                "detected_at": cached_data.get("detected_at") if cached_data else None,
                "deployed_at": onchain_at_val,  # preserve on-chain detection time
                "tweeted_at": datetime.now(timezone.utc).isoformat(),
                "buy_fee_pct": _fee_buy,
                "sell_fee_pct": _fee_sell,
            }

            updated = await self.log_writer.deployed_bankr_update(
                contract_address=contract_address,
                new_source_id=tweet_id,
                content=full_content,
                html_content=new_text,
            )
            if not updated:
                # No on-chain log found to update -- insert fresh
                logger.info("[DEPLOYED] No on-chain log to update, inserting new")
                await self.log_writer.deployed_bankr(
                    source_id=tweet_id,
                    content=full_content,
                    html_content=new_text,
                )

            self.stats["messages_processed"] += 1
            return

        # No on-chain message found -- send new DEPLOYED message
        if contract_address:
            if await self.storage.has_notified_contract(contract_address):
                return
            await self.storage.mark_contract_notified(contract_address)

        logger.info("[DEPLOYED] %s ($%s) contract=%s", token_name, token_symbol, contract_address[:16])

        # Get enriched data from cache or enrich fresh
        enriched_from = cached_data.get("enriched_from") if cached_data else None
        enriched_to = cached_data.get("enriched_to", []) if cached_data else []

        # ------------------------------------------------------------------
        # STEP 1: Enrich non-cached users (always, before log save)
        # ------------------------------------------------------------------
        if not enriched_from and from_username:
            enriched_from = await self._enrich_user(from_username)

        for i, uname in enumerate(to_usernames):
            if i >= len(enriched_to) or not enriched_to[i]:
                data = await self._enrich_user(uname)
                if data:
                    while len(enriched_to) <= i:
                        enriched_to.append(None)
                    enriched_to[i] = data

        liked_by: list[str] = []
        if original_tweet_id:
            liked_by = await self.storage.get_liked_usernames(original_tweet_id)
            if liked_by:
                logger.info("[DEPLOYED] %d likes found: %s", len(liked_by), ", ".join(liked_by[:5]))

        text = self._build_deployed_message(
            token_name, token_symbol, contract_address,
            from_username, to_usernames, tweet_url,
            enriched_from, enriched_to, "", {},
            liked_by=liked_by,
        )
        buttons = self._build_deployed_buttons(
            contract_address, tweet_url, original_tweet_id,
        )

        # ------------------------------------------------------------------
        # STEP 2: Save log to Supabase (ALWAYS, with enriched data)
        # ------------------------------------------------------------------
        # Determine REAL on-chain status from tokens table / Redis
        is_onchain = False
        onchain_at_val = None
        if contract_address:
            ti = await self.storage.get_token_by_address(contract_address)
            if ti and ti.get("on_chain") in ("1", True, "true", "True"):
                is_onchain = True
                onchain_at_val = ti.get("onchain_at")
        if not is_onchain and original_tweet_id:
            ti = await self.storage.get_token_info(original_tweet_id)
            if ti:
                if ti.get("on_chain") in ("1", True, "true", "True"):
                    is_onchain = True
                onchain_at_val = onchain_at_val or ti.get("onchain_at")

        # Store the bankrbot tweet contract address in the tokens table
        # and mark the PRE-BANKR row as on_chain=true.  The bankrbot tweet
        # IS the deployment confirmation – no need to wait for on-chain
        # listener.  update_token_address also reconciles pool/fee data
        # from any sibling on-chain row written by token_watcher.
        if contract_address and original_tweet_id:
            try:
                await self.storage.update_token_address(original_tweet_id, contract_address)
                is_onchain = True  # refresh local flag
            except Exception:
                logger.warning("[DEPLOYED] Failed to store contract in tokens table (non-fatal)")

        # Read fee data for log content
        _fee_buy = None
        _fee_sell = None
        _fee_token_info = None
        if contract_address:
            _fee_token_info = await self.storage.get_token_by_address(contract_address)
        if not _fee_token_info and original_tweet_id:
            _fee_token_info = await self.storage.get_token_info(original_tweet_id)
        if _fee_token_info:
            _fb = _fee_token_info.get("buy_fee_pct")
            _fs = _fee_token_info.get("sell_fee_pct")
            if _fb is not None:
                try:
                    _fee_buy = float(_fb)
                except (ValueError, TypeError):
                    pass
            if _fs is not None:
                try:
                    _fee_sell = float(_fs)
                except (ValueError, TypeError):
                    pass

        await self.log_writer.deployed_bankr(
            source_id=tweet_id,
            content={
                # Link to PRE-BANKR
                "original_tweet_id": original_tweet_id,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "contract_address": contract_address.lower() if contract_address else "",
                "from_username": from_username,
                "to_usernames": to_usernames,
                "tweet_url": tweet_url,
                "enriched_from": enriched_from,
                "enriched_to": enriched_to,
                # On-chain status: real check against tokens table
                "on_chain": is_onchain,
                # 3 timestamps
                "onchain_at": onchain_at_val,
                "detected_at": cached_data.get("detected_at") if cached_data else None,
                "deployed_at": onchain_at_val,  # on-chain time (may be None)
                "tweeted_at": datetime.now(timezone.utc).isoformat(),
                # LP fees
                "buy_fee_pct": _fee_buy,
                "sell_fee_pct": _fee_sell,
            },
            html_content=text,
        )

        # ------------------------------------------------------------------
        # STEP 3: Send to Telegram (best-effort, failure does NOT block)
        # ------------------------------------------------------------------
        msg_id = await self._send_message(
            self._deployed_channel, text, buttons, pool=True,
        )
        if not msg_id:
            logger.warning("[DEPLOYED] Telegram send failed (best-effort) -- log saved to Supabase")
        else:
            logger.info("[DEPLOYED] Sent Telegram message %d", msg_id)

        self.stats["messages_processed"] += 1

    # ==================================================================
    # ON-CHAIN TOKEN HANDLER
    # ==================================================================

    async def _handle_onchain_token(self, event: dict[str, Any]) -> None:
        """Handle on-chain TokenCreated event."""
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        creator_wallet = event.get("creator_wallet", "")
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        platform = event.get("platform", "X")
        interface = event.get("interface", "")

        if not token_address:
            return

        # New interface detection
        if interface and interface not in self._seen_interfaces:
            metadata = event.get("metadata_parsed", {})
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}
            desc = metadata.get("description", "") if isinstance(metadata, dict) else ""
            if desc != "Deployed using @bankrbot on X":
                await self._send_new_interface_alert(event, interface)
            self._seen_interfaces.add(interface)
            await self.storage.add_seen_interface(interface)

        # Route Clawnch tokens to separate channel
        clawnch_platforms = {"4claw", "moltbook", "moltx", "clawstr"}
        if interface == "Clawnch" or platform.lower() in clawnch_platforms:
            await self._handle_clawnch_token(event)
            return

        # =======================================================================
        # ROUTE: Clanker tokens (Twitter/Farcaster via clanker interface)
        # =======================================================================
        if interface.lower() == "clanker":
            await self._handle_clanker_token(event)
            return

        # --- Deployer whitelist: only process tokens from bankr ----
        # New Bankr Launcher: msg_sender = launcher contract address
        # Legacy Clanker V4: msg_sender = bankr deployer wallet
        msg_sender = event.get("msg_sender", "").lower()
        is_bankr_launcher = (
            msg_sender == BANKR_LAUNCHER_ADDRESS
            or event.get("launcher") == "bankr"
        )
        is_bankr_legacy = (msg_sender == BANKR_DEPLOYER)
        if not is_bankr_launcher and not is_bankr_legacy:
            return

        # Both Bankr Launcher and legacy bankr tokens are shown instantly.
        # For legacy tokens without tweet_id, verify metadata/interface.
        if not is_bankr_launcher and not tweet_id:
            if not is_bankr_legacy:
                return  # not from bankr at all
            # Legacy token without tweet_id: check metadata early to filter
            # non-bankr tokens (clank.fun etc.) from the same deployer wallet.
            _meta = event.get("metadata_parsed", {})
            if isinstance(_meta, str):
                try:
                    _meta = json.loads(_meta)
                except (json.JSONDecodeError, TypeError):
                    _meta = {}
            _is_bankr_meta = (
                isinstance(_meta, dict)
                and _meta.get("description", "") == "Deployed using @bankrbot on X"
            ) or interface.lower() == "bankr"
            if not _is_bankr_meta:
                return  # deployer wallet but not a bankr token

        # Contract dedupe
        if await self.storage.has_notified_contract(token_address):
            return
        await self.storage.mark_contract_notified(token_address)

        # Cross-contract dedup (same token on Bankr Launcher + Clanker V4)
        # Skip name-based dedup when name is empty (instant publish before RPC)
        if token_name:
            name_dedup = f"bankr:name_dedup:{token_name.lower().strip()}:{token_symbol.lower().strip()}"
            if not await self.redis.setnx(name_dedup, token_address):
                logger.debug("[DEDUP] %s ($%s) already shown from other contract", token_name, token_symbol)
                return
            await self.redis.expire(name_dedup, 300)  # 5 min window

        tag = "BANKR-ONCHAIN" if is_bankr_launcher else "ONCHAIN"
        logger.info(
            "[%s] %s ($%s) contract=%s tweet=%s",
            tag, token_name, token_symbol, token_address[:16],
            tweet_id[:12] if tweet_id else "pending",
        )

        # Quick Redis lookups (no API calls)
        cached_data = await self.storage.get_prebankr_data(tweet_id) if tweet_id else None
        enriched_from = cached_data.get("enriched_from") if cached_data else None
        enriched_to = cached_data.get("enriched_to", []) if cached_data else []

        # Build and send immediately
        text = self._build_onchain_message(
            token_name, token_symbol, token_address, creator_wallet,
            platform, tweet_url, enriched_from, enriched_to,
        )
        buttons = self._build_deployed_buttons(token_address, tweet_url, tweet_id)

        msg_id = await self._send_message(
            self._deployed_channel, text, buttons, pool=True,
        )
        if not msg_id:
            return

        logger.info("[%s] Sent message %d instantly", tag, msg_id)

        # Store for DEPLOYED tweet to update later (keyed by tweet_id or token_address)
        pending_key = tweet_id or token_address.lower()
        pending_data = {
            "message_id": msg_id,
            "channel": self._deployed_channel,
            "token": self._get_pool_token(),
            "creator_wallet": creator_wallet,
            "creator_info": {},
            "onchain_at": datetime.now(timezone.utc).isoformat(),
        }
        self._pending_onchain[pending_key] = pending_data
        if tweet_url:
            self._pending_onchain[tweet_url] = pending_data
        # For Bankr Launcher or legacy tokens without tweet_id, also store by
        # token_address so _handle_deployed can find them by contract_address.
        if (is_bankr_launcher or (is_bankr_legacy and not tweet_id)) and token_address:
            self._pending_onchain[token_address.lower()] = pending_data

        # Instant deployed_bankr log -- create in Supabase so the frontend
        # shows the token immediately.  For Bankr Launcher tokens (no tweet_id
        # yet), _handle_deployed will UPDATE this same row later when the
        # bankrbot tweet arrives.
        metadata = event.get("metadata_parsed", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        is_bankr = (
            isinstance(metadata, dict)
            and metadata.get("description", "") == "Deployed using @bankrbot on X"
        ) or interface.lower() == "bankr" or is_bankr_launcher

        if is_bankr:
            from_user = cached_data.get("from_username", "") if cached_data else ""
            to_users = cached_data.get("to_usernames", []) if cached_data else []
            onchain_now = datetime.now(timezone.utc).isoformat()

            # Read fee data for instant deployed log
            _oc_fee_buy = None
            _oc_fee_sell = None
            _oc_source = tweet_id or token_address
            _oc_token_info = await self.storage.get_token_info(_oc_source)
            if _oc_token_info:
                _ofb = _oc_token_info.get("buy_fee_pct")
                _ofs = _oc_token_info.get("sell_fee_pct")
                if _ofb is not None:
                    try:
                        _oc_fee_buy = float(_ofb)
                    except (ValueError, TypeError):
                        pass
                if _ofs is not None:
                    try:
                        _oc_fee_sell = float(_ofs)
                    except (ValueError, TypeError):
                        pass

            # Use tweet_id as source_id when available (enables frontend
            # lookup by the same key used for PRE-BANKR logs).  Fall back
            # to onchain:{address} when tweet_id is still unknown.
            _log_source_id = tweet_id if tweet_id else f"onchain:{token_address.lower()}"

            await self.log_writer.deployed_bankr(
                source_id=_log_source_id,
                content={
                    "original_tweet_id": tweet_id or "",
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "contract_address": token_address.lower(),
                    "from_username": from_user,
                    "to_usernames": to_users,
                    "tweet_url": tweet_url or "",
                    "enriched_from": enriched_from,
                    "enriched_to": enriched_to,
                    "on_chain": True,
                    "detected_at": cached_data.get("detected_at") if cached_data else None,
                    "deployed_at": onchain_now,
                    "tweeted_at": None,
                    # Token image (IPFS gateway URL)
                    "image": event.get("token_image", ""),
                    # LP fees
                    "buy_fee_pct": _oc_fee_buy,
                    "sell_fee_pct": _oc_fee_sell,
                },
                html_content=text,
            )

        # Enrich creator wallet in background
        self.create_background_task(
            self._enrich_creator_and_update(
                msg_id, text, buttons, token_name, token_symbol,
                token_address, creator_wallet, platform, tweet_url,
                enriched_from, enriched_to,
            )
        )

        # Background enrichment if no cache
        if not enriched_from and tweet_url:
            self.create_background_task(
                self._bg_enrich_onchain(
                    msg_id, buttons, token_name, token_symbol,
                    token_address, creator_wallet, platform, tweet_url, tweet_id,
                )
            )

        # Limit pending cache
        if len(self._pending_onchain) > 200:
            keys = list(self._pending_onchain.keys())
            for k in keys[:100]:
                self._pending_onchain.pop(k, None)

        self.stats["messages_processed"] += 1

    # ==================================================================
    # CLAWNCH TOKEN HANDLER
    # ==================================================================

    async def _handle_clawnch_token(self, event: dict[str, Any]) -> None:
        """Handle Clawnch token (moltbook, 4claw, moltx)."""
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        deployer = event.get("msg_sender", "") or event.get("creator_wallet", "")
        platform = event.get("platform", "unknown")

        context = event.get("context_parsed", {})
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (json.JSONDecodeError, TypeError):
                context = {}

        post_id = context.get("messageId", "") if isinstance(context, dict) else ""

        if not token_address or not self._clawnch_token:
            return

        if await self.storage.has_notified_contract(token_address):
            return
        await self.storage.mark_contract_notified(token_address)

        # New platform detection
        p_lower = platform.lower() if isinstance(platform, str) else ""
        if p_lower and p_lower not in self._known_clawnch_platforms:
            if p_lower not in self._seen_platforms:
                self._seen_platforms.add(p_lower)
                alert = (
                    "<b>NOVA PLATAFORMA INTEGRADA COM CLAWNCH</b>\n"
                    f"<code>{platform}</code>"
                )
                await self._api.send_message(
                    self._clawnch_token, self._clawnch_channel, alert,
                )
                await self.log_writer.new_platform(
                    source_id=token_address,
                    content={"platform": platform, "token_symbol": token_symbol},
                )

        logger.info(
            "[CLAWNCH] %s ($%s) platform=%s post=%s",
            token_name, token_symbol, platform, post_id,
        )

        # Get cached PRE-CLAWNCH data
        preclawnch = await self.storage.get_preclawnch_token(post_id) if post_id else None
        x_handle = preclawnch.get("x_handle", "") if preclawnch else ""
        agent_name = preclawnch.get("agent_name", "") if preclawnch else ""
        if not agent_name:
            agent_name = context.get("id", "") if isinstance(context, dict) else ""

        # Enrichment from cache
        molt_agent = await self.storage.get_molt_agent(x_handle) if x_handle else None
        twitter_data = None
        alphagate_data = None
        if x_handle:
            twitter_data = await self.redis.hgetall(f"twitter_user:{x_handle.lower()}")
            if not twitter_data:
                twitter_data = None
            alphagate_data = await self.storage.get_alphagate_data(x_handle)

        text = self._build_clawnch_message(
            token_name, token_symbol, token_address, deployer, platform,
            agent_name, x_handle, post_id, molt_agent, twitter_data,
            alphagate_data,
        )
        buttons = self._build_clawnch_buttons(
            token_address, post_id, platform,
        )

        msg_id = await self._api.send_message(
            self._clawnch_token, self._clawnch_channel, text,
            reply_markup={"inline_keyboard": buttons},
        )
        if msg_id:
            logger.info("[CLAWNCH] Sent message %d", msg_id)

            # Log to Supabase
            await self.log_writer.deployed_clawnch(
                source_id=post_id or token_address,
                content={
                    "token_address": token_address,
                    "token_symbol": token_symbol,
                    "token_name": token_name,
                    "platform": platform,
                    "agent_name": agent_name,
                    "x_handle": x_handle,
                    "post_id": post_id,
                    "deployer": deployer,
                    "molt_agent": molt_agent,
                    "twitter": twitter_data,
                    "alphagate": alphagate_data,
                },
                html_content=text,
            )

        self.stats["messages_processed"] += 1

    # ==================================================================
    # CLANKER TOKEN HANDLING
    # ==================================================================

    async def _handle_clanker_token(self, event: dict[str, Any]) -> None:
        """Handle Clanker token deployed via @clanker_world on Twitter."""
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        creator_wallet = event.get("msg_sender", "") or event.get("creator_wallet", "")
        block_number = event.get("block_number", "")

        context = event.get("context_parsed", {})
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (json.JSONDecodeError, TypeError):
                context = {}

        tweet_url = context.get("messageId", "") if isinstance(context, dict) else ""
        tweet_id = ""
        from_username = ""

        # Extract tweet_id and username from messageId URL
        # Format: https://x.com/{username}/status/{id}
        if tweet_url and "/status/" in tweet_url:
            parts = tweet_url.split("/")
            try:
                status_idx = parts.index("status")
                tweet_id = parts[status_idx + 1].split("?")[0] if status_idx + 1 < len(parts) else ""
                from_username = parts[status_idx - 1] if status_idx > 0 else ""
            except (ValueError, IndexError):
                pass

        if not token_address:
            return

        # Dedupe by contract address
        if await self.storage.has_notified_contract(token_address):
            logger.debug("[CLANKER] Contract %s already notified", token_address[:10])
            return
        await self.storage.mark_contract_notified(token_address)

        # Cross-contract dedup (Bankr Launcher may have already sent this token)
        if token_name:
            name_dedup = f"bankr:name_dedup:{token_name.lower().strip()}:{token_symbol.lower().strip()}"
            if not await self.redis.setnx(name_dedup, token_address):
                logger.info("[CLANKER] Skipped %s ($%s) -- already sent via Bankr", token_name, token_symbol)
                return
            await self.redis.expire(name_dedup, 300)

        # ------------------------------------------------------------------
        # Instant PRE-CLANKER enrichment: tweet_id is available from
        # tokenContext.messageId on-chain (no IPFS needed like Bankr).
        # ------------------------------------------------------------------
        cached: dict[str, Any] | None = None
        enriched_from: dict[str, Any] = {}
        enriched_to: list[dict[str, Any]] = []
        to_usernames: list[str] = []
        detected_at: str | None = None
        if tweet_id:
            cached = await self.storage.get_preclanker_data(tweet_id)
            if cached:
                from_username = cached.get("from_username") or from_username
                to_usernames = cached.get("to_usernames") or []
                token_name = token_name or cached.get("token_name", "")
                token_symbol = token_symbol or cached.get("token_symbol", "")
                enriched_from = cached.get("enriched_from") or {}
                enriched_to = cached.get("enriched_to") or []
                detected_at = cached.get("detected_at")
                logger.info("[CLANKER] Merged PRE-CLANKER cache for tweet=%s", tweet_id)

        logger.info(
            "[CLANKER] %s ($%s) tweet=%s from=@%s",
            token_name, token_symbol, tweet_id, from_username,
        )
        logger.info("  Contract: %s", token_address)
        logger.info("  Creator: %s", creator_wallet[:10] if creator_wallet else "?")
        logger.info("  Block: %s", block_number)

        # Build Clanker message
        text = self._build_clanker_message(
            token_name, token_symbol, token_address, creator_wallet,
            from_username, tweet_url, event.get("image", ""),
        )
        buttons = self._build_clanker_buttons(token_address, tweet_url, tweet_id)

        # Send to dedicated Clanker channel (single send via pool bot)
        msg_id = await self._send_message(
            self._clanker_channel, text, buttons, pool=True,
        )
        if msg_id:
            logger.info("[CLANKER] Sent message %d", msg_id)

        # ------------------------------------------------------------------
        # Store in _pending_onchain so _handle_deployed_clanker can find
        # and EDIT this message when the @clanker_world tweet arrives.
        # ------------------------------------------------------------------
        if msg_id:
            pending_data = {
                "message_id": msg_id,
                "channel": self._clanker_channel,
                "token": self._get_pool_token(),
                "creator_wallet": creator_wallet,
                "creator_info": {},
                "onchain_at": datetime.now(timezone.utc).isoformat(),
            }
            if tweet_id:
                self._pending_onchain[tweet_id] = pending_data
            if tweet_url:
                self._pending_onchain[tweet_url] = pending_data
            self._pending_onchain[token_address.lower()] = pending_data
            logger.info("[CLANKER] Stored _pending_onchain keys: tweet=%s addr=%s", tweet_id, token_address[:10])

        # Log to Supabase ALWAYS (independent of Telegram send)
        onchain_at = datetime.now(timezone.utc).isoformat()
        clanker_content = {
            "token_address": token_address,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "contract_address": token_address,
            "creator_wallet": creator_wallet,
            "tweet_url": tweet_url,
            "tweet_id": tweet_id,
            "from_username": from_username,
            "to_usernames": to_usernames,
            "platform": "clanker",
            "interface": "clanker",
            "on_chain": True,
            "deployed_at": onchain_at,
            "onchain_at": onchain_at,
            "detected_at": detected_at,
            "enriched_from": enriched_from,
            "enriched_to": enriched_to,
            "image": event.get("image", ""),
        }
        await self.log_writer.deployed_clanker(
            source_id=tweet_id or token_address,
            content=clanker_content,
            html_content=text,
        )

        self.stats["messages_processed"] += 1

    @staticmethod
    def _build_clanker_message(
        token_name: str, token_symbol: str, contract: str,
        creator_wallet: str, from_username: str, tweet_url: str,
        image: str,
    ) -> str:
        """Build Clanker deployment message (v1 style)."""
        lines = [
            f"🟣 <b>DEPLOYED CLANKR ({token_symbol})</b>",
            " ",
            f"<code><b>{token_name}</b> (${token_symbol})</code>",
            "",
        ]
        if from_username:
            lines.append("━━━━━━━💼 FROM━━━━━━━")
            lines.append("")
            lines.append(
                f'👑 <b>Twitter:</b> <a href="https://x.com/{from_username}">@{from_username}</a>'
            )
        lines.append("")
        lines.append(f"🔖 <b>Contract:</b> <code>{contract}</code>")
        if creator_wallet:
            lines.append(f"👤 <b>Creator:</b> <code>{creator_wallet}</code>")
        if tweet_url:
            lines.append(f'🐦 <a href="{tweet_url}">Tweet</a>')
        lines.append("")
        lines.append(f"<b>{token_name} ({token_symbol}) - CLANKR</b>")
        return "\n".join(lines)

    @staticmethod
    def _build_clanker_buttons(
        contract: str, tweet_url: str, tweet_id: str,
    ) -> list[list[dict[str, Any]]]:
        """Build inline buttons for Clanker message (v1 style, same as BANKR)."""
        row1: list[dict[str, Any]] = []
        if contract:
            row1.append({
                "text": "Sigma",
                "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{contract}",
            })
            row1.append({
                "text": "GMGN",
                "url": f"https://gmgn.ai/base/token/qquante_{contract}",
            })
            row1.append({
                "text": "VIP",
                "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{contract}",
            })
            row1.append({
                "text": "Clanker",
                "url": f"https://www.clanker.world/clanker/{contract}",
            })
        row2: list[dict[str, Any]] = []
        if tweet_id:
            row2.append({"text": "💰 Buy", "callback_data": f"buy:{tweet_id}"})
        if tweet_url:
            row2.append({"text": "Tweet", "url": tweet_url})
        rows = []
        if row1:
            rows.append(row1)
        if row2:
            rows.append(row2)
        return rows

    # ==================================================================
    # PRE-CLANKER HANDLER
    # ==================================================================

    async def _handle_preclanker(self, event: dict[str, Any]) -> None:
        """Handle PRE-CLANKER: enrich users, log to Supabase, send to Telegram.

        Mirrors ``_handle_prebankr`` but writes to the preclanker channel
        and uses ``preclanker_data`` Redis cache.
        """
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        from_username = event.get("from_username", "")
        to_usernames: list[str] = event.get("to_usernames", []) or []
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        raw_tweet: dict = event.get("raw_tweet", {})

        # Global dedupe
        if not await self.redis.setnx(f"preclanker_processed:{tweet_id}", "1"):
            return
        await self.redis.expire(f"preclanker_processed:{tweet_id}", 3600)

        # Blacklist check (reuse bankr blacklists -- same spam patterns)
        if from_username and await self.storage.is_prebankr_blacklisted(from_username):
            logger.info("[PRE-CLANKER] Blacklisted @%s -- skipping", from_username)
            return

        # Word blacklist check
        text_to_check = f"{token_name} {token_symbol} {raw_tweet.get('text', '')}"
        matched = await self.storage.check_prebankr_word_blacklist(text_to_check)
        if matched:
            logger.info("[PRE-CLANKER] Word blacklisted '%s' -- skipping", matched)
            return

        # Thread inheritance (for reply chains)
        in_reply_to = raw_tweet.get("inReplyToId", "")
        parent_cache = None
        if in_reply_to:
            parent_cache = await self.storage.get_preclanker_data(in_reply_to)
            if parent_cache:
                token_name = token_name or parent_cache.get("token_name", "")
                token_symbol = token_symbol or parent_cache.get("token_symbol", "")
                from_username = from_username or parent_cache.get("from_username", "")
                to_usernames = to_usernames or parent_cache.get("to_usernames", [])
                await self.storage.link_pending_buys_to_thread(in_reply_to, tweet_id)

        logger.info("[PRE-CLANKER] %s ($%s) from @%s", token_name, token_symbol, from_username)

        # Save PRE-CLANKER data cache (initial, before enrichment)
        await self.storage.save_preclanker_data(
            tweet_id=tweet_id,
            from_username=from_username,
            to_usernames=to_usernames,
            token_name=token_name,
            token_symbol=token_symbol,
            enriched_from=parent_cache.get("enriched_from") if parent_cache else None,
            enriched_to=parent_cache.get("enriched_to") if parent_cache else None,
        )

        # ------------------------------------------------------------------
        # STEP 1: Enrich users FIRST (always, regardless of Telegram)
        # ------------------------------------------------------------------
        enriched_from: dict[str, Any] | None = (
            parent_cache.get("enriched_from") if parent_cache else None
        )
        enriched_to: list[dict[str, Any] | None] = [None] * len(to_usernames)
        if parent_cache and parent_cache.get("enriched_to"):
            cached_et = parent_cache["enriched_to"]
            for i in range(min(len(cached_et), len(enriched_to))):
                enriched_to[i] = cached_et[i]

        async def enrich_from_user() -> None:
            nonlocal enriched_from
            if enriched_from:
                return
            enriched_from = await self._enrich_user(from_username)
            if enriched_from:
                molt = await self.storage.get_molt_agent(from_username)
                if molt:
                    enriched_from.update({
                        "is_molt_agent": True,
                        "molt_agent_name": molt.get("agent_name", ""),
                        "molt_karma": molt.get("karma", 0),
                    })

        async def enrich_to_user(idx: int, username: str) -> None:
            if enriched_to[idx]:
                return
            data = await self._enrich_user(username)
            if data:
                molt = await self.storage.get_molt_agent(username)
                if molt:
                    data.update({
                        "is_molt_agent": True,
                        "molt_agent_name": molt.get("agent_name", ""),
                    })
                enriched_to[idx] = data

        enrich_tasks = [enrich_from_user()]
        for i, uname in enumerate(to_usernames):
            enrich_tasks.append(enrich_to_user(i, uname))
        await asyncio.gather(*enrich_tasks, return_exceptions=True)

        logger.info(
            "[PRE-CLANKER] Enrichment done: from=%s to=%d/%d",
            "yes" if enriched_from else "no",
            sum(1 for e in enriched_to if e),
            len(to_usernames),
        )

        # Update cache with enriched data
        await self.storage.save_preclanker_data(
            tweet_id=tweet_id,
            from_username=from_username,
            to_usernames=to_usernames,
            token_name=token_name,
            token_symbol=token_symbol,
            enriched_from=enriched_from,
            enriched_to=enriched_to,
        )

        # ------------------------------------------------------------------
        # STEP 1.5: Extract parent tweet data
        # ------------------------------------------------------------------
        parent_tweet: dict[str, Any] | None = None
        try:
            parent_tweet = await self._extract_parent_tweet(raw_tweet)
        except Exception:
            logger.debug("[PRE-CLANKER] Parent tweet extraction failed")

        preclanker_at = datetime.now(timezone.utc).isoformat()

        # ------------------------------------------------------------------
        # STEP 2: Save log to Supabase (ALWAYS, with enriched data)
        # ------------------------------------------------------------------
        final_text = self._build_preclanker_enriched(
            enriched_from, enriched_to, tweet_url, token_name,
            token_symbol, raw_tweet.get("text", ""), tweet_id,
        )
        log_content: dict[str, Any] = {
            "from_username": from_username,
            "to_usernames": to_usernames,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "tweet_url": tweet_url,
            "tweet_text": raw_tweet.get("text", "")[:500],
            "confidence": event.get("confidence", 0),
            "enriched_from": enriched_from,
            "enriched_to": enriched_to,
            "parent_tweet": parent_tweet,
            "preclanker_at": preclanker_at,
            "buy_fee_pct": None,
            "sell_fee_pct": None,
        }
        await self.log_writer.preclanker(
            source_id=tweet_id,
            content=log_content,
            html_content=final_text,
        )

        # ------------------------------------------------------------------
        # STEP 3: Send to Telegram (best-effort, failure does NOT block)
        # ------------------------------------------------------------------
        buttons = self._build_preclanker_buttons(tweet_url, tweet_id)
        msg_id = await self._send_message(
            self._preclanker_channel, final_text, buttons, pool=True,
        )
        if not msg_id:
            logger.warning("[PRE-CLANKER] Telegram send failed (best-effort) -- log saved to Supabase")
        else:
            logger.info("[PRE-CLANKER] Sent Telegram message %d", msg_id)

        self.stats["messages_processed"] += 1

    # ==================================================================
    # DEPLOYED CLANKER HANDLER (tweet-based)
    # ==================================================================

    async def _handle_deployed_clanker(self, event: dict[str, Any]) -> None:
        """Handle DEPLOYED CLANKER tweet: link to PRE-CLANKER, send/edit msg.

        Simplified version of ``_handle_deployed`` -- the on-chain-first
        flow is already covered by ``_handle_clanker_token``.
        """
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        original_tweet_id = event.get("original_tweet_id", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        contract_address = event.get("contract_address", "")
        from_username = event.get("from_username", "")
        to_usernames: list[str] = event.get("to_usernames", []) or []

        # Global dedupe
        if not await self.redis.setnx(f"deployed_clanker_processed:{tweet_id}", "1"):
            return
        await self.redis.expire(f"deployed_clanker_processed:{tweet_id}", 3600)

        # Check PRE-CLANKER cache
        cached_data = None
        if original_tweet_id:
            cached_data = await self.storage.get_preclanker_data(original_tweet_id)
            if cached_data:
                from_username = cached_data.get("from_username") or from_username
                to_usernames = cached_data.get("to_usernames") or to_usernames
                token_name = token_name or cached_data.get("token_name", "")
                token_symbol = token_symbol or cached_data.get("token_symbol", "")

        # Fallback: get token info from Redis
        if (not token_name or not token_symbol) and contract_address:
            token_info = await self.storage.get_token_by_address(contract_address)
            if token_info:
                token_name = token_name or token_info.get("token_name", "")
                token_symbol = token_symbol or token_info.get("token_symbol", "")
        if (not token_name or not token_symbol) and original_tweet_id:
            token_info = await self.storage.get_token_info(original_tweet_id)
            if token_info:
                token_name = token_name or token_info.get("token_name", "")
                token_symbol = token_symbol or token_info.get("token_symbol", "")

        # Check for pending on-chain message (on-chain-first flow)
        onchain_msg = self._pending_onchain.pop(original_tweet_id, None)
        if not onchain_msg and contract_address:
            onchain_msg = self._pending_onchain.pop(contract_address.lower(), None)

        if onchain_msg:
            # Update existing on-chain message with full Twitter data
            logger.info(
                "[DEPLOYED-CLANKER] Updating on-chain message for %s ($%s)",
                token_name, token_symbol,
            )
            msg_id = onchain_msg.get("message_id", 0)
            channel = onchain_msg.get("channel", self._clanker_channel)
            token = onchain_msg.get("token", self._bot_token)

            enriched_from = cached_data.get("enriched_from") if cached_data else None
            enriched_to = cached_data.get("enriched_to", []) if cached_data else []

            if not enriched_from and from_username:
                enriched_from = await self._enrich_user(from_username)
            for i, uname in enumerate(to_usernames):
                if i >= len(enriched_to) or not enriched_to[i]:
                    data = await self._enrich_user(uname)
                    if i < len(enriched_to):
                        enriched_to[i] = data
                    else:
                        enriched_to.append(data)

            new_text = self._build_clanker_message(
                token_name, token_symbol, contract_address,
                onchain_msg.get("creator_wallet", ""),
                from_username, tweet_url, "",
            )
            buttons = self._build_clanker_buttons(contract_address, tweet_url, original_tweet_id)

            try:
                await self._api.edit_message(
                    token, channel, msg_id, new_text,
                    reply_markup={"inline_keyboard": buttons},
                )
            except Exception:
                logger.warning("[DEPLOYED-CLANKER] edit_message failed (non-fatal)")

            # Link PRE-CLANKER to on-chain token
            if contract_address and original_tweet_id:
                try:
                    await self.storage.update_token_address(original_tweet_id, contract_address)
                except Exception:
                    logger.warning("[DEPLOYED-CLANKER] update_token_address failed (non-fatal)")

            # Update Supabase log
            await self.log_writer.deployed_clanker(
                source_id=tweet_id,
                content={
                    "original_tweet_id": original_tweet_id,
                    "token_name": token_name,
                    "token_symbol": token_symbol,
                    "contract_address": contract_address.lower() if contract_address else "",
                    "from_username": from_username,
                    "to_usernames": to_usernames,
                    "tweet_url": tweet_url,
                    "enriched_from": enriched_from,
                    "enriched_to": enriched_to,
                    "on_chain": True,
                    "platform": "clanker",
                    "deployed_at": datetime.now(timezone.utc).isoformat(),
                },
                html_content=new_text,
            )

            self.stats["messages_processed"] += 1
            return

        # No on-chain message found -- send new DEPLOYED CLANKER message
        if contract_address:
            if await self.storage.has_notified_contract(contract_address):
                return
            await self.storage.mark_contract_notified(contract_address)

        logger.info(
            "[DEPLOYED-CLANKER] %s ($%s) contract=%s",
            token_name, token_symbol, contract_address[:16] if contract_address else "?",
        )

        enriched_from = cached_data.get("enriched_from") if cached_data else None
        enriched_to = cached_data.get("enriched_to", []) if cached_data else []

        if not enriched_from and from_username:
            enriched_from = await self._enrich_user(from_username)
        for i, uname in enumerate(to_usernames):
            if i >= len(enriched_to) or not enriched_to[i]:
                data = await self._enrich_user(uname)
                if data:
                    while len(enriched_to) <= i:
                        enriched_to.append(None)
                    enriched_to[i] = data

        text = self._build_clanker_message(
            token_name, token_symbol, contract_address, "",
            from_username, tweet_url, "",
        )
        buttons = self._build_clanker_buttons(contract_address, tweet_url, original_tweet_id)

        # Log to Supabase ALWAYS
        await self.log_writer.deployed_clanker(
            source_id=tweet_id,
            content={
                "original_tweet_id": original_tweet_id,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "contract_address": contract_address.lower() if contract_address else "",
                "from_username": from_username,
                "to_usernames": to_usernames,
                "tweet_url": tweet_url,
                "enriched_from": enriched_from,
                "enriched_to": enriched_to,
                "on_chain": False,
                "platform": "clanker",
                "detected_at": cached_data.get("detected_at") if cached_data else None,
                "deployed_at": datetime.now(timezone.utc).isoformat(),
            },
            html_content=text,
        )

        # Send to Telegram
        msg_id = await self._send_message(
            self._clanker_channel, text, buttons, pool=True,
        )
        if not msg_id:
            logger.warning("[DEPLOYED-CLANKER] Telegram send failed (best-effort)")
        else:
            logger.info("[DEPLOYED-CLANKER] Sent Telegram message %d", msg_id)

        self.stats["messages_processed"] += 1

    # ==================================================================
    # ADMIN BLACKLIST POLLING
    # ==================================================================

    async def _blacklist_poll_loop(self) -> None:
        """Poll for admin blacklist commands."""
        if not self._admin_token or not self._admin_group:
            logger.info("[ADMIN] No admin token/group -- skipping blacklist polling")
            return

        logger.info("[ADMIN] Starting blacklist command poll loop")
        while self._running:
            try:
                updates = await self._api.get_updates(
                    self._admin_token,
                    offset=self._last_update_id + 1 if self._last_update_id else None,
                    timeout=30,
                )
                for update in updates:
                    self._last_update_id = update.get("update_id", self._last_update_id)
                    msg = update.get("message")
                    if msg:
                        await self._handle_admin_command(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.debug("[ADMIN] Poll error")
                await asyncio.sleep(5)

    async def _handle_admin_command(self, message: dict[str, Any]) -> None:
        """Handle admin blacklist commands."""
        chat_id = message.get("chat", {}).get("id", 0)
        admin_user_id = message.get("from", {}).get("id")
        text = (message.get("text") or "").strip().lower()

        if str(chat_id) != self._admin_group:
            return
        if not text or not text.startswith("/"):
            return

        parts = text.split()
        cmd = parts[0]

        if cmd == "/blacklist" and len(parts) >= 3 and parts[1] == "bankr":
            if parts[2] == "word" and len(parts) >= 4:
                # Add words to blacklist
                words = parts[3:]
                added = 0
                for word in words:
                    word = word.strip().lower()
                    if word:
                        await self.db.add_blacklist("word", "bankr", word, created_by=admin_user_id)
                        await self.redis.hset("prebankr:wordblacklist:list", {word: word})
                        added += 1
                await self._api.send_message(
                    self._admin_token, chat_id,
                    f"Added {added} word(s) to blacklist",
                )
            else:
                # Add handles to blacklist
                handles = parts[2:]
                added = 0
                for h in handles:
                    clean = h.strip().lstrip("@")
                    if "x.com/" in clean or "twitter.com/" in clean:
                        clean = clean.split("/")[-1].split("?")[0]
                    if clean:
                        await self.db.add_blacklist("handle", "bankr", clean, created_by=admin_user_id)
                        await self.redis.set(f"prebankr:blacklist:handle:{clean.lower()}", "1")
                        added += 1
                await self._api.send_message(
                    self._admin_token, chat_id,
                    f"Added {added} handle(s) to blacklist",
                )

        elif cmd == "/listblacklist" and len(parts) >= 2 and parts[1] == "bankr":
            bl_type = "word" if len(parts) >= 3 and parts[2] == "word" else "handle"
            entries = await self.db.list_blacklist(bl_type, "bankr")
            if not entries:
                await self._api.send_message(
                    self._admin_token, chat_id,
                    f"Blacklist ({bl_type}) is empty",
                )
            else:
                lines = [f"<b>BANKR {bl_type.upper()} Blacklist ({len(entries)})</b>\n"]
                for e in entries:
                    lines.append(f"  #{e['id']}. {e['value']}")
                await self._api.send_message(
                    self._admin_token, chat_id, "\n".join(lines),
                )

        elif cmd == "/removeblacklist" and len(parts) >= 3 and parts[1] == "bankr":
            try:
                bl_id = int(parts[-1])
                # Fetch entry before deletion to know its type and value for Redis cleanup
                entry = await self.db.fetch_one(
                    "SELECT * FROM blacklists WHERE id = $1", bl_id
                )
                if entry is None:
                    await self._api.send_message(
                        self._admin_token, chat_id, f"Entry #{bl_id} not found",
                    )
                    return

                entry_dict = dict(entry)
                bl_type = entry_dict.get("type", "")
                bl_value = entry_dict.get("value", "")

                await self.db.remove_blacklist(bl_id)

                # Clean corresponding Redis keys to maintain dual-write consistency
                if bl_type == "handle" and bl_value:
                    await self.redis.delete(f"prebankr:blacklist:handle:{bl_value.lower()}")
                    await self.redis.hdel("prebankr:blacklist:list", str(bl_id))
                elif bl_type == "word" and bl_value:
                    await self.redis.delete(f"prebankr:wordblacklist:word:{bl_value.lower()}")
                    await self.redis.hdel("prebankr:wordblacklist:list", str(bl_id))

                await self._api.send_message(
                    self._admin_token, chat_id,
                    f"Removed #{bl_id} ({bl_type}: {bl_value})",
                )
            except ValueError:
                await self._api.send_message(
                    self._admin_token, chat_id, "Invalid ID",
                )

    # ==================================================================
    # NEW INTERFACE ALERT
    # ==================================================================

    async def _send_new_interface_alert(
        self, event: dict[str, Any], interface: str
    ) -> None:
        """Send alert for new on-chain interface."""
        token_address = event.get("token_address", "")
        token_symbol = event.get("token_symbol", "")
        token_name = event.get("token_name", "")

        context = event.get("context_parsed", {})
        metadata = event.get("metadata_parsed", {})
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except (json.JSONDecodeError, TypeError):
                context = {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}

        text = (
            "<b>NEW INTERFACE DETECTED</b>\n\n"
            f"Interface: <code>{interface}</code>\n"
            f"Token: {token_name} (${token_symbol})\n"
            f"Contract: <code>{token_address}</code>\n\n"
            f"Context: <code>{json.dumps(context, indent=2, ensure_ascii=False)[:500]}</code>\n\n"
            f"Metadata: <code>{json.dumps(metadata, indent=2, ensure_ascii=False)[:500]}</code>"
        )

        buttons = [[
            {"text": "Basescan", "url": f"https://basescan.org/address/{token_address}"},
            {"text": "Clanker", "url": f"https://www.clanker.world/clanker/{token_address}"},
        ]]

        token = self._clawnch_token or self._bot_token
        if token:
            await self._api.send_message(
                token, self._newinterface_channel, text,
                reply_markup={"inline_keyboard": buttons},
            )
            await self.log_writer.new_interface(
                source_id=token_address,
                content={"interface": interface, "token_symbol": token_symbol},
            )

    # ==================================================================
    # USER ENRICHMENT
    # ==================================================================

    async def _enrich_user(self, username: str) -> dict[str, Any] | None:
        """Enrich a user with parallel API calls and caching."""
        if not username:
            return None

        username = username.lstrip("@")
        cache_key = username.lower()

        # Memory cache (check for stale entries without deploys_made)
        if cache_key in self._user_cache:
            mem = self._user_cache[cache_key]
            if "deploys_made" in mem:
                return mem.copy()
            # Stale cache without deploy stats — re-enrich
            del self._user_cache[cache_key]

        # Redis cache (check for stale entries without deploys_made)
        cached = await self.storage.get_enriched_user(username)
        if cached and "deploys_made" in cached:
            self._user_cache[cache_key] = cached
            return cached.copy()
        # If cached but missing deploys_made, treat as stale and re-enrich

        user_data: dict[str, Any] = {
            "username": username,
            "user_id": "",
            "name": username,
            "followers": 0,
            "following": 0,
            "is_blue_verified": False,
            "verified_type": "",
            "bio": "",
            "created_at": "",
            "profile_image_url": "",
            "smart_followers": 0,
            "prev_usernames_count": 0,
            "contracts_count": 0,
            "affiliate_label": "",
        }

        # Parallel API calls
        tasks: list[tuple[str, Any]] = []

        # Check Twitter cache
        tw_cache = await self.redis.hgetall(f"twitter_user:{cache_key}")
        if tw_cache:
            user_data.update({
                "user_id": tw_cache.get("user_id", ""),
                "followers": int(tw_cache.get("followers", 0)),
                "following": int(tw_cache.get("following", 0)),
                "is_blue_verified": tw_cache.get("is_blue_verified") in ("1", "true", "True"),
                "verified_type": tw_cache.get("verified_type", ""),
                "bio": tw_cache.get("bio", ""),
                "affiliate_label": tw_cache.get("affiliate_label", ""),
                "name": tw_cache.get("name", username),
                "created_at": tw_cache.get("created_at", ""),
                "profile_image_url": tw_cache.get("profile_image_url", ""),
            })
        else:
            tasks.append(("twitter", self._fetch_twitter(username)))
            tasks.append(("twitter_about", self._fetch_twitter_about(username)))

        ag_cache = await self.storage.get_alphagate_data(username)
        if ag_cache:
            user_data.update({
                "prev_usernames_count": ag_cache["prev_usernames_count"],
                "contracts_count": ag_cache["contracts_count"],
                "smart_followers": ag_cache["smart_followers"],
            })
        else:
            tasks.append(("alphagate", self._fetch_alphagate(username)))

        if tasks:
            results = await asyncio.gather(
                *[t[1] for t in tasks], return_exceptions=True,
            )
            for i, (name, _) in enumerate(tasks):
                r = results[i]
                if isinstance(r, Exception) or not r:
                    continue
                if name == "twitter":
                    user_data.update({
                        "user_id": r.get("id", "") or r.get("userId", ""),
                        "name": r.get("name", username),
                        "followers": r.get("followers", 0),
                        "following": r.get("following", 0),
                        "is_blue_verified": r.get("isBlueVerified", False),
                        "verified_type": r.get("verifiedType", "") or "",
                        "bio": r.get("description", ""),
                        "created_at": r.get("createdAt", ""),
                        "profile_image_url": r.get("profileImageUrl", "") or r.get("profilePicture", "") or "",
                    })
                    # Save to Twitter cache
                    await self.redis.hset(f"twitter_user:{cache_key}", {
                        "user_id": str(user_data.get("user_id", "")),
                        "name": user_data["name"],
                        "followers": str(user_data["followers"]),
                        "following": str(user_data.get("following", 0)),
                        "is_blue_verified": "1" if user_data["is_blue_verified"] else "0",
                        "verified_type": user_data.get("verified_type", ""),
                        "bio": user_data.get("bio", ""),
                        "created_at": user_data.get("created_at", ""),
                        "profile_image_url": user_data.get("profile_image_url", ""),
                    })
                    await self.redis.expire(f"twitter_user:{cache_key}", 86400)
                elif name == "twitter_about":
                    user_data["affiliate_label"] = r.get("affiliate_label", "")
                elif name == "alphagate":
                    user_data.update(r)

        # Fetch deploy stats from Supabase (cached in Redis)
        try:
            deploy_stats = await self.storage.get_deploy_stats(username)
            user_data["deploys_made"] = deploy_stats.get("deploys_made", 0)
            user_data["deploys_received"] = deploy_stats.get("deploys_received", 0)
        except Exception:
            logger.debug("[ENRICH] Deploy stats failed for %s", username)
            user_data["deploys_made"] = 0
            user_data["deploys_received"] = 0

        # Save enriched user
        await self.storage.save_enriched_user(username, user_data)
        self._user_cache[cache_key] = user_data

        # Limit cache size
        if len(self._user_cache) > 1000:
            keys = list(self._user_cache.keys())
            for k in keys[:500]:
                del self._user_cache[k]

        return user_data

    async def _enrich_creator_and_update(
        self, msg_id: int, current_text: str, buttons: list,
        token_name: str, token_symbol: str, token_address: str,
        creator_wallet: str, platform: str, tweet_url: str,
        enriched_from: dict | None, enriched_to: list,
    ) -> None:
        """Fetch creator wallet info and update message."""
        # This would use WS RPC for balance/tx count -- simplified for now
        if creator_wallet and self._pending_onchain.get(token_address):
            self._pending_onchain[token_address]["creator_info"] = {
                "wallet": creator_wallet,
            }

    async def _bg_enrich_onchain(
        self, msg_id: int, buttons: list,
        token_name: str, token_symbol: str, token_address: str,
        creator_wallet: str, platform: str, tweet_url: str,
        tweet_id: str,
    ) -> None:
        """Background enrichment for on-chain message."""
        username = self._extract_username_from_url(tweet_url)
        if not username:
            return

        data = await self._enrich_user(username)
        if data:
            text = self._build_onchain_message(
                token_name, token_symbol, token_address, creator_wallet,
                platform, tweet_url, data, [],
            )
            await self._edit_message(
                self._deployed_channel, msg_id, text, buttons, pool=True,
            )

    # ==================================================================
    # API CALLS
    # ==================================================================

    async def _fetch_twitter(self, username: str) -> dict[str, Any] | None:
        if not self._twitter_api_key:
            return None
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/info?userName={username}",
                headers={"X-API-Key": self._twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
        except Exception:
            pass
        return None

    async def _fetch_tweet_by_id(self, tweet_id: str) -> dict[str, Any] | None:
        """Fetch a single tweet by ID via twitterapi.io."""
        if not self._twitter_api_key or not tweet_id:
            return None
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/tweets?tweet_ids={tweet_id}",
                headers={"X-API-Key": self._twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    tweets = data.get("tweets", [])
                    if tweets:
                        return tweets[0]
        except Exception:
            logger.debug("[ENRICH] Failed to fetch tweet %s", tweet_id)
        return None

    async def _extract_parent_tweet(self, raw_tweet: dict[str, Any]) -> dict[str, Any] | None:
        """Extract parent tweet data from raw_tweet (quotedTweet or reply parent).

        Returns a dict with normalised fields, or None if unavailable.
        """
        parent: dict[str, Any] | None = None

        # 1. Try quotedTweet (embedded in raw_tweet for quote tweets)
        quoted = raw_tweet.get("quotedTweet") or raw_tweet.get("quoted_tweet")
        if quoted and isinstance(quoted, dict):
            parent = quoted

        # 2. Fallback: fetch reply parent by inReplyToId
        if not parent:
            in_reply_to = raw_tweet.get("inReplyToId", "")
            if in_reply_to:
                parent = await self._fetch_tweet_by_id(in_reply_to)

        if not parent:
            return None

        # Normalise author data
        author = parent.get("author", {}) or {}
        author_username = (
            author.get("userName", "") or author.get("username", "")
            or parent.get("author_username", "")
        )

        # Extract media URL (first image if any)
        media_url = ""
        media_list = parent.get("media", []) or parent.get("extendedEntities", {}).get("media", []) or []
        if media_list and isinstance(media_list, list):
            first = media_list[0] if media_list else {}
            media_url = first.get("media_url_https", "") or first.get("url", "") or ""

        # Build parent tweet URL
        parent_id = parent.get("id", "") or parent.get("tweetId", "")
        parent_url = parent.get("url", "")
        if not parent_url and parent_id and author_username:
            parent_url = f"https://x.com/{author_username}/status/{parent_id}"

        result: dict[str, Any] = {
            "tweet_id": str(parent_id),
            "author_username": author_username,
            "author_name": author.get("name", "") or parent.get("author_name", ""),
            "author_followers": author.get("followers", 0) or author.get("followersCount", 0),
            "author_bio": (author.get("description", "") or author.get("bio", ""))[:300],
            "author_profile_image": author.get("profileImageUrl", "") or author.get("profilePicture", "") or author.get("profile_image_url", ""),
            "author_verified": bool(author.get("isBlueVerified", False)),
            "author_verified_type": author.get("verifiedType", "") or "",
            "text": (parent.get("text", "") or "")[:500],
            "url": parent_url,
            "media_url": media_url,
        }

        # Enrich parent author with smart_followers if we can
        if author_username:
            enriched = await self._enrich_user(author_username)
            if enriched:
                result["author_followers"] = enriched.get("followers", result["author_followers"])
                result["author_smart_followers"] = enriched.get("smart_followers", 0)
                result["author_bio"] = enriched.get("bio", result["author_bio"])[:300]
                result["author_profile_image"] = enriched.get("profile_image_url", result["author_profile_image"])
                result["author_verified"] = enriched.get("is_blue_verified", result["author_verified"])
                result["author_verified_type"] = enriched.get("verified_type", result["author_verified_type"])
                result["author_name"] = enriched.get("name", result["author_name"])
                result["author_prev_usernames_count"] = enriched.get("prev_usernames_count", 0)
                result["author_contracts_count"] = enriched.get("contracts_count", 0)
                result["author_deploys_made"] = enriched.get("deploys_made", 0)
                result["author_deploys_received"] = enriched.get("deploys_received", 0)
                result["author_affiliate_label"] = enriched.get("affiliate_label", "")

        return result

    async def _fetch_twitter_about(self, username: str) -> dict[str, Any] | None:
        if not self._twitter_api_key:
            return None
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/about?userName={username}",
                headers={"X-API-Key": self._twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    highlights = data.get("data", {}).get("highlightedLabel", {})
                    if highlights.get("label"):
                        return {"affiliate_label": highlights["label"]}
        except Exception:
            pass
        return None

    async def _fetch_alphagate(self, username: str) -> dict[str, int] | None:
        if not self._alphagate_user_id or not self._alphagate_session_id:
            return None
        try:
            async with self._session.get(
                f"https://api.alphagate.io/api/v1/child?username={username}",
                headers={
                    "Cookie": (
                        f"userId={self._alphagate_user_id}; "
                        f"extensionSessionId={self._alphagate_session_id}"
                    ),
                    "Origin": "https://alphagate.io",
                    "Referer": "https://alphagate.io/",
                },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    child = data.get("data", {}).get("child", {}) or {}
                    result = {
                        "prev_usernames_count": len(child.get("prev_usernames", []) or []),
                        "contracts_count": len(child.get("contracts", []) or []),
                        "smart_followers": child.get("key_followers_count", 0) or 0,
                    }
                    await self.storage.save_alphagate_data(
                        username,
                        result["prev_usernames_count"],
                        result["contracts_count"],
                        result["smart_followers"],
                    )
                    return result
        except Exception:
            pass
        return None

    # ==================================================================
    # MESSAGE BUILDING (v1-style rich formatting)
    # ==================================================================

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape HTML special characters for Telegram."""
        if not text:
            return ""
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @staticmethod
    def _format_followers(followers: int) -> str:
        """Format follower count with Brazilian dot separator."""
        if followers >= 1_000_000:
            return f"{followers / 1_000_000:.2f}M"
        elif followers >= 1_000:
            return f"{followers:,.0f}".replace(",", ".")
        return str(followers)

    @staticmethod
    def _format_date(created_at: str) -> str:
        """Format ISO date to DD MMM YYYY."""
        if not created_at:
            return ""
        try:
            dt = datetime.fromisoformat(
                created_at.replace("Z", "+00:00").replace(".000000", "")
            )
            return dt.strftime("%d %b %Y")
        except Exception:
            return created_at[:10] if len(created_at) >= 10 else created_at

    def _format_user(
        self,
        data: dict[str, Any] | None,
        prefix: str = "",
        is_from_user: bool = False,
    ) -> str:
        """Format a user data dict into rich HTML lines (v1 style).

        Shows: Twitter link, wallet, followers, smart followers with alert,
        created date, bio, MOLT Agent banners.
        """
        if not data:
            lines = [
                f'👑 <b>Twitter:</b> @{prefix.replace("FROM: ", "").replace("TO[0]: ", "").replace("TO[1]: ", "").strip()}',
                "📁 <b>Wallet:</b>",
                "👤 <b>Followers:</b> ⏳",
                "⭐️ <b>Smart Followers:</b> ⏳",
            ]
            return "\n".join(lines)

        username = data.get("username", "?")
        try:
            followers = int(data.get("followers", 0) or 0)
        except (ValueError, TypeError):
            followers = 0
        try:
            sf = int(data.get("smart_followers", 0) or 0)
        except (ValueError, TypeError):
            sf = 0
        try:
            r_count = int(data.get("prev_usernames_count", 0) or 0)
        except (ValueError, TypeError):
            r_count = 0
        try:
            c_count = int(data.get("contracts_count", 0) or 0)
        except (ValueError, TypeError):
            c_count = 0

        is_verified = data.get("is_blue_verified", False)
        if isinstance(is_verified, str):
            is_verified = is_verified in ("1", "true", "True")
        verified_type = data.get("verified_type", "")
        bio = data.get("bio", "")
        created_at = data.get("created_at", "")
        wallet = data.get("wallet", "")
        affiliate_label = data.get("affiliate_label", "")
        is_molt_agent = data.get("is_molt_agent", False)
        molt_verified = data.get("molt_verified", False)
        passes_molt_filter = data.get("passes_molt_filter", False)
        vip_followers = data.get("vip_followers", []) or []

        lines: list[str] = []

        # MOLT AGENT FILTER BANNER
        if is_molt_agent and passes_molt_filter:
            lines.extend(["🛑🛑🛑🛑🛑🛑", "   <b>MOLT AGENT FILTER</b> ✅", "🛑🛑🛑🛑🛑🛑", ""])

        # MOLT AGENT BANNER
        if is_molt_agent:
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            if molt_verified:
                lines.append("   <b>MOLT AGENT VERIFIED</b> ✅")
            else:
                molt_name = data.get("molt_agent_name", "")
                lines.append(f"   <b>MOLT AGENT</b>{' — ' + self._escape_html(molt_name) if molt_name else ''}")
            lines.extend(["🟠🟠🟠🟠🟠🟠🟠🟠🟠", ""])

        # Badge
        if is_verified:
            badge = " ☑️" if verified_type == "Business" else " ✅"
        else:
            badge = ""

        label_str = f" {self._escape_html(affiliate_label)}" if affiliate_label else ""

        # 👑 Twitter line
        twitter_url = f"https://x.com/{username}"
        twitter_line = f'👑 <b>Twitter:</b> <a href="{twitter_url}">@{self._escape_html(username)}</a>'
        twitter_line += f" (R: {r_count} | C: {c_count})"
        twitter_line += badge
        twitter_line += label_str
        lines.append(twitter_line)

        # 📁 Wallet
        if wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{wallet}</code>")
        else:
            lines.append("📁 <b>Wallet:</b>")

        # 👤 Followers
        f_str = self._format_followers(followers)
        lines.append(f"👤 <b>Followers:</b> {f_str}")

        # ⭐️ Smart Followers with alert
        sf_threshold = 15 if is_from_user else 30
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if sf >= sf_threshold else ""
        lines.append(f"⭐️ <b>Smart Followers:</b> {sf}+{sf_emoji}")

        # VIP followers
        if vip_followers:
            for vip in vip_followers[:5]:
                lines.append(f"└ @{self._escape_html(str(vip))} 🔑")

        # 📅 Criado em
        date_str = self._format_date(created_at)
        if date_str:
            lines.append(f"📅 <b>Criado em:</b> {date_str}")

        # 📕 Bio
        if bio:
            bio_preview = self._escape_html(bio[:200])
            lines.append(f"📕 <b>Bio:</b> {bio_preview}")

        return "\n".join(lines)

    def _build_prebankr_initial(
        self, from_username: str, to_usernames: list[str],
        tweet_url: str, token_name: str, token_symbol: str,
        tweet_text: str, tweet_id: str,
    ) -> str:
        """Build initial PRE-BANKR message with loading placeholders (v1 style)."""
        tn = self._escape_html(token_name)
        ts = self._escape_html(token_symbol)
        symbol_display = f"[{ts}]" if ts else ""

        lines = [
            f"🟢 <b>NEW PRE BANKR</b>  {symbol_display}",
            "",
        ]
        if tn:
            lines.append(f"<code><b>{tn}</b> (${ts})</code>")
            lines.append("")

        # FROM (loading)
        lines.append("━━━━━━━💼 <b>FROM</b>━━━━━━━")
        lines.append("")
        lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{from_username}">@{self._escape_html(from_username)}</a>')
        lines.append("📁 <b>Wallet:</b>")
        lines.append("👤 <b>Followers:</b> ⏳")
        lines.append("⭐️ <b>Smart Followers:</b> ⏳")

        # Tweet text
        if tweet_text:
            lines.append("")
            lines.append(f"📖 <b>Tweet:</b> {self._escape_html(tweet_text[:200])}")

        # TO (loading)
        if to_usernames:
            lines.append("")
            lines.append("━━━━━━━📬 <b>TO</b>━━━━━━━━")
            for uname in to_usernames:
                lines.append("")
                lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{uname}">@{self._escape_html(uname)}</a>')
                lines.append("📁 <b>Wallet:</b>")
                lines.append("👤 <b>Followers:</b> ⏳")
                lines.append("⭐️ <b>Smart Followers:</b> ⏳")

        # Footer
        if tn:
            lines.append("")
            footer = tn
            if ts:
                footer += f" ({ts})"
            lines.append(f"<b>{footer} - BANKR</b>")

        return "\n".join(lines)

    def _build_prebankr_enriched(
        self, enriched_from: dict | None, enriched_to: list,
        tweet_url: str, token_name: str, token_symbol: str,
        tweet_text: str, tweet_id: str,
    ) -> str:
        """Build enriched PRE-BANKR message (v1 style)."""
        tn = self._escape_html(token_name)
        ts = self._escape_html(token_symbol)
        symbol_display = f"[{ts}]" if ts else ""

        lines = [
            f"🟢 <b>NEW PRE BANKR</b>  {symbol_display}",
            "",
        ]
        if tn:
            lines.append(f"<code><b>{tn}</b> (${ts})</code>")
            lines.append("")

        # FROM
        lines.append("━━━━━━━💼 <b>FROM</b>━━━━━━━")
        lines.append("")
        lines.append(self._format_user(enriched_from, is_from_user=True))

        # Tweet text
        if tweet_text:
            lines.append("")
            lines.append(f"📖 <b>Tweet:</b> {self._escape_html(tweet_text[:200])}")

        # TO
        if enriched_to:
            lines.append("")
            lines.append("━━━━━━━📬 <b>TO</b>━━━━━━━━")
            for data in enriched_to:
                lines.append("")
                lines.append(self._format_user(data, is_from_user=False))

        # Footer
        if tn:
            lines.append("")
            footer = tn
            if ts:
                footer += f" ({ts})"
            lines.append(f"<b>{footer} - BANKR</b>")

        return "\n".join(lines)

    @staticmethod
    def _build_prebankr_buttons(
        tweet_url: str, tweet_id: str
    ) -> list[list[dict[str, Any]]]:
        """Build inline buttons for PRE-BANKR message (v1 style).

        Single row: [Tweet] [From] [❤️ Like] [💰 BUY]
        """
        row: list[dict[str, Any]] = []
        if tweet_url:
            row.append({"text": "Tweet", "url": tweet_url})
        if tweet_id:
            row.append({"text": "❤️ Like", "callback_data": f"like:{tweet_id}"})
            row.append({"text": "💰 BUY", "callback_data": f"buy:{tweet_id}"})
        return [row] if row else []

    def _build_preclanker_enriched(
        self, enriched_from: dict | None, enriched_to: list,
        tweet_url: str, token_name: str, token_symbol: str,
        tweet_text: str, tweet_id: str,
    ) -> str:
        """Build enriched PRE-CLANKER message (mirrors prebankr but with Clanker branding)."""
        tn = self._escape_html(token_name)
        ts = self._escape_html(token_symbol)
        symbol_display = f"[{ts}]" if ts else ""

        lines = [
            f"🟣 <b>NEW PRE CLANKR</b>  {symbol_display}",
            "",
        ]
        if tn:
            lines.append(f"<code><b>{tn}</b> (${ts})</code>")
            lines.append("")

        # FROM
        lines.append("━━━━━━━💼 <b>FROM</b>━━━━━━━")
        lines.append("")
        lines.append(self._format_user(enriched_from, is_from_user=True))

        # Tweet text
        if tweet_text:
            lines.append("")
            lines.append(f"📖 <b>Tweet:</b> {self._escape_html(tweet_text[:200])}")

        # TO
        if enriched_to:
            lines.append("")
            lines.append("━━━━━━━📬 <b>TO</b>━━━━━━━━")
            for data in enriched_to:
                lines.append("")
                lines.append(self._format_user(data, is_from_user=False))

        # Footer
        if tn:
            lines.append("")
            footer = tn
            if ts:
                footer += f" ({ts})"
            lines.append(f"<b>{footer} - CLANKR</b>")

        return "\n".join(lines)

    @staticmethod
    def _build_preclanker_buttons(
        tweet_url: str, tweet_id: str
    ) -> list[list[dict[str, Any]]]:
        """Build inline buttons for PRE-CLANKER message.

        Single row: [Tweet] [❤️ Like] [💰 BUY]
        """
        row: list[dict[str, Any]] = []
        if tweet_url:
            row.append({"text": "Tweet", "url": tweet_url})
        if tweet_id:
            row.append({"text": "❤️ Like", "callback_data": f"like:{tweet_id}"})
            row.append({"text": "💰 BUY", "callback_data": f"buy:{tweet_id}"})
        return [row] if row else []

    def _build_deployed_message(
        self, token_name: str, token_symbol: str, contract: str,
        from_username: str, to_usernames: list[str], tweet_url: str,
        enriched_from: dict | None, enriched_to: list,
        creator_wallet: str = "", creator_info: dict | None = None,
        liked_by: list[str] | None = None,
        original_tweet_url: str = "",
    ) -> str:
        """Build DEPLOYED BANKR message (v1 style)."""
        tn = self._escape_html(token_name)
        ts = self._escape_html(token_symbol)

        lines = [
            f"🟠 <b>DEPLOYED BANKR ({ts})</b>",
            " ",
            f"<code><b>{tn}</b> (${ts})</code>",
            "",
        ]

        # Likes: ❤️❤️❤️ usernames
        if liked_by:
            hearts = "❤️" * min(len(liked_by), 5)
            usernames = ", ".join(liked_by[:5])
            if len(liked_by) > 5:
                usernames += f" +{len(liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
            lines.append("")

        # FROM
        lines.append("━━━━━━━💼 FROM━━━━━━━")
        lines.append("")
        lines.append(self._format_user(enriched_from, is_from_user=True))

        # TO
        if enriched_to:
            lines.append("")
            lines.append("━━━━━━━📬 TO━━━━━━━━")
            for data in (enriched_to or []):
                lines.append("")
                lines.append(self._format_user(data, is_from_user=False))

        # Contract
        lines.append("")
        lines.append(f"🔖 <b>Contract:</b> <code>{contract}</code>")

        # Creator info
        if creator_wallet:
            lines.append("")
            lines.append(f"👤 <b>Creator:</b> <code>{creator_wallet}</code>")
            if creator_info:
                holding = creator_info.get("holding", "")
                balance = creator_info.get("balance", "")
                txs = creator_info.get("txs", "")
                if holding:
                    lines.append(f"           ├─ Holding → {holding}")
                if balance:
                    lines.append(f"           ├─ Balance → {balance}")
                if txs:
                    lines.append(f"           └─ Txs → {txs}")

        # Footer: token name + wallets
        lines.append("")
        footer = tn
        if ts:
            footer += f" ({ts})"
        lines.append(f"<b>{footer} - BANKR</b>")

        # FROM/TO wallets
        from_wallet = (enriched_from or {}).get("wallet", "")
        if from_wallet:
            lines.append(f"💼 FROM: <code>{from_wallet}</code>")
        if enriched_to:
            to_wallet = (enriched_to[0] or {}).get("wallet", "") if enriched_to else ""
            if to_wallet:
                lines.append(f"📬 TO: <code>{to_wallet}</code>")

        # Tweet links
        tweet_links = []
        if original_tweet_url:
            tweet_links.append(f'<a href="{original_tweet_url}">Tweet</a>')
        if tweet_url:
            tweet_links.append(f'<a href="{tweet_url}">Tweet Bankr</a>')
        if tweet_links:
            lines.append("")
            lines.append(" | ".join(tweet_links))

        # Likes at footer too
        if liked_by:
            lines.append("")
            hearts = "❤️" * min(len(liked_by), 5)
            usernames = ", ".join(liked_by[:5])
            if len(liked_by) > 5:
                usernames += f" +{len(liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")

        return "\n".join(lines)

    @staticmethod
    def _build_deployed_buttons(
        contract: str, tweet_url: str, tweet_id: str
    ) -> list[list[dict[str, Any]]]:
        """Build inline buttons for DEPLOYED message (v1 style).

        Row 1: [Sigma] [GMGN] [VIP] [Clanker]
        Row 2: [Like] [Buy] [Tweet]
        """
        row1: list[dict[str, Any]] = []
        if contract:
            row1.append({
                "text": "Sigma",
                "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{contract}",
            })
            row1.append({
                "text": "GMGN",
                "url": f"https://gmgn.ai/base/token/qquante_{contract}",
            })
            row1.append({
                "text": "VIP",
                "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{contract}",
            })
            row1.append({
                "text": "Clanker",
                "url": f"https://www.clanker.world/clanker/{contract}",
            })
        row2: list[dict[str, Any]] = []
        if tweet_id:
            row2.append({"text": "❤️ Like", "callback_data": f"like:{tweet_id}"})
            row2.append({"text": "💰 Buy", "callback_data": f"buy:{tweet_id}"})
        if tweet_url:
            row2.append({"text": "Tweet", "url": tweet_url})
        rows = []
        if row1:
            rows.append(row1)
        if row2:
            rows.append(row2)
        return rows

    def _build_onchain_message(
        self, token_name: str, token_symbol: str, contract: str,
        creator_wallet: str, platform: str, tweet_url: str,
        enriched_from: dict | None, enriched_to: list,
    ) -> str:
        """Build on-chain token message (v1 style)."""
        tn = self._escape_html(token_name)
        ts = self._escape_html(token_symbol)

        lines = [
            f"🟠 <b>DEPLOYED BANKR ({ts})</b>",
            " ",
            f"<code><b>{tn}</b> (${ts})</code>",
            "",
        ]

        # FROM
        if enriched_from:
            lines.append("━━━━━━━💼 FROM━━━━━━━")
            lines.append("")
            lines.append(self._format_user(enriched_from, is_from_user=True))

        # TO
        if enriched_to:
            lines.append("")
            lines.append("━━━━━━━📬 TO━━━━━━━━")
            for data in (enriched_to or []):
                if data:
                    lines.append("")
                    lines.append(self._format_user(data, is_from_user=False))

        # Contract
        lines.append("")
        lines.append(f"🔖 <b>Contract:</b> <code>{contract}</code>")

        # Creator
        if creator_wallet:
            lines.append("")
            lines.append(f"👤 <b>Creator:</b> <code>{creator_wallet}</code>")

        # Platform
        if platform:
            lines.append(f"🌐 Platform: {platform}")

        # Footer
        lines.append("")
        footer = tn
        if ts:
            footer += f" ({ts})"
        lines.append(f"<b>{footer} - BANKR</b>")

        return "\n".join(line for line in lines if line)

    @staticmethod
    def _build_clawnch_message(
        token_name: str, token_symbol: str, contract: str,
        deployer: str, platform: str, agent_name: str,
        x_handle: str, post_id: str,
        molt_agent: dict | None, twitter_data: dict | None,
        alphagate_data: dict | None,
    ) -> str:
        """Build Clawnch deployment message."""
        lines = [
            f"<b>DEPLOYED CLAWNCH ({platform.upper()})</b>",
            "",
            f"Token: <b>{token_name}</b> (${token_symbol})",
            f"Contract: <code>{contract}</code>",
        ]
        if deployer:
            lines.append(f"Deployer: <code>{deployer[:20]}...</code>")
        if agent_name:
            lines.append(f"Agent: <b>{agent_name}</b>")
        if x_handle:
            lines.append(f"X: @{x_handle}")
        lines.append("")

        if molt_agent:
            karma = molt_agent.get("karma", 0)
            followers = molt_agent.get("followers", 0)
            verified = molt_agent.get("is_verified", False)
            lines.append(
                f"MOLT: karma={karma} followers={followers}"
                + (" [verified]" if verified in ("1", "true", True) else "")
            )

        if twitter_data:
            tw_followers = twitter_data.get("followers", 0)
            if isinstance(tw_followers, str):
                tw_followers = int(tw_followers) if tw_followers.isdigit() else 0
            tw_verified = twitter_data.get("is_blue_verified", "0")
            lines.append(
                f"Twitter: {tw_followers:,} followers"
                + (" [verified]" if tw_verified in ("1", "true", True) else "")
            )

        if alphagate_data:
            lines.append(
                f"AlphaGate: R={alphagate_data.get('prev_usernames_count', 0)} "
                f"C={alphagate_data.get('contracts_count', 0)} "
                f"SF={alphagate_data.get('smart_followers', 0)}"
            )

        return "\n".join(lines)

    @staticmethod
    def _build_clawnch_buttons(
        contract: str, post_id: str, platform: str,
    ) -> list[list[dict[str, Any]]]:
        """Build inline buttons for Clawnch message."""
        row1 = []
        if contract:
            row1.append({
                "text": "Basescan",
                "url": f"https://basescan.org/token/{contract}",
            })
            row1.append({
                "text": "GMGN",
                "url": f"https://gmgn.ai/base/token/{contract}",
            })
            row1.append({
                "text": "Clanker",
                "url": f"https://www.clanker.world/clanker/{contract}",
            })
        row2 = []
        if post_id:
            row2.append({"text": "Like", "callback_data": f"like:{post_id}"})
            row2.append({"text": "Buy", "callback_data": f"buy:{post_id}"})

            # Platform-specific link
            if platform.lower() == "moltbook":
                row2.append({
                    "text": "MoltBook",
                    "url": f"https://www.moltbook.com/post/{post_id}",
                })
            elif platform.lower() == "moltx":
                row2.append({
                    "text": "MoltX",
                    "url": f"https://moltx.io/post/{post_id}",
                })
            elif platform.lower() == "4claw":
                row2.append({
                    "text": "4claw",
                    "url": f"https://4claw.org/b/crypto/thread/{post_id}",
                })

        rows = []
        if row1:
            rows.append(row1)
        if row2:
            rows.append(row2)
        return rows

    # ==================================================================
    # TELEGRAM SEND/EDIT HELPERS
    # ==================================================================

    def _get_pool_token(self) -> str:
        """Round-robin bot token from pool."""
        if not self._pool_tokens:
            return self._bot_token
        token = self._pool_tokens[self._pool_index % len(self._pool_tokens)]
        self._pool_index += 1
        return token

    async def _send_message(
        self, channel: str, text: str,
        buttons: list[list[dict[str, Any]]] | None = None,
        pool: bool = False,
    ) -> int | None:
        """Send a message using bot pool or default token."""
        token = self._get_pool_token() if pool else self._bot_token
        markup = {"inline_keyboard": buttons} if buttons else None
        return await self._api.send_message(
            token, channel, text, reply_markup=markup,
        )

    async def _edit_message(
        self, channel: str, msg_id: int, text: str,
        buttons: list[list[dict[str, Any]]] | None = None,
        pool: bool = False,
    ) -> bool:
        """Edit a message using bot pool or default token."""
        token = self._get_pool_token() if pool else self._bot_token
        markup = {"inline_keyboard": buttons} if buttons else None
        return await self._api.edit_message(
            token, channel, msg_id, text, reply_markup=markup,
        )

    @staticmethod
    def _extract_username_from_url(url: str) -> str:
        """Extract Twitter username from tweet URL."""
        if not url:
            return ""
        match = re.search(r"(?:twitter\.com|x\.com)/([^/]+)/status/", url)
        if match:
            handle = match.group(1).lower().lstrip("@")
            if handle not in ("i", "intent", "share", "search"):
                return handle
        return ""


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    TelegramProgressiveWorker.main()
