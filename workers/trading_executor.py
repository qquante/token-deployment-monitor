"""
Trading Executor Worker

Executes trades when target blocks arrive.

Responsibilities:
- Subscribe to onchain:block for new blocks
- Subscribe to onchain:token for new token deployments
- Link pending buys with on-chain tokens
- Execute swaps when target block is reached
- Notify users of results

Input Channels:
- onchain:block - New block headers
- onchain:token - Token created events
- trading:buy_approved - New buy approvals

Output Channels:
- trading:result - Trade execution results
- telegram:send - Notifications to users
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from workers.base import BaseWorker
from shared.pubsub import Channels, Events
from shared.models import PendingBuy, TokenInfo, UserConfig, get_fee_for_block
from shared.formatters import build_trade_notification
from shared.rpc_pool import get_ws_pool
from src.trading.swap_executor import SwapExecutor, SwapResult


class TradingExecutorWorker(BaseWorker):
    """
    Executes trades at target blocks using Uniswap V4.
    
    Features:
    - Multi-RPC fire pattern for ultra-fast execution
    - Pre-signed transactions
    - Automatic retry on failure
    - Parallel lock acquisition and release
    - Background receipt waiting (non-blocking)
    - Fire-and-forget notifications
    """
    
    WORKER_NAME = "trading-executor"
    
    # Contracts
    CLANKER_V4 = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"
    MEV_MODULE_V2 = "0xebb25bb797d82cb78e1bc70406b13233c0854413"
    WETH = "0x4200000000000000000000000000000000000006"
    
    # Concurrency limits
    MAX_CONCURRENT_WALLETS = int(os.getenv("MAX_CONCURRENT_WALLETS", "10"))
    
    def __init__(self):
        super().__init__()
        
        self.rpc_http = os.getenv("ANKR_RPC_HTTP", "")
        self.rpc_wss = os.getenv("ANKR_RPC_WSS", "")
        
        # Multi-RPC URLs for fire pattern
        self.rpc_urls = self._get_rpc_urls()
        
        if not self.rpc_urls:
            self.logger.warning("No RPC URLs configured - swaps will be simulated")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Shared WS RPC pool (initialized in setup)
        self._ws_pool = None
        
        # Swap executors per user (cached)
        self._swap_executors: Dict[int, SwapExecutor] = {}
        
        # Current block
        self.current_block = 0
        
        # Concurrency control - limits parallel wallet executions
        self._wallet_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_WALLETS)
        
        # Background tasks tracking (for graceful shutdown)
        self._background_tasks: set = set()
        
        # Stats
        self.stats.update({
            "tokens_linked": 0,
            "trades_executed": 0,
            "trades_failed": 0,
            "receipts_pending": 0,
        })
    
    def _get_rpc_urls(self) -> List[str]:
        """Get all configured RPC URLs for multi-fire."""
        rpcs = []
        
        # Primary RPC
        if self.rpc_http:
            rpcs.append(self.rpc_http)
        
        # Additional RPCs from environment
        additional = os.getenv("BASE_RPC_URLS", "").split(",")
        for rpc in additional:
            rpc = rpc.strip()
            if rpc and rpc not in rpcs:
                rpcs.append(rpc)
        
        # Fallback public RPCs
        if not rpcs:
            rpcs = [
                "https://mainnet.base.org",
                "https://base.llamarpc.com",
            ]
        
        return rpcs
    
    async def setup(self):
        """Initialize HTTP session and WS pool."""
        await super().setup()
        self._session = aiohttp.ClientSession()
        
        # Get shared WS pool
        self._ws_pool = await get_ws_pool()
        
        # Log RPC configuration
        self.logger.info(
            f"[TRADING] Initialized with {len(self.rpc_urls)} RPCs, "
            f"WS pool: {self._ws_pool.connected_count} endpoints"
        )
    
    async def teardown(self):
        """Close HTTP session and swap executors."""
        # Close all swap executors
        for user_id, executor in self._swap_executors.items():
            try:
                await executor.close()
            except:
                pass
        self._swap_executors.clear()
        
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def _get_swap_executor(self, private_key: str, gwei: float) -> SwapExecutor:
        """Get or create swap executor for a wallet.
        
        Uses cache to avoid recreating executor and fetching nonce each trade.
        
        Args:
            private_key: Wallet private key
            gwei: User's configured max gwei for gas fees
        """
        # Use wallet address as cache key (derive from private key)
        from eth_account import Account
        if private_key.startswith("0x"):
            private_key_clean = private_key[2:]
        else:
            private_key_clean = private_key
        wallet_address = Account.from_key(bytes.fromhex(private_key_clean)).address
        
        # Check cache
        if wallet_address in self._swap_executors:
            executor = self._swap_executors[wallet_address]
            # Update gas if changed
            executor.max_priority_fee = int(gwei * 10**9)
            executor.max_fee = int(gwei * 10**9)
            self.logger.debug(f"[SWAP] Using cached executor for {wallet_address[:10]}...")
            return executor
        
        # Create new executor with WebSocket support
        executor = SwapExecutor(
            private_key=private_key,
            rpc_urls=self.rpc_urls,
            ws_url=self.rpc_wss,  # Pass WSS URL for WS-based TX sending
            max_priority_fee_gwei=gwei,
            max_fee_gwei=gwei,
        )
        await executor.connect()
        
        # Cache it
        self._swap_executors[wallet_address] = executor
        self.logger.info(f"[SWAP] Created and cached executor for {wallet_address[:10]}...")
        
        return executor
    
    async def run(self):
        """Subscribe to relevant channels."""
        self.logger.info("Subscribing to trading channels...")
        
        async for channel, message in self.redis.subscribe(
            Channels.ONCHAIN_BLOCK,
            Channels.ONCHAIN_TOKEN,
            Channels.TRADING_BUY_APPROVED,
        ):
            if not self._running:
                break
            
            try:
                if channel == Channels.ONCHAIN_BLOCK:
                    await self._handle_block(message)
                elif channel == Channels.ONCHAIN_TOKEN:
                    await self._handle_token(message)
                elif channel == Channels.TRADING_BUY_APPROVED:
                    await self._handle_buy_approved(message)
            except Exception as e:
                self.logger.error(f"Handler error: {e}")
                self.stats["errors"] += 1
    
    async def _handle_block(self, event: Dict[str, Any]):
        """Handle new block - check and execute pending trades.
        
        Groups trades by user (wallet) and executes each wallet's trades
        in parallel with pre-allocated nonces for maximum speed.
        
        Uses semaphore to limit concurrent wallet executions and prevent
        RPC overload when many users buy simultaneously.
        """
        block_number = event.get("block_number", 0)
        if not block_number:
            return
        
        self.current_block = block_number
        
        # Get all WAITING buys for this block
        ready_buys = await self.storage.get_buys_for_block(block_number)
        
        if ready_buys:
            # Group by user_id (each user has their own wallet)
            buys_by_user = {}
            for buy in ready_buys:
                buys_by_user.setdefault(buy.user_id, []).append(buy)
            
            self.logger.info(
                f"[BLOCK {block_number}] {len(ready_buys)} trades for "
                f"{len(buys_by_user)} wallets (max concurrent: {self.MAX_CONCURRENT_WALLETS})"
            )
            
            # Execute each wallet's trades with semaphore-limited concurrency
            async def execute_with_semaphore(user_id: int, buys: List[PendingBuy]):
                async with self._wallet_semaphore:
                    return await self._execute_user_batch(user_id, buys)
            
            wallet_tasks = [
                execute_with_semaphore(user_id, buys)
                for user_id, buys in buys_by_user.items()
            ]
            await asyncio.gather(*wallet_tasks, return_exceptions=True)
        
        self.stats["messages_processed"] += 1
    
    async def _handle_token(self, event: Dict[str, Any]):
        """Handle new token - link with pending buys."""
        tweet_id = event.get("tweet_id", "")
        token_address = event.get("token_address", "")
        token_symbol = event.get("token_symbol", "")
        pool_id = event.get("pool_id", "")
        block_number = event.get("block_number", 0)
        
        # Get context_parsed for Clawnch platform detection
        context_parsed = event.get("context_parsed", {})
        interface = context_parsed.get("interface", "").lower()
        platform = context_parsed.get("platform", "").lower()
        message_id = context_parsed.get("messageId", "")
        
        self.logger.info(f"[TOKEN] {token_symbol} at block {block_number} (interface={interface}, platform={platform}, tweet_id={tweet_id or 'none'})")
        
        # Get tweet username for handle sniper matching
        tweet_username = event.get("tweet_username", "")
        
        # Check for handle-based snipers (Bankr tokens only)
        if interface == "bankr":
            if tweet_username:
                self.logger.info(f"[HANDLE CHECK] @{tweet_username} (interface={interface})")
                await self._check_handle_snipers(
                    tweet_username=tweet_username,
                    token_address=token_address,
                    token_symbol=token_symbol,
                    pool_id=pool_id,
                    tweet_id=tweet_id,
                    block_number=block_number,
                )
            else:
                self.logger.debug(f"[HANDLE CHECK] No tweet_username in event for {token_symbol}")
        
        # Get auction expire block
        auction_expire_block = await self._get_auction_expire_block(pool_id)
        if not auction_expire_block:
            # Estimate: current block + 2 (auction duration)
            auction_expire_block = block_number + 2
        
        # Find all pending buys for this token
        # Strategy:
        # 1. Try tweet_id first (Twitter/Bankr flow)
        # 2. Try messageId (Clawnch flow: MoltX/Moltbook/4claw)
        pending_buys = []
        
        if tweet_id:
            pending_buys = await self.storage.get_pending_buys_by_tweet(tweet_id)
            if pending_buys:
                self.logger.info(f"[MATCH] Found {len(pending_buys)} buys via tweet_id: {tweet_id}")
            else:
                self.logger.debug(f"[NO MATCH] No pending buys for tweet_id: {tweet_id}")
        
        # For Clawnch tokens, also try messageId
        if not pending_buys and interface == "clawnch" and message_id:
            # Try direct messageId first (MoltX/Moltbook use plain UUID)
            pending_buys = await self.storage.get_pending_buys_by_post_id(message_id)
            if pending_buys:
                self.logger.info(f"[CLAWNCH] Found {len(pending_buys)} buys via messageId: {message_id[:16]}...")
            else:
                # Try with thread: prefix (4Claw stores post_id as "thread:{uuid}")
                thread_prefixed_id = f"thread:{message_id}"
                pending_buys = await self.storage.get_pending_buys_by_post_id(thread_prefixed_id)
                if pending_buys:
                    self.logger.info(f"[4CLAW] Found {len(pending_buys)} buys via thread:{message_id[:16]}...")
        
        # === BATCH FETCH USER CONFIGS (parallel) ===
        approved_buys = [b for b in pending_buys if b.status == "APPROVED"]
        if not approved_buys:
            # Log skipped buys
            for buy in pending_buys:
                self.logger.debug(f"[SKIP] Buy {buy.user_id}:{buy.tweet_id} status={buy.status} (not APPROVED)")
        
        # Pre-fetch all configs in parallel
        unique_user_ids = list(set(b.user_id for b in approved_buys))
        if unique_user_ids:
            config_results = await asyncio.gather(
                *[self.storage.get_user_config(uid) for uid in unique_user_ids]
            )
            config_map = dict(zip(unique_user_ids, config_results))
        else:
            config_map = {}
        
        for buy in approved_buys:
            # Get pre-fetched config (O(1) lookup)
            config = config_map.get(buy.user_id)
            if not config:
                self.logger.warning(f"[SKIP] No config for user {buy.user_id}")
                continue
            
            # Check if buy has expired based on user's TTL setting
            if config.buy_ttl_minutes > 0 and buy.approved_at:
                try:
                    approved_time = datetime.fromisoformat(buy.approved_at)
                    expiry_time = approved_time + timedelta(minutes=config.buy_ttl_minutes)
                    
                    if datetime.now() > expiry_time:
                        # Buy expired - cancel and notify user
                        minutes_waited = int((datetime.now() - approved_time).total_seconds() / 60)
                        await self.storage.cancel_buy(buy.user_id, buy.tweet_id)
                        self.logger.info(
                            f"[EXPIRED] Buy {buy.user_id}:{buy.tweet_id} expired after {minutes_waited}min "
                            f"(TTL={config.buy_ttl_minutes}min)"
                        )
                        await self._notify_user(
                            buy.user_id,
                            f"""<b>⏰ BUY EXPIRADO</b>

Token: <b>{buy.token_symbol or token_symbol}</b>
Tempo de espera: <b>{minutes_waited} min</b>
TTL configurado: <b>{config.buy_ttl_minutes} min</b>

O token não foi deployado a tempo.
Sua ordem foi cancelada automaticamente.

💡 Você pode aumentar o TTL em /config"""
                        )
                        continue
                except Exception as e:
                    self.logger.warning(f"[TTL] Error checking expiry for {buy.buy_key}: {e}")
            
            self.logger.info(f"[LINKING] Buy {buy.user_id}:{buy.tweet_id} -> {token_symbol} ({token_address[:16]}...)")
            
            # Link token to buy
            await self.storage.link_token_to_buy(
                buy,
                token_address=token_address,
                token_symbol=token_symbol,
                pool_id=pool_id,
                auction_expire_block=auction_expire_block,
            )
            
            self.stats["tokens_linked"] += 1
            
            # Check if should auto-approve:
            # 1. auto_buy is ON, OR
            # 2. max_eth_approved > 0 AND buy amount <= max_eth_approved
            should_auto_approve = config.auto_buy
            pre_approve_reason = ""
            
            if not should_auto_approve and config.max_eth_approved > 0:
                if buy.eth_amount <= config.max_eth_approved:
                    should_auto_approve = True
                    pre_approve_reason = f" (Pre-approve ≤{config.max_eth_approved} ETH)"
            
            if should_auto_approve:
                # AUTO-BUY ON or PRE-APPROVED: Proceed automatically
                # Status is already WAITING from link_token_to_buy
                await self._notify_user(
                    buy.user_id,
                    f"""<b>🚀 TOKEN ON-CHAIN!</b>

Token: <b>{token_symbol}</b>
Contract: <code>{token_address}</code>

<b>Compra agendada{pre_approve_reason}:</b>
Bloco: <b>{buy.target_block}</b> (+{buy.block_offset})
Fee: <b>{buy.fee_expected:.1f}%</b>
ETH: <b>{buy.eth_amount}</b>

⏳ Aguardando bloco alvo..."""
                )
                self.logger.info(f"[AUTO-BUY] {token_symbol} -> @{buy.username} target={buy.target_block}{pre_approve_reason}")
            else:
                # AUTO-BUY OFF: Ask for confirmation
                # Change status from WAITING to PENDING_CONFIRM (properly update index)
                await self.storage.update_buy_status(buy, "PENDING_CONFIRM")
                
                await self._notify_user(
                    buy.user_id,
                    f"""<b>🔔 TOKEN ON-CHAIN!</b>

Token: <b>{token_symbol}</b>
Contract: <code>{token_address}</code>

<b>Sua compra está pronta:</b>
ETH: <b>{buy.eth_amount}</b>
Bloco: <b>+{buy.block_offset}</b> ({buy.fee_expected:.1f}% fee)

<b>Deseja executar a compra?</b>""",
                    reply_markup={
                        "inline_keyboard": [
                            [
                                {"text": "✅ SIM, COMPRAR!", "callback_data": f"buyconfirm:{tweet_id}"},
                                {"text": "❌ Cancelar", "callback_data": f"buycancel:{tweet_id}"},
                            ]
                        ]
                    }
                )
                self.logger.info(f"[CONFIRM NEEDED] {token_symbol} -> @{buy.username}")
        
        # Also update token info in storage
        token_info = await self.storage.get_token_info(tweet_id)
        if token_info:
            token_info.token_address = token_address
            token_info.pool_id = pool_id
            token_info.auction_expire_block = auction_expire_block
            token_info.on_chain = True
            token_info.onchain_at = datetime.now().isoformat()
            await self.storage.save_token_info(token_info)
        
        self.stats["messages_processed"] += 1
    
    async def _check_handle_snipers(
        self,
        tweet_username: str,
        token_address: str,
        token_symbol: str,
        pool_id: str,
        tweet_id: str,
        block_number: int,
    ):
        """
        Check if any user has this Twitter handle in their sniper list.
        If so, create automatic buy with their default config.
        
        Guards:
        - User must have wallet configured
        - User can't have existing buy for this tweet_id
        """
        handle = tweet_username.lower().strip().lstrip("@")
        if not handle:
            return
        
        # Get all users who want to snipe this handle
        user_ids = await self.storage.get_users_by_handle(handle)
        
        self.logger.info(f"[HANDLE SNIPER] Checking @{handle} -> {len(user_ids)} users found")
        
        for user_id in user_ids:
            try:
                # Get user config
                config = await self.storage.get_user_config(user_id)
                
                # Guard: must have wallet
                if not config.is_configured():
                    self.logger.warning(f"[HANDLE SNIPER] User {user_id} skipped - no wallet")
                    continue
                
                # Guard: check if already has pending buy for this token
                existing_buy = await self.storage.get_pending_buy(user_id, tweet_id)
                if existing_buy:
                    self.logger.info(f"[HANDLE SNIPER] User {user_id} skipped - already has buy for {tweet_id}")
                    continue
                
                # Get auction expire block for fee calculation
                auction_expire_block = await self._get_auction_expire_block(pool_id)
                if not auction_expire_block:
                    auction_expire_block = block_number + 2
                
                # Calculate target block and fee
                # Fee schedule: block 0 = 1%, block 1 = 0.5%, block 2+ = 0%
                target_block = auction_expire_block + config.block_offset
                if config.block_offset == 0:
                    fee_expected = 1.0
                elif config.block_offset == 1:
                    fee_expected = 0.5
                else:
                    fee_expected = 0.0
                
                # Create PendingBuy with user's default config
                from shared.models import PendingBuy
                
                buy = PendingBuy(
                    user_id=user_id,
                    username=config.username,
                    tweet_id=tweet_id,
                    token_symbol=token_symbol,
                    token_address=token_address,
                    pool_id=pool_id,
                    eth_amount=config.eth_amount,
                    block_offset=config.block_offset,
                    gwei=config.gwei,
                    slippage=config.slippage,
                    target_block=target_block,
                    fee_expected=fee_expected,
                    status="WAITING",  # Already on-chain, go straight to WAITING
                )
                
                await self.storage.save_pending_buy(buy)
                
                self.logger.info(
                    f"[HANDLE SNIPER] Auto-buy created: @{handle} -> User {user_id} "
                    f"({token_symbol}, {config.eth_amount} ETH, target={target_block})"
                )
                
                # Notify user
                await self._notify_user(
                    user_id,
                    f"""<b>🎯 TRIGGER ATIVADO!</b>

@{handle} criou um token!

Token: <b>{token_symbol}</b>
Contract: <code>{token_address}</code>

<b>Compra automática criada:</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>{target_block}</b> (+{config.block_offset})
Fee: <b>{fee_expected:.1f}%</b>

⏳ Executando no bloco alvo..."""
                )
                
                self.stats["tokens_linked"] += 1
                
            except Exception as e:
                self.logger.error(f"[HANDLE SNIPER] Error for user {user_id}: {e}")
    
    async def _handle_buy_approved(self, event: Dict[str, Any]):
        """Handle new buy approval."""
        user_id = event.get("user_id", 0)
        username = event.get("username", "")
        tweet_id = event.get("tweet_id", "")
        token_symbol = event.get("token_symbol", "")
        
        self.logger.info(f"[BUY APPROVED] @{username} -> {token_symbol}")
        
        # Check if token already on-chain
        token_info = await self.storage.get_token_info(tweet_id)
        
        if token_info and token_info.on_chain and token_info.token_address:
            # Token already deployed - link immediately
            buy = await self.storage.get_pending_buy(user_id, tweet_id)
            
            if buy and buy.status == "APPROVED":
                auction_expire_block = token_info.auction_expire_block
                if not auction_expire_block:
                    auction_expire_block = await self._get_auction_expire_block(token_info.pool_id)
                
                if auction_expire_block:
                    await self.storage.link_token_to_buy(
                        buy,
                        token_address=token_info.token_address,
                        token_symbol=token_info.token_symbol,
                        pool_id=token_info.pool_id,
                        auction_expire_block=auction_expire_block,
                    )
                    
                    self.stats["tokens_linked"] += 1
                    
                    # Check if target block already passed
                    if self.current_block > 0 and buy.target_block <= self.current_block:
                        # Target block already passed - execute immediately or at minimum fee
                        current_fee = get_fee_for_block(self.current_block - auction_expire_block)
                        
                        if current_fee <= 5.0:  # Fee is low enough (<=5%) - execute immediately
                            self.logger.info(f"[LATE BUY] Target passed but fee OK ({current_fee:.1f}%) - executing now")
                            await self._notify_user(
                                user_id,
                                f"""<b>TOKEN JA ON-CHAIN!</b>

Token: <b>{token_info.token_symbol}</b>
Fee atual: <b>{current_fee:.1f}%</b> (minima!)

Executando AGORA..."""
                            )
                            await self._execute_trade(buy)
                        else:
                            # Fee still high - notify user
                            await self._notify_user(
                                user_id,
                                f"""<b>TOKEN JA ON-CHAIN!</b>

Token: <b>{token_info.token_symbol}</b>
Fee atual: <b>{current_fee:.1f}%</b>

Target bloco {buy.target_block} ja passou.
Bloco atual: {self.current_block}

Aguardando fee cair para executar..."""
                            )
                    else:
                        await self._notify_user(
                            user_id,
                            f"""<b>TOKEN JA ON-CHAIN!</b>

Token: <b>{token_info.token_symbol}</b>
Target: <b>bloco {buy.target_block}</b>

Executando em breve..."""
                        )
        
        self.stats["messages_processed"] += 1
    
    async def _execute_user_batch(self, user_id: int, buys: List[PendingBuy]):
        """
        Execute all trades for a single user/wallet with pre-allocated nonces.
        
        Optimizations:
        - Parallel lock acquisition (asyncio.gather)
        - Pool data pre-fetch before execution
        - Background receipt waiting (non-blocking)
        - Parallel lock release
        """
        if not buys:
            return
        
        # Get user config once
        config = await self.storage.get_user_config(user_id)
        
        if not config.is_configured():
            self.logger.error(f"[BATCH] User {user_id} wallet not configured")
            return
        
        # Sort by target_block (oldest first - execute most urgent first)
        buys.sort(key=lambda b: b.target_block)
        
        # === PARALLEL LOCK ACQUISITION ===
        async def try_lock(buy: PendingBuy) -> Optional[PendingBuy]:
            if await self.storage.try_acquire_trade_lock(buy.buy_key):
                return buy
            self.logger.warning(f"[BATCH] Trade {buy.buy_key} already locked, skipping")
            return None
        
        lock_results = await asyncio.gather(*[try_lock(b) for b in buys])
        locked_buys = [b for b in lock_results if b is not None]
        
        if not locked_buys:
            return
        
        self.logger.info(
            f"[BATCH] User {user_id}: {len(locked_buys)} trades locked, "
            f"executing in parallel"
        )
        
        try:
            # Get swap executor for this wallet
            executor = await self._get_swap_executor(config.wallet_private_key, config.gwei)
            
            # === POOL DATA PRE-FETCH (parallel) ===
            unique_tokens = list(set(b.token_address for b in locked_buys if b.token_address))
            if unique_tokens:
                pool_data_results = await asyncio.gather(
                    *[self.storage.get_pool_data(t) for t in unique_tokens]
                )
                pool_data_map = dict(zip(unique_tokens, pool_data_results))
            else:
                pool_data_map = {}
            
            # PRE-ALLOCATE ALL NONCES IN ONE OPERATION
            nonces = await executor.allocate_nonces(len(locked_buys))
            
            self.logger.info(
                f"[BATCH] Pre-allocated nonces: {nonces[0]} to {nonces[-1]}"
            )
            
            # Execute ALL swaps in parallel with pre-allocated nonces and pre-fetched pool data
            swap_tasks = []
            for buy, nonce in zip(locked_buys, nonces):
                # Use UPDATED config values (user may have changed settings after clicking BUY)
                buy.eth_amount = config.eth_amount
                buy.gwei = config.gwei
                buy.slippage = config.slippage
                # Note: block_offset already used for target_block calculation, keep as-is
                
                task = self._execute_single_with_nonce(
                    buy=buy,
                    executor=executor,
                    nonce=nonce,
                    slippage=config.slippage,
                    pool_data=pool_data_map.get(buy.token_address),  # Pre-fetched!
                )
                swap_tasks.append(task)
            
            # Fire all swaps at once!
            results = await asyncio.gather(*swap_tasks, return_exceptions=True)
            
            # Log results
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            self.logger.info(
                f"[BATCH] TX send completed: {success_count}/{len(locked_buys)} successful "
                f"(receipts polling in background)"
            )
            
        finally:
            # === PARALLEL LOCK RELEASE ===
            await asyncio.gather(
                *[self.storage.release_trade_lock(buy.buy_key) for buy in locked_buys],
                return_exceptions=True  # Don't fail if one release fails
            )
    
    async def _execute_single_with_nonce(
        self,
        buy: PendingBuy,
        executor,
        nonce: int,
        slippage: float,
        pool_data: Optional[Dict] = None,
    ):
        """
        Execute a single swap with a pre-allocated nonce.
        
        Optimizations:
        - Accepts pre-fetched pool_data (no blocking fetch)
        - Uses background receipt waiting (non-blocking)
        - Uses fire-and-forget notifications
        """
        try:
            # Fresh read from Redis
            current_buy = await self.storage.get_pending_buy(buy.user_id, buy.tweet_id)
            if not current_buy:
                self.logger.error(f"[BATCH] Buy {buy.buy_key} not found")
                return
            
            # Validate status
            if current_buy.status not in ["WAITING", "PENDING_CONFIRM"]:
                self.logger.warning(
                    f"[BATCH] Trade {buy.buy_key} status is {current_buy.status}, skipping"
                )
                return
            
            # Update status
            await self.storage.update_buy_status(
                current_buy, "EXECUTING", executed_block=self.current_block
            )
            
            # Fire-and-forget notification (non-blocking)
            self._notify_user_async(
                buy.user_id,
                f"""<b>EXECUTANDO COMPRA...</b>

Token: <b>{current_buy.token_symbol}</b>
Bloco: <b>{self.current_block}</b>
ETH: <b>{current_buy.eth_amount}</b>
Fee: <b>{current_buy.fee_expected:.1f}%</b>
Nonce: <b>{nonce}</b>"""
            )
            
            # Use pre-fetched pool_data or fetch if not provided
            if pool_data is None:
                pool_data = await self.storage.get_pool_data(current_buy.token_address)
            
            # Execute swap with pre-allocated nonce
            result = await executor.swap_eth_to_token_with_nonce(
                token_address=current_buy.token_address,
                amount_in_wei=int(current_buy.eth_amount * 10**18),
                nonce=nonce,
                slippage=slippage,
                pool_id=current_buy.pool_id,
                pool_data=pool_data,
            )
            
            if result.success:
                # TX sent - fire-and-forget notification
                self._notify_user_async(
                    buy.user_id,
                    f"""<b>TX ENVIADA - AGUARDANDO CONFIRMAÇÃO...</b>

Token: <b>{current_buy.token_symbol}</b>
TX: <code>{result.tx_hash}</code>"""
                )
                
                self.logger.info(f"[BATCH TX SENT] {current_buy.token_symbol} @ nonce {nonce}")
                
                # === BACKGROUND RECEIPT WAITING ===
                # Create callback for receipt processing
                async def on_receipt(tx_hash: str, receipt: Optional[Dict]):
                    """Process receipt in background - does not block main execution."""
                    try:
                        self.stats["receipts_pending"] -= 1
                        
                        if receipt:
                            tx_status = int(receipt.get("status", "0x0"), 16)
                            gas_used = int(receipt.get("gasUsed", "0x0"), 16)
                            
                            if tx_status == 1:
                                # ON-CHAIN SUCCESS!
                                await self.storage.complete_buy(current_buy, tx_hash=tx_hash, gas_used=gas_used)
                                self.stats["trades_executed"] += 1
                                
                                success_keyboard = {
                                    "inline_keyboard": [
                                        [{"text": "❌ Cancelar Ordem", "callback_data": f"cancel_order:{current_buy.token_address}"}],
                                        [
                                            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{current_buy.token_address}"},
                                            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{current_buy.token_address}"},
                                            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{current_buy.token_address}"},
                                        ],
                                    ]
                                }
                                
                                # Final notification with keyboard (can await - we're in background)
                                await self._notify_user(
                                    buy.user_id,
                                    f"""<b>✅ COMPRA CONFIRMADA ON-CHAIN!</b>

Token: <b>{current_buy.token_symbol}</b>
ETH: <b>{current_buy.eth_amount}</b>
Fee: <b>{current_buy.fee_expected:.1f}%</b>
Gas: <b>{gas_used:,}</b>

TX: <code>{tx_hash}</code>

Contract: <code>{current_buy.token_address}</code>""",
                                    reply_markup=success_keyboard,
                                )
                                
                                self.logger.info(f"[RECEIPT OK] {current_buy.token_symbol} @ nonce {nonce} CONFIRMED")
                            else:
                                # ON-CHAIN REVERTED!
                                await self._fail_trade(current_buy, f"TX reverted on-chain (gas={gas_used})")
                                self.logger.error(f"[RECEIPT REVERTED] {current_buy.token_symbol} @ nonce {nonce}")
                        else:
                            # Timeout - TX might still be pending
                            await self.storage.complete_buy(current_buy, tx_hash=tx_hash)
                            self.stats["trades_executed"] += 1
                            
                            success_keyboard = {
                                "inline_keyboard": [
                                    [{"text": "❌ Cancelar Ordem", "callback_data": f"cancel_order:{current_buy.token_address}"}],
                                    [
                                        {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{current_buy.token_address}"},
                                        {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{current_buy.token_address}"},
                                        {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{current_buy.token_address}"},
                                    ],
                                ]
                            }
                            
                            await self._notify_user(
                                buy.user_id,
                                f"""<b>⚠️ TX ENVIADA (AGUARDANDO BLOCO)</b>

Token: <b>{current_buy.token_symbol}</b>
ETH: <b>{current_buy.eth_amount}</b>

TX: <code>{tx_hash}</code>

<i>Receipt não recebido em 120s. Verifique no explorer.</i>

Contract: <code>{current_buy.token_address}</code>""",
                                reply_markup=success_keyboard,
                            )
                            
                            self.logger.warning(f"[RECEIPT TIMEOUT] {current_buy.token_symbol} @ nonce {nonce}")
                    except Exception as e:
                        self.logger.error(f"[RECEIPT CALLBACK ERROR] {tx_hash}: {e}")
                
                # Start background receipt waiting (fire and forget!)
                self.stats["receipts_pending"] += 1
                executor.wait_for_receipt_background(
                    result.tx_hash,
                    callback=on_receipt,
                    timeout=120,  # Longer timeout for background (more tolerant)
                )
                
                # Return immediately - don't wait for receipt!
                return {"success": True, "tx_hash": result.tx_hash}
            else:
                # Failed
                await self._fail_trade(current_buy, result.error or "Swap failed")
                return {"success": False, "error": result.error}
                
        except Exception as e:
            self.logger.error(f"[BATCH] Error executing {buy.buy_key}: {e}")
            try:
                await self._fail_trade(buy, str(e))
            except:
                pass
            return {"success": False, "error": str(e)}

    async def _execute_trade(self, buy: PendingBuy):
        """
        Execute a single trade with atomic lock protection.
        
        This method ensures:
        1. Only ONE execution per trade (via SETNX lock)
        2. Fresh read from Redis to avoid stale object issues
        3. Status validation before proceeding
        """
        # STEP 1: Acquire atomic lock - only ONE execution can proceed
        if not await self.storage.try_acquire_trade_lock(buy.buy_key):
            self.logger.warning(
                f"[SKIP] Trade {buy.buy_key} already executing (lock held)"
            )
            return
        
        try:
            # STEP 2: Fresh read - get current state from Redis
            current_buy = await self.storage.get_pending_buy(buy.user_id, buy.tweet_id)
            if not current_buy:
                self.logger.error(f"[SKIP] Buy {buy.buy_key} not found in Redis")
                return
            
            # STEP 3: Validate status - only execute if WAITING
            if current_buy.status not in ["WAITING", "PENDING_CONFIRM"]:
                self.logger.warning(
                    f"[SKIP] Trade {buy.buy_key} status is {current_buy.status}, not WAITING"
                )
                return
            
            # STEP 4: Validate block tolerance
            config = await self.storage.get_user_config(current_buy.user_id)
            tolerance = config.block_tolerance
            block_diff = self.current_block - current_buy.target_block
            
            if block_diff < 0:
                # Too early - shouldn't happen with current query logic
                self.logger.warning(
                    f"[EARLY] {buy.buy_key} target={current_buy.target_block} "
                    f"current={self.current_block}"
                )
                return
            
            if block_diff > tolerance:
                # Too late - exceeds user's tolerance
                # Use CANCELLED (valid from WAITING) instead of FAILED
                self.logger.error(
                    f"[LATE] {buy.buy_key} target={current_buy.target_block} "
                    f"current={self.current_block} tolerance=±{tolerance}"
                )
                await self.storage.update_buy_status(
                    current_buy, "CANCELLED",
                    error=f"Bloco alvo perdido (+{block_diff} > ±{tolerance})",
                    executed_block=self.current_block
                )
                await self._notify_user(
                    buy.user_id,
                    f"""<b>❌ COMPRA FALHOU</b>

Token: <b>{current_buy.token_symbol}</b>
Motivo: Bloco alvo perdido

Alvo: bloco <b>{current_buy.target_block}</b>
Atual: bloco <b>{self.current_block}</b>
Tolerância: <b>±{tolerance}</b>

O bloco passou {block_diff} blocos além do esperado."""
                )
                return
            
            # Within tolerance - log if executing late
            if block_diff > 0:
                self.logger.info(
                    f"[TOLERANCE] {buy.buy_key} executing {block_diff} block(s) late "
                    f"(within ±{tolerance} tolerance)"
                )
            
            # STEP 5: Execute with the fresh buy object
            await self._execute_trade_inner(current_buy)
            
        finally:
            # ALWAYS release lock, even on exceptions
            await self.storage.release_trade_lock(buy.buy_key)
    
    async def _execute_trade_inner(self, buy: PendingBuy):
        """Inner trade execution logic (called after lock acquired)."""
        self.logger.info(
            f"[EXECUTING] {buy.token_symbol} for @{buy.username}\n"
            f"  Block: {self.current_block} (target: {buy.target_block})\n"
            f"  ETH: {buy.eth_amount}"
        )
        
        # Update status to EXECUTING and record which block we're executing on
        status_updated = await self.storage.update_buy_status(
            buy, "EXECUTING",
            executed_block=self.current_block
        )
        if not status_updated:
            self.logger.error(f"[ABORT] Failed to update status for {buy.buy_key}")
            return
        
        # Notify user
        await self._notify_user(
            buy.user_id,
            f"""<b>EXECUTANDO COMPRA...</b>

Token: <b>{buy.token_symbol}</b>
Bloco: <b>{self.current_block}</b>
ETH: <b>{buy.eth_amount}</b>
Fee: <b>{buy.fee_expected:.1f}%</b>"""
        )
        
        # Get user config for wallet
        config = await self.storage.get_user_config(buy.user_id)
        
        if not config.is_configured():
            await self._fail_trade(buy, "Wallet not configured")
            return
        
        # Execute swap
        try:
            if self.rpc_http and config.wallet_private_key:
                tx_hash = await self._execute_swap(
                    buy=buy,
                    wallet_address=config.wallet_address,
                    private_key=config.wallet_private_key,
                    gwei=config.gwei,
                    slippage=config.slippage,
                )
                
                # TX sent - notify user
                await self._notify_user(
                    buy.user_id,
                    f"""<b>TX ENVIADA - AGUARDANDO CONFIRMAÇÃO...</b>

Token: <b>{buy.token_symbol}</b>
TX: <code>{tx_hash}</code>"""
                )
                
                # Wait for on-chain receipt (max 30s)
                executor = await self._get_swap_executor(config.wallet_private_key, config.gwei)
                receipt = await executor.wait_for_receipt(tx_hash, timeout=30, poll_interval=1.0)
                
                if receipt:
                    tx_status = int(receipt.get("status", "0x0"), 16)
                    gas_used = int(receipt.get("gasUsed", "0x0"), 16)
                    
                    if tx_status == 1:
                        # ON-CHAIN SUCCESS!
                        await self.storage.complete_buy(buy, tx_hash=tx_hash, gas_used=gas_used)
                        self.stats["trades_executed"] += 1
                        
                        success_keyboard = {
                            "inline_keyboard": [
                                [{"text": "❌ Cancelar Ordem", "callback_data": f"cancel_order:{buy.token_address}"}],
                                [
                                    {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{buy.token_address}"},
                                    {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{buy.token_address}"},
                                    {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{buy.token_address}"},
                                ],
                            ]
                        }
                        
                        await self._notify_user(
                            buy.user_id,
                            f"""<b>✅ COMPRA CONFIRMADA ON-CHAIN!</b>

Token: <b>{buy.token_symbol}</b>
ETH: <b>{buy.eth_amount}</b>
Fee: <b>{buy.fee_expected:.1f}%</b>
Gas: <b>{gas_used:,}</b>

TX: <code>{tx_hash}</code>

Contract: <code>{buy.token_address}</code>""",
                            reply_markup=success_keyboard,
                        )
                        
                        self.logger.info(f"[SUCCESS] {buy.token_symbol} for @{buy.username} CONFIRMED")
                    else:
                        # ON-CHAIN REVERTED!
                        await self._fail_trade(buy, f"TX reverted on-chain (gas={gas_used})")
                        return
                else:
                    # No receipt in 30s - TX might still be pending
                    await self.storage.complete_buy(buy, tx_hash=tx_hash)
                    self.stats["trades_executed"] += 1
                    
                    success_keyboard = {
                        "inline_keyboard": [
                            [{"text": "❌ Cancelar Ordem", "callback_data": f"cancel_order:{buy.token_address}"}],
                            [
                                {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{buy.token_address}"},
                                {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{buy.token_address}"},
                                {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{buy.token_address}"},
                            ],
                        ]
                    }
                    
                    await self._notify_user(
                        buy.user_id,
                        f"""<b>⚠️ TX ENVIADA (AGUARDANDO BLOCO)</b>

Token: <b>{buy.token_symbol}</b>
ETH: <b>{buy.eth_amount}</b>

TX: <code>{tx_hash}</code>

<i>Receipt não recebido em 30s. Verifique no explorer.</i>

Contract: <code>{buy.token_address}</code>""",
                        reply_markup=success_keyboard,
                    )
                    
                    self.logger.warning(f"[PENDING] {buy.token_symbol} for @{buy.username} - no receipt in 30s")
            else:
                # Simulate
                await asyncio.sleep(0.5)
                tx_hash = "0x_simulated_not_real"
                
                await self.storage.complete_buy(buy, tx_hash=tx_hash)
                self.stats["trades_executed"] += 1
                
                await self._notify_user(
                    buy.user_id,
                    f"""<b>COMPRA SIMULADA!</b>

Token: <b>{buy.token_symbol}</b>
ETH: <b>{buy.eth_amount}</b>

<i>Modo simulação - nenhuma TX real enviada.</i>

Contract: <code>{buy.token_address}</code>"""
                )
                
                self.logger.info(f"[SIMULATED] {buy.token_symbol} for @{buy.username}")
            
            # Publish result
            result_event = Events.trade_result(
                user_id=buy.user_id,
                tweet_id=buy.tweet_id,
                token_symbol=buy.token_symbol,
                status="DONE",
                tx_hash=tx_hash,
                eth_spent=buy.eth_amount,
            )
            await self.publish(Channels.TRADING_RESULT, result_event)
        
        except Exception as e:
            await self._fail_trade(buy, str(e))
    
    async def _fail_trade(self, buy: PendingBuy, error: str):
        """Mark trade as failed."""
        await self.storage.complete_buy(buy, error=error)
        self.stats["trades_failed"] += 1
        
        await self._notify_user(
            buy.user_id,
            f"""<b>COMPRA FALHOU</b>

Token: <b>{buy.token_symbol}</b>
Erro: {error[:100]}"""
        )
        
        # Publish result
        result_event = Events.trade_result(
            user_id=buy.user_id,
            tweet_id=buy.tweet_id,
            token_symbol=buy.token_symbol,
            status="FAILED",
            error=error,
        )
        await self.publish(Channels.TRADING_RESULT, result_event)
        
        self.logger.error(f"[FAILED] {buy.token_symbol} for @{buy.username}: {error}")
    
    async def _execute_swap(
        self,
        buy: PendingBuy,
        wallet_address: str,
        private_key: str,
        gwei: float,
        slippage: float,
    ) -> str:
        """
        Execute swap on-chain using Uniswap V4.
        
        Uses multi-RPC fire pattern for maximum speed.
        """
        if not self.rpc_urls or not private_key:
            self.logger.warning("[SWAP] No RPC or key - simulating")
            await asyncio.sleep(0.5)
            return "0x_simulated_no_rpc"
        
        try:
            # Get swap executor with user's gwei config
            executor = await self._get_swap_executor(private_key, gwei)
            
            # Calculate amount in wei
            amount_wei = int(buy.eth_amount * 10**18)
            
            # Get cached pool data from clanker_enricher
            pool_data = await self.storage.get_pool_data(buy.token_address)
            
            if pool_data:
                self.logger.info(
                    f"[SWAP] Using cached pool data: fee={pool_data.get('fee')} "
                    f"hooks={pool_data.get('hooks', '')[:16]}..."
                )
            else:
                self.logger.warning("[SWAP] No cached pool data - using defaults")
            
            self.logger.info(
                f"[SWAP] Executing V4 swap:\n"
                f"  Token: {buy.token_address}\n"
                f"  Amount: {buy.eth_amount} ETH\n"
                f"  Gwei: {gwei}\n"
                f"  Pool: {buy.pool_id or 'auto'}\n"
                f"  RPCs: {len(self.rpc_urls)}"
            )
            
            # Execute swap with multi-RPC fire!
            result: SwapResult = await executor.swap_eth_to_token(
                token_address=buy.token_address,
                amount_in_wei=amount_wei,
                slippage=slippage,
                pool_id=buy.pool_id,
                pool_data=pool_data,  # Pass cached pool data!
            )
            
            # Don't close executor - it's cached for reuse (keeps nonce)
            
            if result.success:
                self.logger.info(f"[SWAP] Success! TX: {result.tx_hash} (via {result.rpc_used})")
                return result.tx_hash
            else:
                raise Exception(result.error or "Swap failed")
        
        except Exception as e:
            self.logger.error(f"[SWAP] Error: {e}")
            raise
    
    async def _get_auction_expire_block(self, pool_id: str) -> int:
        """Get auction expire block from contract via WebSocket RPC."""
        if not pool_id or not self._ws_pool:
            return 0
        
        try:
            # nextAuctionBlock(bytes32 poolId) -> uint256
            selector = "0x383048ca"
            pool_id_clean = pool_id[2:] if pool_id.startswith("0x") else pool_id
            call_data = selector + pool_id_clean.zfill(64)
            
            result = await self._ws_pool.eth_call(
                to=self.MEV_MODULE_V2,
                data=call_data,
                block="latest",
            )
            return int(result, 16) if result and result != "0x" else 0
        except Exception as e:
            self.logger.error(f"Get auction block error: {e}")
            return 0
    
    async def _notify_user(self, user_id: int, text: str, reply_markup: dict = None):
        """Send notification to user via Telegram (blocking)."""
        event = Events.telegram_send(
            chat_id=str(user_id),
            text=text,
            channel="trading",
            reply_markup=reply_markup,
        )
        await self.publish(Channels.TELEGRAM_SEND, event)
    
    def _notify_user_async(self, user_id: int, text: str, reply_markup: dict = None):
        """
        Fire-and-forget notification - does NOT block execution.
        
        Use this for non-critical notifications where we don't need to wait
        for the message to be published before continuing.
        """
        event = Events.telegram_send(
            chat_id=str(user_id),
            text=text,
            channel="trading",
            reply_markup=reply_markup,
        )
        # Create task without awaiting - fire and forget
        task = asyncio.create_task(self.publish(Channels.TELEGRAM_SEND, event))
        # Track task to prevent garbage collection
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)


# Entry point
if __name__ == "__main__":
    TradingExecutorWorker.main()
