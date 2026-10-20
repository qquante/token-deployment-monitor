"""
Trading executor - handles swap execution.

Tracks approved buys and executes when target block arrives.
"""

import os
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, Awaitable, Any
from datetime import datetime

import aiohttp

from .config import (
    TradingConfig,
    UserConfig,
    get_user_config,
    save_user_config,
    get_fee_for_block,
    FEE_SCHEDULE,
)
from .swap_executor import SwapExecutor, SwapResult

logger = logging.getLogger(__name__)

# MEV Module selectors
NEXT_AUCTION_BLOCK_SELECTOR = "0x383048ca"


@dataclass
class PendingBuy:
    """A pending buy waiting for target block."""
    
    tweet_id: str
    user_id: int
    username: str
    
    # Token info
    token_address: str = ""
    token_symbol: str = ""
    pool_id: str = ""
    
    # Trading params
    eth_amount: float = 0.01
    block_offset: int = 6
    gwei: float = 0.1
    
    # Calculated
    auction_expire_block: int = 0
    target_block: int = 0
    fee_expected: float = 0.0
    
    # Status
    status: str = "APPROVED"  # APPROVED -> WAITING -> EXECUTING -> DONE/FAILED
    created_at: datetime = field(default_factory=datetime.now)
    executed_at: Optional[datetime] = None
    tx_hash: str = ""
    error: str = ""


class TradingExecutor:
    """
    Executes trades when target blocks arrive.
    
    Flow:
    1. User clicks BUY on PRE-BANKR
    2. approve_buy() stores pending buy
    3. Token goes on-chain -> link_token() updates with contract/pool
    4. Block arrives -> check_and_execute() runs trades
    """
    
    def __init__(self, redis_client=None):
        self.config = TradingConfig()
        
        # Pending buys: tweet_id -> PendingBuy
        self.pending_buys: Dict[str, PendingBuy] = {}
        
        # Notification callback (user_id, message, keyboard)
        self._notify_callback: Optional[Callable[[int, str, Optional[dict]], Awaitable[None]]] = None
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Redis client for pool data cache
        self.redis = redis_client
        
        # Current block
        self.current_block = 0
    
    async def _ensure_session(self):
        """Ensure HTTP session exists."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
    
    async def close(self):
        """Close session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def set_notify_callback(self, callback: Callable[[int, str, Optional[dict]], Awaitable[None]]):
        """Set callback for notifying users. callback(user_id, message, keyboard)"""
        self._notify_callback = callback
    
    async def notify_user(self, user_id: int, message: str, keyboard: dict = None):
        """Notify user via callback."""
        if self._notify_callback:
            try:
                await self._notify_callback(user_id, message, keyboard)
            except Exception as e:
                logger.error(f"[TRADING] Notify error: {e}")
    
    # APPROVE BUY
    
    def approve_buy(
        self,
        tweet_id: str,
        user_id: int,
        username: str,
        token_symbol: str = "",
    ) -> PendingBuy:
        """
        Approve a token for buying.
        
        Called when user clicks BUY and confirms.
        Token not yet on-chain, so we don't have contract/pool yet.
        """
        config = get_user_config(user_id, username)
        
        buy = PendingBuy(
            tweet_id=tweet_id,
            user_id=user_id,
            username=username,
            token_symbol=token_symbol,
            eth_amount=config.eth_amount,
            block_offset=config.block_offset,
            gwei=config.gwei,
            fee_expected=get_fee_for_block(config.block_offset),
            status="APPROVED",
        )
        
        self.pending_buys[tweet_id] = buy
        
        # Also store in user config
        config.approved_buys[tweet_id] = {
            "symbol": token_symbol,
            "eth": config.eth_amount,
            "block": config.block_offset,
            "approved_at": datetime.now().isoformat(),
        }
        save_user_config(user_id)
        
        logger.info(f"[TRADING] Approved buy: {token_symbol} for @{username} (tweet {tweet_id[:8]}...)")
        
        return buy
    
    def cancel_buy(self, tweet_id: str) -> bool:
        """Cancel a pending buy."""
        if tweet_id in self.pending_buys:
            buy = self.pending_buys[tweet_id]
            buy.status = "CANCELLED"
            del self.pending_buys[tweet_id]
            
            # Remove from user config
            config = get_user_config(buy.user_id)
            config.approved_buys.pop(tweet_id, None)
            save_user_config(buy.user_id)
            
            logger.info(f"[TRADING] Cancelled buy: {buy.token_symbol}")
            return True
        return False
    
    def get_pending_buy(self, tweet_id: str) -> Optional[PendingBuy]:
        """Get pending buy by tweet ID."""
        return self.pending_buys.get(tweet_id)
    
    # LINK TOKEN (when it goes on-chain)
    
    async def link_token(
        self,
        tweet_id: str,
        token_address: str,
        token_symbol: str,
        pool_id: str,
        auction_expire_block: int,
    ):
        """
        Link approved buy with on-chain token.
        
        Called when token appears on-chain and we can get auction info.
        """
        buy = self.pending_buys.get(tweet_id)
        if not buy:
            # Check if any user had approved via tweet URL match
            logger.debug(f"[TRADING] No pending buy for tweet {tweet_id[:8]}...")
            return
        
        buy.token_address = token_address
        buy.token_symbol = token_symbol
        buy.pool_id = pool_id
        buy.auction_expire_block = auction_expire_block
        buy.target_block = auction_expire_block + buy.block_offset
        buy.status = "WAITING"
        
        logger.info(
            f"[TRADING] Linked token: {token_symbol}\n"
            f"  Contract: {token_address}\n"
            f"  Auction expires: {auction_expire_block}\n"
            f"  Target block: {buy.target_block} (+{buy.block_offset})\n"
            f"  Expected fee: {buy.fee_expected:.1f}%"
        )
        
        # Notify user
        await self.notify_user(buy.user_id, f"""<b>TOKEN DETECTADO!</b>

Token: <b>{token_symbol}</b>
Contract: <code>{token_address}</code>

Compra agendada:
Bloco: <b>{buy.target_block}</b> (+{buy.block_offset})
Fee: <b>{buy.fee_expected:.1f}%</b>
ETH: <b>{buy.eth_amount}</b>

Aguardando bloco...""")
    
    # GET AUCTION INFO
    
    async def get_auction_expire_block(self, pool_id: str) -> int:
        """Get auction expire block from contract."""
        await self._ensure_session()
        
        if not self.config.rpc_http:
            return 0
        
        try:
            pool_id_clean = pool_id[2:] if pool_id.startswith("0x") else pool_id
            call_data = NEXT_AUCTION_BLOCK_SELECTOR + pool_id_clean.zfill(64)
            
            async with self._session.post(self.config.rpc_http, json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": self.config.mev_module_v2, "data": call_data}, "latest"]
            }) as resp:
                data = await resp.json()
                result = data.get("result", "0x0")
                return int(result, 16) if result and result != "0x" else 0
        except Exception as e:
            logger.error(f"[TRADING] Failed to get auction block: {e}")
            return 0
    
    # CHECK AND EXECUTE
    
    async def check_and_execute(self, current_block: int):
        """
        Check pending buys and execute if target block reached.
        
        Called on each new block.
        """
        self.current_block = current_block
        
        for tweet_id, buy in list(self.pending_buys.items()):
            if buy.status != "WAITING":
                continue
            
            if buy.target_block == 0:
                continue
            
            if current_block >= buy.target_block:
                await self._execute_buy(buy)
    
    async def _execute_buy(self, buy: PendingBuy):
        """Execute a single buy using SwapExecutor."""
        buy.status = "EXECUTING"
        
        logger.info(
            f"[TRADING] EXECUTING BUY!\n"
            f"  Token: {buy.token_symbol} ({buy.token_address})\n"
            f"  Block: {self.current_block} (target was {buy.target_block})\n"
            f"  ETH: {buy.eth_amount}\n"
            f"  User: @{buy.username}"
        )
        
        # Notify user
        await self.notify_user(buy.user_id, f"""<b>🔄 EXECUTANDO COMPRA...</b>

Token: <b>{buy.token_symbol}</b>
Bloco: <b>{self.current_block}</b>
ETH: <b>{buy.eth_amount}</b>
Fee: <b>{buy.fee_expected:.1f}%</b>

<i>Enviando transacao...</i>""")
        
        # Get user config for wallet
        config = get_user_config(buy.user_id)
        
        if not config.is_configured():
            buy.status = "FAILED"
            buy.error = "Wallet not configured"
            await self.notify_user(buy.user_id, f"""<b>❌ COMPRA FALHOU</b>

Erro: Wallet nao configurada.
Use /config para configurar.""")
            return
        
        try:
            # Get pool data from Redis cache (if available)
            pool_data = None
            if self.redis:
                pool_key = f"pool:{buy.token_address.lower()}"
                cached_pool = await self.redis.hgetall(pool_key)
                if cached_pool:
                    # Parse cached data
                    pool_data = {}
                    for k, v in cached_pool.items():
                        if k in ("zero_for_one",):
                            pool_data[k] = v == "1"
                        elif k in ("fee", "tick_spacing", "tick", "block_number"):
                            pool_data[k] = int(v) if v else 0
                        else:
                            pool_data[k] = v
                    logger.info(f"[TRADING] Using cached pool data for {buy.token_symbol}")
                else:
                    logger.warning(f"[TRADING] No cached pool data for {buy.token_symbol}")
            
            # Create swap executor with user's wallet
            executor = SwapExecutor(
                private_key=config.wallet_private_key,
                max_priority_fee_gwei=config.gwei or 0.01,
                max_fee_gwei=(config.gwei or 0.01) * 10,  # Max fee = 10x priority
            )
            
            await executor.connect()
            
            try:
                # Execute the swap using ETH mode (most reliable)
                amount_wei = int(buy.eth_amount * 10**18)
                
                result: SwapResult = await executor.swap_eth_to_token(
                    token_address=buy.token_address,
                    amount_in_wei=amount_wei,
                    slippage=config.slippage or 0.10,  # 10% slippage default for meme coins
                    deadline_seconds=60,
                    pool_data=pool_data,
                    use_weth=False,  # ALWAYS use ETH mode for reliability
                    force_eth_mode=True,
                )
                
                if result.success:
                    buy.status = "DONE"
                    buy.executed_at = datetime.now()
                    buy.tx_hash = result.tx_hash
                    
                    # Success notification with bot buttons
                    success_keyboard = {
                        "inline_keyboard": [[
                            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{buy.token_address}"},
                            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{buy.token_address}"},
                            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{buy.token_address}"},
                        ]]
                    }
                    
                    await self.notify_user(buy.user_id, f"""<b>✅ COMPRA EXECUTADA!</b>

Token: <b>{buy.token_symbol}</b>
ETH: <b>{buy.eth_amount}</b>
Fee: <b>{buy.fee_expected:.1f}%</b>

TX: <a href="https://basescan.org/tx/{result.tx_hash}">{result.tx_hash[:16]}...</a>
Contract: <code>{buy.token_address}</code>""", keyboard=success_keyboard)
                    
                    logger.info(f"[TRADING] ✅ Swap SUCCESS: {result.tx_hash}")
                else:
                    buy.status = "FAILED"
                    buy.error = result.error or "Unknown error"
                    
                    # Failure notification with details
                    await self.notify_user(buy.user_id, f"""<b>❌ COMPRA FALHOU</b>

Token: <b>{buy.token_symbol}</b>
Erro: {result.error}

<i>Possiveis causas:</i>
• ETH insuficiente no wallet
• Pool nao existe
• Slippage muito baixo
• Rede congestionada

Contract: <code>{buy.token_address}</code>""")
                    
                    logger.error(f"[TRADING] ❌ Swap FAILED: {result.error}")
            
            finally:
                await executor.close()
            
            # Remove from pending (whether success or fail)
            del self.pending_buys[buy.tweet_id]
            
            # Remove from user config
            config.approved_buys.pop(buy.tweet_id, None)
            save_user_config(buy.user_id)
            
        except Exception as e:
            buy.status = "FAILED"
            buy.error = str(e)
            
            logger.error(f"[TRADING] Exception during swap: {e}", exc_info=True)
            
            await self.notify_user(buy.user_id, f"""<b>❌ COMPRA FALHOU</b>

Token: <b>{buy.token_symbol}</b>
Erro: {str(e)[:100]}

<i>Verifique os logs para mais detalhes.</i>""")
    
    # STATS
    
    def get_stats(self) -> dict:
        """Get trading stats."""
        by_status = {}
        for buy in self.pending_buys.values():
            by_status[buy.status] = by_status.get(buy.status, 0) + 1
        
        return {
            "total_pending": len(self.pending_buys),
            "by_status": by_status,
            "current_block": self.current_block,
        }


# Global instance
_executor: Optional[TradingExecutor] = None


def get_executor(redis_client=None) -> TradingExecutor:
    """Get global trading executor instance."""
    global _executor
    if _executor is None:
        _executor = TradingExecutor(redis_client=redis_client)
    elif redis_client and not _executor.redis:
        # Update redis client if not set
        _executor.redis = redis_client
    return _executor
