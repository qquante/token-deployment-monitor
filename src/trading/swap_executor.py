"""
Ultra-Fast Uniswap V4 Swap Executor

Features:
- Pre-signed transactions for instant execution
- Multi-RPC fire pattern (send to all RPCs simultaneously)
- Connection pooling with aiohttp for speed
- Uniswap V4 via Universal Router on Base

Usage:
    executor = SwapExecutor(private_key, rpcs)
    await executor.connect()
    tx_hash = await executor.swap_eth_to_token(
        token_address="0x...",
        amount_in_wei=10**16,  # 0.01 ETH
        slippage=0.05,  # 5%
    )
"""

import os
import asyncio
import logging
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from decimal import Decimal

import aiohttp
from eth_account import Account
from eth_account.signers.local import LocalAccount
from eth_abi import encode
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)


# CONSTANTS - Base Mainnet

CHAIN_ID = 8453  # Base Mainnet

# Uniswap V4 Contracts on Base (checksummed addresses)
UNIVERSAL_ROUTER = to_checksum_address("0x6ff5693b99212da76ad316178a184ab56d299b43")
POOL_MANAGER = to_checksum_address("0x498581ff718922c3f8e6a244956af099b2652b2b")
QUOTER = to_checksum_address("0x0d5e0f971ed27fbff6c2837bf31316121532048d")
PERMIT2 = to_checksum_address("0x000000000022D473030F116dDEE9F6B43aC78BA3")
WETH = to_checksum_address("0x4200000000000000000000000000000000000006")

# Universal Router Commands
CMD_V4_SWAP = 0x10
CMD_WRAP_ETH = 0x0b
CMD_UNWRAP_WETH = 0x0c
CMD_SWEEP = 0x04

# V4 Actions (inside V4_SWAP) - FROM OFFICIAL Actions.sol
# https://github.com/Uniswap/v4-periphery/blob/main/src/libraries/Actions.sol
ACTION_SWAP_EXACT_IN_SINGLE = 0x06  # Performs single-hop exact input swap
ACTION_SWAP_EXACT_IN = 0x07         # Multi-hop exact input swap
ACTION_SETTLE = 0x0b                # settle(currency, amount, payerIsUser)
ACTION_SETTLE_ALL = 0x0c            # settleAll(currency, maxAmount) - pulls from user via Permit2
ACTION_TAKE = 0x0e                  # take(currency, recipient, amount)
ACTION_TAKE_ALL = 0x0f              # takeAll(currency, minAmount) - takes all output

# Default fee tier for Clanker V4.1 pools
# 0x800000 = DYNAMIC_FEE_FLAG - hook determines the actual fee
DEFAULT_FEE = 0x800000  # 8388608 - Dynamic fee flag for Clanker v4.1
DEFAULT_TICK_SPACING = 200

# Clanker Hook addresses
# V1 hooks (older pools)
CLANKER_HOOK_STATIC_FEE_V1 = to_checksum_address("0xDd5EeaFf7BD481AD55Db083062b13a3cdf0A68CC")
CLANKER_HOOK_DYNAMIC_FEE_V1 = to_checksum_address("0x34a45c6B61876d739400Bd71228CbcbD4F53E8cC")

# V2 hooks (Clanker v4.1 - current default)
CLANKER_HOOK_STATIC_FEE_V2 = to_checksum_address("0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC")
CLANKER_HOOK_DYNAMIC_FEE_V2 = to_checksum_address("0xd60d6b218116cfd801e28f78d011a203d2b068CC")

# Default to V2 hooks (most new Clanker tokens use this)
CLANKER_HOOK_STATIC_FEE = CLANKER_HOOK_STATIC_FEE_V2
CLANKER_HOOK_DYNAMIC_FEE = CLANKER_HOOK_DYNAMIC_FEE_V2


@dataclass
class PoolData:
    """Pool parameters for V4 swap."""
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str
    zero_for_one: bool


@dataclass
class SwapResult:
    """Result of a swap execution."""
    success: bool
    tx_hash: Optional[str] = None
    error: Optional[str] = None
    gas_used: int = 0
    effective_gas_price: int = 0
    amount_out: int = 0
    rpc_used: Optional[str] = None


class SwapExecutor:
    """
    Ultra-fast swap executor for Uniswap V4 on Base.
    
    Uses multi-RPC fire pattern for maximum speed and reliability.
    """
    
    def __init__(
        self,
        private_key: str,
        rpc_urls: List[str] = None,
        max_priority_fee_gwei: float = 0.01,
        max_fee_gwei: float = 0.1,
    ):
        """
        Initialize swap executor.
        
        Args:
            private_key: Wallet private key (hex string, with or without 0x)
            rpc_urls: List of RPC endpoints for multi-fire
            max_priority_fee_gwei: Priority fee in gwei
            max_fee_gwei: Max fee per gas in gwei
        """
        # Clean private key
        if private_key.startswith("0x"):
            private_key = private_key[2:]
        
        self.account: LocalAccount = Account.from_key(bytes.fromhex(private_key))
        self.address = self.account.address
        
        # RPC endpoints
        self.rpc_urls = rpc_urls or self._get_default_rpcs()
        
        # Gas settings (very low for Base L2)
        self.max_priority_fee = int(max_priority_fee_gwei * 10**9)
        self.max_fee = int(max_fee_gwei * 10**9)
        
        # HTTP session with connection pooling
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        
        # Nonce tracking
        self._nonce: Optional[int] = None
        self._nonce_lock = asyncio.Lock()
    
    def _get_default_rpcs(self) -> List[str]:
        """Get default RPC endpoints from environment or hardcoded."""
        rpcs = []
        
        # Check environment variables
        env_rpcs = os.getenv("BASE_RPC_URLS", "").split(",")
        for rpc in env_rpcs:
            if rpc.strip():
                rpcs.append(rpc.strip())
        
        # Add ANKR RPC if available
        ankr_rpc = os.getenv("ANKR_RPC_HTTP", "")
        if ankr_rpc and ankr_rpc not in rpcs:
            rpcs.append(ankr_rpc)
        
        # Fallback to public RPCs
        if not rpcs:
            rpcs = [
                "https://mainnet.base.org",
                "https://base.llamarpc.com",
                "https://1rpc.io/base",
                "https://base.blockpi.network/v1/rpc/public",
            ]
        
        return rpcs
    
    async def connect(self):
        """Initialize HTTP session with connection pooling."""
        if self._session:
            return
        
        # Create connector with keepalive
        self._connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
        )
        
        # Create session
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=aiohttp.ClientTimeout(total=30, connect=5),
        )
        
        # Pre-fetch nonce
        await self._refresh_nonce()
        
        logger.info(f"[SWAP] Connected with {len(self.rpc_urls)} RPCs, address: {self.address}")
    
    async def close(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._connector:
            await self._connector.close()
            self._connector = None
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    # NONCE MANAGEMENT
    
    async def _do_refresh_nonce(self):
        """Internal: Fetch nonce from chain (no lock, caller must hold lock)."""
        last_error = None
        for rpc_url in self.rpc_urls:
            try:
                nonce = await self._rpc_call("eth_getTransactionCount", [self.address, "pending"], rpc_url)
                self._nonce = int(nonce, 16)
                logger.debug(f"[SWAP] Nonce refreshed: {self._nonce} (via {rpc_url[:30]}...)")
                return
            except Exception as e:
                last_error = e
                logger.warning(f"[NONCE] Failed on {rpc_url[:30]}...: {e}")
                continue
        
        # All RPCs failed
        raise Exception(f"Failed to get nonce from all {len(self.rpc_urls)} RPCs: {last_error}")
    
    async def _refresh_nonce(self):
        """Fetch current nonce from chain with RPC fallback (public, with lock)."""
        async with self._nonce_lock:
            await self._do_refresh_nonce()
    
    async def _get_next_nonce(self) -> int:
        """Get next nonce (always fresh from RPC to avoid stale cache).
        
        IMPORTANT: We always fetch from RPC because the wallet might have
        sent transactions from other platforms (e.g., Metamask, other bots)
        which would make our cached nonce stale.
        """
        async with self._nonce_lock:
            # Always fetch fresh nonce from RPC
            await self._do_refresh_nonce()
            nonce = self._nonce
            self._nonce += 1
            return nonce
    
    async def allocate_nonces(self, count: int) -> List[int]:
        """
        Pre-allocate multiple sequential nonces for batch parallel execution.
        
        This allows executing multiple swaps in parallel without nonce conflicts.
        All nonces are reserved upfront in a single lock acquisition.
        
        Args:
            count: Number of nonces to allocate
            
        Returns:
            List of sequential nonces
        """
        if count <= 0:
            return []
        
        async with self._nonce_lock:
            # Fresh fetch from RPC
            await self._do_refresh_nonce()
            base_nonce = self._nonce
            self._nonce += count  # Reserve all nonces
            
            nonces = list(range(base_nonce, base_nonce + count))
            logger.info(f"[NONCE] Allocated {count} nonces: {base_nonce} to {base_nonce + count - 1}")
            return nonces
    
    # RPC CALLS
    
    async def _rpc_call(
        self,
        method: str,
        params: List[Any],
        rpc_url: str = None,
    ) -> Any:
        """Make a single RPC call."""
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        rpc_url = rpc_url or self.rpc_urls[0]
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        
        try:
            async with self._session.post(rpc_url, json=payload) as resp:
                data = await resp.json()
                
                if "error" in data:
                    raise Exception(f"RPC error: {data['error']}")
                
                return data.get("result")
        except Exception as e:
            logger.error(f"[RPC] Error calling {method} on {rpc_url}: {e}")
            raise
    
    async def _fire_transaction(self, signed_tx_hex: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Fire transaction to ALL RPCs simultaneously.
        
        Returns the first successful tx_hash and the RPC that succeeded.
        """
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        
        async def send_to_rpc(rpc_url: str) -> Tuple[str, Optional[str], Optional[str]]:
            """Send to single RPC, return (rpc_url, tx_hash, error)."""
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendRawTransaction",
                "params": [signed_tx_hex],
            }
            
            try:
                async with self._session.post(rpc_url, json=payload) as resp:
                    data = await resp.json()
                    
                    if "error" in data:
                        return (rpc_url, None, str(data["error"]))
                    
                    tx_hash = data.get("result")
                    return (rpc_url, tx_hash, None)
            
            except Exception as e:
                return (rpc_url, None, str(e))
        
        # Fire to ALL RPCs simultaneously
        logger.info(f"[FIRE] Sending to {len(self.rpc_urls)} RPCs...")
        tasks = [send_to_rpc(rpc) for rpc in self.rpc_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Find first success
        tx_hash = None
        rpc_used = None
        errors = []
        
        for result in results:
            if isinstance(result, Exception):
                errors.append(str(result))
                continue
            
            rpc_url, hash_result, error = result
            
            if hash_result:
                tx_hash = hash_result
                rpc_used = rpc_url
                logger.info(f"[FIRE] Success from {rpc_url}: {tx_hash}")
                break
            else:
                errors.append(f"{rpc_url}: {error}")
        
        if not tx_hash:
            logger.error(f"[FIRE] All RPCs failed: {errors}")
        
        return tx_hash, rpc_used
    
    # POOL DATA VALIDATION
    
    def _validate_pool_data(
        self,
        pool_data: Dict[str, Any],
        token_address: str,
    ) -> Dict[str, Any]:
        """
        Validate pool data before using it.
        
        Checks:
        - Required fields exist
        - Hooks address is known (or logs warning)
        - Fee is reasonable
        """
        required_fields = ["currency0", "currency1", "fee", "tick_spacing", "hooks"]
        
        # Check required fields
        for field in required_fields:
            if field not in pool_data:
                logger.warning(f"[POOL] Missing field '{field}' in pool_data, using defaults")
                return None  # Will use defaults
        
        # Validate hooks address
        hooks = pool_data.get("hooks", "").lower()
        known_hooks = {
            CLANKER_HOOK_STATIC_FEE_V1.lower(): "ClankerHookStaticFee (v1)",
            CLANKER_HOOK_DYNAMIC_FEE_V1.lower(): "ClankerHookDynamicFee (v1)",
            CLANKER_HOOK_STATIC_FEE_V2.lower(): "ClankerHookStaticFeeV2 (v4.1)",
            CLANKER_HOOK_DYNAMIC_FEE_V2.lower(): "ClankerHookDynamicFeeV2 (v4.1)",
        }
        
        if hooks and hooks not in known_hooks:
            logger.warning(
                f"[POOL] Unknown hooks address: {hooks}\n"
                f"  Known V1: {CLANKER_HOOK_STATIC_FEE_V1[:16]}... (static)\n"
                f"  Known V2: {CLANKER_HOOK_STATIC_FEE_V2[:16]}... (static v4.1)\n"
                f"  Using provided hooks anyway - pool might not exist!"
            )
        elif hooks:
            logger.info(f"[POOL] Hooks: {known_hooks.get(hooks, 'Unknown')}")
        
        # Validate fee
        fee = pool_data.get("fee", 0)
        DYNAMIC_FEE_FLAG = 0x800000  # Bit 23
        
        if fee & DYNAMIC_FEE_FLAG:
            logger.info(f"[POOL] Dynamic fee pool detected (fee=0x{fee:x})")
        elif fee > 1000000:  # > 100%
            logger.warning(f"[POOL] Unusual fee: {fee} ({fee/10000:.2f}%)")
        
        # Validate currencies match token
        token_lower = token_address.lower()
        c0 = pool_data.get("currency0", "").lower()
        c1 = pool_data.get("currency1", "").lower()
        weth_lower = WETH.lower()
        
        if token_lower not in [c0, c1]:
            logger.error(
                f"[POOL] Token mismatch! Token {token_address[:10]}... not in pool\n"
                f"  currency0: {c0[:10]}...\n"
                f"  currency1: {c1[:10]}..."
            )
            return None
        
        if weth_lower not in [c0, c1]:
            logger.error(f"[POOL] WETH not in pool currencies!")
            return None
        
        logger.info(
            f"[POOL] Validated pool data:\n"
            f"  fee={fee} ({fee/10000:.2f}% or dynamic)\n"
            f"  tick_spacing={pool_data.get('tick_spacing')}\n"
            f"  hooks={hooks[:16]}...\n"
            f"  zero_for_one={pool_data.get('zero_for_one')}"
        )
        
        return pool_data
    
    # QUOTER
    
    async def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee: int = DEFAULT_FEE,
    ) -> int:
        """
        Get quote for swap amount.
        
        Returns expected amount out.
        """
        # For V4, we use the V4Quoter contract
        # quoteExactInputSingle((address,address,uint256,uint24,bytes32))
        
        # Simplified - for production use proper quoter encoding
        # For now, return amount_in as placeholder (will be overwritten)
        logger.warning("[QUOTE] Using simplified quote - implement proper V4 quoter")
        return amount_in
    
    # SWAP EXECUTION
    
    async def swap_eth_to_token(
        self,
        token_address: str,
        amount_in_wei: int,
        slippage: float = 0.05,
        deadline_seconds: int = 300,
        pool_id: str = None,
        pool_data: Dict[str, Any] = None,
        use_weth: bool = False,
        force_eth_mode: bool = True,
    ) -> SwapResult:
        """
        Swap ETH/WETH to token using Uniswap V4.
        
        Args:
            token_address: Token to buy
            amount_in_wei: Amount of ETH/WETH in wei
            slippage: Slippage tolerance (0.05 = 5%)
            deadline_seconds: Transaction deadline
            pool_id: Optional specific pool ID (for Clanker pools)
            pool_data: Optional cached pool data from clanker_enricher
                       (fee, tick_spacing, hooks, zero_for_one)
            use_weth: If True, use WETH mode (requires WETH + Permit2 approval)
                      If False, use ETH mode (wraps ETH first)
            force_eth_mode: If True (default), ALWAYS use ETH mode for reliability
        
        Returns:
            SwapResult with tx_hash or error
        """
        if not self._session:
            await self.connect()
        
        # SAFETY: Force ETH mode unless explicitly disabled
        # WETH mode requires setup (WETH balance + Permit2 approval)
        if force_eth_mode:
            if use_weth:
                logger.warning(
                    "[SWAP] use_weth=True requested but force_eth_mode=True. "
                    "Using ETH mode for reliability. Set force_eth_mode=False to override."
                )
            use_weth = False
        
        try:
            # Normalize and checksum addresses
            token_address = to_checksum_address(token_address.lower())
            
            # Validate pool data if provided
            if pool_data:
                pool_data = self._validate_pool_data(pool_data, token_address)
            
            # Calculate minimum output (with slippage)
            # For now, use 0 as min (dangerous in production!)
            # In production, use quoter to get expected output
            min_amount_out = 0  # TODO: Use quoter
            
            # Build transaction with pool data if available
            tx_data = self._build_v4_swap_data(
                token_address=token_address,
                amount_in=amount_in_wei,
                min_amount_out=min_amount_out,
                deadline_seconds=deadline_seconds,
                pool_id=pool_id,
                pool_data=pool_data,
                use_weth=use_weth,
            )
            
            # Get nonce
            nonce = await self._get_next_nonce()
            
            # Gas and value depend on mode
            # NOTE: Clanker hooks do complex operations (fee collection, multiple swaps)
            # and require significantly more gas than standard V4 swaps
            if use_weth:
                # WETH mode: no ETH sent, higher gas for Permit2
                gas_limit = 800000  # Increased for Clanker hooks
                tx_value = 0
                mode_str = "WETH"
            else:
                # ETH mode: send ETH to wrap
                gas_limit = 800000  # Clanker hooks need ~600-800k gas
                tx_value = amount_in_wei
                mode_str = "ETH"
            
            # Build transaction dict
            tx = {
                "from": self.address,
                "to": UNIVERSAL_ROUTER,
                "value": tx_value,
                "gas": gas_limit,
                "maxFeePerGas": self.max_fee,
                "maxPriorityFeePerGas": self.max_priority_fee,
                "nonce": nonce,
                "chainId": CHAIN_ID,
                "data": tx_data,
                "type": 2,  # EIP-1559
            }
            
            # Detailed logging for debugging
            logger.info(
                f"[SWAP] {mode_str} -> {token_address[:10]}...\n"
                f"  Amount: {amount_in_wei / 10**18:.6f} {mode_str}\n"
                f"  TX Value: {tx_value / 10**18:.6f} ETH\n"
                f"  Nonce: {nonce}\n"
                f"  Gas: {gas_limit}\n"
                f"  Pool data: {pool_data.get('hooks', 'default')[:16] if pool_data else 'DEFAULT'}..."
            )
            
            # Sign transaction
            signed = self.account.sign_transaction(tx)
            signed_tx_hex = signed.raw_transaction.hex()
            if not signed_tx_hex.startswith("0x"):
                signed_tx_hex = "0x" + signed_tx_hex
            
            # FIRE to all RPCs!
            tx_hash, rpc_used = await self._fire_transaction(signed_tx_hex)
            
            if tx_hash:
                return SwapResult(
                    success=True,
                    tx_hash=tx_hash,
                    rpc_used=rpc_used,
                )
            else:
                # Refresh nonce on failure (might have been used)
                await self._refresh_nonce()
                return SwapResult(
                    success=False,
                    error="All RPCs failed to accept transaction",
                )
        
        except Exception as e:
            logger.error(f"[SWAP] Error: {e}")
            return SwapResult(success=False, error=str(e))
    
    async def swap_eth_to_token_with_nonce(
        self,
        token_address: str,
        amount_in_wei: int,
        nonce: int,
        slippage: float = 0.05,
        deadline_seconds: int = 300,
        pool_id: str = None,
        pool_data: Dict[str, Any] = None,
    ) -> SwapResult:
        """
        Swap ETH to token using a pre-allocated nonce.
        
        This method is designed for batch parallel execution where nonces
        are pre-allocated via allocate_nonces() to avoid lock contention.
        
        Args:
            token_address: Token to buy
            amount_in_wei: Amount of ETH in wei
            nonce: Pre-allocated nonce (from allocate_nonces)
            slippage: Slippage tolerance (0.05 = 5%)
            deadline_seconds: Transaction deadline
            pool_id: Optional specific pool ID
            pool_data: Optional cached pool data
            
        Returns:
            SwapResult with tx_hash or error
        """
        if not self._session:
            await self.connect()
        
        try:
            token_address = to_checksum_address(token_address.lower())
            
            if pool_data:
                pool_data = self._validate_pool_data(pool_data, token_address)
            
            min_amount_out = 0  # TODO: Use quoter
            
            tx_data = self._build_v4_swap_data(
                token_address=token_address,
                amount_in=amount_in_wei,
                min_amount_out=min_amount_out,
                deadline_seconds=deadline_seconds,
                pool_id=pool_id,
                pool_data=pool_data,
                use_weth=False,  # Always ETH mode
            )
            
            # Build transaction with pre-allocated nonce
            gas_limit = 800000
            tx = {
                "from": self.address,
                "to": UNIVERSAL_ROUTER,
                "value": amount_in_wei,
                "gas": gas_limit,
                "maxFeePerGas": self.max_fee,
                "maxPriorityFeePerGas": self.max_priority_fee,
                "nonce": nonce,
                "chainId": CHAIN_ID,
                "data": tx_data,
                "type": 2,
            }
            
            logger.info(
                f"[BATCH SWAP] ETH -> {token_address[:10]}...\n"
                f"  Amount: {amount_in_wei / 10**18:.6f} ETH\n"
                f"  Nonce: {nonce}\n"
                f"  Pool: {pool_data.get('hooks', 'default')[:16] if pool_data else 'DEFAULT'}..."
            )
            
            # Sign and fire
            signed = self.account.sign_transaction(tx)
            signed_tx_hex = signed.raw_transaction.hex()
            if not signed_tx_hex.startswith("0x"):
                signed_tx_hex = "0x" + signed_tx_hex
            
            tx_hash, rpc_used = await self._fire_transaction(signed_tx_hex)
            
            if tx_hash:
                return SwapResult(success=True, tx_hash=tx_hash, rpc_used=rpc_used)
            else:
                return SwapResult(success=False, error="All RPCs failed")
        
        except Exception as e:
            logger.error(f"[BATCH SWAP] Error: {e}")
            return SwapResult(success=False, error=str(e))
    
    def _build_v4_swap_data(
        self,
        token_address: str,
        amount_in: int,
        min_amount_out: int,
        deadline_seconds: int,
        pool_id: str = None,
        pool_data: Dict[str, Any] = None,
        use_weth: bool = False,
    ) -> str:
        """
        Build calldata for Universal Router V4 swap.
        
        Two modes:
        1. use_weth=False (default, RECOMMENDED): WRAP_ETH + V4_SWAP
           - User sends ETH with the transaction
           - Router wraps ETH to WETH, then swaps
           - No prior setup required
           
        2. use_weth=True (advanced): V4_SWAP only with SETTLE_ALL
           - Requires WETH already in wallet
           - Requires Permit2 approval for WETH
           - Will FAIL if not set up properly!
        
        ALWAYS use ETH mode (use_weth=False) unless you've explicitly set up WETH + Permit2.
        """
        import time
        
        deadline = int(time.time()) + deadline_seconds
        
        # Build the path for V4
        # PoolKey structure: (currency0, currency1, fee, tickSpacing, hooks)
        
        # Use cached pool data if available (from clanker_enricher)
        if pool_data:
            currency0 = to_checksum_address(pool_data.get("currency0", WETH))
            currency1 = to_checksum_address(pool_data.get("currency1", token_address))
            fee = pool_data.get("fee", DEFAULT_FEE)
            tick_spacing = pool_data.get("tick_spacing", DEFAULT_TICK_SPACING)
            hooks = to_checksum_address(pool_data.get("hooks", CLANKER_HOOK_STATIC_FEE))
            zero_for_one = pool_data.get("zero_for_one", True)
            
            logger.info(
                f"[V4] Using cached pool data:\n"
                f"  currency0: {currency0}\n"
                f"  currency1: {currency1}\n"
                f"  fee: {fee} (0x{fee:x})\n"
                f"  tick_spacing: {tick_spacing}\n"
                f"  hooks: {hooks}\n"
                f"  zero_for_one: {zero_for_one}"
            )
        else:
            # Fallback: determine token order (V4 requires currency0 < currency1)
            weth_addr = WETH
            token_addr = to_checksum_address(token_address)
            
            if int(weth_addr.lower(), 16) < int(token_addr.lower(), 16):
                currency0 = weth_addr
                currency1 = token_addr
                zero_for_one = True
            else:
                currency0 = token_addr
                currency1 = weth_addr
                zero_for_one = False
            
            # Default Clanker pool params
            fee = DEFAULT_FEE  # 100000 = 10%
            tick_spacing = DEFAULT_TICK_SPACING  # 200
            hooks = CLANKER_HOOK_STATIC_FEE  # Clanker static fee hook
            
            logger.warning(
                f"[V4] No cached pool data - using defaults: fee={fee} hooks={hooks[:10]}..."
            )
        
        if use_weth:
            # MODE: V4_SWAP only (user has WETH, uses Permit2/SETTLE_ALL)
            # This is the PROVEN working approach from successful TXs
            logger.info("[V4] Using WETH mode (SETTLE_ALL from user wallet)")
            
            v4_swap_input = self._encode_v4_swap_actions_settle_all(
                currency0=currency0,
                currency1=currency1,
                fee=fee,
                tick_spacing=tick_spacing,
                hooks=hooks,
                zero_for_one=zero_for_one,
                amount_in=amount_in,
                min_amount_out=min_amount_out,
            )
            
            commands = bytes([CMD_V4_SWAP])
            inputs = [v4_swap_input]
        else:
            # MODE: WRAP_ETH + V4_SWAP (user sends ETH)
            # Uses SETTLE with payerIsUser=false to settle from router
            logger.info("[V4] Using ETH mode (WRAP_ETH + SETTLE from router)")
            
            # WRAP_ETH: (address recipient, uint256 amountMin)
            wrap_input = encode(
                ["address", "uint256"],
                [
                    "0x0000000000000000000000000000000000000002",  # ADDRESS_THIS
                    amount_in,
                ]
            )
            
            v4_swap_input = self._encode_v4_swap_actions(
                currency0=currency0,
                currency1=currency1,
                fee=fee,
                tick_spacing=tick_spacing,
                hooks=hooks,
                zero_for_one=zero_for_one,
                amount_in=amount_in,
                min_amount_out=min_amount_out,
            )
            
            commands = bytes([CMD_WRAP_ETH, CMD_V4_SWAP])
            inputs = [wrap_input, v4_swap_input]
        
        # Encode execute(bytes,bytes[],uint256)
        # Function selector: 0x3593564c
        selector = bytes.fromhex("3593564c")
        
        encoded_params = encode(
            ["bytes", "bytes[]", "uint256"],
            [commands, inputs, deadline]
        )
        
        return "0x" + selector.hex() + encoded_params.hex()
    
    def _encode_v4_swap_actions(
        self,
        currency0: str,
        currency1: str,
        fee: int,
        tick_spacing: int,
        hooks: str,
        zero_for_one: bool,
        amount_in: int,
        min_amount_out: int,
    ) -> bytes:
        """
        Encode V4 swap actions for ETH mode (WRAP_ETH + V4_SWAP).
        
        CRITICAL: This mode uses SETTLE with payerIsUser=false because after WRAP_ETH,
        the WETH is in the ROUTER, not in the user's wallet!
        
        ExactInputSingleParams struct (NO sqrtPriceLimitX96 - V4Router sets it internally!):
        struct ExactInputSingleParams {
            PoolKey poolKey;        // (currency0, currency1, fee, tickSpacing, hooks)
            bool zeroForOne;
            uint128 amountIn;
            uint128 amountOutMinimum;
            bytes hookData;
        }
        
        IMPORTANT: The struct MUST be encoded as a tuple with proper ABI offset encoding!
        The V4Router's CalldataDecoder expects: [offset to struct][struct data at offset]
        
        Actions: SWAP_EXACT_IN_SINGLE (0x06) -> SETTLE (0x0b) -> TAKE_ALL (0x0f)
        SETTLE with payerIsUser=false pulls WETH from the router (after WRAP_ETH)
        """
        
        logger.info(
            f"[V4] Building swap (ETH mode - SETTLE from router):\n"
            f"  zeroForOne: {zero_for_one}\n"
            f"  fee: {fee} (0x{fee:x})\n"
            f"  hooks: {hooks}\n"
            f"  amountIn: {amount_in}"
        )
        
        # Encode ExactInputSingleParams as a TUPLE (struct) with proper ABI encoding
        # This ensures the decoder gets the correct offset-based format
        # Structure: ((PoolKey), zeroForOne, amountIn, amountOutMinimum, hookData)
        # PoolKey: (currency0, currency1, fee, tickSpacing, hooks)
        swap_params = encode(
            ["((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"],
            [
                (
                    (currency0, currency1, fee, tick_spacing, hooks),  # PoolKey as tuple
                    zero_for_one,
                    amount_in,
                    min_amount_out,
                    b"",  # Empty hookData
                )
            ]
        )
        
        # Actions: SWAP_EXACT_IN_SINGLE -> SETTLE -> TAKE_ALL
        # SETTLE (not SETTLE_ALL) because WETH is in the router after WRAP_ETH
        actions = bytes([ACTION_SWAP_EXACT_IN_SINGLE, ACTION_SETTLE, ACTION_TAKE_ALL])
        
        # SETTLE params: (Currency currency, uint256 amount, bool payerIsUser)
        # payerIsUser=false means the router pays (has WETH from WRAP_ETH)
        input_currency = currency0 if zero_for_one else currency1
        settle_params = encode(
            ["address", "uint256", "bool"],
            [input_currency, amount_in, False]  # payerIsUser=false!
        )
        
        # TAKE_ALL params: (Currency currency, uint256 minAmount)
        # Takes all output tokens to user
        output_currency = currency1 if zero_for_one else currency0
        take_all_params = encode(
            ["address", "uint256"],
            [output_currency, min_amount_out]
        )
        
        # Combine all params
        params_array = [swap_params, settle_params, take_all_params]
        
        # Final V4_SWAP encoding: (bytes actions, bytes[] params)
        return encode(["bytes", "bytes[]"], [actions, params_array])
    
    def _encode_v4_swap_actions_settle_all(
        self,
        currency0: str,
        currency1: str,
        fee: int,
        tick_spacing: int,
        hooks: str,
        zero_for_one: bool,
        amount_in: int,
        min_amount_out: int,
    ) -> bytes:
        """
        Encode V4 swap actions using SETTLE_ALL (0x0c).
        
        This mode pulls WETH directly from the user's wallet via Permit2.
        
        User must have:
        1. WETH in wallet
        2. Permit2 approved for WETH
        
        IMPORTANT: The struct MUST be encoded as a tuple with proper ABI offset encoding!
        
        Actions:
        1. SWAP_EXACT_IN_SINGLE (0x06)
        2. SETTLE_ALL (0x0c) - pulls from user wallet via Permit2
        3. TAKE_ALL (0x0f) - takes all output tokens
        """
        
        logger.info(
            f"[V4] WETH mode (SETTLE_ALL) - tuple encoding:\n"
            f"  zeroForOne: {zero_for_one}\n"
            f"  fee: {fee} (0x{fee:x})\n"
            f"  hooks: {hooks}"
        )
        
        # Encode ExactInputSingleParams as a TUPLE (struct) with proper ABI encoding
        swap_params = encode(
            ["((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"],
            [
                (
                    (currency0, currency1, fee, tick_spacing, hooks),  # PoolKey as tuple
                    zero_for_one,
                    amount_in,
                    min_amount_out,
                    b"",  # Empty hookData
                )
            ]
        )
        
        # Actions: SWAP_EXACT_IN_SINGLE -> SETTLE_ALL -> TAKE_ALL
        actions = bytes([ACTION_SWAP_EXACT_IN_SINGLE, ACTION_SETTLE_ALL, ACTION_TAKE_ALL])
        
        # SETTLE_ALL params: (Currency currency, uint256 maxAmount)
        settle_currency = currency0 if zero_for_one else currency1
        settle_params = encode(["address", "uint256"], [settle_currency, amount_in])
        
        # TAKE_ALL params: (Currency currency, uint256 minAmount)
        take_currency = currency1 if zero_for_one else currency0
        take_params = encode(["address", "uint256"], [take_currency, min_amount_out])
        
        params_array = [swap_params, settle_params, take_params]
        
        return encode(["bytes", "bytes[]"], [actions, params_array])
    
    # WETH & PERMIT2 SETUP
    
    async def wrap_eth(self, amount_wei: int) -> Optional[str]:
        """
        Wrap ETH to WETH by depositing to WETH contract.
        
        Returns tx_hash on success, None on failure.
        """
        # WETH deposit() - just send ETH to WETH contract
        # deposit() selector: 0xd0e30db0
        deposit_data = "0xd0e30db0"
        
        nonce = await self._get_next_nonce()
        
        tx = {
            "from": self.address,
            "to": WETH,
            "value": amount_wei,
            "gas": 50000,
            "maxFeePerGas": self.max_fee,
            "maxPriorityFeePerGas": self.max_priority_fee,
            "nonce": nonce,
            "chainId": CHAIN_ID,
            "data": deposit_data,
            "type": 2,
        }
        
        signed = self.account.sign_transaction(tx)
        signed_tx_hex = "0x" + signed.raw_transaction.hex()
        
        logger.info(f"[WRAP] Wrapping {amount_wei / 10**18:.6f} ETH to WETH...")
        
        tx_hash, rpc_used = await self._fire_transaction(signed_tx_hex)
        
        if tx_hash:
            logger.info(f"[WRAP] Success! TX: {tx_hash}")
            return tx_hash
        else:
            logger.error("[WRAP] Failed to wrap ETH")
            await self._refresh_nonce()
            return None
    
    async def approve_permit2(self, amount: int = 2**256 - 1) -> Optional[str]:
        """
        Approve Permit2 contract to spend WETH.
        
        This is required once before using WETH mode swaps.
        Returns tx_hash on success, None on failure.
        """
        # approve(address spender, uint256 amount)
        # selector: 0x095ea7b3
        approve_data = "0x095ea7b3" + encode(
            ["address", "uint256"],
            [PERMIT2, amount]
        ).hex()
        
        nonce = await self._get_next_nonce()
        
        tx = {
            "from": self.address,
            "to": WETH,
            "value": 0,
            "gas": 60000,
            "maxFeePerGas": self.max_fee,
            "maxPriorityFeePerGas": self.max_priority_fee,
            "nonce": nonce,
            "chainId": CHAIN_ID,
            "data": approve_data,
            "type": 2,
        }
        
        signed = self.account.sign_transaction(tx)
        signed_tx_hex = "0x" + signed.raw_transaction.hex()
        
        logger.info(f"[APPROVE] Approving Permit2 for WETH...")
        
        tx_hash, rpc_used = await self._fire_transaction(signed_tx_hex)
        
        if tx_hash:
            logger.info(f"[APPROVE] Success! TX: {tx_hash}")
            return tx_hash
        else:
            logger.error("[APPROVE] Failed to approve Permit2")
            await self._refresh_nonce()
            return None
    
    async def get_weth_balance(self) -> int:
        """Get WETH balance."""
        return await self.get_token_balance(WETH)
    
    async def get_permit2_allowance(self) -> int:
        """Get WETH allowance for Permit2."""
        # allowance(address owner, address spender)
        data = "0xdd62ed3e" + encode(["address", "address"], [self.address, PERMIT2]).hex()
        
        result = await self._rpc_call("eth_call", [{"to": WETH, "data": data}, "latest"])
        return int(result, 16) if result and result != "0x" else 0
    
    async def setup_for_weth_swap(self, amount_wei: int) -> bool:
        """
        Setup wallet for WETH swap mode.
        
        1. Check WETH balance, wrap if needed
        2. Check Permit2 allowance, approve if needed
        
        Returns True if ready, False if setup failed.
        """
        # Check WETH balance
        weth_balance = await self.get_weth_balance()
        logger.info(f"[SETUP] WETH balance: {weth_balance / 10**18:.6f}")
        
        if weth_balance < amount_wei:
            # Need to wrap more ETH
            wrap_amount = amount_wei - weth_balance
            eth_balance = await self.get_balance()
            
            if eth_balance < wrap_amount:
                logger.error(f"[SETUP] Insufficient ETH! Need {wrap_amount / 10**18:.6f}, have {eth_balance / 10**18:.6f}")
                return False
            
            tx_hash = await self.wrap_eth(wrap_amount)
            if not tx_hash:
                return False
            
            # Wait for confirmation
            await asyncio.sleep(2)
        
        # Check Permit2 allowance
        allowance = await self.get_permit2_allowance()
        logger.info(f"[SETUP] Permit2 allowance: {allowance}")
        
        if allowance < amount_wei:
            tx_hash = await self.approve_permit2()
            if not tx_hash:
                return False
            
            # Wait for confirmation
            await asyncio.sleep(2)
        
        logger.info("[SETUP] Ready for WETH swap!")
        return True
    
    # UTILITY METHODS
    
    async def get_balance(self) -> int:
        """Get ETH balance in wei."""
        result = await self._rpc_call("eth_getBalance", [self.address, "latest"])
        return int(result, 16)
    
    async def get_token_balance(self, token_address: str) -> int:
        """Get token balance."""
        # balanceOf(address)
        token_address = to_checksum_address(token_address)
        data = "0x70a08231" + encode(["address"], [self.address]).hex()
        
        result = await self._rpc_call("eth_call", [
            {"to": token_address, "data": data},
            "latest"
        ])
        
        return int(result, 16) if result and result != "0x" else 0
    
    async def wait_for_receipt(
        self,
        tx_hash: str,
        timeout: int = 60,
        poll_interval: float = 1.0,
    ) -> Optional[Dict[str, Any]]:
        """Wait for transaction receipt."""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                receipt = await self._rpc_call("eth_getTransactionReceipt", [tx_hash])
                if receipt:
                    return receipt
            except:
                pass
            
            await asyncio.sleep(poll_interval)
        
        return None


# CONVENIENCE FUNCTION

async def execute_swap(
    private_key: str,
    token_address: str,
    amount_eth: float,
    slippage: float = 0.05,
    rpc_urls: List[str] = None,
) -> SwapResult:
    """
    Convenience function to execute a swap.
    
    Args:
        private_key: Wallet private key
        token_address: Token to buy
        amount_eth: Amount of ETH to spend
        slippage: Slippage tolerance
        rpc_urls: Optional list of RPC URLs
    
    Returns:
        SwapResult
    """
    amount_wei = int(amount_eth * 10**18)
    
    async with SwapExecutor(private_key, rpc_urls) as executor:
        return await executor.swap_eth_to_token(
            token_address=token_address,
            amount_in_wei=amount_wei,
            slippage=slippage,
        )


# TEST

if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    async def test():
        # Test with a dummy key (won't actually execute)
        executor = SwapExecutor(
            private_key="0x" + "a" * 64,  # Dummy key
            rpc_urls=["https://mainnet.base.org"],
        )
        
        await executor.connect()
        
        balance = await executor.get_balance()
        print(f"Balance: {balance / 10**18:.6f} ETH")
        
        await executor.close()
    
    asyncio.run(test())
