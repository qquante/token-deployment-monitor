"""
Clanker Enricher Worker

Listens for new tokens from onchain_listener and immediately fetches
pool data from the blockchain, caching it for later use.

Responsibilities:
- Subscribe to onchain:token events
- Fetch pool info (fee, tickSpacing, hooks, sqrtPriceX96)
- Cache pool data in Redis for trading

Input Channels:
- onchain:token - TokenCreated events

Output:
- Redis cache: pool:{token_address}
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from datetime import datetime

from eth_abi import decode
from eth_utils import keccak, to_checksum_address

from workers.base import BaseWorker
from shared.pubsub import Channels


# Uniswap V4 / Clanker contracts on Base
POOL_MANAGER = to_checksum_address("0x498581ff718922c3f8e6a244956af099b2652b2b")
WETH = to_checksum_address("0x4200000000000000000000000000000000000006")

# Clanker Hook addresses (v4.0.0)
CLANKER_HOOK_STATIC_FEE = to_checksum_address("0xDd5EeaFf7BD481AD55Db083062b13a3cdf0A68CC")
CLANKER_HOOK_DYNAMIC_FEE = to_checksum_address("0x34a45c6B61876d739400Bd71228CbcbD4F53E8cC")

# Pool cache TTL
POOL_CACHE_TTL = 86400 * 7  # 7 days - pool params don't change


class ClankerEnricherWorker(BaseWorker):
    """
    Enriches tokens with Clanker pool data immediately when detected.
    """
    
    WORKER_NAME = "clanker-enricher"
    
    def __init__(self):
        super().__init__()
        
        self.http_url = os.getenv("ANKR_RPC_HTTP", "")
        
        if not self.http_url:
            raise ValueError("ANKR_RPC_HTTP not set!")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Stats
        self.stats.update({
            "tokens_enriched": 0,
            "pools_found": 0,
            "pools_not_found": 0,
            "cache_hits": 0,
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
        """Main loop - subscribe to onchain:token events."""
        self.logger.info("Subscribing to onchain:token events...")
        
        async for channel, event in self.redis.subscribe(Channels.ONCHAIN_TOKEN):
            if not self._running:
                break
            
            try:
                await self._process_token(event)
            except Exception as e:
                self.logger.error(f"Error processing token: {e}", exc_info=True)
                self.stats["errors"] += 1
    
    async def _process_token(self, event: Dict[str, Any]):
        """Process a new token event and fetch pool data."""
        token_address = event.get("token_address", "")
        token_symbol = event.get("token_symbol", "")
        pool_id = event.get("pool_id", "")
        
        if not token_address:
            return
        
        self.logger.info(f"[ENRICH] Processing ${token_symbol} ({token_address[:10]}...)")
        
        # Check if already cached
        cached = await self._get_cached_pool(token_address)
        if cached:
            self.logger.info(f"[ENRICH] Pool already cached for ${token_symbol}")
            self.stats["cache_hits"] += 1
            return
        
        # Fetch pool data from blockchain
        pool_data = await self._fetch_pool_data(token_address)
        
        if pool_data:
            # Add token metadata
            pool_data["token_address"] = token_address
            pool_data["token_symbol"] = token_symbol
            pool_data["pool_id_event"] = pool_id  # From TokenCreated event
            pool_data["enriched_at"] = datetime.now().isoformat()
            
            # Save to Redis
            await self._cache_pool(token_address, pool_data)
            
            self.logger.info(
                f"[ENRICH] ✅ Pool cached for ${token_symbol}: "
                f"fee={pool_data.get('fee', 0)} hooks={pool_data.get('hooks', '')[:16]}..."
            )
            self.stats["pools_found"] += 1
        else:
            self.logger.warning(f"[ENRICH] ❌ Pool not found for ${token_symbol}")
            self.stats["pools_not_found"] += 1
        
        self.stats["tokens_enriched"] += 1
        self.stats["messages_processed"] += 1
    
    async def _fetch_pool_data(self, token_address: str) -> Optional[Dict[str, Any]]:
        """
        Fetch pool data by searching for Initialize event in PoolManager logs.
        
        Initialize event signature:
        Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, 
                   uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)
        """
        try:
            token = to_checksum_address(token_address)
            
            # Initialize event topic
            init_topic = "0x" + keccak(
                text="Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
            ).hex()
            
            # Determine token ordering (currency0 < currency1)
            if int(token.lower(), 16) < int(WETH.lower(), 16):
                currency0 = token
                currency1 = WETH
                currency0_topic = "0x" + token.lower()[2:].zfill(64)
                currency1_topic = "0x" + WETH.lower()[2:].zfill(64)
                zero_for_one = False  # WETH -> Token (buying)
            else:
                currency0 = WETH
                currency1 = token
                currency0_topic = "0x" + WETH.lower()[2:].zfill(64)
                currency1_topic = "0x" + token.lower()[2:].zfill(64)
                zero_for_one = True  # WETH -> Token (buying)
            
            # Get latest block
            latest_block = await self._rpc_call("eth_blockNumber", [])
            latest = int(latest_block, 16)
            
            # Search last 5000 blocks (pool should be in recent blocks for new tokens)
            search_range = 5000
            
            for start in range(latest - search_range, latest, 2000):
                end = min(start + 2000, latest)
                
                filter_params = {
                    "address": POOL_MANAGER,
                    "topics": [
                        init_topic,
                        None,  # pool id (any)
                        currency0_topic,
                        currency1_topic,
                    ],
                    "fromBlock": hex(start),
                    "toBlock": hex(end),
                }
                
                logs = await self._rpc_call("eth_getLogs", [filter_params])
                
                if logs:
                    # Get the most recent Initialize event
                    log = logs[-1]
                    pool_id = log["topics"][1]
                    
                    # Decode the data (fee, tickSpacing, hooks, sqrtPriceX96, tick)
                    data = bytes.fromhex(log["data"][2:])
                    fee, tick_spacing, hooks, sqrt_price_x96, tick = decode(
                        ["uint24", "int24", "address", "uint160", "int24"],
                        data
                    )
                    
                    return {
                        "pool_id": pool_id,
                        "currency0": currency0,
                        "currency1": currency1,
                        "fee": fee,
                        "tick_spacing": tick_spacing,
                        "hooks": hooks,
                        "sqrt_price_x96": str(sqrt_price_x96),
                        "tick": tick,
                        "zero_for_one": zero_for_one,
                        "block_number": int(log["blockNumber"], 16),
                        "tx_hash": log.get("transactionHash", ""),
                    }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error fetching pool data: {e}")
            return None
    
    async def _rpc_call(self, method: str, params: list) -> Any:
        """Make RPC call."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        async with self._session.post(self.http_url, json=payload) as resp:
            data = await resp.json()
            if "error" in data:
                raise Exception(f"RPC error: {data['error']}")
            return data.get("result")
    
    async def _cache_pool(self, token_address: str, pool_data: Dict[str, Any]):
        """Cache pool data in Redis."""
        key = f"pool:{token_address.lower()}"
        
        # Convert all values to strings for Redis
        redis_data = {}
        for k, v in pool_data.items():
            if isinstance(v, bool):
                redis_data[k] = "1" if v else "0"
            elif v is None:
                redis_data[k] = ""
            else:
                redis_data[k] = str(v)
        
        await self.redis.hset(key, redis_data)
        await self.redis.expire(key, POOL_CACHE_TTL)
    
    async def _get_cached_pool(self, token_address: str) -> Optional[Dict[str, Any]]:
        """Get cached pool data from Redis."""
        key = f"pool:{token_address.lower()}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        # Parse data back to proper types
        result = {}
        for k, v in data.items():
            if k in ("zero_for_one",):
                result[k] = v == "1"
            elif k in ("fee", "tick_spacing", "tick", "block_number"):
                result[k] = int(v) if v else 0
            else:
                result[k] = v
        
        return result


# Entry point
if __name__ == "__main__":
    ClankerEnricherWorker.main()
