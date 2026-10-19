"""
Async ENS and Basename resolver using aiohttp.

Resolves wallet addresses to human-readable names:
- ENS names (.eth) via ENS reverse resolution
- Basenames (.base.eth) via Base Name Service

PARALLEL resolution - tries both APIs simultaneously.
"""

import os
import logging
import asyncio
from typing import Optional, Dict, Any, List, Tuple

import aiohttp

logger = logging.getLogger(__name__)


class AsyncENSResolver:
    """
    Async resolver for ENS and Basename lookups.
    
    Key advantage: resolves Basename AND ENS in PARALLEL,
    returning whichever responds first with a valid name.
    
    Example:
        async with AsyncENSResolver() as resolver:
            name = await resolver.reverse_lookup("0x...")
            print(name)  # "vitalik.eth" or "jesse.base.eth"
    """
    
    BASENAME_API = "https://api.basename.app/v1"
    ENS_API = "https://api.ensideas.com/ens/resolve"
    
    def __init__(self, cache_ttl: int = 300):
        """
        Initialize async resolver.
        
        Args:
            cache_ttl: Cache time-to-live in seconds (default 5 min)
        """
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, str] = {}
        self._cache_ttl = cache_ttl
    
    async def __aenter__(self):
        """Create session on context enter."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session on context exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def reverse_lookup(self, address: str) -> Optional[str]:
        """
        Get ENS/Basename for a wallet address.
        
        PARALLEL: Tries Basename AND ENS simultaneously.
        Returns first valid result.
        
        Args:
            address: Ethereum/Base wallet address (0x...)
            
        Returns:
            ENS/Basename if found, None otherwise
        """
        if not address or not address.startswith("0x"):
            return None
        
        address = address.lower()
        
        # Check cache
        if address in self._cache:
            return self._cache[address]
        
        await self._ensure_session()
        
        # Try BOTH in parallel - return first success
        basename_task = self._reverse_basename(address)
        ens_task = self._reverse_ens(address)
        
        # Use asyncio.gather to run both
        results = await asyncio.gather(basename_task, ens_task, return_exceptions=True)
        
        basename_result = results[0] if not isinstance(results[0], Exception) else None
        ens_result = results[1] if not isinstance(results[1], Exception) else None
        
        # Prefer Basename (Base chain specific), fallback to ENS
        name = basename_result or ens_result
        
        if name:
            self._cache[address] = name
        
        return name
    
    async def _reverse_basename(self, address: str) -> Optional[str]:
        """
        Reverse lookup via Base Name Service API (async).
        
        Args:
            address: Wallet address
            
        Returns:
            Basename (e.g., "jesse.base.eth") or None
        """
        try:
            url = f"{self.BASENAME_API}/reverse"
            async with self._session.get(url, params={"address": address}) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data.get("name"):
                        name = data["name"]
                        logger.debug(f"Basename found: {address} -> {name}")
                        return name
        except asyncio.TimeoutError:
            logger.debug(f"Basename lookup timeout for {address}")
        except Exception as e:
            logger.debug(f"Basename lookup error for {address}: {e}")
        
        return None
    
    async def _reverse_ens(self, address: str) -> Optional[str]:
        """
        Reverse lookup via ENS API (async).
        
        Args:
            address: Wallet address
            
        Returns:
            ENS name (e.g., "vitalik.eth") or None
        """
        try:
            url = f"{self.ENS_API}/{address}"
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and data.get("name"):
                        name = data["name"]
                        logger.debug(f"ENS found: {address} -> {name}")
                        return name
        except asyncio.TimeoutError:
            logger.debug(f"ENS lookup timeout for {address}")
        except Exception as e:
            logger.debug(f"ENS lookup error for {address}: {e}")
        
        return None
    
    async def batch_reverse_lookup(self, addresses: List[str]) -> Dict[str, Optional[str]]:
        """
        Reverse lookup for multiple addresses IN PARALLEL.
        
        Args:
            addresses: List of wallet addresses
            
        Returns:
            Dict mapping address -> name (or None)
        """
        if not addresses:
            return {}
        
        # Deduplicate and filter
        unique_addresses = list(set(a.lower() for a in addresses if a and a.startswith("0x")))
        
        # Lookup all in parallel
        tasks = [self.reverse_lookup(addr) for addr in unique_addresses]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for addr, result in zip(unique_addresses, results):
            if isinstance(result, str):
                output[addr] = result
            else:
                output[addr] = None
        
        return output


# =============================================================================
# Module-level convenience
# =============================================================================

_resolver: Optional[AsyncENSResolver] = None


async def get_resolver() -> AsyncENSResolver:
    """Get or create singleton async resolver."""
    global _resolver
    if _resolver is None:
        _resolver = AsyncENSResolver()
    await _resolver._ensure_session()
    return _resolver


async def resolve_ens(address: str) -> Optional[str]:
    """Convenience function to resolve address to name."""
    resolver = await get_resolver()
    return await resolver.reverse_lookup(address)
