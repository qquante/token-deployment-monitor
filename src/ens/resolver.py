"""
ENS and Basename resolver.

Resolves wallet addresses to human-readable names:
- ENS names (.eth) via ENS reverse resolution
- Basenames (.base.eth) via Base Name Service

Uses multiple APIs for redundancy.
"""

import os
import logging
from typing import Optional, Dict, Any
from functools import lru_cache

import requests

logger = logging.getLogger(__name__)


class ENSResolver:
    """
    Resolver for ENS and Basename lookups.
    
    Supports:
    - Reverse resolution (wallet -> name)
    - Forward resolution (name -> wallet)
    
    Example:
        resolver = ENSResolver()
        
        # Reverse lookup
        name = resolver.reverse_lookup("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
        # Returns: "vitalik.eth"
        
        # Forward lookup
        address = resolver.forward_lookup("vitalik.eth")
        # Returns: "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
    """
    
    # API endpoints
    BASENAME_API = "https://api.basename.app/v1"
    ENS_GRAPH_API = "https://api.thegraph.com/subgraphs/name/ensdomains/ens"
    
    # Base mainnet RPC (free)
    BASE_RPC = "https://mainnet.base.org"
    ETH_RPC = "https://eth.llamarpc.com"
    
    def __init__(self, cache_ttl: int = 300):
        """
        Initialize resolver.
        
        Args:
            cache_ttl: Cache time-to-live in seconds (default 5 min)
        """
        self._session = requests.Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        self._cache: Dict[str, str] = {}
    
    def reverse_lookup(self, address: str) -> Optional[str]:
        """
        Get ENS/Basename for a wallet address.
        
        Tries multiple resolution methods:
        1. Basename API (for .base.eth)
        2. ENS reverse resolution (for .eth)
        
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
        
        # Try Basename first (Base chain)
        basename = self._reverse_basename(address)
        if basename:
            self._cache[address] = basename
            return basename
        
        # Try ENS (Ethereum mainnet)
        ens_name = self._reverse_ens(address)
        if ens_name:
            self._cache[address] = ens_name
            return ens_name
        
        return None
    
    def _reverse_basename(self, address: str) -> Optional[str]:
        """
        Reverse lookup via Base Name Service API.
        
        Args:
            address: Wallet address
            
        Returns:
            Basename (e.g., "jesse.base.eth") or None
        """
        try:
            url = f"{self.BASENAME_API}/reverse"
            response = self._session.get(url, params={"address": address}, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data and data.get("name"):
                    name = data["name"]
                    logger.debug(f"Basename found: {address} -> {name}")
                    return name
        except requests.exceptions.RequestException as e:
            logger.debug(f"Basename lookup failed for {address}: {e}")
        except Exception as e:
            logger.debug(f"Basename lookup error: {e}")
        
        return None
    
    def _reverse_ens(self, address: str) -> Optional[str]:
        """
        Reverse lookup via ENS (Ethereum mainnet).
        
        Uses the ENS reverse registrar contract.
        
        Args:
            address: Wallet address
            
        Returns:
            ENS name (e.g., "vitalik.eth") or None
        """
        try:
            # Use eth_call to reverse resolver
            # addr.reverse namespace
            reverse_node = self._get_reverse_node(address)
            
            # Call ENS public resolver
            # For simplicity, use a public API
            url = "https://api.ensideas.com/ens/resolve/" + address
            response = self._session.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data and data.get("name"):
                    name = data["name"]
                    logger.debug(f"ENS found: {address} -> {name}")
                    return name
        except requests.exceptions.RequestException as e:
            logger.debug(f"ENS lookup failed for {address}: {e}")
        except Exception as e:
            logger.debug(f"ENS lookup error: {e}")
        
        return None
    
    def _get_reverse_node(self, address: str) -> str:
        """Get the reverse node for an address (used in ENS reverse lookup)."""
        # Remove 0x prefix and lowercase
        addr = address[2:].lower()
        return f"{addr}.addr.reverse"
    
    def forward_lookup(self, name: str) -> Optional[str]:
        """
        Get wallet address for an ENS/Basename.
        
        Args:
            name: ENS or Basename (e.g., "vitalik.eth" or "jesse.base.eth")
            
        Returns:
            Wallet address or None
        """
        if not name:
            return None
        
        name = name.lower()
        
        # Determine which resolver to use
        if name.endswith(".base.eth") or name.endswith(".base"):
            return self._forward_basename(name)
        elif name.endswith(".eth"):
            return self._forward_ens(name)
        
        return None
    
    def _forward_basename(self, name: str) -> Optional[str]:
        """Forward lookup via Base Name Service API."""
        try:
            url = f"{self.BASENAME_API}/resolve"
            response = self._session.get(url, params={"name": name}, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data and data.get("address"):
                    return data["address"]
        except Exception as e:
            logger.debug(f"Basename forward lookup failed for {name}: {e}")
        
        return None
    
    def _forward_ens(self, name: str) -> Optional[str]:
        """Forward lookup via ENS."""
        try:
            url = f"https://api.ensideas.com/ens/resolve/{name}"
            response = self._session.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data and data.get("address"):
                    return data["address"]
        except Exception as e:
            logger.debug(f"ENS forward lookup failed for {name}: {e}")
        
        return None
    
    def batch_reverse_lookup(self, addresses: list) -> Dict[str, Optional[str]]:
        """
        Reverse lookup for multiple addresses.
        
        Args:
            addresses: List of wallet addresses
            
        Returns:
            Dict mapping address -> name (or None)
        """
        results = {}
        for address in addresses:
            if address:
                results[address.lower()] = self.reverse_lookup(address)
        return results


# Module-level convenience functions
_resolver: Optional[ENSResolver] = None


def _get_resolver() -> ENSResolver:
    """Get or create singleton resolver."""
    global _resolver
    if _resolver is None:
        _resolver = ENSResolver()
    return _resolver


def resolve_ens(address: str) -> Optional[str]:
    """
    Resolve wallet address to ENS/Basename.
    
    Args:
        address: Wallet address (0x...)
        
    Returns:
        Name if found, None otherwise
    """
    return _get_resolver().reverse_lookup(address)


def resolve_basename(address: str) -> Optional[str]:
    """
    Resolve wallet address to Basename specifically.
    
    Args:
        address: Wallet address (0x...)
        
    Returns:
        Basename if found, None otherwise
    """
    return _get_resolver()._reverse_basename(address)


def resolve_any_name(address: str) -> Optional[str]:
    """
    Resolve wallet to any supported name (ENS or Basename).
    
    This is an alias for resolve_ens() which tries both.
    """
    return resolve_ens(address)
