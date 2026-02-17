"""
ENS and Basename resolver module.

Provides functions to resolve:
- ENS names on Ethereum mainnet (.eth)
- Basenames on Base (.base.eth)
"""

from .resolver import (
    resolve_ens,
    resolve_basename,
    resolve_any_name,
    ENSResolver,
)

__all__ = [
    "resolve_ens",
    "resolve_basename", 
    "resolve_any_name",
    "ENSResolver",
]
