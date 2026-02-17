"""
AlphaGate API client module.

Provides access to AlphaGate's smart followers data.
"""

from .client import (
    AlphaGateClient,
    get_smart_followers,
    get_smart_followers_count,
)

__all__ = [
    "AlphaGateClient",
    "get_smart_followers",
    "get_smart_followers_count",
]
