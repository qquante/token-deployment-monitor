"""
User data enrichment module.

Combines data from multiple sources:
- Twitter API (user profile info)
- ENS/Basename resolver
- On-chain data (optional)
"""

from .user_enricher import (
    UserEnricher,
    enrich_user,
    enrich_users,
)

__all__ = [
    "UserEnricher",
    "enrich_user",
    "enrich_users",
]
