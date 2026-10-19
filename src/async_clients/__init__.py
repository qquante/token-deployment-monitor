"""
Async API clients for high-performance parallel requests.

All clients use aiohttp for non-blocking HTTP calls.
"""

from .twitter import AsyncTwitterClient
from .ens import AsyncENSResolver
from .alphagate import AsyncAlphaGateClient
from .telegram import (
    AsyncTelegramClient,
    ProgressiveMessage,
    PreBankrMessage,
    OnchainDeploymentMessage,
    ClawnchDeploymentMessage,
    TradingCallbackHandler,
    TradingMessageHandler,
    # Menu builders
    FEE_SCHEDULE,
    get_fee_for_block,
    build_main_menu_text,
    build_main_menu_keyboard,
    build_block_menu_text,
    build_block_menu_keyboard,
    build_eth_menu_text,
    build_eth_menu_keyboard,
    build_gwei_menu_text,
    build_gwei_menu_keyboard,
    build_fees_text,
    build_buy_confirm_text,
    build_buy_confirm_keyboard,
    build_status_text,
    build_wallet_config_text,
)
from .agent import AsyncAgentClient, classify_tweet, format_agent_result_compact

__all__ = [
    "AsyncTwitterClient",
    "AsyncENSResolver", 
    "AsyncAlphaGateClient",
    "AsyncTelegramClient",
    "ProgressiveMessage",
    "PreBankrMessage",
    "OnchainDeploymentMessage",
    "ClawnchDeploymentMessage",
    "TradingCallbackHandler",
    "TradingMessageHandler",
    # Menu builders
    "FEE_SCHEDULE",
    "get_fee_for_block",
    "build_main_menu_text",
    "build_main_menu_keyboard",
    "build_block_menu_text",
    "build_block_menu_keyboard",
    "build_eth_menu_text",
    "build_eth_menu_keyboard",
    "build_gwei_menu_text",
    "build_gwei_menu_keyboard",
    "build_fees_text",
    "build_buy_confirm_text",
    "build_buy_confirm_keyboard",
    "build_status_text",
    "build_wallet_config_text",
    # Agent
    "AsyncAgentClient",
    "classify_tweet",
    "format_agent_result_compact",
]
