"""
Trading configuration and user settings.

FEE_SCHEDULE: Block-by-block fee after auction expires
UserConfig: Per-user trading settings
"""

import os
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)

# =============================================================================
# FEE SCHEDULE - SIMPLES: BLOCO + FEE%
# =============================================================================
# Auction dura 2 blocos. Depois de expirar, fee decai em ~8 blocos.

FEE_SCHEDULE = {
    0: 66.7,   # Auction acabou de expirar
    1: 58.3,   # Primeiro bloco decay
    2: 50.0,   # 
    3: 41.7,   #
    4: 33.3,   #
    5: 25.0,   #
    6: 16.7,   #
    7: 8.3,    #
    8: 4.2,    # Fee minima
    9: 4.2,
    10: 4.2,
}

def get_fee_for_block(blocks_after_expire: int) -> float:
    """Get fee % for X blocks after auction expires."""
    if blocks_after_expire in FEE_SCHEDULE:
        return FEE_SCHEDULE[blocks_after_expire]
    elif blocks_after_expire >= 8:
        return 4.2
    else:
        return 66.7


# =============================================================================
# TRADING CONFIG (Global)
# =============================================================================

@dataclass
class TradingConfig:
    """Global trading configuration."""
    
    # Bot token for private chat
    bot_token: str = ""
    
    # Contracts
    clanker_v4: str = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"
    mev_module_v2: str = "0xebb25bb797d82cb78e1bc70406b13233c0854413"
    weth: str = "0x4200000000000000000000000000000000000006"
    universal_router: str = "0x198EF79F1F515F02dFE9e3115eD9fC07183f02fC"
    
    # RPC
    rpc_http: str = ""
    rpc_wss: str = ""
    
    def __post_init__(self):
        self.bot_token = os.getenv("TELEGRAM_TRADING_BOT_TOKEN", self.bot_token)
        self.rpc_http = os.getenv("ANKR_RPC_HTTP", self.rpc_http)
        self.rpc_wss = os.getenv("ANKR_RPC_WSS", self.rpc_wss)


# =============================================================================
# USER CONFIG (Per-user)
# =============================================================================

@dataclass
class UserConfig:
    """Per-user trading configuration."""
    
    user_id: int
    username: str = ""
    
    # Wallet
    wallet_address: str = ""
    wallet_private_key: str = ""  # NEVER log this!
    
    # Trading params
    eth_amount: float = 0.01      # ETH per trade
    block_offset: int = 6         # Blocks after auction expire (+6 = 16.7% fee)
    gwei: float = 0.1             # Gas price in gwei
    slippage: float = 5.0         # Slippage tolerance %
    
    # Toggles
    auto_buy: bool = False        # Auto-buy all tokens (dangerous!)
    alerts_enabled: bool = True   # Receive alerts
    
    # Approved tokens (tweet_id -> approved for buy)
    approved_buys: Dict[str, dict] = field(default_factory=dict)
    
    def is_configured(self) -> bool:
        """Check if user has minimum config for trading."""
        return bool(self.wallet_address and self.wallet_private_key)
    
    def get_missing_config(self) -> List[str]:
        """Get list of missing required config."""
        missing = []
        if not self.wallet_address:
            missing.append("wallet")
        if not self.wallet_private_key:
            missing.append("private_key")
        return missing
    
    def to_dict(self) -> dict:
        """Convert to dict (excludes private key for safety)."""
        d = asdict(self)
        d.pop("wallet_private_key", None)  # Never expose
        return d


# =============================================================================
# USER CONFIG STORAGE
# =============================================================================

# In-memory storage (user_id -> UserConfig)
_user_configs: Dict[int, UserConfig] = {}

# Persistent storage path
_config_path = Path(__file__).parent.parent.parent / "data" / "trading_users.json"


def get_user_config(user_id: int, username: str = "") -> UserConfig:
    """Get or create user config."""
    if user_id not in _user_configs:
        _user_configs[user_id] = UserConfig(user_id=user_id, username=username)
        _load_user_config(user_id)
    
    config = _user_configs[user_id]
    if username and not config.username:
        config.username = username
    
    return config


def save_user_config(user_id: int):
    """Save user config to disk."""
    if user_id not in _user_configs:
        return
    
    config = _user_configs[user_id]
    
    # Load existing data
    all_configs = {}
    if _config_path.exists():
        try:
            with open(_config_path, "r") as f:
                all_configs = json.load(f)
        except:
            pass
    
    # Update this user
    all_configs[str(user_id)] = {
        "username": config.username,
        "wallet_address": config.wallet_address,
        # NOTE: Private key stored separately or not at all for security
        "eth_amount": config.eth_amount,
        "block_offset": config.block_offset,
        "gwei": config.gwei,
        "slippage": config.slippage,
        "auto_buy": config.auto_buy,
        "alerts_enabled": config.alerts_enabled,
    }
    
    # Save
    _config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(_config_path, "w") as f:
        json.dump(all_configs, f, indent=2)
    
    logger.info(f"[TRADING] Saved config for user {user_id}")


def _load_user_config(user_id: int):
    """Load user config from disk."""
    if not _config_path.exists():
        return
    
    try:
        with open(_config_path, "r") as f:
            all_configs = json.load(f)
        
        user_data = all_configs.get(str(user_id))
        if user_data and user_id in _user_configs:
            config = _user_configs[user_id]
            config.username = user_data.get("username", "")
            config.wallet_address = user_data.get("wallet_address", "")
            config.eth_amount = user_data.get("eth_amount", 0.01)
            config.block_offset = user_data.get("block_offset", 6)
            config.gwei = user_data.get("gwei", 0.1)
            config.slippage = user_data.get("slippage", 5.0)
            config.auto_buy = user_data.get("auto_buy", False)
            config.alerts_enabled = user_data.get("alerts_enabled", True)
            logger.info(f"[TRADING] Loaded config for user {user_id}")
    except Exception as e:
        logger.error(f"[TRADING] Failed to load config for user {user_id}: {e}")


# =============================================================================
# MENU BUILDERS
# =============================================================================

def build_main_menu_text(config: UserConfig) -> str:
    """Build main menu message text."""
    wallet_status = f"0x...{config.wallet_address[-6:]}" if config.wallet_address else "NAO CONFIGURADA"
    auto_status = "ON" if config.auto_buy else "OFF"
    fee = get_fee_for_block(config.block_offset)
    
    return f"""<b>TRADING BOT</b>

<b>Config:</b>
Wallet: <code>{wallet_status}</code>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Gwei: <b>{config.gwei}</b>
Auto-buy: <b>{auto_status}</b>

<b>Fee Schedule:</b>
<code>+0=66.7% +1=58.3% +2=50%
+3=41.7% +4=33.3% +5=25%
+6=16.7% +7=8.3%  +8=4.2%</code>

Clique para configurar:"""


def build_main_menu_keyboard() -> dict:
    """Build main menu inline keyboard."""
    return {
        "inline_keyboard": [
            [
                {"text": "Wallet", "callback_data": "cfg:wallet"},
                {"text": "ETH", "callback_data": "cfg:eth"},
            ],
            [
                {"text": "Bloco/Fee", "callback_data": "cfg:block"},
                {"text": "Gwei", "callback_data": "cfg:gwei"},
            ],
            [
                {"text": "Auto-buy ON/OFF", "callback_data": "cfg:autobuy"},
            ],
            [
                {"text": "Ver Fees", "callback_data": "cfg:fees"},
                {"text": "Status", "callback_data": "cfg:status"},
            ],
        ]
    }


def build_block_menu_text(config: UserConfig) -> str:
    """Build block selection menu text."""
    lines = ["<b>SELECIONAR BLOCO</b>\n"]
    lines.append("Escolha quantos blocos esperar apos auction expirar.\n")
    lines.append("<code>BLOCO    FEE</code>")
    
    for blk in range(0, 9):
        fee = FEE_SCHEDULE[blk]
        marker = " <--" if blk == config.block_offset else ""
        lines.append(f"<code>  +{blk}   {fee:>5.1f}%{marker}</code>")
    
    lines.append(f"\nSelecionado: <b>+{config.block_offset}</b> ({get_fee_for_block(config.block_offset):.1f}%)")
    return "\n".join(lines)


def build_block_menu_keyboard(config: UserConfig) -> dict:
    """Build block selection keyboard."""
    buttons = []
    
    # Row 1: blocks 0-3
    row1 = []
    for blk in [0, 1, 2, 3]:
        marker = ">" if blk == config.block_offset else ""
        row1.append({"text": f"{marker}+{blk}", "callback_data": f"setblk:{blk}"})
    buttons.append(row1)
    
    # Row 2: blocks 4-7
    row2 = []
    for blk in [4, 5, 6, 7]:
        marker = ">" if blk == config.block_offset else ""
        row2.append({"text": f"{marker}+{blk}", "callback_data": f"setblk:{blk}"})
    buttons.append(row2)
    
    # Row 3: block 8 + back
    buttons.append([
        {"text": ">+8" if config.block_offset == 8 else "+8", "callback_data": "setblk:8"},
        {"text": "< Voltar", "callback_data": "cfg:main"},
    ])
    
    return {"inline_keyboard": buttons}


def build_eth_menu_text(config: UserConfig) -> str:
    """Build ETH amount menu text."""
    return f"""<b>QUANTIDADE ETH</b>

Quanto ETH usar em cada compra.

Atual: <b>{config.eth_amount} ETH</b>

Selecione:"""


def build_eth_menu_keyboard(config: UserConfig) -> dict:
    """Build ETH amount keyboard."""
    amounts = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]
    buttons = []
    
    row = []
    for eth in amounts:
        marker = ">" if eth == config.eth_amount else ""
        row.append({"text": f"{marker}{eth}", "callback_data": f"seteth:{eth}"})
        if len(row) == 4:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([{"text": "✏️ Custom...", "callback_data": "custometh"}])
    buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
    
    return {"inline_keyboard": buttons}


def build_gwei_menu_text(config: UserConfig) -> str:
    """Build gwei menu text."""
    return f"""<b>GAS PRICE (GWEI)</b>

Gas price para transacoes.

Atual: <b>{config.gwei} gwei</b>

Selecione:"""


def build_gwei_menu_keyboard(config: UserConfig) -> dict:
    """Build gwei keyboard."""
    gweis = [0.1, 0.5, 1, 2, 5, 10, 50, 100, 500, 1000]
    buttons = []
    
    row = []
    for g in gweis:
        marker = ">" if g == config.gwei else ""
        row.append({"text": f"{marker}{g}", "callback_data": f"setgwei:{g}"})
        if len(row) == 5:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([{"text": "✏️ Custom...", "callback_data": "customgwei"}])
    buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
    
    return {"inline_keyboard": buttons}


def build_fees_text() -> str:
    """Build full fee schedule text."""
    lines = ["<b>FEE SCHEDULE COMPLETO</b>\n"]
    lines.append("Quando token e criado:\n")
    lines.append("1. Auction ativa por 2 blocos (66.7%)")
    lines.append("2. Auction expira (bloco +0)")
    lines.append("3. Fee decai por ~8 blocos\n")
    lines.append("<code>BLOCO     FEE    RISCO</code>")
    
    risks = {
        0: "EXTREMO", 1: "MUITO ALTO", 2: "ALTO",
        3: "MEDIO-ALTO", 4: "MEDIO", 5: "MEDIO",
        6: "BAIXO", 7: "MUITO BAIXO", 8: "MINIMO"
    }
    
    for blk in range(0, 9):
        fee = FEE_SCHEDULE[blk]
        risk = risks.get(blk, "MINIMO")
        lines.append(f"<code>  +{blk}    {fee:>5.1f}%   {risk}</code>")
    
    lines.append("\nRecomendado: <b>+6 a +8</b> (fee baixa)")
    return "\n".join(lines)


def build_buy_confirm_text(config: UserConfig, token_symbol: str, tweet_id: str) -> str:
    """Build buy confirmation text."""
    fee = get_fee_for_block(config.block_offset)
    
    if not config.is_configured():
        missing = config.get_missing_config()
        return f"""<b>CONFIG INCOMPLETA</b>

Falta configurar: {', '.join(missing)}

Use /config para configurar antes de comprar."""
    
    return f"""<b>CONFIRMAR COMPRA</b>

Token: <b>{token_symbol or 'Novo Token'}</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Gwei: <b>{config.gwei}</b>

Confirmar?"""


def build_buy_confirm_keyboard(tweet_id: str, configured: bool) -> dict:
    """Build buy confirmation keyboard."""
    if not configured:
        return {
            "inline_keyboard": [
                [{"text": "Configurar", "callback_data": "cfg:main"}],
            ]
        }
    
    return {
        "inline_keyboard": [
            [
                {"text": "CONFIRMAR", "callback_data": f"buyconfirm:{tweet_id}"},
                {"text": "Cancelar", "callback_data": "buycancel"},
            ],
            [{"text": "Mudar Config", "callback_data": "cfg:main"}],
        ]
    }
