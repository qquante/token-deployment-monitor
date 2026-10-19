# Bankr Monitor - Documentação Completa do Código

## Índice

1. [Visão Geral](#visão-geral)
2. [Arquitetura de Microserviços](#arquitetura-de-microserviços)
3. [Fluxo de Dados](#fluxo-de-dados)
4. [Arquivos Raiz](#arquivos-raiz)
5. [config/](#config)
6. [shared/](#shared)
7. [workers/](#workers)
8. [src/](#src)
9. [Redis Channels (Pub/Sub)](#redis-channels-pubsub)
10. [Modelos de Dados](#modelos-de-dados)

---

## Visão Geral

O **Bankr Monitor** é um sistema de monitoramento em tempo real para tokens deployados via @bankrbot no Twitter/X. O sistema:

- Monitora tweets em tempo real via WebSocket
- Detecta tokens on-chain via Base blockchain WebSocket
- Envia notificações progressivas no Telegram
- Permite trading automatizado de tokens novos

### Stack Tecnológico

- **Python 3.10+** - Linguagem principal
- **Redis** - Pub/Sub para comunicação entre workers + Storage
- **aiohttp** - HTTP client assíncrono
- **websockets** - WebSocket client para Twitter/Base
- **eth-abi** - Decodificação de eventos blockchain
- **Docker Compose** - Orquestração de containers

---

## Arquitetura de Microserviços

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LISTENERS (Input)                           │
├─────────────────────────────────────────────────────────────────────┤
│  twitter_listener.py     │  onchain_listener.py                     │
│  - WebSocket Twitter API │  - WebSocket Base RPC                    │
│  - Publica: twitter:tweet│  - Publica: onchain:token, onchain:block │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PROCESSORS (Logic)                          │
├─────────────────────────────────────────────────────────────────────┤
│  tweet_processor.py      │  clanker_enricher.py │  user_enricher.py │
│  - Classifica tweets LLM │  - Busca pool data   │  - Twitter API    │
│  - PRE-BANKR/DEPLOYED    │  - Cache pool info   │  - AlphaGate API  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         TELEGRAM (Output)                           │
├─────────────────────────────────────────────────────────────────────┤
│  telegram_progressive.py │  telegram_callback.py│  telegram_sender.py│
│  - Mensagens progressivas│  - Handles callbacks │  - Envio simples   │
│  - Edição em tempo real  │  - BUY, LIKE, CONFIG │  - Rate limiting   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         TRADING (Execution)                         │
├─────────────────────────────────────────────────────────────────────┤
│  trading_executor.py                                                │
│  - Monitora blocos para execução                                    │
│  - Link token → PendingBuy                                          │
│  - Executa swaps via Uniswap V4                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Fluxo de Dados

### 1. PRE-BANKR Flow (Tweet → Telegram)

```
User tweets @bankrbot "deploy MyToken"
         │
         ▼
[twitter_listener] → twitter:tweet
         │
         ▼
[tweet_processor] → LLM classifica → twitter:prebankr
         │
         ▼
[telegram_progressive] → Envia PRE-BANKR msg
         │                    │
         ▼                    ▼
[user_enricher]          Edita msg progressivamente
    - Twitter API            com dados do usuário
    - AlphaGate API
```

### 2. DEPLOYED Flow (On-chain → Telegram)

```
Token deployado on-chain (Clanker V4)
         │
         ▼
[onchain_listener] → onchain:token
         │
         ├──────────────────────────────┐
         ▼                              ▼
[telegram_progressive]           [clanker_enricher]
  - Envia DEPLOYED msg             - Busca pool data
  - Usa cache PRE-BANKR            - Salva em Redis
         │
         ▼
[trading_executor]
  - Link token → PendingBuy
  - Notifica usuários
  - Executa quando bloco alvo
```

### 3. Trading Flow (BUY → Swap)

```
User clica [BUY] no Telegram
         │
         ▼
[telegram_callback] 
  - Cria PendingBuy (status: APPROVED)
  - Publica: trading:buy_approved
         │
         ▼
[trading_executor]
  - Recebe buy_approved
  - Espera onchain:token (se não on-chain ainda)
         │
         ▼
Token vai on-chain
         │
         ▼
[trading_executor]
  - link_token_to_buy() → status: WAITING
  - Calcula target_block = auction_expire + offset
         │
         ▼
Bloco alvo chega (onchain:block)
         │
         ▼
[trading_executor]
  - Executa swap via SwapExecutor
  - Multi-RPC fire pattern
  - Notifica resultado
```

---

## Arquivos Raiz

### `docker-compose.yml`
Orquestração de todos os serviços:

| Serviço | Tipo | Descrição |
|---------|------|-----------|
| `redis` | Infra | Pub/Sub + Storage |
| `postgres` | Infra | (Opcional) |
| `twitter-listener` | Listener | WebSocket Twitter |
| `onchain-listener` | Listener | WebSocket Base RPC |
| `tweet-processor` | Processor | LLM classification |
| `user-enricher` | Processor | API enrichment |
| `clanker-enricher` | Processor | Pool data |
| `telegram-progressive` | Telegram | Progressive msgs |
| `telegram-callback` | Telegram | Button handlers |
| `telegram-sender` | Telegram | Simple sender |
| `trading-executor` | Trading | Swap execution |

### `requirements.txt`
Dependências principais:
- `web3`, `eth-abi`, `eth-account` - Blockchain
- `aiohttp`, `websockets` - Async networking
- `redis` - Redis client
- `python-dotenv` - Environment vars

### `.env.example`
Variáveis de ambiente necessárias:
- `REDIS_HOST/PORT` - Redis connection
- `TWITTER_API_KEY` - Twitter WebSocket
- `ANKR_RPC_HTTP/WSS` - Base RPC endpoints
- `TELEGRAM_BOT_TOKEN` - Bot token
- `TELEGRAM_CHANNEL_ID` - Channel for DEPLOYED
- `TELEGRAM_PREBANKR_CHANNEL_ID` - Channel for PRE-BANKR
- `TELEGRAM_TRADING_BOT_TOKEN` - Trading bot (private)
- `GOOGLE_API_KEY` / `OPENROUTER_API_KEY` - LLM
- `ALPHAGATE_USER_ID/SESSION_ID` - AlphaGate API

---

## config/

### `settings.py`

Configurações centralizadas em dataclasses:

```python
@dataclass
class RPCConfig:
    ENDPOINTS = ["https://mainnet.base.org", ...]
    DEFAULT_ENDPOINT: str
    TIMEOUT: int = 30
    MAX_RETRIES: int = 3

@dataclass
class ContractConfig:
    CLANKER_V4: str = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"
    LP_LOCKER: str = "0x63D2DfEA64b3433F4071A98665bcD7Ca14d93496"
    POOL_HOOK: str = "0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC"
    MEV_MODULE: str = "0xebB25BB797D82CB78E1bc70406b13233c0854413"
    WETH: str = "0x4200000000000000000000000000000000000006"

@dataclass
class EventConfig:
    TOKEN_CREATED_TOPIC: str  # Event signature hash
    TOKEN_CREATED_ABI_TYPES: list  # For decoding

@dataclass  
class BankrConfig:
    INTERFACE_NAME: str = "Bankr"
    DEPLOYERS: list  # Known deployer addresses
    PLATFORMS: list  # ["farcaster", "X", "Bankr"]
```

---

## shared/

### `models.py`

Modelos de dados armazenados no Redis:

#### `UserConfig`
Configuração de trading por usuário.

```python
@dataclass
class UserConfig:
    user_id: int              # Telegram user ID
    username: str
    wallet_address: str       # 0x...
    wallet_private_key: str   # NEVER log!
    eth_amount: float = 0.01  # ETH por compra
    block_offset: int = 6     # Blocos após auction
    gwei: float = 0.1         # Gas price
    slippage: float = 5.0     # Slippage %
    auto_buy: bool = False    # Auto-execute
    max_eth_approved: float = 0.0  # Pre-approve limit
```

Redis key: `user:{user_id}`

#### `PendingBuy`
Compra pendente aguardando execução.

```python
@dataclass
class PendingBuy:
    user_id: int
    tweet_id: str
    token_address: str = ""
    token_symbol: str = ""
    pool_id: str = ""
    eth_amount: float = 0.01
    block_offset: int = 6
    auction_expire_block: int = 0
    target_block: int = 0      # Bloco para executar
    status: str = "APPROVED"   # APPROVED→WAITING→EXECUTING→DONE/FAILED
```

Redis key: `buy:{user_id}:{tweet_id}`
Indexes: `buys_by_status:{status}`, `buys_by_tweet:{tweet_id}`

#### `TokenInfo`
Cache de informações do token.

```python
@dataclass
class TokenInfo:
    tweet_id: str
    token_address: str = ""
    token_symbol: str = ""
    pool_id: str = ""
    auction_expire_block: int = 0
    on_chain: bool = False
```

Redis key: `token:{tweet_id}`

#### `EnrichedUser`
Cache de dados enriched do usuário.

```python
@dataclass
class EnrichedUser:
    username: str
    followers: int = 0
    smart_followers: int = 0
    prev_usernames_count: int = 0  # R (renames)
    contracts_count: int = 0       # C (contracts)
    is_blue_verified: bool = False
```

Redis key: `enriched_user:{username}`
TTL: 1 hora

#### `FEE_SCHEDULE`
Tabela de fees por bloco após auction:

```python
FEE_SCHEDULE = {
    0: 66.7,  # Auction expirou
    1: 58.3,
    2: 50.0,
    3: 41.7,
    4: 33.3,
    5: 25.0,
    6: 16.7,  # Recomendado
    7: 8.3,
    8: 4.2,   # Mínimo
}
```

### `storage.py`

Interface de alto nível para Redis:

```python
class Storage:
    # User Config
    async def get_user_config(user_id, username) -> UserConfig
    async def save_user_config(config) -> bool
    
    # Pending Buys
    async def approve_buy(user_id, tweet_id, ...) -> PendingBuy
    async def get_pending_buy(user_id, tweet_id) -> PendingBuy
    async def get_pending_buys_by_tweet(tweet_id) -> List[PendingBuy]
    async def get_buys_for_block(block_number) -> List[PendingBuy]
    async def link_token_to_buy(buy, token_address, ...) -> bool
    async def update_buy_status(buy, new_status) -> bool
    async def complete_buy(buy, tx_hash="", error="") -> bool
    
    # Token Info
    async def save_token_info(token) -> bool
    async def get_token_info(tweet_id) -> TokenInfo
    
    # Enriched Users
    async def save_enriched_user(user) -> bool
    async def get_enriched_user(username) -> EnrichedUser
    
    # Caches (24h TTL)
    async def save_twitter_user(...) -> bool
    async def get_twitter_user(username) -> Dict
    async def save_alphagate_data(...) -> bool
    async def get_alphagate_data(username) -> Dict
    
    # PRE-BANKR Cache (para DEPLOYED usar)
    async def save_prebankr_data(tweet_id, from_username, ...) -> bool
    async def get_prebankr_data(tweet_id) -> Dict
    
    # Pool Data (7 days TTL)
    async def save_pool_data(token_address, ...) -> bool
    async def get_pool_data(token_address) -> Dict
    
    # Likes
    async def add_like(tweet_id, user_id, username) -> int
    async def remove_like(tweet_id, user_id) -> int
    async def has_liked(tweet_id, user_id) -> bool
```

### `pubsub.py`

Definição de canais Redis Pub/Sub:

```python
class Channels:
    # Twitter
    TWITTER_TWEET = "twitter:tweet"           # Raw tweets
    TWITTER_PREBANKR = "twitter:prebankr"     # PRE-BANKR detected
    TWITTER_DEPLOYED = "twitter:deployed"     # DEPLOYED detected
    
    # On-chain
    ONCHAIN_TOKEN = "onchain:token"           # TokenCreated event
    ONCHAIN_BLOCK = "onchain:block"           # New block header
    
    # Telegram
    TELEGRAM_SEND = "telegram:send"           # Send message
    TELEGRAM_EDIT = "telegram:edit"           # Edit message
    
    # Trading
    TRADING_BUY_APPROVED = "trading:buy_approved"
    TRADING_TOKEN_LINKED = "trading:token_linked"
    TRADING_RESULT = "trading:result"
    
    # Enrichment
    ENRICH_REQUEST = "enrich:request"
    ENRICH_RESULT = "enrich:result"
```

### `redis_client.py`

Cliente Redis assíncrono com connection pooling:

```python
class RedisClient:
    # Basic operations
    async def get(key) -> str
    async def set(key, value, ex=None) -> bool
    async def setnx(key, value) -> bool  # Set if not exists (dedupe)
    
    # Hash operations (for objects)
    async def hset(key, mapping) -> int
    async def hgetall(key) -> Dict
    
    # Set operations (for indexes)
    async def sadd(key, *members) -> int
    async def smembers(key) -> set
    async def sismember(key, member) -> bool
    
    # Pub/Sub
    async def publish(channel, message) -> int
    async def subscribe(*channels) -> AsyncIterator
```

### `formatters.py`

Formatadores de mensagens Telegram:

```python
def format_number(n: int) -> str  # 1000 → "1K"

def build_user_line(
    username, followers, smart_followers, 
    prev_usernames, contracts, ...
) -> str
# Retorna: "FROM @user (Name) | 10K | SF:5 | R:0 | C:0"

def build_deployed_message(...) -> (text, keyboard)
def build_prebankr_message(...) -> (text, keyboard)
def build_trade_notification(status, ...) -> str
```

---

## workers/

### `base.py` - BaseWorker

Classe base para todos os workers:

```python
class BaseWorker(ABC):
    WORKER_NAME = "base"
    
    def __init__(self):
        self.redis: RedisClient
        self.storage: Storage
        self._running: bool
        self.stats: Dict
    
    async def setup(self):
        """Conecta Redis, inicializa storage"""
    
    async def teardown(self):
        """Fecha conexões"""
    
    async def publish(self, channel, event):
        """Publica evento no Redis"""
    
    async def heartbeat_loop(self, interval=30):
        """Envia heartbeats periódicos"""
    
    @abstractmethod
    async def run(self):
        """Loop principal - implementar no subclass"""
    
    @classmethod
    def main(cls):
        """Entry point: python -m workers.worker_name"""
```

### `twitter_listener.py`

**Responsabilidade:** Conectar ao WebSocket do Twitter e publicar tweets.

```python
class TwitterListenerWorker(BaseWorker):
    WORKER_NAME = "twitter-listener"
    
    async def run(self):
        # Conecta WebSocket: wss://ws.twitterapi.io/...
        # Para cada tweet recebido:
        #   - Cria Events.twitter_tweet(...)
        #   - Publica em Channels.TWITTER_TWEET
```

**Output:** `twitter:tweet` com raw tweet data.

### `tweet_processor.py`

**Responsabilidade:** Classificar tweets usando LLM.

```python
class TweetProcessorWorker(BaseWorker):
    WORKER_NAME = "tweet-processor"
    
    async def run(self):
        # Subscribe: twitter:tweet
        # Para cada tweet:
        #   - Se autor == "bankrbot" → parse_deployment()
        #   - Se menciona @bankrbot → classify_with_agent() (LLM)
        
    async def _classify_with_agent(self, author, text):
        # Chama Google AI ou OpenRouter
        # Retorna: {is_deploy, confidence, token, fee}
```

**Input:** `twitter:tweet`
**Output:** `twitter:prebankr` ou `twitter:deployed`

### `onchain_listener.py`

**Responsabilidade:** Escutar eventos on-chain da Base.

```python
class OnchainListenerWorker(BaseWorker):
    WORKER_NAME = "onchain-listener"
    
    async def run(self):
        # WebSocket: eth_subscribe("logs", {address: CLANKER_V4})
        # WebSocket: eth_subscribe("newHeads")
        
        # Para TokenCreated:
        #   - Decodifica evento
        #   - Extrai tweet_id do context JSON
        #   - Publica onchain:token
        
        # Para newHeads:
        #   - Publica onchain:block
```

**Output:** `onchain:token`, `onchain:block`

### `clanker_enricher.py`

**Responsabilidade:** Buscar dados do pool imediatamente quando token é criado.

```python
class ClankerEnricherWorker(BaseWorker):
    WORKER_NAME = "clanker-enricher"
    
    async def run(self):
        # Subscribe: onchain:token
        # Para cada token:
        #   - Busca Initialize event do PoolManager
        #   - Extrai: fee, tickSpacing, hooks, sqrtPriceX96
        #   - Salva em Redis: pool:{token_address}
```

**Input:** `onchain:token`
**Output:** Redis cache `pool:{address}` (TTL 7 dias)

### `user_enricher.py`

**Responsabilidade:** Enriquecer dados de usuário via APIs.

```python
class UserEnricherWorker(BaseWorker):
    WORKER_NAME = "user-enricher"
    
    async def run(self):
        # Subscribe: enrich:request
        # Para cada request:
        #   1. Check cache Redis (24h)
        #   2. Se miss: call APIs em paralelo:
        #      - Twitter API (followers, bio, verified)
        #      - AlphaGate child API (R, C, SF)
        #   3. Salva cache
        #   4. Publica enrich:result
```

**APIs usadas:**
- `api.twitterapi.io/twitter/user/info`
- `api.twitterapi.io/twitter/user/about`
- `api.alphagate.io/api/v1/child`
- `api.ensideas.com/ens/resolve`

### `telegram_progressive.py`

**Responsabilidade:** Enviar mensagens progressivas com edição em tempo real.

```python
class TelegramProgressiveWorker(BaseWorker):
    WORKER_NAME = "telegram-progressive"
    
    async def run(self):
        # Subscribe: twitter:prebankr, twitter:deployed, onchain:token
        
    async def _handle_prebankr(self, event):
        # 1. Envia mensagem inicial com "loading..."
        # 2. Enriquece FROM e TO em paralelo
        # 3. Edita mensagem conforme dados chegam
        # 4. Salva cache para DEPLOYED usar
        
    async def _handle_deployed(self, event):
        # 1. Verifica se tem mensagem on-chain pendente
        # 2. Se sim: atualiza mensagem existente
        # 3. Se não: envia nova mensagem progressiva
        
    async def _handle_onchain_token(self, event):
        # OnChain-first flow (mais rápido que Twitter!)
        # 1. Busca cache PRE-BANKR
        # 2. Envia OnchainDeploymentMessage
        # 3. Busca creator info em background
        # 4. Guarda para linking com Twitter
```

### `telegram_callback.py`

**Responsabilidade:** Handler de callbacks (botões) e comandos.

```python
class TelegramCallbackWorker(BaseWorker):
    WORKER_NAME = "telegram-callback"
    
    async def run(self):
        # Poll getUpdates para ambos bots (prebankr + trading)
        
    # === LIKE ===
    async def _handle_like(callback):
        # Toggle like no Redis
        # Atualiza contador no botão
        
    # === BUY ===
    async def _handle_buy_click(callback):
        # Cria PendingBuy imediatamente
        # Envia confirmação para user
        # Publica trading:buy_approved
        
    async def _handle_buy_confirm(callback):
        # Quando auto_buy=OFF e token on-chain
        # Muda status para WAITING
        
    async def _handle_buy_cancel(callback):
        # Cancela PendingBuy
        
    # === CONFIG MENU ===
    async def _handle_config(callback):
        # Navega menus de configuração:
        # - cfg:main → Menu principal
        # - cfg:wallet → Configurar private key
        # - cfg:eth → Selecionar ETH amount
        # - cfg:block → Selecionar block offset
        # - cfg:gwei → Selecionar gas price
        # - cfg:autobuy → Toggle auto-buy
        # - cfg:preapprove → Pre-approve limit
        # - custometh/customgwei → Input customizado
```

**Fluxo de estados para input customizado:**
```
User clica "Custom..." 
    → _waiting_for_eth[user_id] = True
    → Envia msg "Digite o valor..."
    
User digita "0.15"
    → _handle_message() detecta _waiting_for_eth
    → Salva config.eth_amount = 0.15
    → Confirma salvamento
```

### `telegram_sender.py`

**Responsabilidade:** Envio simples de mensagens (fallback/legacy).

```python
class TelegramSenderWorker(BaseWorker):
    WORKER_NAME = "telegram-sender"
    
    async def run(self):
        # Subscribe: telegram:send, telegram:edit
        # Rate limiting: 50ms entre mensagens
```

### `trading_executor.py`

**Responsabilidade:** Executar trades quando bloco alvo chega.

```python
class TradingExecutorWorker(BaseWorker):
    WORKER_NAME = "trading-executor"
    
    async def run(self):
        # Subscribe: onchain:block, onchain:token, trading:buy_approved
        
    async def _handle_block(self, event):
        # Verifica buys com target_block <= current_block
        # Executa todos em paralelo
        
    async def _handle_token(self, event):
        # Link token com PendingBuys pelo tweet_id
        # Calcula target_block = auction_expire + offset
        # Se auto_buy: status → WAITING
        # Se manual: status → PENDING_CONFIRM, pede confirmação
        
    async def _execute_trade(self, buy):
        # 1. Muda status para EXECUTING
        # 2. Notifica user
        # 3. Chama SwapExecutor
        # 4. Muda status para DONE/FAILED
        # 5. Notifica resultado
```

---

## src/

### `src/async_clients/telegram.py`

Classes para mensagens progressivas:

```python
class AsyncTelegramClient:
    """Cliente básico para envio/edição de mensagens"""
    async def send_message(text, reply_markup=None) -> message_id
    async def edit_message(message_id, text, reply_markup=None) -> bool

class ProgressiveMessage:
    """Mensagem DEPLOYED com edição progressiva"""
    def __init__(client, token_name, token_symbol, contract, ...)
    async def send_initial() -> message_id  # Envia com "loading..."
    async def update_from_user(enriched_data)  # Atualiza FROM
    async def update_to_user(index, enriched_data)  # Atualiza TO
    async def update_smart_followers(target, sf_list)
    async def mark_complete()

class PreBankrMessage:
    """Mensagem PRE-BANKR com edição progressiva"""
    # Similar ao ProgressiveMessage

class OnchainDeploymentMessage:
    """Mensagem para OnChain-first flow"""
    # Suporta atualização posterior com dados do Twitter
```

### `src/trading/swap_executor.py`

Executor de swaps Uniswap V4:

```python
@dataclass
class SwapResult:
    success: bool
    tx_hash: Optional[str]
    error: Optional[str]
    rpc_used: Optional[str]

class SwapExecutor:
    """
    Ultra-fast swap executor para Uniswap V4.
    
    Features:
    - Multi-RPC fire pattern (envia para todos RPCs simultaneamente)
    - Connection pooling com aiohttp
    - Nonce tracking thread-safe
    """
    
    def __init__(self, private_key, rpc_urls, max_priority_fee_gwei=0.01):
        self.account = Account.from_key(private_key)
        self.rpc_urls = rpc_urls  # Lista de RPCs
    
    async def swap_eth_to_token(
        token_address,
        amount_in_wei,
        slippage=0.05,
        pool_id=None,
        pool_data=None,  # Cached from clanker_enricher
    ) -> SwapResult:
        """
        Executa swap ETH → Token.
        
        Modos:
        1. ETH mode (default): WRAP_ETH + V4_SWAP
           - Não precisa setup prévio
        2. WETH mode: V4_SWAP com SETTLE_ALL
           - Requer WETH + Permit2 approval
        """
    
    async def _fire_transaction(signed_tx_hex) -> (tx_hash, rpc_used):
        """Envia TX para TODOS RPCs simultaneamente"""
```

**Contratos usados:**
- `UNIVERSAL_ROUTER` = "0x6ff5693b99212da76ad316178a184ab56d299b43"
- `POOL_MANAGER` = "0x498581ff718922c3f8e6a244956af099b2652b2b"
- `WETH` = "0x4200000000000000000000000000000000000006"
- `CLANKER_HOOK_STATIC_FEE_V2` = "0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC"

### `src/database/parser.py`

Parser de tweets do @bankrbot:

```python
def classify_bankr_tweet(tweet_data) -> Dict:
    """Classifica tweet e extrai dados"""
    # Tipos: deployment, swap, transfer, deployment_failed, other

def parse_deployment(text, tweet_data) -> Dict:
    """
    Extrai de: "@user deployed TokenName contract address is 0x..."
    
    Retorna:
    - token_name, token_symbol
    - contract_address
    - from_user (requester)
    - to_users (fee recipients)
    - fee_percentage
    """

def extract_fee_routing(text) -> Dict:
    """
    Extrai routing de fees de várias formas:
    - "fees routed to @user (0x...)"
    - "rewards routing to @user"
    - "reward recipient updated to 0x..."
    """

def is_prebankr_request(tweet_data) -> bool:
    """Detecta se é request de deploy"""
    # Deve mencionar @bankrbot
    # Deve ter keywords: deploy, create, launch, token...
```

---

## Redis Channels (Pub/Sub)

### Fluxo Completo

```
┌───────────────────────────────────────────────────────────────────┐
│                     TWITTER LISTENER                               │
│                            │                                       │
│                            ▼                                       │
│                    twitter:tweet ──────────────────────────────┐   │
│                            │                                   │   │
│                            ▼                                   │   │
│                   TWEET PROCESSOR                              │   │
│                     │           │                              │   │
│                     ▼           ▼                              │   │
│          twitter:prebankr  twitter:deployed                    │   │
│                │                │                              │   │
│                │                │                              │   │
│                ▼                ▼                              │   │
│      TELEGRAM PROGRESSIVE ◄────┘                               │   │
│                                                                │   │
└────────────────────────────────────────────────────────────────│───┘
                                                                 │
┌────────────────────────────────────────────────────────────────│───┐
│                     ONCHAIN LISTENER                           │   │
│                            │                                   │   │
│              ┌─────────────┴─────────────┐                     │   │
│              ▼                           ▼                     │   │
│       onchain:token                onchain:block               │   │
│           │    │                        │                      │   │
│           │    │                        │                      │   │
│           ▼    ▼                        ▼                      │   │
│  CLANKER_ENRICHER  TRADING_EXECUTOR ◄───┘                      │   │
│       │                  │                                     │   │
│       │                  │                                     │   │
│       │                  ▼                                     │   │
│       │          trading:result ──► USER NOTIFICATION          │   │
│       │                                                        │   │
│       ▼                                                        │   │
│  Redis Cache                                                   │   │
│  pool:{address}                                                │   │
│                                                                │   │
└────────────────────────────────────────────────────────────────────┘
```

---

## Modelos de Dados

### Redis Keys

```
# User Config
user:{telegram_user_id}

# Pending Buys
buy:{user_id}:{tweet_id}

# Indexes
buys_by_user:{user_id}          → Set of "user_id:tweet_id"
buys_by_status:{status}         → Set of "user_id:tweet_id"
buys_by_tweet:{tweet_id}        → Set of "user_id:tweet_id"
buys_by_target_block:{block}    → Set of "user_id:tweet_id"

# Token Info
token:{tweet_id}
token_by_address:{address}      → tweet_id (reverse lookup)

# Enriched User Cache (1h TTL)
enriched_user:{username}

# Twitter Cache (24h TTL)
twitter:{username}

# AlphaGate Cache (24h TTL)
alphagate:{username}

# Pool Cache (7d TTL)
pool:{token_address}

# PRE-BANKR Cache (12h TTL)
prebankr_data:{tweet_id}

# Likes
likes:{tweet_id}                → Set of "user_id:username"

# Deduplication
notified_contracts              → Set of contract addresses
tweet_processed:{tweet_id}      → "1" (1h TTL)
prebankr_processed:{tweet_id}   → "1" (1h TTL)
deployed_processed:{tweet_id}   → "1" (1h TTL)

# MOLT Agents
molt_agent:{x_handle}
molt_agents_index               → Set of handles
```

### Status Flow - PendingBuy

```
APPROVED ──────► WAITING ──────► EXECUTING ──────► DONE
    │               │                │               │
    │               │                │               ▼
    │               │                └──────────► FAILED
    │               │
    │               └──► PENDING_CONFIRM ──► WAITING (após confirm)
    │
    └──────────────────────────────────────────► CANCELLED
```

---

## Comandos Docker

```bash
# Iniciar tudo
docker-compose up -d

# Iniciar só infraestrutura
docker-compose up -d redis postgres

# Ver logs de um worker específico
docker-compose logs -f twitter-listener

# Escalar workers (ex: 3 tweet-processors)
docker-compose up -d --scale tweet-processor=3

# Rebuild e restart
docker-compose up -d --build trading-executor
```

---

## Resumo de Responsabilidades

| Worker | Input | Output | Função |
|--------|-------|--------|--------|
| twitter_listener | WebSocket | twitter:tweet | Recebe tweets |
| tweet_processor | twitter:tweet | prebankr/deployed | Classifica |
| onchain_listener | WebSocket | token/block | Eventos chain |
| clanker_enricher | onchain:token | Redis cache | Pool data |
| user_enricher | enrich:request | enrich:result | APIs externas |
| telegram_progressive | prebankr/deployed/token | Telegram | Msgs progressivas |
| telegram_callback | Telegram API | Redis | Handle buttons |
| telegram_sender | telegram:send | Telegram | Envio simples |
| trading_executor | block/token/buy | Blockchain | Executa swaps |
