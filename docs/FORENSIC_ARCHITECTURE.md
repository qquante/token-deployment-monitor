# Bankr Monitor - Arquitetura Forense dos Workers

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              FONTES DE DADOS                                     │
├───────────────────────────────────┬─────────────────────────────────────────────┤
│       Twitter WebSocket           │           Base Blockchain                   │
│   (tweets em tempo real)          │        (eventos on-chain)                   │
└───────────────┬───────────────────┴────────────────────┬────────────────────────┘
                │                                        │
                ▼                                        ▼
┌───────────────────────────────────┐   ┌─────────────────────────────────────────┐
│     TWITTER LISTENER              │   │        ONCHAIN LISTENER                 │
│  ┌─────────────────────────┐      │   │  ┌─────────────────────────────────┐    │
│  │ WebSocket Connection    │      │   │  │ WebSocket RPC Connection        │    │
│  │ Auto-reconnect          │      │   │  │ Subscribe: TokenCreated events  │    │
│  │ Ping/pong handling      │      │   │  │ Subscribe: NewBlockHeaders      │    │
│  └─────────────────────────┘      │   │  └─────────────────────────────────┘    │
│  Output: twitter:tweet            │   │  Output: onchain:token, onchain:block   │
└───────────────┬───────────────────┘   └────────────────────┬────────────────────┘
                │                                            │
                ▼                                            │
┌───────────────────────────────────┐                        │
│      TWEET PROCESSOR              │                        │
│  ┌─────────────────────────┐      │                        │
│  │ LLM Classification      │      │                        │
│  │ @bankrbot => DEPLOYED   │      │                        │
│  │ mentions => PRE-BANKR   │      │                        │
│  └─────────────────────────┘      │                        │
│  Output: twitter:prebankr         │                        │
│          twitter:deployed         │                        │
└───────────────┬───────────────────┘                        │
                │                                            │
                ├────────────────────────────────────────────┤
                ▼                                            ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                        TELEGRAM PROGRESSIVE                                     │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  Subscribe: twitter:prebankr, twitter:deployed, onchain:token            │  │
│  │                                                                          │  │
│  │  Fluxos Paralelos:                                                       │  │
│  │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │  │
│  │  │ PRE-BANKR Handler  │  │ DEPLOYED Handler   │  │ ONCHAIN Handler    │  │  │
│  │  │                    │  │                    │  │                    │  │  │
│  │  │ 1. Send initial    │  │ 1. Check pending   │  │ 1. Check cache     │  │  │
│  │  │ 2. Cache tweet     │  │ 2. Update or new   │  │ 2. Send initial    │  │  │
│  │  │ 3. Enrich parallel │  │ 3. Enrich parallel │  │ 3. Fetch creator   │  │  │
│  │  │ 4. Edit message    │  │ 4. Edit message    │  │ 4. Link trading    │  │  │
│  │  └────────────────────┘  └────────────────────┘  └────────────────────┘  │  │
│  │                                                                          │  │
│  │  Classes Usadas: PreBankrMessage, ProgressiveMessage, OnchainDeployment  │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│  Output: Mensagens Telegram editadas progressivamente                          │
└────────────────────────────────────────────────────────────────────────────────┘
                │
                │ (enrich:request)
                ▼
┌───────────────────────────────────┐
│       USER ENRICHER               │
│  ┌─────────────────────────┐      │
│  │ APIs Paralelas:         │      │
│  │ - Twitter API           │      │
│  │ - AlphaGate R/C         │      │
│  │ - AlphaGate SmartFolls  │      │
│  │ - ENS Resolution        │      │
│  └─────────────────────────┘      │
│  Cache: Redis                     │
│  Output: enrich:result            │
└───────────────────────────────────┘

┌───────────────────────────────────┐
│     TELEGRAM CALLBACK             │
│  ┌─────────────────────────┐      │
│  │ Long polling (parallel): │      │
│  │ - prebankr bot          │      │
│  │ - trading bot           │      │
│  │                         │      │
│  │ Handlers:               │      │
│  │ - Like button           │      │
│  │ - Buy approvals         │      │
│  │ - Config menu           │      │
│  │ - Block offset          │      │
│  └─────────────────────────┘      │
│  Output: trading:buy_approved     │
│          telegram:send            │
└───────────────────────────────────┘

┌───────────────────────────────────┐
│     TRADING EXECUTOR              │
│  ┌─────────────────────────┐      │
│  │ Subscribe:              │      │
│  │ - onchain:block         │      │
│  │ - onchain:token         │      │
│  │ - trading:buy_approved  │      │
│  │ - twitter:prebankr      │      │
│  │                         │      │
│  │ Execute at target block │      │
│  │ Parallel swap execution │      │
│  └─────────────────────────┘      │
│  Output: trading:result           │
│          telegram:send            │
└───────────────────────────────────┘

┌───────────────────────────────────┐
│     TELEGRAM SENDER               │
│  ┌─────────────────────────┐      │
│  │ Subscribe:              │      │
│  │ - telegram:send         │      │
│  │ - telegram:edit         │      │
│  │                         │      │
│  │ Rate limiting: 20/sec   │      │
│  │ Retry on 429            │      │
│  └─────────────────────────┘      │
│  Output: Telegram API             │
└───────────────────────────────────┘
```

---

## Detalhamento por Worker

### 1. TWITTER LISTENER (Single Instance)

**Responsabilidade:** Manter conexão WebSocket com Twitter API

**Processamento:**
```
SEQUENCIAL:
┌─────────────────────────────────────────────────────────────────┐
│ 1. Connect WebSocket                                            │
│ 2. Wait for message                                             │ ◄─┐
│ 3. Parse JSON                                                   │   │
│ 4. IF event_type == "tweet":                                    │   │
│    └─ FOR each tweet in tweets:                                 │   │
│       └─ Publish to twitter:tweet                               │   │
│ 5. Go to step 2                                                 │ ──┘
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- WebSocket single-threaded (sequencial por design)
- Reconexão em 5s após disconnect
- ~1ms para publish no Redis

**Paralelismo:** NENHUM (e não precisa - é I/O bound na conexão)

---

### 2. ONCHAIN LISTENER (Single Instance)

**Responsabilidade:** Escutar eventos da blockchain Base

**Processamento:**
```
PARALELO via asyncio.gather:
┌─────────────────────────────────────────────────────────────────┐
│ WebSocket com múltiplas subscriptions:                          │
│                                                                 │
│ ┌────────────────────────┐  ┌────────────────────────┐          │
│ │ Subscription 1:        │  │ Subscription 2:        │          │
│ │ eth_subscribe("logs")  │  │ eth_subscribe("newHea..│          │
│ │ TokenCreated events    │  │ New block headers      │          │
│ └──────────┬─────────────┘  └──────────┬─────────────┘          │
│            │                           │                        │
│            ▼                           ▼                        │
│ ┌────────────────────────┐  ┌────────────────────────┐          │
│ │ Decode event data      │  │ Parse block number     │          │
│ │ Extract tweet_id from  │  │                        │          │
│ │ context metadata       │  │                        │          │
│ └──────────┬─────────────┘  └──────────┬─────────────┘          │
│            │                           │                        │
│            ▼                           ▼                        │
│ Publish: onchain:token     Publish: onchain:block               │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- Decode de eventos ABI (~2ms)
- Uma única conexão WebSocket (limitação do RPC provider)

**Paralelismo:** Processamento de logs e blocks é serial (mesmo socket)

---

### 3. TWEET PROCESSOR (Scalable - 2 replicas)

**Responsabilidade:** Classificar tweets usando LLM

**Processamento:**
```
SEQUENCIAL por tweet, PARALELO entre replicas:
┌─────────────────────────────────────────────────────────────────┐
│ Subscribe twitter:tweet                                         │
│                                                                 │
│ FOR each tweet:                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check dedupe cache                          [~0.1ms]     │ │
│ │ 2. IF author == "bankrbot":                                 │ │
│ │    └─ Parse deployment (local)                 [~1ms]       │ │
│ │    └─ Publish twitter:deployed                 [~1ms]       │ │
│ │                                                             │ │
│ │ 3. ELSE IF mentions @bankrbot:                              │ │
│ │    └─ Call LLM for classification              [~500ms] ⚠️  │ │
│ │    └─ Parse token info                         [~1ms]       │ │
│ │    └─ Publish twitter:prebankr                 [~1ms]       │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- ⚠️ **PRINCIPAL:** LLM call (~500ms por tweet)
- Dedupe em memória (não compartilhado entre replicas - pode causar duplicatas)

**Paralelismo:**
- 2 replicas processam tweets diferentes simultaneamente
- Redis Pub/Sub distribui automaticamente

**Mitigação de duplicatas:**
- Cada replica mantém seu próprio set de tweet IDs processados
- Possível duplicata se mesmo tweet chegar em replicas diferentes rapidamente
- Solução: usar Redis SETNX para dedupe global

---

### 4. TELEGRAM PROGRESSIVE (Single Instance)

**Responsabilidade:** Enviar mensagens progressivas com edição em tempo real

**Processamento:**
```
EVENTOS PARALELOS via asyncio.create_task():
┌─────────────────────────────────────────────────────────────────┐
│ Subscribe: twitter:prebankr, twitter:deployed, onchain:token    │
│                                                                 │
│ CADA EVENTO é processado em BACKGROUND TASK SEPARADA:           │
│ - Não bloqueia recebimento de novos eventos!                    │
│ - Múltiplos enrichments podem rodar simultaneamente             │
│                                                                 │
│ PRE-BANKR Handler (asyncio.create_task):                        │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Send initial message                        [~50ms]      │ │
│ │ 2. Cache to Redis                              [~2ms]       │ │
│ │ 3. PARALLEL enrichment via asyncio.gather:                  │ │
│ │    ┌──────────────────┬──────────────────────────────┐      │ │
│ │    │ enrich_from_user │ enrich_to_users (parallel)   │      │ │
│ │    │ ~1000ms          │ ~1000ms each                 │      │ │
│ │    └──────────────────┴──────────────────────────────┘      │ │
│ │ 4. Edit message after each enrichment          [~50ms]      │ │
│ │ 5. Final edit when complete                    [~50ms]      │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ DEPLOYED Handler (asyncio.create_task):                         │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check _pending_onchain_msgs for existing msg             │ │
│ │    IF found: update existing OnchainDeploymentMessage       │ │
│ │    ELSE: create new ProgressiveMessage                      │ │
│ │ 2. PARALLEL enrichment (same as PRE-BANKR)                  │ │
│ │ 3. Progressive edits                                        │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ ONCHAIN Handler (asyncio.create_task):                          │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check contract dedupe (Redis)               [~2ms]       │ │
│ │ 2. Check prebankr_data cache for instant data  [~2ms]       │ │
│ │ 3. Send initial (with cached data if available)[~50ms]      │ │
│ │ 4. BACKGROUND: fetch creator info (PARALLEL RPC)[~100ms]    │ │
│ │    - eth_getBalance + eth_getTransactionCount simultâneos   │ │
│ │ 5. Store in _pending_onchain_msgs for later    [~0.1ms]     │ │
│ │                                                             │ │
│ │ NOTE: Trading link é feito pelo trading_executor            │ │
│ │       que também subscreve em onchain:token                 │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Enrichment Paralelo (asyncio.gather):**
```
┌────────────────────────────────────────────────────────────────┐
│                    enrich_user_async()                         │
│                                                                │
│  asyncio.gather executando SIMULTANEAMENTE:                    │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐      │
│  │ Twitter API    │ │ AlphaGate R/C  │ │ AlphaGate SF   │      │
│  │ followers,     │ │ rugs, calls    │ │ smart followers│      │
│  │ following      │ │ ratio          │ │ list           │      │
│  │ ~800ms         │ │ ~600ms         │ │ ~700ms         │      │
│  └────────┬───────┘ └────────┬───────┘ └────────┬───────┘      │
│           │                  │                  │              │
│           └──────────────────┼──────────────────┘              │
│                              ▼                                 │
│                    Merge results                               │
│                    Total: ~800ms (max of all)                  │
└────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- ⚠️ Enrichment APIs (~800-1000ms por usuário)
- Telegram rate limit (edits)

**Paralelismo:**
- ✅ Eventos são processados em PARALELO (asyncio.create_task)
- ✅ Enrichment de FROM e TO users é PARALELO
- ✅ Enrichment interno (Twitter + AlphaGate + ENS) é PARALELO
- ✅ Background tasks para creator info (RPC calls paralelos)
- ✅ Trading link delegado para trading_executor (sem duplicação)

---

### 5. USER ENRICHER (Scalable - 2 replicas)

**Responsabilidade:** Enriquecer dados de usuários via APIs

**Processamento:**
```
PARALELO interno, SERIAL por request:
┌─────────────────────────────────────────────────────────────────┐
│ Subscribe: enrich:request                                       │
│                                                                 │
│ FOR each request:                                               │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check Redis cache                           [~2ms]       │ │
│ │    IF cached: publish enrich:result, SKIP                   │ │
│ │                                                             │ │
│ │ 2. PARALLEL API calls via asyncio.gather:                   │ │
│ │    ┌────────────────────────────────────────────────────┐   │ │
│ │    │ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │   │ │
│ │    │ │ Twitter API  │ │ AlphaGate    │ │ ENS Resolve  │ │   │ │
│ │    │ │ ~800ms       │ │ ~600ms       │ │ ~300ms       │ │   │ │
│ │    │ └──────────────┘ └──────────────┘ └──────────────┘ │   │ │
│ │    └────────────────────────────────────────────────────┘   │ │
│ │    Total: ~800ms (max)                                      │ │
│ │                                                             │ │
│ │ 3. Merge results + save to Redis cache         [~2ms]       │ │
│ │ 4. Publish enrich:result                       [~1ms]       │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- ⚠️ APIs externas (~800ms)
- Cache miss = ~800ms latência

**Paralelismo:**
- ✅ APIs são chamadas em paralelo (asyncio.gather)
- ✅ 2 replicas processam requests diferentes

---

### 6. TELEGRAM CALLBACK (Single Instance)

**Responsabilidade:** Processar callbacks e comandos do Telegram

**Processamento:**
```
PARALELO via asyncio.gather:
┌─────────────────────────────────────────────────────────────────┐
│ DOIS POLL LOOPS PARALELOS:                                      │
│                                                                 │
│ ┌─────────────────────────────┐ ┌─────────────────────────────┐ │
│ │ prebankr_bot poll loop      │ │ trading_bot poll loop       │ │
│ │                             │ │                             │ │
│ │ WHILE running:              │ │ WHILE running:              │ │
│ │   getUpdates (30s timeout)  │ │   getUpdates (30s timeout)  │ │
│ │   FOR each update:          │ │   FOR each update:          │ │
│ │     handle_callback()       │ │     handle_callback()       │ │
│ │     OR handle_message()     │ │     OR handle_message()     │ │
│ └─────────────────────────────┘ └─────────────────────────────┘ │
│                                                                 │
│ Callback handlers (SERIAL dentro de cada bot):                  │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ - like_{tweet_id}        -> Add like, answer callback       │ │
│ │ - buy_{tweet_id}_{eth}   -> Approve buy, publish event      │ │
│ │ - cfg_{action}           -> Update config, show menu        │ │
│ │ - block_{offset}         -> Update block offset             │ │
│ │ - slippage_{value}       -> Update slippage                 │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- Long polling (30s timeout) - latência até 30s para primeiro update
- Serial processing dentro de cada bot

**Paralelismo:**
- ✅ Dois bots polled simultaneamente
- ❌ Callbacks do mesmo bot são seriais

---

### 7. TRADING EXECUTOR (Single Instance)

**Responsabilidade:** Executar trades no target block

**Processamento:**
```
PARALELO execution, SERIAL event handling:
┌─────────────────────────────────────────────────────────────────┐
│ Subscribe: onchain:block, onchain:token, trading:buy_approved,  │
│            twitter:prebankr                                     │
│                                                                 │
│ Block Handler (critical path):                                  │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Get WAITING buys for this block             [~2ms]       │ │
│ │ 2. PARALLEL execution via asyncio.gather:                   │ │
│ │    ┌────────────────────────────────────────────────────┐   │ │
│ │    │ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │   │ │
│ │    │ │ Execute buy1 │ │ Execute buy2 │ │ Execute buyN │ │   │ │
│ │    │ │ ~500ms       │ │ ~500ms       │ │ ~500ms       │ │   │ │
│ │    │ └──────────────┘ └──────────────┘ └──────────────┘ │   │ │
│ │    └────────────────────────────────────────────────────┘   │ │
│ │    Total: ~500ms (all parallel)                             │ │
│ │                                                             │ │
│ │ 3. Notify results                              [~50ms]      │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Token Handler:                                                  │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Get pending buys for tweet                  [~2ms]       │ │
│ │ 2. Update each with token address              [~2ms each]  │ │
│ │ 3. Calculate target block                      [~1ms]       │ │
│ │ 4. Save updated buys                           [~2ms each]  │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Auto-buy Handler (for PRE-BANKR with auto_buy enabled):         │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check user configs for auto_buy             [~5ms]       │ │
│ │ 2. Create PendingBuy for each                  [~2ms each]  │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- ⚠️ **CRÍTICO:** Execução de swap (~500ms) - mas mitigado por paralelismo
- ⚠️ RPC calls podem ser lentas
- Single instance = ponto único de falha

**Paralelismo:**
- ✅ Múltiplos swaps executados em paralelo
- ✅ Parallel gather para execução

---

### 8. TELEGRAM SENDER (Single Instance)

**Responsabilidade:** Enviar mensagens ao Telegram (fallback/notificações)

**Processamento:**
```
SERIAL com rate limiting:
┌─────────────────────────────────────────────────────────────────┐
│ Subscribe: telegram:send, telegram:edit                         │
│                                                                 │
│ FOR each request (SERIAL):                                      │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 1. Check rate limit (50ms between sends)       [0-50ms]     │ │
│ │ 2. Send HTTP request to Telegram API           [~100ms]     │ │
│ │ 3. Handle 429 retry if needed                  [variable]   │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ Rate limit: 20 msg/sec max                                      │
└─────────────────────────────────────────────────────────────────┘
```

**Gargalos:**
- ⚠️ Rate limit do Telegram (20/sec)
- Serial by design (rate limiting)

**Paralelismo:** NENHUM (by design para rate limiting)

---

## Fluxo Completo de um Tweet

```
Timeline (ms)    Worker                    Ação
─────────────────────────────────────────────────────────────────
0               twitter-listener          Recebe tweet via WebSocket
1               twitter-listener          Publica twitter:tweet
2               tweet-processor           Recebe tweet
3-500           tweet-processor           LLM classifica (GARGALO)
501             tweet-processor           Publica twitter:prebankr
502             telegram-progressive      Recebe PRE-BANKR event
503-553         telegram-progressive      Envia msg inicial (50ms)
554             telegram-progressive      Cache em Redis
555-1355        telegram-progressive      Enrichment paralelo (800ms)
                                         - Twitter API
                                         - AlphaGate R/C
                                         - AlphaGate SF
                                         - ENS resolve
1356-1406       telegram-progressive      Edit message with data
─────────────────────────────────────────────────────────────────
Total: ~1.4 segundos (dominado por LLM e enrichment)
```

## Fluxo OnChain-First (mais rápido!)

```
Timeline (ms)    Worker                    Ação
─────────────────────────────────────────────────────────────────
0               onchain-listener          Detecta TokenCreated
1               onchain-listener          Publica onchain:token
2               telegram-progressive      Recebe ONCHAIN event
3               telegram-progressive      Check cache prebankr_data
4-54            telegram-progressive      Envia msg (com cache!)
55              telegram-progressive      Background: fetch creator
56              telegram-progressive      Background: link trading
─────────────────────────────────────────────────────────────────
Total: ~55ms se cache hit! (vs 1.4s do fluxo Twitter)
```

---

## Gargalos Identificados

| Prioridade | Worker | Gargalo | Latência | Mitigação |
|------------|--------|---------|----------|-----------|
| 🔴 ALTO | tweet-processor | LLM call | ~500ms | Cache classificações |
| 🔴 ALTO | telegram-progressive | API enrichment | ~800ms | Cache agressivo |
| 🟡 MÉDIO | trading-executor | RPC swap | ~500ms | Já paralelo |
| 🟢 BAIXO | telegram-sender | Rate limit | 50ms | By design |

---

## Recomendações de Otimização

### 1. Dedupe Global (tweet-processor)
```python
# Usar Redis SETNX para dedupe global entre replicas
async def should_process(tweet_id: str) -> bool:
    return await redis.setnx(f"processed:{tweet_id}", 1, ex=3600)
```

### 2. Cache de Classificação LLM
```python
# Cache classificações de padrões conhecidos
KNOWN_PATTERNS = {
    "deploy $": "prebankr",
    "launched": "deployed",
}
```

### 3. Pre-warm User Cache
```python
# Quando PRE-BANKR detectado, já enricher os usuários
# para quando DEPLOYED chegar, cache está quente
```

### 4. Scale telegram-progressive
```python
# Considerar múltiplas instâncias com particionamento
# por tweet_id hash para evitar race conditions
```

---

## Conclusão

A arquitetura é bem desenhada com:
- ✅ Listeners single-instance (correto para WebSocket)
- ✅ Processors escaláveis horizontalmente
- ✅ Enrichment paralelo (asyncio.gather)
- ✅ Trading execution paralela
- ✅ OnChain-first flow para latência mínima
- ✅ Eventos paralelos no telegram-progressive (asyncio.create_task)
- ✅ RPC calls paralelos para creator info
- ✅ Separação clara de responsabilidades (trading_executor linka buys)

Principais pontos de atenção:
- ⚠️ LLM latency (~500ms) é o maior gargalo
- ⚠️ Dedupe entre replicas precisa de Redis

## Fluxo BUY Completo

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. PRE-BANKR Message aparece no Telegram                                   │
│                                                                             │
│  2. User clica BUY → telegram_callback                                      │
│     └─ Shows confirmation menu                                              │
│                                                                             │
│  3. User confirma → approve_buy()                                           │
│     └─ Cria PendingBuy(status=APPROVED, block_offset=X)                     │
│     └─ Salva em Redis com indexes:                                          │
│        - buys_by_user:{user_id}                                             │
│        - buys_by_status:APPROVED                                            │
│        - buys_by_tweet:{tweet_id}                                           │
│     └─ Publica trading:buy_approved                                         │
│                                                                             │
│  4. trading_executor recebe trading:buy_approved                            │
│     └─ Verifica se token já está on-chain                                   │
│        - Se sim: link_token_to_buy() imediatamente                          │
│        - Se não: aguarda onchain:token                                      │
│                                                                             │
│  5. Token vai on-chain → onchain_listener publica onchain:token             │
│                                                                             │
│  6. trading_executor recebe onchain:token                                   │
│     └─ Busca todos os buys APPROVED para esse tweet                         │
│     └─ link_token_to_buy():                                                 │
│        - Define token_address, pool_id                                      │
│        - Calcula target_block = auction_expire + block_offset               │
│        - Muda status: APPROVED → WAITING                                    │
│        - Atualiza indexes corretamente                                      │
│     └─ Notifica usuário: "TOKEN DETECTADO! Aguardando bloco..."             │
│                                                                             │
│  7. trading_executor recebe onchain:block                                   │
│     └─ get_buys_for_block(block_number)                                     │
│        - Busca todos WAITING com target_block <= current                    │
│     └─ asyncio.gather() executa todos em PARALELO                           │
│     └─ Notifica resultado                                                   │
└─────────────────────────────────────────────────────────────────────────────┘
```
