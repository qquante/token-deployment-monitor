# REDIS DATA GUIDE - Bankr Monitor

## Visão Geral

O Redis é usado como:
1. **State Store** - Armazena configurações de usuários e estado de trades
2. **Cache** - Dados de APIs externas (Twitter, AlphaGate, Clanker)
3. **Pub/Sub** - Comunicação entre workers em tempo real
4. **Indexes** - Busca rápida por diferentes critérios

---

## Estrutura de Dados

### 1. USER CONFIG (`user:{telegram_user_id}`)
**Tipo:** Hash  
**TTL:** Permanente  
**Descrição:** Configuração completa de cada usuário do bot

```
user:1091994845 = {
    "user_id": "1091994845",
    "username": "meu_username",
    
    # WALLET (CRÍTICO - dados sensíveis!)
    "wallet_address": "0x...",
    "wallet_private_key": "0x...",  # NUNCA LOGAR!
    
    # TRADING PARAMS
    "eth_amount": "0.01",        # ETH por trade
    "block_offset": "6",         # Blocos após auction expirar
    "gwei": "0.1",               # Gas price
    "slippage": "5.0",           # Slippage %
    
    # TOGGLES
    "auto_buy": "1",             # 1=ON, 0=OFF
    "alerts_enabled": "1",       # Receber alertas
    
    # SNIPER HANDLES (auto-buy quando @handle cria token)
    "sniper_handles": "clanker,bankr,jesse",
    
    # BUY TTL
    "buy_ttl_minutes": "60",     # Max tempo esperando on-chain
    
    # PRE-APPROVAL
    "max_eth_approved": "0.5",   # Max ETH auto-aprovado
    "block_tolerance": "1",      # Tolerância de blocos
    
    # MOLTX TRIGGERS
    "moltx_trigger_enabled": "0",
    "moltx_trigger_handles": "",
    "moltx_trigger_min_followers": "0",
    "moltx_trigger_min_moltx_followers": "0",
    "moltx_trigger_min_smart_followers": "0",
    "moltx_trigger_x_verified": "0",
    
    # MOLTBOOK TRIGGERS
    "moltbook_trigger_enabled": "0",
    "moltbook_trigger_handles": "",
    "moltbook_trigger_min_followers": "0",
    "moltbook_trigger_min_moltx_followers": "0",
    "moltbook_trigger_min_smart_followers": "0",
    "moltbook_trigger_min_karma": "0",
    "moltbook_trigger_x_verified": "0",
    
    # STATS
    "total_buys": "15",
    "total_spent": "0.15"
}
```

**Indexes relacionados:**
- `auto_buy_users` (Set) - User IDs com auto_buy=1
- `sniper_handle:{handle}` (Set) - User IDs que snipam esse handle
- `moltx_trigger_users` (Set) - User IDs com MoltX trigger ativo
- `moltbook_trigger_users` (Set) - User IDs com Moltbook trigger ativo

---

### 2. PENDING BUY (`buy:{user_id}:{tweet_id}`)
**Tipo:** Hash  
**TTL:** Permanente  
**Descrição:** Trade pendente/executado

```
buy:1091994845:2017251309762838962 = {
    "user_id": "1091994845",
    "tweet_id": "2017251309762838962",
    "username": "meu_username",
    
    # TOKEN INFO (preenchido quando vai on-chain)
    "token_address": "0x...",
    "token_symbol": "PEPE",
    "token_name": "Pepe Token",
    "pool_id": "0x...",
    
    # TRADING PARAMS (copiados do user config no momento da aprovação)
    "eth_amount": "0.01",
    "block_offset": "6",
    "gwei": "0.1",
    "slippage": "5.0",
    
    # BLOCOS
    "auction_expire_block": "12345678",
    "target_block": "12345684",      # expire + offset
    "fee_expected": "16.7",          # Fee % esperada
    
    # STATUS: APPROVED -> WAITING -> EXECUTING -> DONE/FAILED/CANCELLED
    "status": "DONE",
    
    # TIMESTAMPS
    "created_at": "2024-01-15T10:30:00",
    "approved_at": "2024-01-15T10:30:00",
    "linked_at": "2024-01-15T10:30:05",   # Quando token foi on-chain
    "executed_at": "2024-01-15T10:30:30",
    
    # RESULTADO
    "tx_hash": "0x...",
    "error": "",
    "gas_used": "150000",
    "actual_fee": "16.7",
    "executed_block": "12345684"
}
```

**Status Flow:**
```
APPROVED → WAITING → EXECUTING → DONE
                  ↘           ↘ FAILED
                   → CANCELLED
```

**Indexes relacionados:**
- `buys_by_user:{user_id}` (Set) - Todas as buys do usuário
- `buys_by_status:{status}` (Set) - Buys por status
- `buys_by_tweet:{tweet_id}` (Set) - Buys de um tweet
- `buys_by_target_block:{block}` (Set) - Buys para executar no bloco
- `buys_by_token:{address}` (Set) - Buys de um token

---

### 3. TOKEN INFO (`token:{tweet_id}`)
**Tipo:** Hash  
**TTL:** 24 horas  
**Descrição:** Cache de informações de token

```
token:2017251309762838962 = {
    "tweet_id": "2017251309762838962",
    "token_address": "0x...",
    "token_symbol": "PEPE",
    "token_name": "Pepe Token",
    "pool_id": "0x...",
    
    "creator_wallet": "0x...",
    "platform": "X",              # X ou farcaster
    
    "tweet_url": "https://x.com/...",
    "from_username": "clanker",
    "to_usernames": "[\"jesse\", \"vitalik\"]",
    
    "auction_expire_block": "12345678",
    
    "on_chain": "1",
    "deployed": "1",
    
    "prebankr_at": "2024-01-15T10:30:00",
    "onchain_at": "2024-01-15T10:30:05",
    "deployed_at": "2024-01-15T10:30:10"
}
```

**Index relacionado:**
- `token_by_address:{address}` (String) → tweet_id

---

### 4. ENRICHED USER (`enriched_user:{username}`)
**Tipo:** Hash  
**TTL:** 24 horas  
**Descrição:** Cache combinado de dados Twitter + AlphaGate + ENS

```
enriched_user:jesse = {
    "username": "jesse",
    "user_id": "123456",
    "name": "Jesse Pollak",
    "followers": "500000",
    "following": "1000",
    "is_blue_verified": "1",
    "verified_type": "blue",
    "created_at": "2015-01-01T00:00:00",
    "bio": "Building @base...",
    "profile_image_url": "https://...",
    
    # ENS
    "ens": "jesse.eth",
    "wallet": "0x...",
    
    # ALPHAGATE
    "smart_followers": "150",
    "prev_usernames_count": "0",    # R (renames)
    "contracts_count": "0",          # C (contracts deployed)
    "affiliate_label": "",
    
    "cached_at": "2024-01-15T10:00:00"
}
```

---

### 5. MOLT AGENT (`molt_agent:{x_handle}`)
**Tipo:** Hash  
**TTL:** 24 horas  
**Descrição:** Cache de agentes Moltbook

```
molt_agent:clawpilot00 = {
    "x_handle": "clawpilot00",
    "agent_name": "ClawPilot",
    "agent_id": "uuid-here",
    "is_verified": "1",
    "karma": "500",
    "description": "AI agent that...",
    
    # KOL FILTER DATA
    "followers": "10000",
    "smart_followers_count": "25",
    "vip_followers": "[\"jesse\", \"vitalik\"]",
    "passes_kol_filter": "1",       # 500+ followers OR 2+ VIPs
    
    "cached_at": "2024-01-15T10:00:00"
}
```

**Indexes relacionados:**
- `molt_agents_index` (Set) - Todos os handles de agentes
- `molt_name_to_handle:{name}` (String) → x_handle
- `molt_id_to_handle:{id}` (String) → x_handle

---

### 6. POOL DATA (`pool:{token_address}`)
**Tipo:** Hash  
**TTL:** 7 dias  
**Descrição:** Cache de parâmetros de pool Clanker (não muda)

```
pool:0x1234... = {
    "token_address": "0x1234...",
    "pool_id": "0x...",
    "currency0": "0x...",           # Token ou WETH
    "currency1": "0x...",           # WETH ou Token
    "fee": "10000",                 # 1%
    "tick_spacing": "200",
    "hooks": "0x...",               # Clanker hooks address
    "sqrt_price_x96": "...",
    "tick": "-200000",
    "zero_for_one": "1",            # Direção do swap
    "block_number": "12345678",
    "tx_hash": "0x...",
    "cached_at": "2024-01-15T10:00:00"
}
```

---

### 7. PRE-CLAWNCH (`preclawnch:{post_id}`)
**Tipo:** Hash  
**TTL:** 30 dias  
**Descrição:** Tokens MoltX/Moltbook/4claw antes de ir on-chain

```
preclawnch:8fdd6010-16d1-41c0-88ee-132cfa6b5938 = {
    "post_id": "8fdd6010-16d1-41c0-88ee-132cfa6b5938",
    "platform": "moltx",            # moltx, moltbook, 4claw
    "token_name": "My Token",
    "token_symbol": "MTK",
    "token_wallet": "0x...",        # Wallet que vai criar
    "agent_name": "ClawPilot",
    "x_handle": "clawpilot00",
    "created_at": "2024-01-15T10:00:00"
}
```

**Indexes:**
- `preclawnch_by_wallet:{prefix}` (String) → post_id
- `preclawnch_by_symbol:{symbol}` (String) → post_id

---

### 8. PRE-BANKR DATA (`prebankr_data:{tweet_id}`)
**Tipo:** Hash  
**TTL:** 3 horas  
**Descrição:** Cache de PRE-BANKR para display instantâneo quando DEPLOYED chega

```
prebankr_data:2017251309762838962 = {
    "tweet_id": "2017251309762838962",
    "from_username": "clanker",
    "to_usernames": "[\"jesse\"]",
    "token_name": "Pepe",
    "token_symbol": "PEPE",
    "wallet_map": "{\"jesse\": \"0x...\"}",
    "enriched_from": "{...}",       # JSON do enriched user
    "enriched_to": "[{...}]",       # JSON array
    "cached_at": "2024-01-15T10:00:00"
}
```

---

### 9. MOLTTRACK (`molttrack:list`)
**Tipo:** Hash  
**TTL:** Permanente  
**Descrição:** Tracks de Moltbook (notificar quando match)

```
molttrack:list = {
    "1": "{\"type\": \"twitter\", \"value\": \"clawpilot00\"}",
    "2": "{\"type\": \"ticker\", \"value\": \"PEPE\"}",
    "3": "{\"type\": \"name\", \"value\": \"claw\"}"
}
```

**Tipos:**
- `twitter` - Notificar quando @handle criar
- `ticker` - Notificar quando $TICKER aparecer
- `name` - Notificar quando nome conter string

**Indexes:**
- `molttrack:{type}:{value}` (String) → track_id
- `molttrack:next_id` (String) - Auto-increment

---

### 10. PRE-BANKR BLACKLIST (`prebankr:blacklist:list`)
**Tipo:** Hash  
**TTL:** Permanente  
**Descrição:** Handles ignorados no PRE-BANKR

```
prebankr:blacklist:list = {
    "1": "0xdoha",
    "2": "spammer123"
}
```

**Index:** `prebankr:blacklist:handle:{handle}` → blacklist_id

---

### 11. PRE-BANKR WORD BLACKLIST (`prebankr:wordblacklist:list`)
**Tipo:** Hash  
**TTL:** Permanente  
**Descrição:** Palavras que filtram PRE-BANKR

```
prebankr:wordblacklist:list = {
    "1": "solana",
    "2": "scam"
}
```

**Index:** `prebankr:wordblacklist:word:{word}` → blacklist_id

---

### 12. SEEN INTERFACES (`seen_interfaces`)
**Tipo:** Set  
**TTL:** Permanente  
**Descrição:** Interfaces Clanker já vistas (para alertar novas)

```
seen_interfaces = ["Bankr", "Clawnch", "clanker.world", "SDK", ...]
```

---

### 13. NOTIFIED CONTRACTS (`notified_contracts`)
**Tipo:** Set  
**TTL:** Permanente  
**Descrição:** Contracts já notificados (dedupe)

```
notified_contracts = ["0x123...", "0x456...", ...]
```

---

### 14. LIKES (`likes:{tweet_id}`)
**Tipo:** Set  
**TTL:** Permanente  
**Descrição:** Likes em PRE-BANKR tweets

```
likes:2017251309762838962 = ["1091994845:username1", "833229395:username2"]
```

---

### 15. CACHES TEMPORÁRIOS

#### AlphaGate (`alphagate:{username}`)
**TTL:** 24h - R, C, Smart Followers

#### Twitter (`twitter:{username}`)
**TTL:** 24h - Dados básicos do perfil

#### Trade Lock (`trade_lock:{buy_key}`)
**TTL:** 2 min - Previne execução dupla

#### Buy Creation Lock (`buy_creation_lock:{user}:{tweet}`)
**TTL:** 10s - Previne click duplo

---

## Pub/Sub Channels

| Channel | Descrição | Payload |
|---------|-----------|---------|
| `token_events` | Tokens detectados on-chain | `{event_type, token_address, ...}` |
| `prebankr_events` | PRE-BANKR tweets | `{tweet_id, from, to, ...}` |
| `trade_commands` | Comandos de trade | `{action, user_id, tweet_id, ...}` |
| `trade_results` | Resultados de trade | `{status, tx_hash, error, ...}` |

---

## Backup & Restore

### Exportar
```bash
python export_redis.py
# Gera: redis_backup_YYYYMMDD_HHMMSS.json
```

### Importar
```bash
python import_redis.py redis_backup_20260205_023712.json
```

### Opções de Import
- `--users-only` - Só configs de usuário
- `--skip-cache` - Pular dados de cache (pools, enriched, molt_agents)
- `--clear-first` - Limpar Redis antes de importar

---

## Dados Críticos vs Cache

### CRÍTICOS (manter no backup)
- `user:*` - Configs de usuário (WALLETS!)
- `buy:*` - Histórico de trades
- `molttrack:list` - Tracks configurados
- `prebankr:blacklist:list` - Blacklist de handles
- `prebankr:wordblacklist:list` - Blacklist de palavras
- `likes:*` - Likes em PRE-BANKR

### CACHE (regenera automaticamente)
- `enriched_user:*` - Refetch do Twitter/AlphaGate
- `molt_agent:*` - Refetch do Moltbook API
- `pool:*` - Refetch on-chain
- `token:*` - Refetch quando necessário
- `preclawnch:*` - Temporário
- `prebankr_data:*` - Temporário
- `alphagate:*` - Cache API
- `twitter:*` - Cache API
- `seen_interfaces` - Reconstruído
- `notified_contracts` - Pode resetar (vai re-notificar)

---

## Deploy Checklist

1. **Instalar Redis** (ou usar Docker)
2. **Configurar .env**:
   ```
   REDIS_HOST=localhost
   REDIS_PORT=6379
   REDIS_DB=0
   REDIS_PASSWORD=sua_senha  # opcional
   ```
3. **Importar backup** (se migrando):
   ```bash
   python import_redis.py redis_backup_xxx.json
   ```
4. **Verificar**:
   ```bash
   docker exec bankr-redis redis-cli DBSIZE
   docker exec bankr-redis redis-cli KEYS "user:*"
   ```

---

## Comandos Úteis

```bash
# Ver todas as keys
docker exec bankr-redis redis-cli KEYS "*"

# Ver config de usuário
docker exec bankr-redis redis-cli HGETALL "user:1091994845"

# Ver pending buys de usuário
docker exec bankr-redis redis-cli SMEMBERS "buys_by_user:1091994845"

# Ver status de buys
docker exec bankr-redis redis-cli SCARD "buys_by_status:WAITING"

# Limpar cache (sem perder configs)
docker exec bankr-redis redis-cli KEYS "enriched_user:*" | xargs docker exec -i bankr-redis redis-cli DEL
docker exec bankr-redis redis-cli KEYS "pool:*" | xargs docker exec -i bankr-redis redis-cli DEL

# Backup RDB nativo
docker exec bankr-redis redis-cli BGSAVE
docker cp bankr-redis:/data/dump.rdb ./redis_dump.rdb
```
