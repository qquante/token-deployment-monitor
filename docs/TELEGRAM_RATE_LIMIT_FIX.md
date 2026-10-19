# Telegram Rate Limit Fix (429 Errors)

## Data: 2026-02-04

## Problema

O `telegram-progressive` estava gerando **126+ erros 429** (Too Many Requests) por hora, principalmente no canal DEPLOYED (`-1003708365149`).

### Causa Raiz

1. **Envio paralelo sem rate limiting**: Múltiplos eventos criavam tasks paralelas que enviavam mensagens ao mesmo tempo
2. **Sem fila centralizada**: Cada handler disparava `send_message()` diretamente
3. **Edits múltiplos**: Cada mensagem era editada várias vezes durante enriquecimento

### Logs de Erro (Antes)

```
16:26:09 [ERROR] [TG ERROR] Too Many Requests: retry after 5 (channel=-1003708365149)
16:26:09 [ERROR] [TG ERROR] Too Many Requests: retry after 5 (channel=-1003708365149)
16:26:09 [ERROR] [TG ERROR] Too Many Requests: retry after 5 (channel=-1003708365149)
16:26:09 [ERROR] [TG ERROR] Too Many Requests: retry after 5 (channel=-1003708365149)
```

---

## Solução Implementada

### 1. TelegramBotPool (`src/async_clients/telegram_pool.py`)

Nova classe que centraliza todos os envios com:

- **Fila centralizada** (`asyncio.Queue`): Serializa todas as mensagens
- **Rate limiter per-channel**: 1 mensagem/segundo por canal
- **Pool de bots round-robin**: Distribui carga entre múltiplos bot tokens
- **Retry com backoff**: Respeita `Retry-After` do Telegram

```python
pool = TelegramBotPool(
    tokens=["bot1:token", "bot2:token", "bot3:token"],
    default_channel_id="-1003708365149",
    min_interval=1.0,  # 1 seg entre msgs do mesmo canal
)
await pool.start()

# Todas as mensagens passam pela fila
msg_id = await pool.send_message("Hello!")
await pool.edit_message(msg_id, "Updated!")
```

### 2. PooledTelegramClient

Drop-in replacement para `AsyncTelegramClient` que usa o pool internamente:

```python
# Antes:
client = AsyncTelegramClient(bot_token, channel_id)

# Depois (mesma interface!):
client = PooledTelegramClient(pool, channel_id)
```

### 3. Modificações no `telegram_progressive.py`

```python
# Antes: 3 clientes independentes
self.telegram_deployed = AsyncTelegramClient(...)
self.telegram_prebankr = AsyncTelegramClient(...)

# Depois: 1 pool compartilhado com clientes pooled
self._bot_pool = TelegramBotPool(tokens=[...])
self.telegram_deployed = PooledTelegramClient(pool, channel_deployed)
self.telegram_prebankr = PooledTelegramClient(pool, channel_prebankr)
```

---

## Arquitetura Nova

```
┌─────────────────────────────────────────────────────────────┐
│              Events (ONCHAIN, PREBANKR, DEPLOYED)           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   asyncio.Queue (Fila)                       │
│   [msg1] → [msg2] → [msg3] → [msg4] → ...                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│         PerChannelRateLimiter (1 msg/seg por canal)         │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         ┌────────┐      ┌────────┐      ┌────────┐
         │ Bot 1  │      │ Bot 2  │      │ Bot 3  │
         │Original│      │ Novo 1 │      │ Novo 2 │
         └────────┘      └────────┘      └────────┘
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                      Telegram API
```

---

## Configuração

### Variáveis de Ambiente

```bash
# .env

# Token principal (fallback se pool não configurado)
TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN

# Pool de tokens (separados por vírgula)
TELEGRAM_BOT_POOL_TOKENS=[REMOVED_CREDENTIAL],[REMOVED_CREDENTIAL],[REMOVED_CREDENTIAL]
```

### Pré-requisitos

**IMPORTANTE**: Todos os bots do pool precisam ser **admin** dos canais:
- Canal DEPLOYED: `-1003708365149`
- Canal PREBANKR: `-1003802317678`

Para adicionar um bot como admin:
1. Abra o canal no Telegram
2. Clique no nome do canal → Administradores
3. Adicionar Administrador → Cole o @username do bot
4. Habilite "Postar Mensagens"

---

## Arquivos Modificados

| Arquivo | Ação |
|---------|------|
| `src/async_clients/telegram_pool.py` | **CRIADO** - TelegramBotPool, PerChannelRateLimiter, PooledTelegramClient |
| `workers/telegram_progressive.py` | **MODIFICADO** - Usa pool ao invés de clientes diretos |
| `.env` | **MODIFICADO** - Adicionado TELEGRAM_BOT_POOL_TOKENS |
| `.env.example` | **MODIFICADO** - Documentação da nova variável |
| `docker-compose.yml` | **MODIFICADO** - Passa TELEGRAM_BOT_POOL_TOKENS para telegram-progressive |

---

## Métricas Esperadas

| Métrica | Antes | Depois |
|---------|-------|--------|
| Erros 429/hora | 126+ | ~0 |
| Latência (burst) | Instantâneo + erros | Rate limited (estável) |
| Redundância | 1 bot | 3 bots |
| Recuperação | Nenhuma | Retry com backoff |

---

## Como Testar

1. **Adicionar bots como admin** dos canais (CRÍTICO!)

2. **Rebuild e restart**:
```bash
docker-compose build telegram-progressive
docker-compose up -d telegram-progressive
```

3. **Verificar logs**:
```bash
docker logs -f bankr-telegram-progressive
```

Deve aparecer:
```
[POOL] Using 3 bot tokens from TELEGRAM_BOT_POOL_TOKENS
[POOL] Started with 3 bots, rate limit: 1 msg/sec per channel
```

4. **Monitorar 429**:
```bash
docker logs bankr-telegram-progressive 2>&1 | findstr "429\|Too Many"
```

---

## Troubleshooting

### Bot não consegue enviar
- Verifique se o bot é admin do canal
- Verifique se tem permissão "Postar Mensagens"

### Ainda há 429
- O rate limit é per-channel, não per-bot
- Se múltiplos tokens para o mesmo canal, ainda há limite ~1 msg/seg
- Aumente `min_interval` para 1.5 ou 2.0 se necessário

### Pool não inicia
- Verifique se TELEGRAM_BOT_POOL_TOKENS está definido
- Verifique formato: tokens separados por vírgula, sem espaços

---

## Referências

- [Telegram Bot API Rate Limits](https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this)
- [python-telegram-bot Avoiding Flood Limits](https://github.com/python-telegram-bot/python-telegram-bot/wiki/Avoiding-flood-limits)
- [Grammy Flood Limits Guide](https://grammy.dev/advanced/flood.html)
