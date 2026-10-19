# 🏗️ SISTEMA PRE-BANKR/BANKR E ADAPTAÇÃO PARA CLAWNCH

**Data:** 31 de Janeiro de 2026  
**Objetivo:** Documentar arquitetura completa do sistema BANKR e como replicar para CLAWNCH

---

## 📋 ÍNDICE

1. [Visão Geral do Sistema BANKR](#visão-geral)
2. [Fluxo Completo PRE-BANKR → BANKR](#fluxo-pre-bankr-bankr)
3. [Como Funcionam os Callbacks (Like & Buy)](#callbacks)
4. [Trading Executor - Execução de Trades](#trading-executor)
5. [OnChain Listener - Detectando Deploys](#onchain-listener)
6. [Adaptação para CLAWNCH](#adaptação-clawnch)
7. [Diferenças Chave](#diferenças-chave)
8. [Implementação Passo a Passo](#implementação)

---

## 🎯 VISÃO GERAL DO SISTEMA BANKR

### **Componentes Principais**

```
┌─────────────────────────────────────────────────────────────────┐
│                     SISTEMA BANKR COMPLETO                      │
└─────────────────────────────────────────────────────────────────┘

1. Twitter Parser           → Detecta tweets @bankrbot
2. Telegram Progressive     → Envia PRE-BANKR (antes deploy)
3. OnChain Listener         → Detecta TokenCreated event
4. Telegram Progressive     → Atualiza para DEPLOYED
5. Telegram Callback        → Lida com Like/Buy buttons
6. Trading Executor         → Executa compras on-chain
```

### **Workers Envolvidos**

| Worker | Responsabilidade | Input | Output |
|--------|------------------|-------|--------|
| **telegram_progressive** | Mensagens PRE-BANKR/DEPLOYED | `twitter:prebankr`, `twitter:deployed` | Telegram messages |
| **onchain_listener** | Escuta blockchain | WebSocket (Ankr) | `onchain:token`, `onchain:block` |
| **telegram_callback** | Botões Like/Buy | Telegram updates | `trading:buy_approved` |
| **trading_executor** | Executa trades | `onchain:token`, `onchain:block`, `trading:buy_approved` | On-chain swaps |

---

## 🔄 FLUXO COMPLETO PRE-BANKR → BANKR

### **FASE 1: PRE-BANKR (Antes do Deploy)**

```
1. Usuario tweeta: "@bankrbot deploy $MOON"
              ↓
2. Twitter Parser detecta (via API ou RSS)
              ↓
3. Publica evento: twitter:prebankr
              ↓
4. telegram_progressive recebe
              ↓
5. Envia mensagem Telegram:
   ━━━━━━━━━━━━━━━━━━
   🚀 PRE-BANKR
   
   $MOON by @usuario
   
   ⏳ Aguardando deploy...
   
   [Tweet] [From] [❤️ Like] [💰 BUY]
   ━━━━━━━━━━━━━━━━━━
              ↓
6. Mensagem fica "aberta" (salva message_id em Redis)
```

**Redis Keys Criados:**
```
progressive_msg:{tweet_id} → {
  "message_id": 12345,
  "chat_id": -100123456789,
  "tweet_id": "1234567890",
  "status": "PRE-BANKR",
  "token_symbol": "MOON"
}
```

### **FASE 2: BANKR/DEPLOYED (Após Deploy)**

```
1. Token é deployado on-chain
              ↓
2. onchain_listener detecta TokenCreated event
              ↓
3. Extrai do event:
   - token_address
   - token_symbol
   - pool_id
   - context → tweet_id (messageId)
              ↓
4. Publica evento: onchain:token
              ↓
5. telegram_progressive recebe onchain:token
              ↓
6. Busca progressive_msg:{tweet_id} no Redis
              ↓
7. EDITA a mensagem anterior (mesmo message_id):
   ━━━━━━━━━━━━━━━━━━
   ✅ DEPLOYED
   
   $MOON by @usuario
   Contract: 0xABC...
   
   🔥 ON-CHAIN!
   
   [Tweet] [From] [❤️ Like] [💰 BUY]
   ━━━━━━━━━━━━━━━━━━
              ↓
8. trading_executor também recebe onchain:token
              ↓
9. Busca pending_buys com tweet_id
              ↓
10. Para cada buy:
    - Se auto_buy ON → agenda execução
    - Se auto_buy OFF → pede confirmação
```

**Link Crítico:** `tweet_id` conecta tudo!

---

## 🔘 CALLBACKS - LIKE & BUY

### **Arquivo:** `workers/telegram_callback.py`

### **Como Funciona o LIKE**

```python
# Button criado:
{
    "text": "❤️ Like",
    "callback_data": "like:{tweet_id}"
}

# Quando clicado:
1. telegram_callback recebe update
2. Extrai tweet_id do callback_data
3. Verifica se já curtiu:
   - Se sim: remove like (unlike)
   - Se não: adiciona like
4. Atualiza contador no Redis
5. Edita botão para mostrar contagem: "❤️ 5"
```

**Redis:**
```
likes:{tweet_id} → SET de user_ids
  → SADD/SREM para adicionar/remover
  → SCARD para contar
```

### **Como Funciona o BUY**

```python
# Button criado:
{
    "text": "💰 BUY",
    "callback_data": "buy:{tweet_id}"
}

# Quando clicado:
1. telegram_callback recebe update
2. Extrai tweet_id do callback_data
3. Verifica config do usuário:
   - Tem wallet? → OK
   - Não tem? → Pede configuração
4. Cria PendingBuy IMEDIATAMENTE:
   {
       "user_id": 123,
       "tweet_id": "456",
       "status": "APPROVED",  // Aguardando token on-chain
       "eth_amount": 0.1,
       "block_offset": 6
   }
5. Salva no Redis:
   pending_buy:{user_id}:{tweet_id}
6. Publica evento: trading:buy_approved
7. Envia confirmação privada:
   ━━━━━━━━━━━━━━━━━━
   ✅ BUY REGISTRADO!
   
   Token: $MOON
   ETH: 0.1
   Bloco: +6 (16.7% fee)
   
   ⏳ Aguardando on-chain...
   
   [❌ Cancelar Buy]
   ━━━━━━━━━━━━━━━━━━
```

**Estados do PendingBuy:**
```
APPROVED         → Buy registrado, aguardando token on-chain
WAITING          → Token on-chain, aguardando bloco alvo
PENDING_CONFIRM  → Token on-chain, aguardando confirmação manual
EXECUTING        → Executando swap on-chain
DONE             → Swap executado com sucesso
FAILED           → Swap falhou
CANCELLED        → Usuário cancelou
```

---

## ⚡ TRADING EXECUTOR

### **Arquivo:** `workers/trading_executor.py`

### **Canais que Escuta**

```python
1. onchain:block       → Novos blocos
2. onchain:token       → Tokens deployados
3. trading:buy_approved → Novos buys registrados
```

### **Fluxo de Execução**

#### **1. Recebe `onchain:token`**

```python
async def _handle_token(event):
    # 1. Extrai dados
    tweet_id = event["tweet_id"]
    token_address = event["token_address"]
    block_number = event["block_number"]
    
    # 2. Busca pending buys para este tweet
    buys = await storage.get_pending_buys_by_tweet(tweet_id)
    
    # 3. Para cada buy com status APPROVED:
    for buy in buys:
        # 4. Calcula target block
        auction_expire_block = block_number + 2  # Auction dura 2 blocos
        target_block = auction_expire_block + buy.block_offset
        
        # 5. Link token com buy
        buy.token_address = token_address
        buy.target_block = target_block
        buy.status = "WAITING"
        await storage.save_pending_buy(buy)
        
        # 6. Checa auto-buy
        config = await storage.get_user_config(buy.user_id)
        
        if config.auto_buy:
            # AUTO: Notifica e agenda
            await notify_user(
                buy.user_id,
                "🚀 TOKEN ON-CHAIN! Compra agendada para bloco {target_block}"
            )
        else:
            # MANUAL: Pede confirmação
            buy.status = "PENDING_CONFIRM"
            await storage.save_pending_buy(buy)
            await notify_user_with_buttons(
                buy.user_id,
                "🔔 TOKEN ON-CHAIN! Deseja comprar?",
                buttons=[
                    {"text": "✅ SIM", "callback_data": f"buyconfirm:{tweet_id}"},
                    {"text": "❌ NÃO", "callback_data": f"buycancel:{tweet_id}"}
                ]
            )
```

#### **2. Recebe `onchain:block`**

```python
async def _handle_block(event):
    block_number = event["block_number"]
    
    # 1. Busca todos os buys com target_block = block_number
    ready_buys = await storage.get_buys_for_block(block_number)
    
    # 2. Executa todos em paralelo
    tasks = [execute_trade(buy) for buy in ready_buys]
    await asyncio.gather(*tasks)
```

#### **3. Executa Trade**

```python
async def execute_trade(buy: PendingBuy):
    # 1. Atualiza status
    buy.status = "EXECUTING"
    await storage.save_pending_buy(buy)
    
    # 2. Notifica usuário
    await notify_user(buy.user_id, "⚡ EXECUTANDO COMPRA...")
    
    # 3. Get user config (wallet private key)
    config = await storage.get_user_config(buy.user_id)
    
    # 4. Cria SwapExecutor
    executor = SwapExecutor(
        private_key=config.wallet_private_key,
        rpc_urls=[RPC1, RPC2, RPC3],  # Multi-RPC fire!
        max_priority_fee_gwei=config.gwei,
        max_fee_gwei=config.gwei,
    )
    
    # 5. Executa swap
    result = await executor.swap_eth_to_token(
        token_address=buy.token_address,
        amount_in_wei=int(buy.eth_amount * 10**18),
        slippage=config.slippage,
        pool_id=buy.pool_id,
    )
    
    # 6. Processa resultado
    if result.success:
        buy.status = "DONE"
        buy.tx_hash = result.tx_hash
        await storage.save_pending_buy(buy)
        
        await notify_user(
            buy.user_id,
            f"✅ COMPRA EXECUTADA!\n\nTX: {result.tx_hash}\n\n[Sigma] [GMGN] [VIP]"
        )
    else:
        buy.status = "FAILED"
        buy.error = result.error
        await storage.save_pending_buy(buy)
        
        await notify_user(buy.user_id, f"❌ FALHOU: {result.error}")
```

---

## 🔗 ONCHAIN LISTENER

### **Arquivo:** `workers/onchain_listener.py`

### **O que Faz**

1. **Conecta WebSocket** com Ankr RPC
2. **Subscribe em 2 coisas:**
   - `newHeads` → Novos blocos
   - `logs` → TokenCreated events do Clanker V4

### **Event TokenCreated**

```solidity
// Evento do contrato Clanker V4
event TokenCreated(
    address indexed token,           // topics[1]
    address msgSender,                // data[0]
    string tokenURI,                  // data[1]
    string name,                      // data[2]
    string symbol,                    // data[3]
    string image,                     // data[4]
    string context,                   // data[5] ← IMPORTANTE!
    int24 tickSpacing,                // data[6]
    address locker,                   // data[7]
    bytes32 poolId,                   // data[8]
    address positionManager,          // data[9]
    address vault,                    // data[10]
    address mevModule,                // data[11]
    uint256 tokenId,                  // data[12]
    address[] feeRecipients           // data[13]
);
```

### **Campo CONTEXT - A Chave de Tudo!**

```json
// context é um JSON string:
{
    "platform": "X",
    "messageId": "https://twitter.com/usuario/status/1234567890"
}
```

**Este é o LINK entre on-chain e tweet!**

### **Como Extrai tweet_id**

```python
# 1. Decode event data (eth_abi)
decoded = eth_abi.decode(TOKEN_CREATED_ABI, event_data)

# 2. Parse context
context = decoded[5]  # context é o índice 5
ctx_json = json.loads(context)
message_url = ctx_json["messageId"]

# 3. Extrai tweet_id da URL
# "https://twitter.com/usuario/status/1234567890"
tweet_id = message_url.split("/status/")[-1]  # "1234567890"

# 4. Publica evento com tweet_id!
event = {
    "token_address": decoded["token"],
    "token_symbol": decoded[3],
    "pool_id": decoded[8],
    "tweet_id": tweet_id,  # ← CONECTA com PRE-BANKR!
    "block_number": block_number,
}
await publish("onchain:token", event)
```

---

## 🦞 ADAPTAÇÃO PARA CLAWNCH

### **O que Muda?**

#### **PRE-CLAWNCH (Já Implementado!)**

✅ Já temos: `workers/clawnch_sync_worker.py`
- Poll Moltbook API
- Extrai token JSON do post
- Envia mensagem Telegram
- Buttons: Like e Buy com `wallet` address

#### **CLAWNCH (A Implementar)**

Similar ao DEPLOYED do BANKR, mas com diferenças importantes.

### **Diferenças Chave**

| Item | BANKR | CLAWNCH |
|------|-------|---------|
| **Fonte PRE** | Twitter @bankrbot | Moltbook API `/clawnch` |
| **ID Único** | `tweet_id` (Twitter) | `post_id` (Moltbook) |
| **Deploy Link** | `context.messageId` tem tweet URL | `!clawnch` tem `wallet` address no JSON |
| **Detecção Deploy** | Listen TokenCreated event | Listen TokenCreated + match `wallet` |
| **Callback Like** | `like:{tweet_id}` | `like_clawnch:{post_id}` |
| **Callback Buy** | `buy:{tweet_id}` | `buy_clawnch:{post_id}` |

---

## 🔧 IMPLEMENTAÇÃO PASSO A PASSO

### **FASE 1: Modificar OnChain Listener**

**Arquivo:** `workers/onchain_listener.py`

**Adicionar detecção de tokens CLAWNCH:**

```python
async def _handle_token_event(self, data):
    # ... código existente ...
    
    # Extrai creator wallet (msg_sender)
    msg_sender = decoded.get("msg_sender", "")
    
    # Verifica se é um token CLAWNCH
    # (tokens CLAWNCH têm o wallet do dono no JSON)
    clawnch_post = await self._check_clawnch_token(msg_sender, decoded["token"])
    
    if clawnch_post:
        # É um token CLAWNCH!
        await self._handle_clawnch_deploy(clawnch_post, decoded, event)
    
    # Publica evento normal (para BANKR)
    await self.publish(Channels.ONCHAIN_TOKEN, event)

async def _check_clawnch_token(self, creator_wallet, token_address):
    """
    Verifica se este token corresponde a um post CLAWNCH.
    
    Busca no Redis por pending clawnch posts onde:
    - wallet (do JSON !clawnch) corresponde ao creator_wallet OU
    - token_address corresponde (se já foi linkado)
    """
    # Busca todos os pending clawnch posts
    post_ids = await self.redis.smembers("clawnch_pending_posts")
    
    for post_id in post_ids:
        post_data = await self.redis.get(f"clawnch_post:{post_id}")
        if not post_data:
            continue
        
        post = json.loads(post_data)
        wallet_in_json = post.get("wallet", "").lower()
        
        # Match por wallet
        if wallet_in_json and wallet_in_json == creator_wallet.lower():
            return post
    
    return None

async def _handle_clawnch_deploy(self, clawnch_post, decoded, event):
    """Publica evento específico para CLAWNCH deployment."""
    clawnch_event = {
        "post_id": clawnch_post["post_id"],
        "author_name": clawnch_post["author_name"],
        "token_name": clawnch_post["name"],
        "token_symbol": clawnch_post["symbol"],
        "token_address": decoded["token"],
        "pool_id": decoded["pool_id"],
        "block_number": event["block_number"],
        "tx_hash": event["tx_hash"],
        "wallet_expected": clawnch_post["wallet"],
    }
    
    # Publica em canal separado
    await self.publish("onchain:clawnch", clawnch_event)
    
    self.logger.info(
        f"[CLAWNCH DEPLOYED] {clawnch_post['symbol']} "
        f"post={clawnch_post['post_id']}"
    )
```

### **FASE 2: Salvar Posts PRE-CLAWNCH com Wallet**

**Arquivo:** `workers/clawnch_sync_worker.py`

**Modificar `_process_token_launch`:**

```python
async def _process_token_launch(self, post: Dict, token_data: Dict):
    post_id = post["id"]
    
    # Salvar post com wallet para matching posterior
    pending_post = {
        "post_id": post_id,
        "author_name": post["author"]["name"],
        "name": token_data["name"],
        "symbol": token_data["symbol"],
        "wallet": token_data["wallet"],  # ← IMPORTANTE!
        "description": token_data.get("description", ""),
        "image": token_data.get("image", ""),
        "created_at": post["created_at"],
    }
    
    # Salvar no Redis
    await self.redis.set(
        f"clawnch_post:{post_id}",
        json.dumps(pending_post),
        ex=86400 * 7  # 7 dias
    )
    
    # Adicionar ao set de pending
    await self.redis.sadd("clawnch_pending_posts", post_id)
    
    # ... resto do código (enviar Telegram) ...
```

### **FASE 3: Criar Worker CLAWNCH (DEPLOYED)**

**Novo arquivo:** `workers/clawnch_deployed_worker.py`

```python
"""
CLAWNCH Deployed Worker

Escuta tokens CLAWNCH que foram deployados on-chain.
Similar ao telegram_progressive mas para CLAWNCH.

Input Channels:
- onchain:clawnch - Tokens CLAWNCH deployados

Output Channels:
- telegram:send - Notificações de deploy
"""

class ClawnchDeployedWorker(BaseWorker):
    WORKER_NAME = "clawnch-deployed"
    
    async def run(self):
        """Subscribe to clawnch deployments."""
        async for channel, message in self.redis.subscribe(
            "onchain:clawnch"
        ):
            await self._handle_clawnch_deploy(message)
    
    async def _handle_clawnch_deploy(self, event: Dict):
        """Handle CLAWNCH token deployment."""
        post_id = event["post_id"]
        token_symbol = event["token_symbol"]
        token_address = event["token_address"]
        pool_id = event["pool_id"]
        block_number = event["block_number"]
        
        self.logger.info(f"[CLAWNCH DEPLOY] {token_symbol} post={post_id}")
        
        # 1. Busca progressive message (se enviada por clawnch_sync)
        pm_data = await self.redis.get(f"clawnch_progressive_msg:{post_id}")
        
        if pm_data:
            # Editar mensagem existente
            pm = json.loads(pm_data)
            await self._edit_to_deployed(pm, event)
        else:
            # Enviar nova mensagem (caso não tenha PRE-CLAWNCH)
            await self._send_deployed_message(event)
        
        # 2. Link com pending buys
        await self._link_pending_buys(event)
        
        # 3. Remove do pending
        await self.redis.srem("clawnch_pending_posts", post_id)
    
    async def _edit_to_deployed(self, pm: Dict, event: Dict):
        """Edit PRE-CLAWNCH message to DEPLOYED."""
        message_id = pm["message_id"]
        chat_id = pm["chat_id"]
        post_id = event["post_id"]
        
        # Busca post data
        post_data = await self.redis.get(f"clawnch_post:{post_id}")
        post = json.loads(post_data) if post_data else {}
        
        # Busca agent data (MOLT)
        agent = await self.storage.get_molt_agent_by_name(post.get("author_name", ""))
        
        # Formata mensagem DEPLOYED
        message = f"""🦞 <b>✅ DEPLOYED!</b> 🦞

<b>{event['token_name']}</b> (${event['token_symbol']})

{post.get('description', '')}

━━━━━━━━━━━━━━━━━━
<b>FROM (Moltbook Agent)</b>
━━━━━━━━━━━━━━━━━━

🤖 Agent: <b>@{post.get('author_name', 'N/A')}</b>

━━━━━━━━━━━━━━━━━━
<b>OWNER (Twitter)</b>
━━━━━━━━━━━━━━━━━━
"""
        
        if agent:
            message += f"""
🐦 Twitter: <b>@{agent['x_handle']}</b>
{'✅ Verified' if agent.get('is_verified') else ''}
👥 Followers: {agent.get('followers', 0):,}
{'🔥 <b>KOL DETECTED</b>' if agent.get('passes_kol_filter') else ''}
"""
            vips = agent.get('vip_followers', [])[:3]
            if vips:
                message += f"VIP: {', '.join(f'@{v}' for v in vips)}\n"
        else:
            message += "\n<i>Owner not in MOLT cache</i>\n"
        
        message += f"""
━━━━━━━━━━━━━━━━━━

💎 Contract: <code>{event['token_address']}</code>
⛽ Pool ID: <code>{pool_id_short}</code>
📦 Block: <b>{event['block_number']}</b>

🔥 <b>NOW ON-CHAIN!</b>"""
        
        # Atualizar buttons
        buttons = self._create_deployed_buttons(post, event, agent)
        
        # Edit message
        await self._edit_telegram_message(
            chat_id, message_id, message, buttons
        )
        
        self.logger.info(f"[CLAWNCH] Edited to DEPLOYED: {event['token_symbol']}")
    
    def _create_deployed_buttons(self, post, event, agent):
        """Create buttons for DEPLOYED message."""
        post_id = post.get("post_id", "")
        token_address = event["token_address"]
        
        keyboard = []
        
        # Row 1: Molt profile + Twitter Owner
        row1 = [
            {"text": "Molt profile", "url": f"https://www.moltbook.com/u/{post.get('author_name', '')}"}
        ]
        if agent:
            row1.append({"text": "Twitter Owner", "url": f"https://twitter.com/{agent['x_handle']}"})
        keyboard.append(row1)
        
        # Row 2: Like + Buy (agora com token_address real!)
        row2 = [
            {"text": "❤️ Like", "callback_data": f"like_clawnch:{post_id}"},
            {"text": "💰 BUY", "callback_data": f"buy_clawnch:{post_id}"}  # ← Mesmo callback!
        ]
        keyboard.append(row2)
        
        return {"inline_keyboard": keyboard}
    
    async def _link_pending_buys(self, event: Dict):
        """Link pending buys with deployed token."""
        post_id = event["post_id"]
        
        # Busca pending buys para este post_id
        buys = await self.storage.get_pending_buys_by_clawnch_post(post_id)
        
        for buy in buys:
            # Calcular auction expire block
            auction_expire_block = event["block_number"] + 2
            target_block = auction_expire_block + buy.block_offset
            
            # Link token
            buy.token_address = event["token_address"]
            buy.pool_id = event["pool_id"]
            buy.target_block = target_block
            buy.status = "WAITING"  # ou "PENDING_CONFIRM" se auto_buy OFF
            
            await self.storage.save_pending_buy(buy)
            
            self.logger.info(
                f"[CLAWNCH BUY LINKED] @{buy.username} -> {event['token_symbol']} "
                f"target_block={target_block}"
            )
```

### **FASE 4: Modificar Telegram Callback**

**Arquivo:** `workers/telegram_callback.py`

**Adicionar handlers para CLAWNCH:**

```python
async def _handle_callback(self, bot_name, bot_token, callback):
    data = callback.get("data", "")
    
    # ... código existente ...
    
    # NOVOS HANDLERS CLAWNCH
    elif data.startswith("like_clawnch:"):
        await self._handle_like_clawnch(bot_token, callback_id, data, user_id, username, message_id, chat_id)
    
    elif data.startswith("buy_clawnch:"):
        await self._handle_buy_clawnch(bot_token, callback_id, data, user_id, username, chat_id)

async def _handle_like_clawnch(self, bot_token, callback_id, data, user_id, username, message_id, chat_id):
    """Handle like button for CLAWNCH posts."""
    post_id = data.split(":", 1)[1]
    
    # Similar ao _handle_like mas com clawnch_likes:{post_id}
    if await self.redis.sismember(f"clawnch_likes:{post_id}", user_id):
        await self.redis.srem(f"clawnch_likes:{post_id}", user_id)
        count = await self.redis.scard(f"clawnch_likes:{post_id}")
        await self._answer_callback(bot_token, callback_id, f"Unliked ({count})")
    else:
        await self.redis.sadd(f"clawnch_likes:{post_id}", user_id)
        count = await self.redis.scard(f"clawnch_likes:{post_id}")
        await self._answer_callback(bot_token, callback_id, f"Liked! ({count})")
    
    # Atualiza botão
    if message_id and chat_id:
        # ... atualizar keyboard com novo count ...

async def _handle_buy_clawnch(self, bot_token, callback_id, data, user_id, username, chat_id):
    """Handle buy button for CLAWNCH posts."""
    post_id = data.split(":", 1)[1]
    config = await self.storage.get_user_config(user_id, username)
    
    # Verifica config
    if not config.is_configured():
        await self._answer_callback(bot_token, callback_id)
        await self._send_message(
            bot_token, user_id,
            "⚠️ CONFIG INCOMPLETA\n\nConfigure wallet primeiro.",
            {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
        )
        return
    
    # Busca post data
    post_data = await self.redis.get(f"clawnch_post:{post_id}")
    if not post_data:
        await self._answer_callback(bot_token, callback_id, "Post not found")
        return
    
    post = json.loads(post_data)
    token_symbol = post.get("symbol", "Token")
    
    # Verifica se já tem buy pendente
    existing = await self.storage.get_pending_buy_by_clawnch(user_id, post_id)
    if existing:
        await self._answer_callback(bot_token, callback_id)
        await self._send_message(
            bot_token, user_id,
            "⚠️ Você já tem um BUY registrado para este token!"
        )
        return
    
    # Cria PendingBuy (similar ao BANKR mas com post_id)
    buy = PendingBuy(
        user_id=user_id,
        username=username,
        clawnch_post_id=post_id,  # ← Diferença: post_id ao invés de tweet_id
        token_symbol=token_symbol,
        eth_amount=config.eth_amount,
        block_offset=config.block_offset,
        status="APPROVED",  # Aguardando deploy
    )
    
    await self.storage.save_pending_buy(buy)
    
    # Notifica usuário
    fee = get_fee_for_block(config.block_offset)
    auto_status = "🟢 AUTO" if config.auto_buy else "🟡 MANUAL"
    
    await self._send_message(
        bot_token, user_id,
        f"""<b>✅ BUY REGISTRADO!</b>

Token: <b>{token_symbol}</b>

<b>Config:</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Modo: <b>{auto_status}</b>

⏳ Aguardando deploy on-chain...""",
        {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": f"buycancel_clawnch:{post_id}"}]]}
    )
    
    self.logger.info(f"[CLAWNCH BUY] @{username} -> {token_symbol}")
```

### **FASE 5: Storage - Métodos CLAWNCH**

**Arquivo:** `shared/storage.py`

**Adicionar métodos:**

```python
async def get_pending_buys_by_clawnch_post(self, post_id: str) -> List[PendingBuy]:
    """Get all pending buys for a CLAWNCH post."""
    # Busca no index
    buy_keys = await self.redis.smembers(f"clawnch_buys_by_post:{post_id}")
    
    buys = []
    for buy_key in buy_keys:
        # buy_key = "user_id:post_id"
        parts = buy_key.split(":", 1)
        if len(parts) == 2:
            user_id = int(parts[0])
            buy = await self.get_pending_buy_by_clawnch(user_id, post_id)
            if buy:
                buys.append(buy)
    
    return buys

async def get_pending_buy_by_clawnch(self, user_id: int, post_id: str) -> Optional[PendingBuy]:
    """Get pending buy by clawnch post_id."""
    key = f"pending_buy_clawnch:{user_id}:{post_id}"
    data = await self.redis.get(key)
    if data:
        return PendingBuy.from_dict(json.loads(data))
    return None

async def save_pending_buy_clawnch(self, buy: PendingBuy):
    """Save pending buy for CLAWNCH."""
    key = f"pending_buy_clawnch:{buy.user_id}:{buy.clawnch_post_id}"
    
    # Salvar buy
    await self.redis.set(key, json.dumps(buy.to_dict()), ex=86400 * 7)
    
    # Adicionar aos indexes
    await self.redis.sadd(f"clawnch_buys_by_user:{buy.user_id}", f"{buy.user_id}:{buy.clawnch_post_id}")
    await self.redis.sadd(f"clawnch_buys_by_post:{buy.clawnch_post_id}", f"{buy.user_id}:{buy.clawnch_post_id}")
    
    # Se status WAITING, adicionar ao index por target_block
    if buy.status == "WAITING" and buy.target_block:
        await self.redis.sadd(f"buys_for_block:{buy.target_block}", key)
```

---

## 📊 DIFERENÇAS CHAVE - RESUMO

### **BANKR**

```
ID: tweet_id (Twitter)
Link: context.messageId no TokenCreated event
Flow: Twitter → Redis → Listener → Match tweet_id
Callback: like:{tweet_id}, buy:{tweet_id}
```

### **CLAWNCH**

```
ID: post_id (Moltbook)
Link: wallet address do JSON !clawnch
Flow: Moltbook → Redis (save wallet) → Listener → Match wallet → Link post_id
Callback: like_clawnch:{post_id}, buy_clawnch:{post_id}
```

### **Por que usar `wallet` para matching?**

**Problema:**  
TokenCreated event não tem referência ao post do Moltbook (não existe `context.messageId`).

**Solução:**  
1. PRE-CLAWNCH: Salva o `wallet` do JSON `!clawnch` no Redis
2. DEPLOYED: Listener pega o `msg_sender` (creator) do event
3. Match: Compara `msg_sender` com os `wallet` salvos
4. Link: Encontra o `post_id` correspondente

**Exemplo:**
```
PRE-CLAWNCH:
  post_id: "abc123"
  wallet: "0x1234..." (do JSON !clawnch)
  
DEPLOYED:
  msg_sender: "0x1234..." (do TokenCreated event)
  
MATCH: 0x1234 == 0x1234 → É o mesmo token!
LINK: token_address → post_id "abc123"
```

---

## ✅ CHECKLIST DE IMPLEMENTAÇÃO

### **Arquivos a Criar**

- [ ] `workers/clawnch_deployed_worker.py` (novo worker DEPLOYED)

### **Arquivos a Modificar**

- [ ] `workers/onchain_listener.py`
  - [ ] Adicionar `_check_clawnch_token()`
  - [ ] Adicionar `_handle_clawnch_deploy()`
  - [ ] Publish em `onchain:clawnch` channel

- [ ] `workers/clawnch_sync_worker.py` (PRE-CLAWNCH)
  - [ ] Salvar `wallet` no Redis em `_process_token_launch()`
  - [ ] Adicionar ao set `clawnch_pending_posts`
  - [ ] Salvar `message_id` para edição futura

- [ ] `workers/telegram_callback.py`
  - [ ] Adicionar `_handle_like_clawnch()`
  - [ ] Adicionar `_handle_buy_clawnch()`
  - [ ] Adicionar `_handle_buycancel_clawnch()`

- [ ] `shared/storage.py`
  - [ ] Adicionar `get_pending_buys_by_clawnch_post()`
  - [ ] Adicionar `get_pending_buy_by_clawnch()`
  - [ ] Adicionar `save_pending_buy_clawnch()`

- [ ] `shared/models.py`
  - [ ] Adicionar `clawnch_post_id` field em `PendingBuy`

- [ ] `shared/pubsub.py`
  - [ ] Adicionar `ONCHAIN_CLAWNCH = "onchain:clawnch"` channel

- [ ] `docker-compose.yml`
  - [ ] Adicionar serviço `clawnch-deployed`

- [ ] `.env`
  - [ ] Confirmar `TELEGRAM_CHAT_ID_CLAWNCH` (diferente do PRE)

### **Redis Keys Novos**

```
# Pending posts (PRE-CLAWNCH)
clawnch_post:{post_id} → JSON com wallet, symbol, etc (TTL: 7 dias)
clawnch_pending_posts → SET de post_ids aguardando deploy

# Likes
clawnch_likes:{post_id} → SET de user_ids

# Pending buys
pending_buy_clawnch:{user_id}:{post_id} → JSON do PendingBuy
clawnch_buys_by_user:{user_id} → SET de "user_id:post_id"
clawnch_buys_by_post:{post_id} → SET de "user_id:post_id"

# Progressive messages (para edição)
clawnch_progressive_msg:{post_id} → JSON com message_id, chat_id
```

---

## 🎯 PRÓXIMOS PASSOS

1. **Implementar worker `clawnch_deployed_worker.py`**
   - Subscribe `onchain:clawnch`
   - Editar mensagens PRE → DEPLOYED
   - Link com pending buys

2. **Modificar `onchain_listener.py`**
   - Detectar tokens CLAWNCH por wallet matching
   - Publish em canal separado

3. **Atualizar `clawnch_sync_worker.py`**
   - Salvar wallet no Redis
   - Salvar message_id para edição

4. **Estender `telegram_callback.py`**
   - Handlers like_clawnch, buy_clawnch

5. **Adicionar métodos em `storage.py`**
   - CRUD para pending buys CLAWNCH

6. **Testar fluxo completo:**
   ```
   PRE-CLAWNCH → Like → Buy → DEPLOYED → Execução
   ```

---

## 📝 NOTAS FINAIS

### **Vantagens desta Arquitetura**

✅ **Reutiliza 80% do código BANKR**  
✅ **TradingExecutor funciona igual** (só precisa post_id → token_address)  
✅ **Callbacks separados** (like_clawnch, buy_clawnch) evitam conflitos  
✅ **Matching por wallet** é confiável (endereço único)  

### **Desafios**

⚠️ **Múltiplos posts com mesmo wallet?**  
Se usuário posta 2x com mesmo wallet, pode dar match errado.  
**Solução:** Limpar pending após 7 dias ou ao detectar deploy.

⚠️ **Wallet muda entre post e deploy?**  
Se usuário edita wallet após postar, matching falha.  
**Solução:** Não tem como prevenir, é limitação da plataforma.

⚠️ **Performance do matching?**  
Para cada TokenCreated, busca todos pending posts.  
**Solução:** Usar index `clawnch_wallet_to_post:{wallet}` para O(1) lookup.

---

**FIM DO GUIA DE ARQUITETURA**

*Documento criado em: 31/01/2026*  
*Última atualização: 31/01/2026 16:10 BRT*  
*Status: COMPLETO - Pronto para implementação*
