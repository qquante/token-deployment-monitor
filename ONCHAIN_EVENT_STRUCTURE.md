# 🔗 ESTRUTURA ON-CHAIN - TokenCreated Event

**Data:** 31 de Janeiro de 2026  
**Objetivo:** Documentar EXATAMENTE como extrair dados do evento TokenCreated do Clanker V4

---

## 📋 ÍNDICE

1. [Estrutura do Evento TokenCreated](#estrutura-evento)
2. [Topics vs Data](#topics-vs-data)
3. [Como Decodificar](#decodificação)
4. [Slots Disponíveis](#slots-disponíveis)
5. [Context - Campo Crítico para BANKR](#context)
6. [msg_sender - Campo Crítico para CLAWNCH](#msg-sender)
7. [Código de Exemplo](#código-exemplo)

---

## 🎯 ESTRUTURA DO EVENTO TokenCreated

### **Contrato Clanker V4**

```
Address: 0xE85A59c628F7d27878ACeB4bf3b35733630083a9
Network: Base (Chain ID: 8453)
```

### **Event Signature**

```solidity
event TokenCreated(
    address indexed token,           // INDEXED → topics[1]
    address msgSender,                // data[0]
    string tokenURI,                  // data[1]
    string name,                      // data[2]
    string symbol,                    // data[3]
    string image,                     // data[4]
    string context,                   // data[5] ← CRITICAL FOR BANKR!
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

**Event Topic (Signature Hash):**
```
0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67
```

---

## 📊 TOPICS vs DATA

### **Como Ethereum Events Funcionam**

Eventos Ethereum têm 2 partes:

1. **TOPICS** (indexed fields)
   - Rápido para filtrar
   - Máximo 4 topics por evento
   - Primeiro topic = event signature hash
   - Campos `indexed` vão para topics

2. **DATA** (non-indexed fields)
   - Campos normais (não indexed)
   - Codificados como ABI
   - Precisa decodificar manualmente

### **Estrutura do TokenCreated Event**

```
┌──────────────────────────────────────────────────────────┐
│                     EVENT LOG                            │
├──────────────────────────────────────────────────────────┤
│ address: 0xE85A59c628F7d27878ACeB4bf3b35733630083a9      │  ← Clanker V4 contract
├──────────────────────────────────────────────────────────┤
│ TOPICS:                                                  │
│   [0] = 0x9299d1d1a88d8e1abc...  ← Event signature      │
│   [1] = 0x000...ABC123           ← Token address (indexed) │
│   [2] = 0x000...DEF456           ← Token admin (indexed)   │
├──────────────────────────────────────────────────────────┤
│ DATA: (ABI encoded)                                      │
│   0: address msgSender                                   │
│   1: string tokenURI                                     │
│   2: string name                                         │
│   3: string symbol                                       │
│   4: string image                                        │
│   5: string context    ← TWITTER LINK PARA BANKR!       │
│   6: int24 tickSpacing                                   │
│   7: address locker                                      │
│   8: bytes32 poolId                                      │
│   9: address positionManager                             │
│  10: address vault                                       │
│  11: address mevModule                                   │
│  12: uint256 tokenId                                     │
│  13: address[] feeRecipients                             │
└──────────────────────────────────────────────────────────┘
```

---

## 🔓 COMO DECODIFICAR

### **Passo 1: Extrair Token Address dos Topics**

```python
# Topics structure:
# topics[0] = event signature hash
# topics[1] = token address (indexed, 32 bytes padded)
# topics[2] = token admin (indexed)

def extract_token_address(topics):
    """
    Token address está em topics[1].
    É padded para 32 bytes, pegamos os últimos 20 bytes (40 hex chars).
    """
    if len(topics) < 2:
        return None
    
    # topics[1] = "0x000000000000000000000000abc123..." (32 bytes)
    # Pegar últimos 40 chars (20 bytes = 1 address)
    token_address = "0x" + topics[1][-40:]
    
    return token_address

# Exemplo:
# topics[1] = "0x000000000000000000000000e85a59c628f7d27878aceb4bf3b35733630083a9"
# token_address = "0xe85a59c628f7d27878aceb4bf3b35733630083a9"
```

### **Passo 2: Decodificar DATA com eth_abi**

```python
import eth_abi

# ABI types for data portion
TOKEN_CREATED_ABI = [
    'address',    # 0: msgSender
    'string',     # 1: tokenURI
    'string',     # 2: name
    'string',     # 3: symbol
    'string',     # 4: image
    'string',     # 5: context (JSON with platform, messageId)
    'int24',      # 6: tickSpacing
    'address',    # 7: locker
    'bytes32',    # 8: poolId
    'address',    # 9: positionManager  
    'address',    # 10: vault
    'address',    # 11: mevModule
    'uint256',    # 12: tokenId
    'address[]',  # 13: feeRecipients
]

def decode_token_created_data(data_hex):
    """
    Decodifica o campo DATA do evento.
    
    Args:
        data_hex: string hex do data field (ex: "0x00000...")
    
    Returns:
        tuple com todos os campos decodificados
    """
    # Remove "0x" prefix
    if data_hex.startswith("0x"):
        data_hex = data_hex[2:]
    
    # Convert to bytes
    data_bytes = bytes.fromhex(data_hex)
    
    # Decode usando eth_abi
    decoded = eth_abi.decode(TOKEN_CREATED_ABI, data_bytes)
    
    return decoded

# Exemplo de uso:
# decoded = decode_token_created_data(log["data"])
# 
# decoded[0]  = msgSender (address)
# decoded[1]  = tokenURI (string)
# decoded[2]  = name (string) - "MoonToken"
# decoded[3]  = symbol (string) - "MOON"
# decoded[4]  = image (string) - "https://..."
# decoded[5]  = context (string) - '{"platform":"X","messageId":"https://twitter.com/..."}'
# decoded[6]  = tickSpacing (int) - 60
# decoded[7]  = locker (address)
# decoded[8]  = poolId (bytes32) - b'\x12\x34...'
# decoded[9]  = positionManager (address)
# decoded[10] = vault (address)
# decoded[11] = mevModule (address)
# decoded[12] = tokenId (uint256)
# decoded[13] = feeRecipients (list of addresses)
```

### **Passo 3: Montar Objeto Completo**

```python
def parse_token_created_event(log):
    """
    Parse completo do evento TokenCreated.
    
    Args:
        log: dict com campos 'topics' e 'data' do event
    
    Returns:
        dict com todos os campos extraídos
    """
    topics = log.get("topics", [])
    data_hex = log.get("data", "0x")
    
    # Extrair token address dos topics
    token_address = "0x" + topics[1][-40:]
    
    # Decodificar data
    decoded = decode_token_created_data(data_hex)
    
    # Montar objeto
    return {
        # From topics
        "token_address": token_address,
        
        # From data
        "msg_sender": decoded[0],           # Criador do token
        "token_uri": decoded[1],            # URI metadata
        "name": decoded[2],                 # Nome do token
        "symbol": decoded[3],               # Símbolo ($MOON)
        "image": decoded[4],                # URL da imagem
        "context": decoded[5],              # JSON com platform e messageId
        "tick_spacing": decoded[6],         # Tick spacing do pool
        "locker": decoded[7],               # Address do locker
        "pool_id": "0x" + decoded[8].hex(), # Pool ID (bytes32 -> hex)
        "position_manager": decoded[9],     # Position manager
        "vault": decoded[10],               # Vault address
        "mev_module": decoded[11],          # MEV module
        "token_id": decoded[12],            # NFT token ID
        "fee_recipients": decoded[13],      # Lista de fee recipients
    }
```

---

## 🎰 SLOTS DISPONÍVEIS - TODOS OS CAMPOS

### **RESUMO COMPLETO**

| # | Campo | Tipo | Source | Uso |
|---|-------|------|--------|-----|
| - | **token_address** | `address` | topics[1] | ✅ **Token address** |
| 0 | **msg_sender** | `address` | data[0] | ✅ **Criador** (CRÍTICO CLAWNCH!) |
| 1 | token_uri | `string` | data[1] | ⚪ Metadata URI |
| 2 | **name** | `string` | data[2] | ✅ **Nome do token** |
| 3 | **symbol** | `string` | data[3] | ✅ **Símbolo ($MOON)** |
| 4 | **image** | `string` | data[4] | ✅ **URL da imagem** |
| 5 | **context** | `string` | data[5] | ✅ **JSON com tweet link (BANKR)** |
| 6 | tick_spacing | `int24` | data[6] | ⚪ Pool config |
| 7 | locker | `address` | data[7] | ⚪ Locker contract |
| 8 | **pool_id** | `bytes32` | data[8] | ✅ **Uniswap V4 pool ID** |
| 9 | position_manager | `address` | data[9] | ⚪ Position manager |
| 10 | vault | `address` | data[10] | ⚪ Vault contract |
| 11 | **mev_module** | `address` | data[11] | ✅ **MEV module** (auction) |
| 12 | token_id | `uint256` | data[12] | ⚪ NFT token ID |
| 13 | fee_recipients | `address[]` | data[13] | ⚪ Fee recipients |

### **Campos CRÍTICOS para o Sistema**

#### **Para AMBOS (BANKR e CLAWNCH):**
- ✅ `token_address` (topics[1]) - Address do token deployado
- ✅ `symbol` (data[3]) - Símbolo para mostrar ao usuário
- ✅ `name` (data[2]) - Nome completo do token
- ✅ `pool_id` (data[8]) - ID do pool Uniswap V4 (para swaps!)
- ✅ `image` (data[4]) - Imagem para mostrar na mensagem

#### **Para BANKR:**
- ✅ `context` (data[5]) - **CAMPO CHAVE!** Contém messageId do Twitter

#### **Para CLAWNCH:**
- ✅ `msg_sender` (data[0]) - **CAMPO CHAVE!** Address do criador (matching com wallet!)

---

## 🔑 CONTEXT - Campo Crítico para BANKR

### **Estrutura do Campo context**

```python
# context é um JSON string:
context_json = decoded[5]

# Exemplo:
context_json = '{"platform":"X","messageId":"https://twitter.com/usuario/status/1234567890"}'
```

### **Parse do Context**

```python
import json

def parse_context(context_string):
    """
    Parse o campo context para extrair tweet info.
    
    Args:
        context_string: JSON string do context
    
    Returns:
        dict com platform, messageId, tweet_id, username
    """
    if not context_string:
        return None
    
    try:
        ctx = json.loads(context_string)
        
        platform = ctx.get("platform", "unknown")  # "X" (Twitter)
        message_url = ctx.get("messageId", "")     # Full tweet URL
        
        # Extrair tweet_id da URL
        # Format: "https://twitter.com/usuario/status/1234567890"
        tweet_id = ""
        username = ""
        
        if "/status/" in message_url:
            # Split por /status/ e pegar a parte final
            tweet_id = message_url.split("/status/")[-1].split("?")[0]
            
            # Extrair username
            # Split antes do /status/
            before_status = message_url.split("/status/")[0]
            parts = before_status.split("/")
            if parts:
                username = parts[-1]  # Último elemento antes de /status/
        
        return {
            "platform": platform,
            "message_url": message_url,
            "tweet_id": tweet_id,
            "username": username,
        }
    
    except Exception as e:
        print(f"Failed to parse context: {e}")
        return None

# Exemplo:
# context = '{"platform":"X","messageId":"https://twitter.com/vitalik/status/1234567890"}'
# 
# result = parse_context(context)
# {
#     "platform": "X",
#     "message_url": "https://twitter.com/vitalik/status/1234567890",
#     "tweet_id": "1234567890",
#     "username": "vitalik"
# }
```

### **Como BANKR Usa o Context**

```python
async def handle_token_created_bankr(event_log):
    """Handler para tokens BANKR (via Twitter)."""
    
    # 1. Decode event
    parsed = parse_token_created_event(event_log)
    
    # 2. Parse context
    ctx = parse_context(parsed["context"])
    
    if not ctx or not ctx["tweet_id"]:
        # Não é um token BANKR (sem tweet_id)
        return
    
    tweet_id = ctx["tweet_id"]
    
    # 3. Buscar mensagem PRE-BANKR usando tweet_id
    progressive_msg = await redis.get(f"progressive_msg:{tweet_id}")
    
    if progressive_msg:
        # 4. Editar mensagem de PRE-BANKR para DEPLOYED
        msg = json.loads(progressive_msg)
        await edit_telegram_message(
            chat_id=msg["chat_id"],
            message_id=msg["message_id"],
            text=format_deployed_message(parsed),
        )
    
    # 5. Link com pending buys
    buys = await storage.get_pending_buys_by_tweet(tweet_id)
    for buy in buys:
        await link_token_to_buy(buy, parsed["token_address"], parsed["pool_id"])
```

---

## 🦞 msg_sender - Campo Crítico para CLAWNCH

### **O que é msg_sender?**

```python
# msg_sender = decoded[0]
# Tipo: address (0x...)
# 
# É o endereço que CHAMOU a função de criar o token.
# Para CLAWNCH, este é o WALLET DO DONO que criou o token!
```

### **Por que é Importante para CLAWNCH?**

No Moltbook `/clawnch`, o post contém um JSON:

```json
{
  "name": "MoonToken",
  "symbol": "MOON",
  "wallet": "0x1234567890abcdef...",  // ← ESTE É O msg_sender!
  "description": "...",
  "image": "..."
}
```

**O `wallet` do JSON = `msg_sender` do evento on-chain!**

### **Como CLAWNCH Usa o msg_sender**

```python
async def handle_token_created_clawnch(event_log):
    """Handler para tokens CLAWNCH (via Moltbook)."""
    
    # 1. Decode event
    parsed = parse_token_created_event(event_log)
    
    # 2. Extrair msg_sender (criador)
    creator_wallet = parsed["msg_sender"].lower()
    
    # 3. Buscar posts CLAWNCH pendentes por wallet matching
    post = await find_clawnch_post_by_wallet(creator_wallet)
    
    if not post:
        # Não é um token CLAWNCH conhecido
        return
    
    post_id = post["post_id"]
    
    # 4. Buscar mensagem PRE-CLAWNCH usando post_id
    progressive_msg = await redis.get(f"clawnch_progressive_msg:{post_id}")
    
    if progressive_msg:
        # 5. Editar mensagem de PRE-CLAWNCH para DEPLOYED
        msg = json.loads(progressive_msg)
        await edit_telegram_message(
            chat_id=msg["chat_id"],
            message_id=msg["message_id"],
            text=format_clawnch_deployed_message(parsed, post),
        )
    
    # 6. Link com pending buys
    buys = await storage.get_pending_buys_by_clawnch_post(post_id)
    for buy in buys:
        await link_token_to_buy(buy, parsed["token_address"], parsed["pool_id"])

async def find_clawnch_post_by_wallet(creator_wallet):
    """
    Busca post CLAWNCH por wallet matching.
    
    Args:
        creator_wallet: address do msg_sender (0x...)
    
    Returns:
        dict com post data ou None
    """
    # Buscar todos os posts pendentes
    post_ids = await redis.smembers("clawnch_pending_posts")
    
    for post_id in post_ids:
        post_data = await redis.get(f"clawnch_post:{post_id}")
        if not post_data:
            continue
        
        post = json.loads(post_data)
        wallet_in_json = post.get("wallet", "").lower()
        
        # Match por wallet
        if wallet_in_json == creator_wallet:
            return post
    
    return None
```

---

## 💻 CÓDIGO DE EXEMPLO - IMPLEMENTAÇÃO COMPLETA

### **Worker: onchain_listener.py**

```python
"""
OnChain Listener Worker

Escuta TokenCreated events e publica em canais separados:
- onchain:token (BANKR - tem tweet_id no context)
- onchain:clawnch (CLAWNCH - match por msg_sender/wallet)
"""

import json
import eth_abi
from typing import Dict, Any, Optional

class OnchainListenerWorker(BaseWorker):
    
    # Event signature
    TOKEN_CREATED_TOPIC = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"
    
    # ABI for decoding
    TOKEN_CREATED_ABI = [
        'address',    # 0: msgSender
        'string',     # 1: tokenURI
        'string',     # 2: name
        'string',     # 3: symbol
        'string',     # 4: image
        'string',     # 5: context
        'int24',      # 6: tickSpacing
        'address',    # 7: locker
        'bytes32',    # 8: poolId
        'address',    # 9: positionManager  
        'address',    # 10: vault
        'address',    # 11: mevModule
        'uint256',    # 12: tokenId
        'address[]',  # 13: feeRecipients
    ]
    
    async def _handle_token_event(self, ws_data: Dict[str, Any]):
        """Process TokenCreated event."""
        result = ws_data.get("params", {}).get("result", {})
        if not result:
            return
        
        try:
            # Decode event
            decoded = self._decode_token_created(result)
            if not decoded:
                return
            
            tx_hash = result.get("transactionHash", "")
            block_hex = result.get("blockNumber", "0x0")
            block_number = int(block_hex, 16)
            
            # ====================================================
            # CHECK 1: É um token BANKR? (tem tweet_id no context)
            # ====================================================
            context_data = self._parse_context(decoded.get("context", ""))
            
            if context_data and context_data.get("tweet_id"):
                # É BANKR!
                await self._handle_bankr_token(
                    decoded, context_data, block_number, tx_hash
                )
            
            # ====================================================
            # CHECK 2: É um token CLAWNCH? (match por wallet)
            # ====================================================
            clawnch_post = await self._check_clawnch_token(
                decoded.get("msg_sender", ""),
                decoded.get("token", "")
            )
            
            if clawnch_post:
                # É CLAWNCH!
                await self._handle_clawnch_token(
                    decoded, clawnch_post, block_number, tx_hash
                )
            
            # ====================================================
            # SEMPRE publica evento genérico também
            # ====================================================
            event = {
                "token_address": decoded["token"],
                "token_name": decoded["name"],
                "token_symbol": decoded["symbol"],
                "pool_id": decoded["pool_id"],
                "block_number": block_number,
                "tx_hash": tx_hash,
            }
            await self.publish("onchain:token", event)
            
        except Exception as e:
            self.logger.error(f"Token event error: {e}", exc_info=True)
    
    def _decode_token_created(self, log: Dict) -> Optional[Dict]:
        """Decode TokenCreated event."""
        try:
            topics = log.get("topics", [])
            data_hex = log.get("data", "0x")
            
            if len(topics) < 2:
                return None
            
            # Token address from topics[1]
            token_address = "0x" + topics[1][-40:]
            
            # Decode data portion
            if data_hex.startswith("0x"):
                data_hex = data_hex[2:]
            
            data_bytes = bytes.fromhex(data_hex)
            decoded = eth_abi.decode(self.TOKEN_CREATED_ABI, data_bytes)
            
            return {
                "token": token_address,
                "msg_sender": decoded[0],      # ← CRITICAL FOR CLAWNCH!
                "token_uri": decoded[1],
                "name": decoded[2],
                "symbol": decoded[3],
                "image": decoded[4],
                "context": decoded[5],         # ← CRITICAL FOR BANKR!
                "tick_spacing": decoded[6],
                "locker": decoded[7],
                "pool_id": "0x" + decoded[8].hex(),
                "position_manager": decoded[9],
                "vault": decoded[10],
                "mev_module": decoded[11],
                "token_id": decoded[12],
                "fee_recipients": decoded[13],
            }
        
        except Exception as e:
            self.logger.error(f"Decode error: {e}")
            return None
    
    def _parse_context(self, context_string: str) -> Optional[Dict]:
        """Parse context field to extract tweet info."""
        if not context_string:
            return None
        
        try:
            ctx = json.loads(context_string)
            message_url = ctx.get("messageId", "")
            
            tweet_id = ""
            username = ""
            
            if "/status/" in message_url:
                tweet_id = message_url.split("/status/")[-1].split("?")[0]
                before_status = message_url.split("/status/")[0]
                parts = before_status.split("/")
                if parts:
                    username = parts[-1]
            
            return {
                "platform": ctx.get("platform", "unknown"),
                "message_url": message_url,
                "tweet_id": tweet_id,
                "username": username,
            }
        
        except:
            return None
    
    async def _check_clawnch_token(
        self, 
        msg_sender: str, 
        token_address: str
    ) -> Optional[Dict]:
        """
        Check if this token matches a pending CLAWNCH post.
        
        Matches by comparing msg_sender with wallet from !clawnch JSON.
        """
        if not msg_sender:
            return None
        
        creator_wallet = msg_sender.lower()
        
        # Get all pending clawnch posts
        post_ids = await self.redis.smembers("clawnch_pending_posts")
        
        for post_id in post_ids:
            post_data = await self.redis.get(f"clawnch_post:{post_id}")
            if not post_data:
                continue
            
            post = json.loads(post_data)
            wallet_in_json = post.get("wallet", "").lower()
            
            # Match by wallet
            if wallet_in_json and wallet_in_json == creator_wallet:
                self.logger.info(
                    f"[CLAWNCH MATCH] Wallet {creator_wallet[:10]}... "
                    f"matches post {post_id}"
                )
                return post
        
        return None
    
    async def _handle_bankr_token(
        self, 
        decoded: Dict, 
        context_data: Dict, 
        block_number: int, 
        tx_hash: str
    ):
        """Handle BANKR token deployment."""
        tweet_id = context_data["tweet_id"]
        
        self.logger.info(
            f"[BANKR TOKEN] {decoded['symbol']} "
            f"tweet={tweet_id} block={block_number}"
        )
        
        # Publish BANKR-specific event
        event = {
            "token_address": decoded["token"],
            "token_name": decoded["name"],
            "token_symbol": decoded["symbol"],
            "pool_id": decoded["pool_id"],
            "tweet_id": tweet_id,
            "tweet_username": context_data["username"],
            "block_number": block_number,
            "tx_hash": tx_hash,
            "image": decoded.get("image", ""),
        }
        
        await self.publish("onchain:token", event)
    
    async def _handle_clawnch_token(
        self, 
        decoded: Dict, 
        clawnch_post: Dict, 
        block_number: int, 
        tx_hash: str
    ):
        """Handle CLAWNCH token deployment."""
        post_id = clawnch_post["post_id"]
        
        self.logger.info(
            f"[CLAWNCH TOKEN] {decoded['symbol']} "
            f"post={post_id} block={block_number}"
        )
        
        # Publish CLAWNCH-specific event
        event = {
            "post_id": post_id,
            "author_name": clawnch_post.get("author_name", ""),
            "token_address": decoded["token"],
            "token_name": decoded["name"],
            "token_symbol": decoded["symbol"],
            "pool_id": decoded["pool_id"],
            "block_number": block_number,
            "tx_hash": tx_hash,
            "image": decoded.get("image", ""),
            "wallet_expected": clawnch_post.get("wallet", ""),
            "wallet_actual": decoded["msg_sender"],
        }
        
        await self.publish("onchain:clawnch", event)
        
        # Remove from pending
        await self.redis.srem("clawnch_pending_posts", post_id)
```

---

## 📊 COMPARAÇÃO: BANKR vs CLAWNCH

| Aspecto | BANKR | CLAWNCH |
|---------|-------|---------|
| **Campo usado** | `context` (data[5]) | `msg_sender` (data[0]) |
| **Tipo** | `string` (JSON) | `address` |
| **Contém** | Tweet URL com status ID | Wallet address do criador |
| **Matching** | Direto: extrai tweet_id | Indireto: compara com wallet salvo |
| **Confiabilidade** | ✅ Alta (URL única) | ⚠️ Média (wallet pode ser reusado) |
| **Canal Pub/Sub** | `onchain:token` | `onchain:clawnch` |

---

## 🎯 RESUMO EXECUTIVO

### **SLOTS CRÍTICOS ON-CHAIN**

```
Para BANKR:
├─ topics[1]  → token_address
├─ data[2]    → name
├─ data[3]    → symbol
├─ data[5]    → context (JSON com tweet_id) ✅ CHAVE!
└─ data[8]    → pool_id

Para CLAWNCH:
├─ topics[1]  → token_address
├─ data[0]    → msg_sender (wallet do criador) ✅ CHAVE!
├─ data[2]    → name
├─ data[3]    → symbol
└─ data[8]    → pool_id
```

### **FLOW COMPLETO**

```
1. WebSocket recebe TokenCreated event
         ↓
2. Decode topics[1] → token_address
         ↓
3. Decode data com eth_abi → 14 campos
         ↓
4. Parse context (data[5]) → tweet_id (BANKR)
   Parse msg_sender (data[0]) → wallet (CLAWNCH)
         ↓
5. Publica em canais:
   - onchain:token (genérico + BANKR)
   - onchain:clawnch (CLAWNCH específico)
         ↓
6. Workers downstream processam:
   - telegram_progressive (BANKR)
   - clawnch_deployed_worker (CLAWNCH)
   - trading_executor (ambos)
```

---

**FIM DA DOCUMENTAÇÃO ON-CHAIN**

*Documento criado em: 31/01/2026*  
*Última atualização: 31/01/2026 16:45 BRT*  
*Status: COMPLETO - Pronto para implementação*
