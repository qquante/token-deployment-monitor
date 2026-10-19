# Deploy Metrics - Documentação

## Resumo

Sistema de rastreamento de métricas de deploy para usuários do @bankrbot.

**Data do Backfill:** Janeiro 2026  
**Tweets Processados:** 30,019  
**Tempo de Processamento:** ~507 minutos (~8.5 horas)

---

## Métricas Coletadas

### 1. `deploys_made` (Deploys Feitos)
- **O que é:** Quantas vezes o usuário **PEDIU** um deploy de token
- **Quem recebe:** O usuário que fez a requisição (FROM)
- **Exemplo:** `@alice` pede `@bankrbot deploy token XYZ` → `@alice` ganha `deploys_made += 1`

### 2. `deploys_received` (Deploys Recebidos)
- **O que é:** Quantas vezes o usuário foi designado para **RECEBER FEES** de um deploy
- **Quem recebe:**
  - Se o requester especificou `fees to @bob` → `@bob` ganha `deploys_received += 1`
  - Se NÃO especificou recipient → o próprio requester ganha (fees vão para ele por padrão)
- **Exemplo 1:** `@alice` pede `deploy XYZ fees to @bob` → `@bob` ganha `deploys_received += 1`
- **Exemplo 2:** `@alice` pede `deploy XYZ` (sem fees) → `@alice` ganha `deploys_received += 1`

---

## Estatísticas Finais do Backfill

```
============================================================
PROCESSING COMPLETE
============================================================
Processed this run: 30,019
Deploys found (total): 3,277
Deploys with fees: 681
Fee recipients found: 3,344
Time elapsed: 30403.9s (506.7m)

--- Current Stats ---
Unique users: 1,515
Total deploys_made: 3,277
Total deploys_received: 3,344
============================================================
```

### Interpretação:
| Métrica | Valor | Significado |
|---------|-------|-------------|
| Deploys found | 3,277 | Total de tokens deployados |
| Deploys with fees | 681 | Deploys que especificaram fee recipient explícito |
| Fee recipients found | 3,344 | Total de incrementos em `deploys_received` |
| Unique users | 1,515 | Usuários únicos com pelo menos 1 deploy/receive |
| Total deploys_made | 3,277 | Soma de todos `deploys_made` |
| Total deploys_received | 3,344 | Soma de todos `deploys_received` |

**Nota:** `deploys_received (3,344) > deploys_made (3,277)` porque alguns deploys tinham múltiplos fee recipients especificados.

---

## Database Schema

### Tabela: `user_stats`
```sql
CREATE TABLE IF NOT EXISTS user_stats (
    username TEXT PRIMARY KEY,
    deploys_made INTEGER DEFAULT 0,      -- Quantos deploys o usuário pediu
    deploys_received INTEGER DEFAULT 0,  -- Quantas vezes recebeu fees
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
)
```

### Métodos Disponíveis (models.py)
```python
# Incrementar deploys_made
db.increment_deploys_made(username: str, count: int = 1) -> int

# Incrementar deploys_received  
db.increment_deploys_received(username: str, count: int = 1) -> int

# Consultar valores
db.get_deploys_made(username: str) -> int
db.get_deploys_received(username: str) -> int

# Listar todos os usuários
db.get_all_user_stats(order_by: str = "deploys_received") -> List[Dict]
```

---

## Integração com Telegram (PENDENTE)

### Formato Proposto para Mensagens

Quando exibir informações de um usuário no Telegram, adicionar as métricas:

```
👤 @username
├─ Followers: 10,234
├─ Deploys: 15        ← deploys_made (quantos tokens deployou)
└─ Received: 23       ← deploys_received (quantas vezes recebeu fees)
```

### Exemplo de Mensagem Completa

```
🚀 NEW DEPLOY

Token: $EXAMPLE
Contract: 0x1234...5678

👤 FROM: @alice
├─ Followers: 5,000
├─ Deploys: 12
└─ Received: 8

👤 TO (Fee Recipient): @bob
├─ Followers: 15,000
├─ Deploys: 3
└─ Received: 45
```

### Regras de Exibição

1. **`Deploys` (deploys_made):** Sempre exibir se > 0
2. **`Received` (deploys_received):** Sempre exibir se > 0
3. **Se ambos forem 0:** Não exibir a seção de métricas (usuário novo)

### Código para Buscar Métricas

```python
from src.database.models import Database

db = Database()

# Buscar métricas de um usuário
username = "alice"
deploys_made = db.get_deploys_made(username)
deploys_received = db.get_deploys_received(username)

# Formatar para exibição
if deploys_made > 0 or deploys_received > 0:
    metrics_text = f"├─ Deploys: {deploys_made}\n└─ Received: {deploys_received}"
```

---

## Arquivos Relacionados

| Arquivo | Descrição |
|---------|-----------|
| `src/database/models.py` | Schema e métodos do banco de dados |
| `scripts/backfill_bankr_tweets.py` | Script para baixar tweets do @bankrbot |
| `scripts/process_backfill.py` | Script para processar tweets com Agent |
| `scripts/check_status.py` | Script para verificar status do backfill |
| `data/bankr_monitor.db` | Banco de dados SQLite |

---

## Scripts Úteis

### Verificar Top Usuários por Deploys Made
```bash
python scripts/process_backfill.py --top 20
```

### Verificar Status do Backfill
```bash
python scripts/check_status.py
```

### Consulta SQL Direta
```sql
-- Top 10 por deploys feitos
SELECT username, deploys_made, deploys_received 
FROM user_stats 
ORDER BY deploys_made DESC 
LIMIT 10;

-- Top 10 por deploys recebidos
SELECT username, deploys_made, deploys_received 
FROM user_stats 
ORDER BY deploys_received DESC 
LIMIT 10;

-- Usuários que mais receberam fees (mas não deployaram muito)
SELECT username, deploys_made, deploys_received,
       (deploys_received - deploys_made) as net_received
FROM user_stats 
WHERE deploys_received > deploys_made
ORDER BY net_received DESC 
LIMIT 10;
```

---

## Próximos Passos

1. [ ] Integrar métricas nas mensagens do Telegram
2. [ ] Atualizar métricas em tempo real no `websocket_async.py`
3. [ ] Adicionar cache para evitar queries repetidas
4. [ ] Criar comando `/stats @username` no Telegram bot

---

## Notas Técnicas

- O backfill processa apenas tweets que contêm "deployed" ou "contract address"
- Tweets originais deletados/protegidos são marcados como `NO_ORIGINAL`
- A classificação usa o Agent (Google Gemini) para determinar se é um deploy
- Rate limit: ~0.2s entre chamadas ao Agent (~5 tweets/segundo)
