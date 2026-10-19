# Forensic Analysis Report: Bankr Token Deployments

**Report Date:** January 28, 2026  
**Blockchain:** Base (L2 on Ethereum)  
**Analysis Period:** ~100,000 blocks (~2-3 days)  
**Block Range:** 41,313,190 → 41,413,167

---

## Executive Summary

This report documents the forensic analysis of token deployments on the Base blockchain, specifically focusing on tokens deployed through the Bankr interface via the Clanker v4.0.0 smart contract.

### Key Findings

| Metric | Value |
|--------|-------|
| Total TokenCreated Events | 7,784 |
| Bankr Deployments | 3,643 (46.8%) |
| Other Interface Deployments | 4,141 (53.2%) |
| Unique Bankr Pool IDs | 3,643 (100% unique) |
| Twitter URLs Extracted | 958 |
| Token Images | 544 |

---

## 1. Smart Contract Architecture

### 1.1 Primary Contracts

The Clanker ecosystem operates through a set of interconnected smart contracts on Base:

| Contract | Address | Role |
|----------|---------|------|
| **Clanker v4.0.0** | `0xE85A59c628F7d27878ACeB4bf3b35733630083a9` | Main token deployment factory |
| **LP Locker Fee Conversion v4.0.0** | `0x63D2DfEA64b3433F4071A98665bcD7Ca14d93496` | Liquidity pool locker |
| **Hook Static Fee V2 v4.1.0** | `0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC` | Uniswap V4 pool hook |
| **Sniper Auction V2 v4.1.0** | `0xebB25BB797D82CB78E1bc70406b13233c0854413` | MEV protection module |

### 1.2 Uniswap V4 Integration

| Contract | Address |
|----------|---------|
| Pool Manager | `0x498581fF718922c3f8e6A244956aF099B2652b2b` |
| Position Manager | `0x7C5f5A4bBd8fD63184577525326123B519429bDc` |
| Permit2 | `0x000000000022D473030F116dDEE9F6B43aC78BA3` |
| WETH (Base) | `0x4200000000000000000000000000000000000006` |

### 1.3 Deployment Flow

```
User Request (Twitter/Farcaster/XMTP)
         ↓
    Bankr Bot
         ↓
  deployToken() call
         ↓
   Clanker v4.0.0
         ↓
   ┌─────────────────┐
   │ Token Creation  │
   │ Pool Creation   │
   │ LP Lock         │
   │ MEV Protection  │
   └─────────────────┘
         ↓
   TokenCreated Event
```

---

## 2. Event Signature Analysis

### 2.1 TokenCreated Event

**Event Topic (keccak256):**
```
0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67
```

**Solidity Signature:**
```solidity
event TokenCreated(
    address msgSender,           // Deployer EOA/contract
    address indexed tokenAddress, // New token address
    address indexed tokenAdmin,   // Token admin address
    string tokenImage,           // Image URL
    string tokenName,            // Token name
    string tokenSymbol,          // Token symbol
    string tokenMetadata,        // JSON metadata
    string tokenContext,         // JSON context (interface info)
    int24 startingTick,          // Uniswap pool starting tick
    address poolHook,            // Pool hook contract
    bytes32 poolId,              // Uniswap pool ID
    address pairedToken,         // Paired token (WETH)
    address locker,              // LP locker contract
    address mevModule,           // MEV protection module
    uint256 extensionsSupply,    // Supply for extensions
    address[] extensions         // Extension contracts
);
```

### 2.2 ABI Decoding

**Indexed Parameters (Topics):**
- `topics[0]` = Event signature hash
- `topics[1]` = `tokenAddress` (rightmost 20 bytes)
- `topics[2]` = `tokenAdmin` (rightmost 20 bytes)

**Non-Indexed Parameters (Data):**
```
['address', 'string', 'string', 'string', 'string', 'string', 
 'int24', 'address', 'bytes32', 'address', 'address', 'address', 
 'uint256', 'address[]']
```

---

## 3. Bankr Identification

### 3.1 Interface Detection

Bankr deployments are identified by the `tokenContext` JSON field:

```json
{
    "interface": "Bankr",
    "platform": "farcaster" | "X" | "x",
    "messageId": "https://twitter.com/..." | "bankr deployment",
    "id": "886870" | "unique_id"
}
```

**Detection Rule:**
```python
context = json.loads(token_context)
is_bankr = context.get('interface') == 'Bankr'
```

### 3.2 Metadata Patterns

```json
{
    "description": "Deployed using @bankrbot on X" | 
                   "Deployed through the Bankr Terminal @ bankr.bot" |
                   "Deployed using Bankr on XMTP",
    "socialMediaUrls": [],
    "auditUrls": []
}
```

---

## 4. Statistical Analysis

### 4.1 Platform Distribution

| Platform | Count | Percentage |
|----------|-------|------------|
| farcaster | 2,656 | 72.9% |
| X | 884 | 24.3% |
| x (lowercase) | 96 | 2.6% |
| Bankr | 4 | 0.1% |
| unknown/empty | 3 | 0.1% |

### 4.2 Description Distribution

| Description | Count |
|-------------|-------|
| "Deployed through the Bankr Terminal @ bankr.bot" | 1,682 |
| "Deployed using @bankrbot on X" | 972 |
| "Deployed using Bankr on XMTP" | 893 |
| "Deployed using Bankr on Farcaster" | 43 |
| "Deployed using Bankr" | 39 |
| (empty or custom) | 14 |

### 4.3 Deployer Addresses

| Address | Count | Role |
|---------|-------|------|
| `0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9` | 3,522 | **Main Bankr Deployer** |
| `0x10904ad5228820a81ece2661f40dbd987469fccb` | 49 | Secondary Deployer |
| `0x88075f206d78434c1b8c6b694581a09895f58ca9` | 27 | X Bot Deployer |
| Others | 45 | Various |

### 4.4 ID Analysis

| ID | Count | Context |
|----|-------|---------|
| `886870` | 2,648 | **Generic Farcaster/XMTP ID** |
| `746124968` | 78 | X Bot batch ID |
| (empty) | 32 | Missing |
| Various Twitter IDs | 885 | Unique per tweet |

**Total Unique IDs:** 472

### 4.5 Token Admin Patterns

| Pattern | Count |
|---------|-------|
| Unique admins | 1,485 |
| Dead address (`0x...dead`) | 349 (renounced) |
| Top admin address | 229 |

---

## 5. Other Interfaces (Non-Bankr)

| Interface | Count | Percentage |
|-----------|-------|------------|
| clanker.world | 3,402 | 82.2% |
| Clank.fun | 282 | 6.8% |
| clanker | 128 | 3.1% |
| brownie | 77 | 1.9% |
| SDK | 64 | 1.5% |
| Streamm | 38 | 0.9% |
| Wanted | 31 | 0.7% |
| Clanker SDK | 18 | 0.4% |
| Clanker Telegram Mini App | 13 | 0.3% |
| DilbertBot | 13 | 0.3% |
| Others | 74 | 1.8% |

---

## 6. Technical Constants

### 6.1 Invariant Parameters

All Bankr deployments share these constant values:

| Parameter | Value |
|-----------|-------|
| `starting_tick` | -230400 |
| `paired_token` | `0x4200000000000000000000000000000000000006` (WETH) |
| `locker` | `0x63d2dfea64b3433f4071a98665bcd7ca14d93496` |
| `mev_module` | `0xebb25bb797d82cb78e1bc70406b13233c0854413` |

### 6.2 Pool Hook Distribution

| Hook Address | Count | Percentage |
|--------------|-------|------------|
| `0xb429d62f8f3bffb98cdb9569533ea23bf0ba28cc` | 3,633 | 99.7% |
| `0xd60d6b218116cfd801e28f78d011a203d2b068cc` | 10 | 0.3% |

### 6.3 Extensions

| Metric | Value |
|--------|-------|
| With extensions | 296 (8.1%) |
| Without extensions | 3,347 (91.9%) |

**Extension Addresses:**

| Address | Count |
|---------|-------|
| `0x8e845ead15737bf71904a30bddd3aee76d6adf6c` | 296 |
| `0xf652b3610d75d81871bf96db50825d9af28391e0` | 4 |
| `0x1331f0788f9c08c8f38d52c7a1152250a9de00be` | 1 |

---

## 7. Data Extraction Summary

### 7.1 Twitter/X URLs

- **Total Extracted:** 958 unique URLs
- **Format:** `https://twitter.com/{username}/status/{tweet_id}`
- **Use Case:** Link deployments to source tweets

### 7.2 Token Images

- **Total:** 544 (14.9% of Bankr deployments)
- **Sources:** Twitter media, IPFS, custom CDNs
- **Format:** Direct URLs in `token_image` field

### 7.3 Social Media URLs (Metadata)

| Platform | With URL |
|----------|----------|
| website | 9 |
| x | 6 |
| source | 3 |
| reddit | 1 |
| Others | 4 |

---

## 8. Data Integrity Verification

### 8.1 Validation Results

| Check | Status |
|-------|--------|
| All required fields present | ✅ PASS |
| Address format (0x + 40 hex) | ✅ PASS |
| TX hash format (64 hex) | ✅ PASS |
| Pool IDs unique | ✅ PASS (3,643/3,643) |
| JSON parsing | ✅ PASS |

### 8.2 Cross-Reference

Sample verification against BaseScan for transaction:
`0x7218891e2ddbc248d0b6c646c5a01775922c8438bcf556e8445ac5d8a2c20d49`

| Field | BaseScan | Our Decode | Match |
|-------|----------|------------|-------|
| Token Address | `0x4bB79800...cb07` | `0x4bb79800...cb07` | ✅ |
| Token Name | DroppingBeans | DroppingBeans | ✅ |
| Token Symbol | Beans | Beans | ✅ |
| Platform | X | X | ✅ |
| Message ID | twitter.com/kingtuak/... | twitter.com/kingtuak/... | ✅ |

---

## 9. Query Methodology

### 9.1 RPC Query

```python
logs = w3.eth.get_logs({
    'fromBlock': from_block,
    'toBlock': to_block,
    'address': '0xE85A59c628F7d27878ACeB4bf3b35733630083a9',
    'topics': ['0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67']
})
```

### 9.2 Pagination Strategy

- **Chunk Size:** 2,000 blocks
- **Rate Limit:** 0.1s between requests
- **Error Handling:** Automatic chunk reduction on "range too large" errors

---

## 10. Conclusions

### 10.1 Key Observations

1. **Bankr Market Share:** 46.8% of all Clanker deployments use the Bankr interface
2. **Platform Dominance:** Farcaster accounts for 72.9% of Bankr deployments
3. **Centralized Deployment:** 96.7% of Bankr tokens deployed by single address
4. **Standardized Parameters:** All deployments use identical pool/hook configuration

### 10.2 Recommendations

1. Monitor main deployer address for real-time tracking
2. Use `interface` field for reliable Bankr identification
3. Parse `messageId` for source attribution
4. Track `token_admin` for ownership analysis

---

## Appendix A: File Outputs

| File | Records | Description |
|------|---------|-------------|
| `bankr_deployments.json` | 3,643 | Complete Bankr deployment data |
| `summary.json` | 1 | Statistical summary |
| `twitter_urls.json` | 958 | Extracted Twitter URLs |
| `token_images.json` | 544 | Token image URLs |

## Appendix B: Sample Records

### B.1 Farcaster Deployment

```json
{
    "token_address": "0xcd1fe718f95a7bbcd36026472d7159e276035b07",
    "token_name": "Clawd BankrBot",
    "token_symbol": "CLAWDBNKRBOT",
    "context_parsed": {
        "interface": "Bankr",
        "platform": "farcaster",
        "messageId": "bankr deployment",
        "id": "886870"
    },
    "metadata_parsed": {
        "description": "Deployed through the Bankr Terminal @ bankr.bot"
    }
}
```

### B.2 X/Twitter Deployment

```json
{
    "token_address": "0x4bb79800c4405e059c3a2bbf73c614584119cb07",
    "token_name": "DroppingBeans",
    "token_symbol": "Beans",
    "context_parsed": {
        "interface": "Bankr",
        "platform": "X",
        "messageId": "https://twitter.com/kingtuak/status/2016534180608495697",
        "id": "1407322472676921348"
    },
    "metadata_parsed": {
        "description": "Deployed using @bankrbot on X"
    }
}
```

---

*Report generated by Bankr Monitor v1.0.0*
