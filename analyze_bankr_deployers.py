#!/usr/bin/env python3
"""
Analyze ALL Bankr interface deployments to find all deployer wallets.
"""

import sys
import json
from pathlib import Path
from collections import Counter

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

project_root = Path(__file__).parent

# Load all deployments
with open(project_root / "data" / "all_deployments.json", "r", encoding="utf-8") as f:
    all_deployments = json.load(f)

print(f"Total deployments: {len(all_deployments)}")

# Filter for Bankr interface only
bankr_deployments = []
for dep in all_deployments:
    interface = dep.get("context_parsed", {}).get("interface", "")
    if interface.lower() == "bankr":
        bankr_deployments.append(dep)

print(f"Bankr interface deployments: {len(bankr_deployments)}")

# Count deployer wallets
deployer_counts = Counter()
for dep in bankr_deployments:
    sender = dep.get("msg_sender", "").lower()
    if sender:
        deployer_counts[sender] += 1

print(f"\n{'='*60}")
print("ALL BANKR INTERFACE DEPLOYER WALLETS")
print("="*60)

for wallet, count in deployer_counts.most_common():
    pct = (count / len(bankr_deployments)) * 100
    print(f"  {wallet}: {count:5d} ({pct:6.2f}%)")

# Show sample scam tokens (non-main deployer)
main_deployer = "0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9"
scam_tokens = []
for dep in bankr_deployments:
    sender = dep.get("msg_sender", "").lower()
    if sender and sender != main_deployer:
        scam_tokens.append({
            "token": dep.get("token_name", ""),
            "symbol": dep.get("token_symbol", ""),
            "address": dep.get("token_address", ""),
            "deployer": sender,
            "tx": dep.get("tx_hash", "")
        })

print(f"\n{'='*60}")
print(f"SCAM TOKENS (non-{main_deployer[:10]}...)")
print("="*60)
print(f"\nTotal scam tokens: {len(scam_tokens)}")

for t in scam_tokens[:30]:
    print(f"\n  Token: {t['token']} ({t['symbol']})")
    print(f"  Address: {t['address']}")
    print(f"  Deployer: {t['deployer']}")
    print(f"  TX: 0x{t['tx']}")
