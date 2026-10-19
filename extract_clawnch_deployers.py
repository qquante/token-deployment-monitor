"""
Extract token addresses from contract.json and cross-reference with all_deployments.json
to find deployer wallets for Clawnch ecosystem tokens
"""
import json
from collections import Counter

print("Loading contract.json (Clawnch API data)...")
with open("contract.json", "r", encoding="utf-8") as f:
    contract_data = json.load(f)

print("Loading all_deployments.json...")
with open("data/all_deployments.json", "r", encoding="utf-8") as f:
    deployments = json.load(f)

# Create lookup by token address
deployment_by_address = {}
for d in deployments:
    addr = d.get("token_address", "").lower()
    if addr:
        deployment_by_address[addr] = d

# Get all tokens from contract.json
all_tokens = contract_data.get("allTokens", [])
print(f"\nContract.json has {len(all_tokens)} tokens")

# Stats
deployers = Counter()
sources = Counter()
matched = 0
unmatched = []

for token in all_tokens:
    addr = token.get("address", "").lower()
    source = token.get("source", "unknown")
    sources[source] += 1
    
    # Look up in deployments
    if addr in deployment_by_address:
        d = deployment_by_address[addr]
        msg_sender = d.get("msg_sender", "").lower()
        deployers[msg_sender] += 1
        matched += 1
    else:
        unmatched.append({
            "address": addr,
            "name": token.get("name"),
            "symbol": token.get("symbol"),
            "source": source
        })

print(f"\n=== SOURCES ===")
for source, count in sources.most_common():
    print(f"  {source}: {count}")

print(f"\n=== MATCHED TOKENS ===")
print(f"Matched: {matched}")
print(f"Unmatched: {len(unmatched)}")

print(f"\n=== DEPLOYER WALLETS (from matched tokens) ===")
for deployer, count in deployers.most_common(20):
    pct = count / matched * 100 if matched > 0 else 0
    print(f"  {deployer}: {count} ({pct:.1f}%)")

if unmatched:
    print(f"\n=== SAMPLE UNMATCHED TOKENS ===")
    for t in unmatched[:5]:
        print(f"  {t['name']} (${t['symbol']}) - {t['source']}")
        print(f"    Address: {t['address']}")
