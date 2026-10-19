"""
Analyze Clawnch deployer wallets from all_deployments.json
"""
import json
from collections import Counter

# Load data
print("Loading all_deployments.json...")
with open("data/all_deployments.json", "r", encoding="utf-8") as f:
    deployments = json.load(f)

print(f"Total deployments: {len(deployments)}")

# Separate by interface
interface_counts = Counter()
clawnch_tokens = []

for d in deployments:
    ctx = d.get("context_parsed", {})
    interface = ctx.get("interface", "")
    interface_counts[interface] += 1
    
    if interface == "Clawnch":
        clawnch_tokens.append(d)

print("\n=== INTERFACES ===")
for interface, count in interface_counts.most_common():
    print(f"  {interface}: {count}")

print(f"\n=== CLAWNCH TOKENS ===")
print(f"Total: {len(clawnch_tokens)}")

if clawnch_tokens:
    # Analyze deployers
    deployers = Counter()
    platforms = Counter()
    
    for t in clawnch_tokens:
        msg_sender = t.get("msg_sender", "").lower()
        ctx = t.get("context_parsed", {})
        platform = ctx.get("platform", "unknown")
        
        deployers[msg_sender] += 1
        platforms[platform] += 1
    
    print("\n=== CLAWNCH PLATFORMS ===")
    for platform, count in platforms.most_common():
        print(f"  {platform}: {count}")
    
    print("\n=== CLAWNCH DEPLOYERS ===")
    for deployer, count in deployers.most_common(20):
        print(f"  {deployer}: {count}")
    
    # Show sample tokens
    print("\n=== SAMPLE CLAWNCH TOKENS ===")
    for t in clawnch_tokens[:5]:
        print(f"\n  Token: {t.get('token_name')} (${t.get('token_symbol')})")
        print(f"  Deployer: {t.get('msg_sender')}")
        print(f"  Block: {t.get('block_number')}")
        ctx = t.get("context_parsed", {})
        print(f"  Platform: {ctx.get('platform')}")
        print(f"  Agent: {ctx.get('id', 'N/A')}")
else:
    print("No Clawnch tokens found!")
    
    # Check for similar interfaces
    print("\nLooking for similar interfaces...")
    for interface in interface_counts.keys():
        if "claw" in interface.lower() or "molt" in interface.lower():
            print(f"  Found: {interface} ({interface_counts[interface]} tokens)")
