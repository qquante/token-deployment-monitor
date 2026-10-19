#!/usr/bin/env python3
import json
import sys
from collections import Counter
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

data = json.load(open('data/all_deployments.json', encoding='utf-8'))
target = '0xd9acd656a5f1b519c9e76a2a6092265a74186e58'
found = [d for d in data if d.get('msg_sender', '').lower() == target]
print(f'Found {len(found)} tokens from {target}')

# Count by interface
interfaces = Counter()
for d in found:
    interface = d.get('context_parsed', {}).get('interface', 'unknown')
    interfaces[interface] += 1

print("\nBreakdown by interface:")
for iface, count in interfaces.most_common():
    print(f"  {iface}: {count}")

# Show Bankr ones if any
bankr_ones = [d for d in found if d.get('context_parsed', {}).get('interface', '').lower() == 'bankr']
print(f"\nBankr interface tokens from this wallet: {len(bankr_ones)}")
for d in bankr_ones[:10]:
    print(f"  {d['token_name']} ({d['token_symbol']})")
