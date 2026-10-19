#!/usr/bin/env python3
"""
Verify DEPLOYED tweets on-chain to identify real BANKR deployer wallets.

For each contract address from Twitter DEPLOYED tweets:
1. Get the token creation TX
2. Extract the msg_sender (deployer wallet)
3. Build statistics of which wallets are legitimate BANKR deployers
"""

import sys
import json
import asyncio
from pathlib import Path
from collections import Counter
import os

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from web3 import Web3

# RPC endpoint
RPC_URL = os.getenv("ANKR_RPC_HTTP", "https://rpc.ankr.com/base")

# Clanker V4 contract
CLANKER_V4 = "0x1346737aab67626e79cb42cfcffce11a3ffcf9aa"

# TokenCreated event signature
TOKEN_CREATED_TOPIC = "0x" + Web3.keccak(text="TokenCreated(address,address,string,string,string,string,int24,address,bytes32,address,address,address,uint256,address[])").hex()


def get_token_creation_tx(w3: Web3, contract_address: str):
    """Get the creation TX for a token by looking at its first transaction."""
    contract_address = contract_address.lower()
    
    # Method 1: Get first log for this token from Clanker contract
    # We'll search for TokenCreated events that mention this token
    try:
        # Get the code at address to verify it's a contract
        code = w3.eth.get_code(Web3.to_checksum_address(contract_address))
        if code == b'' or code == b'0x':
            return None, None, "Not a contract"
        
        # Get earliest block for this contract (expensive, skip for now)
        # Instead, we'll use the all_deployments.json if available
        return None, None, "Need TX lookup"
        
    except Exception as e:
        return None, None, str(e)


def main():
    print("=" * 60)
    print("VERIFYING DEPLOYED TWEETS ON-CHAIN")
    print("=" * 60)
    
    # Load deployed tweets
    deployed_file = project_root / "data" / "deployed_tweets_all.json"
    with open(deployed_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    tweets = data["tweets"]
    print(f"\n[LOADED] {len(tweets)} DEPLOYED tweets")
    
    # Extract unique contract addresses
    contracts = []
    for tweet in tweets:
        addr = tweet.get("contract_address", "")
        if addr and addr.startswith("0x"):
            contracts.append(addr.lower())
    
    unique_contracts = list(set(contracts))
    print(f"[CONTRACTS] {len(unique_contracts)} unique addresses")
    
    # Load all_deployments.json which has msg_sender
    all_deployments_file = project_root / "data" / "all_deployments.json"
    
    print(f"\n[LOADING] {all_deployments_file}")
    with open(all_deployments_file, "r", encoding="utf-8") as f:
        all_deployments = json.load(f)
    
    print(f"[LOADED] {len(all_deployments)} on-chain deployments")
    
    # Build index: token_address -> deployment data
    deployment_index = {}
    for dep in all_deployments:
        addr = dep.get("token_address", "").lower()
        if addr:
            deployment_index[addr] = dep
    
    print(f"[INDEX] {len(deployment_index)} indexed by token address")
    
    # Match Twitter DEPLOYED tweets with on-chain data
    matched = []
    unmatched = []
    deployer_wallets = Counter()
    
    for tweet in tweets:
        contract = tweet.get("contract_address", "").lower()
        if not contract:
            continue
        
        dep = deployment_index.get(contract)
        if dep:
            msg_sender = dep.get("msg_sender", "").lower()
            interface = dep.get("context_parsed", {}).get("interface", "")
            
            matched.append({
                "contract": contract,
                "msg_sender": msg_sender,
                "interface": interface,
                "token_name": tweet.get("token_name", ""),
                "requester": tweet.get("requester_username", ""),
                "tx_hash": dep.get("tx_hash", "")
            })
            
            if msg_sender:
                deployer_wallets[msg_sender] += 1
        else:
            unmatched.append(contract)
    
    print(f"\n[MATCHED] {len(matched)} tweets matched with on-chain data")
    print(f"[UNMATCHED] {len(unmatched)} tweets not found in on-chain data")
    
    # Analyze deployer wallets
    print(f"\n{'='*60}")
    print("DEPLOYER WALLET STATISTICS")
    print("="*60)
    
    print(f"\nTotal unique deployer wallets: {len(deployer_wallets)}")
    print(f"\nTop deployer wallets (by deployment count):\n")
    
    for wallet, count in deployer_wallets.most_common(20):
        pct = (count / len(matched)) * 100
        print(f"  {wallet}: {count:4d} deployments ({pct:5.2f}%)")
    
    # Identify the main BANKR deployer wallets (those with high counts)
    print(f"\n{'='*60}")
    print("LIKELY LEGITIMATE BANKR DEPLOYERS")
    print("="*60)
    
    # Wallets with > 10 deployments are likely legitimate
    legitimate = [(w, c) for w, c in deployer_wallets.most_common() if c >= 10]
    
    print(f"\nWallets with 10+ deployments (likely legitimate):\n")
    for wallet, count in legitimate:
        print(f"  {wallet}: {count} deployments")
    
    # Check for scam wallets (low count, copying Bankr interface)
    print(f"\n{'='*60}")
    print("POTENTIAL SCAM DEPLOYERS")
    print("="*60)
    
    # Group by interface
    interface_by_wallet = {}
    for m in matched:
        wallet = m["msg_sender"]
        interface = m["interface"]
        if wallet not in interface_by_wallet:
            interface_by_wallet[wallet] = set()
        interface_by_wallet[wallet].add(interface)
    
    # Low count wallets using "Bankr" interface are suspicious
    suspicious = []
    for wallet, count in deployer_wallets.items():
        if count < 5:  # Low deployment count
            interfaces = interface_by_wallet.get(wallet, set())
            if "Bankr" in interfaces:
                suspicious.append((wallet, count, interfaces))
    
    print(f"\nLow-count wallets (<5) using 'Bankr' interface:\n")
    for wallet, count, interfaces in suspicious[:20]:
        print(f"  {wallet}: {count} deployments, interfaces: {interfaces}")
    
    # Save results
    output = {
        "total_tweets": len(tweets),
        "matched": len(matched),
        "unmatched": len(unmatched),
        "deployer_wallet_stats": dict(deployer_wallets.most_common()),
        "legitimate_deployers": [w for w, c in legitimate],
        "suspicious_deployers": [w for w, c, i in suspicious],
        "matched_details": matched[:100]  # First 100 for inspection
    }
    
    output_path = project_root / "data" / "deployer_wallet_analysis.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n[SAVED] {output_path}")
    print("\n[DONE]")


if __name__ == "__main__":
    main()
