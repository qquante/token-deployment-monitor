"""
Debug script to analyze BANKR token deployments from TX hashes.
Full forensic analysis of each transaction.
"""
import os
import asyncio
import aiohttp
import json
from eth_abi import decode

RPC_URL = os.environ.get("ANKR_RPC_HTTP", "")
if not RPC_URL:
    raise RuntimeError("Set ANKR_RPC_HTTP locally before running this integration diagnostic")

# Clanker V4 contract
CLANKER_V4 = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9".lower()
TOKEN_CREATED_TOPIC = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"

# Transactions to analyze
TRANSACTIONS = [
    "0xa219d9ef4000e81132f9c511c7acd27e9b18a639d07f5864390166cc72450204",
    "0xffdc07d07aad74cd98d1ff592faba945ef3ecd4fa68174be43c5ed31ceb27c77",
    "0xaf77681c35ac4471ef2d34fe6cef83bda6f74119c91fb469a524dd727cc8ec5a",
    "0xa41ac23b45cbf94354581b9f2dbf670c5ae5b3b6d904bdc2a019c63b729d915a",
    "0x96becba47249922f6c1ddcf4a8a9f7ad736865acb162ecb235f7c301cb2d23dd",
    "0xb809b62df86248a2d7a2a768ab86ec37eab1e34006c38a1362a0a3ab6d2f3d57",
    "0x05b88cb77ce52c57b8d03fab4c31ff3a9e4359553b7f84618e3996325bca22ca",
    "0xc0c4ef78e95d55f26ed51f88b4d3fdf9a3b5b0ac5b33ba27ab6ee0410b45eaf8",
    "0xfcdd21c0398c0d01b53e153e88df92154da12c687c26829194cd075f3fab2e75",
    "0x78d00cd34dedf6b827bb017baf94bad3e9f3fe5c48bfab3cedcb945d0db1e1e3",
    "0x2565ef5a5fa45a1cabd3659c572a3771ff70124e4bb4fbcd98391577698c44fb",
    "0x5cd023a7ead2686c9aff9b3d8e488e25e5701deb20d6c4849a8813286f434adc",
    "0x186dcb11775616e3465b0f28a8d5ac81c3519ab8a1836786806fd258b1102a8e",
]

async def rpc_call(session, method, params):
    """Make RPC call"""
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    }
    async with session.post(RPC_URL, json=payload) as resp:
        data = await resp.json()
        if "error" in data:
            print(f"   RPC ERROR: {data['error']}")
        return data.get("result")

async def get_transaction(session, tx_hash):
    """Get full transaction details"""
    return await rpc_call(session, "eth_getTransactionByHash", [tx_hash])

async def get_transaction_receipt(session, tx_hash):
    """Get transaction receipt"""
    return await rpc_call(session, "eth_getTransactionReceipt", [tx_hash])

def decode_token_created_event(log):
    """Decode the TokenCreated event data"""
    topics = log["topics"]
    data = bytes.fromhex(log["data"][2:])
    
    # Indexed fields from topics
    token_address = "0x" + topics[1][-40:]
    token_admin = "0x" + topics[2][-40:]
    
    # ABI types for non-indexed fields
    abi_types = [
        "address",    # msg_sender (0)
        "string",     # token_image (1)
        "string",     # token_name (2)
        "string",     # token_symbol (3)
        "string",     # token_metadata (4)
        "string",     # token_context (5)
        "int24",      # starting_tick (6)
        "address",    # pool_hook (7)
        "bytes32",    # pool_id (8)
        "address",    # paired_token (9)
        "address",    # locker (10)
        "address",    # mev_module (11)
        "uint256",    # extensions_supply (12)
        "address[]",  # extensions (13)
    ]
    
    decoded = decode(abi_types, data)
    
    return {
        "token_address": token_address,
        "token_admin": token_admin,
        "msg_sender": decoded[0],
        "token_image": decoded[1],
        "token_name": decoded[2],
        "token_symbol": decoded[3],
        "token_metadata": decoded[4],
        "token_context": decoded[5],
        "starting_tick": decoded[6],
        "pool_hook": decoded[7],
        "pool_id": decoded[8].hex(),
        "paired_token": decoded[9],
        "locker": decoded[10],
        "mev_module": decoded[11],
        "extensions_supply": decoded[12],
        "extensions": decoded[13],
    }

async def analyze_transaction(session, tx_hash, idx):
    """Compact analysis of a transaction"""
    print(f"\n{'='*100}")
    print(f"[TX_{idx}] {tx_hash}")
    print(f"{'='*100}")
    
    # Get transaction
    tx = await get_transaction(session, tx_hash)
    if not tx:
        print(f"[ERROR] Transaction not found!")
        return None
    
    # Get receipt
    receipt = await get_transaction_receipt(session, tx_hash)
    if not receipt:
        print(f"[ERROR] Receipt not found!")
        return None
    
    print(f"\n[TRANSACTION]")
    print(f"   from (EOA):   {tx['from']}")
    print(f"   to (contract): {tx['to']}")
    print(f"   block:         {int(tx['blockNumber'], 16)}")
    
    # Find TokenCreated event
    token_created_log = None
    for log in receipt['logs']:
        if log['address'].lower() == CLANKER_V4 and log['topics'][0] == TOKEN_CREATED_TOPIC:
            token_created_log = log
            break
    
    if not token_created_log:
        print(f"\n[WARNING] No TokenCreated event found!")
        # Check what contract was called
        print(f"   This tx called: {tx['to']}")
        return None
    
    # Decode event
    decoded = decode_token_created_event(token_created_log)
    
    print(f"\n[TOKEN]")
    print(f"   address:       {decoded['token_address']}")
    print(f"   name:          {decoded['token_name']}")
    print(f"   symbol:        {decoded['token_symbol']}")
    
    print(f"\n[DEPLOYER INFO]")
    print(f"   tx.from:       {tx['from']}")
    print(f"   msg_sender:    {decoded['msg_sender']}")
    print(f"   token_admin:   {decoded['token_admin']}")
    
    print(f"\n[CONTEXT]")
    try:
        ctx = json.loads(decoded['token_context'])
        print(f"   interface:     {ctx.get('interface', 'N/A')}")
        print(f"   platform:      {ctx.get('platform', 'N/A')}")
        print(f"   messageId:     {ctx.get('messageId', 'N/A')}")
    except:
        print(f"   RAW: {decoded['token_context']}")
    
    print(f"\n[CONTRACTS]")
    print(f"   pool_hook:     {decoded['pool_hook']}")
    print(f"   locker:        {decoded['locker']}")
    print(f"   mev_module:    {decoded['mev_module']}")
    
    return {
        "idx": idx,
        "tx_hash": tx_hash,
        "tx_from": tx['from'],
        "tx_to": tx['to'],
        "decoded": decoded,
    }

async def main():
    async with aiohttp.ClientSession() as session:
        results = []
        
        for idx, tx_hash in enumerate(TRANSACTIONS, 1):
            result = await analyze_transaction(session, tx_hash, idx)
            results.append(result)
        
        # Summary
        print(f"\n\n{'='*100}")
        print(f"[DEPLOYER WALLET SUMMARY]")
        print(f"{'='*100}")
        
        wallet_counts = {}
        for r in results:
            if r:
                wallet = r['tx_from'].lower()
                if wallet not in wallet_counts:
                    wallet_counts[wallet] = []
                wallet_counts[wallet].append(r)
        
        print(f"\nUnique deployer wallets found: {len(wallet_counts)}")
        
        for wallet, txs in wallet_counts.items():
            print(f"\n{wallet}")
            print(f"   Count: {len(txs)}")
            for t in txs:
                try:
                    ctx = json.loads(t['decoded']['token_context'])
                    iface = ctx.get('interface', '?')
                except:
                    iface = '?'
                print(f"   - TX_{t['idx']}: {t['decoded']['token_symbol']} ({iface})")

if __name__ == "__main__":
    asyncio.run(main())
