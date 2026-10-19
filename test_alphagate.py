import asyncio
from src.async_clients.alphagate import AsyncAlphaGateClient

async def test():
    client = AsyncAlphaGateClient()
    await client._ensure_session()
    
    handles = ['0xZhang_Crypto', 'morganlinton', 'SchizoToken', 'OnChainRev', 'clawsdotfun', 'Rogers200019']
    
    for h in handles:
        try:
            result = await client.get_child_data(h)
            if result:
                r = result.get("prev_usernames_count", 0)
                c = result.get("contracts_count", 0)
                sf = result.get("smart_followers", 0)
                print(f"@{h}: R={r} C={c} SF={sf}")
            else:
                print(f"@{h}: No data (None)")
        except Exception as e:
            print(f"@{h}: ERROR - {e}")
    
    await client.close()

asyncio.run(test())
