"""
Export all Redis data to JSON for backup/migration.
"""
import asyncio
import json
from datetime import datetime
from shared.redis_client import RedisClient


async def export_all():
    """Export all Redis data to JSON file."""
    
    redis = RedisClient(host="localhost", port=6379)
    await redis.connect()
    
    export_data = {
        "exported_at": datetime.now().isoformat(),
        "users": {},
        "pending_buys": {},
        "tokens": {},
        "enriched_users": {},
        "molt_agents": {},
        "pools": {},
        "preclawnch": {},
        "prebankr_data": {},
        "molttracks": [],
        "prebankr_blacklist": [],
        "prebankr_word_blacklist": [],
        "sniper_handles": {},
        "moltx_triggers": {},
        "moltbook_triggers": {},
        "seen_interfaces": [],
        "notified_contracts": [],
        "other_keys": {},
    }
    
    # Get all keys
    keys = []
    cursor = 0
    while True:
        cursor, batch = await redis._client.scan(cursor, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    
    print(f"Found {len(keys)} keys in Redis")
    
    for key in keys:
        try:
            key_type = await redis._client.type(key)
            
            # User configs
            if key.startswith("user:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["users"][key] = data
                print(f"  [USER] {key}")
            
            # Pending buys
            elif key.startswith("buy:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["pending_buys"][key] = data
                print(f"  [BUY] {key}")
            
            # Token info
            elif key.startswith("token:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["tokens"][key] = data
            
            # Enriched users cache
            elif key.startswith("enriched_user:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["enriched_users"][key] = data
            
            # MOLT agents
            elif key.startswith("molt_agent:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["molt_agents"][key] = data
            
            # Pool data
            elif key.startswith("pool:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["pools"][key] = data
            
            # Pre-clawnch tokens
            elif key.startswith("preclawnch:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["preclawnch"][key] = data
            
            # PRE-BANKR data cache
            elif key.startswith("prebankr_data:") and key_type == "hash":
                data = await redis.hgetall(key)
                export_data["prebankr_data"][key] = data
            
            # Molttrack list
            elif key == "molttrack:list" and key_type == "hash":
                data = await redis.hgetall(key)
                for track_id, track_json in data.items():
                    track = json.loads(track_json)
                    export_data["molttracks"].append({
                        "id": int(track_id),
                        "type": track["type"],
                        "value": track["value"],
                    })
                print(f"  [MOLTTRACK] {len(export_data['molttracks'])} tracks")
            
            # PRE-BANKR blacklist
            elif key == "prebankr:blacklist:list" and key_type == "hash":
                data = await redis.hgetall(key)
                for bl_id, handle in data.items():
                    export_data["prebankr_blacklist"].append({
                        "id": int(bl_id),
                        "handle": handle,
                    })
                print(f"  [BLACKLIST] {len(export_data['prebankr_blacklist'])} handles")
            
            # PRE-BANKR word blacklist
            elif key == "prebankr:wordblacklist:list" and key_type == "hash":
                data = await redis.hgetall(key)
                for bl_id, word in data.items():
                    export_data["prebankr_word_blacklist"].append({
                        "id": int(bl_id),
                        "word": word,
                    })
                print(f"  [WORDBLACKLIST] {len(export_data['prebankr_word_blacklist'])} words")
            
            # Sniper handle indexes
            elif key.startswith("sniper_handle:") and key_type == "set":
                members = await redis.smembers(key)
                handle = key.replace("sniper_handle:", "")
                export_data["sniper_handles"][handle] = list(members)
            
            # MoltX trigger handle indexes
            elif key.startswith("moltx_trigger_handle:") and key_type == "set":
                members = await redis.smembers(key)
                handle = key.replace("moltx_trigger_handle:", "")
                export_data["moltx_triggers"][handle] = list(members)
            
            # Moltbook trigger handle indexes
            elif key.startswith("moltbook_trigger_handle:") and key_type == "set":
                members = await redis.smembers(key)
                handle = key.replace("moltbook_trigger_handle:", "")
                export_data["moltbook_triggers"][handle] = list(members)
            
            # Seen interfaces
            elif key == "seen_interfaces" and key_type == "set":
                members = await redis.smembers(key)
                export_data["seen_interfaces"] = list(members)
                print(f"  [INTERFACES] {len(members)} seen")
            
            # Notified contracts
            elif key == "notified_contracts" and key_type == "set":
                members = await redis.smembers(key)
                export_data["notified_contracts"] = list(members)
                print(f"  [NOTIFIED] {len(members)} contracts")
            
            # Skip index keys (they're derived from data)
            elif key.startswith("buys_by_"):
                pass
            elif key.startswith("molttrack:") and key != "molttrack:list":
                pass
            elif key.startswith("prebankr:blacklist:") and key != "prebankr:blacklist:list":
                pass
            elif key.startswith("prebankr:wordblacklist:") and key != "prebankr:wordblacklist:list":
                pass
            elif key in ["auto_buy_users", "moltx_trigger_users", "moltbook_trigger_users", "molt_agents_index"]:
                pass
            elif key.startswith("molt_name_to_handle:") or key.startswith("molt_id_to_handle:"):
                pass
            elif key.startswith("token_by_address:") or key.startswith("preclawnch_by_"):
                pass
            elif key.startswith("alphagate:") or key.startswith("twitter:"):
                pass  # Cache, will regenerate
            elif key.startswith("trade_lock:") or key.startswith("buy_creation_lock:"):
                pass  # Temporary locks
            
            # Other keys (capture anything else)
            else:
                if key_type == "string":
                    value = await redis.get(key)
                    export_data["other_keys"][key] = {"type": "string", "value": value}
                elif key_type == "hash":
                    value = await redis.hgetall(key)
                    export_data["other_keys"][key] = {"type": "hash", "value": value}
                elif key_type == "set":
                    value = list(await redis.smembers(key))
                    export_data["other_keys"][key] = {"type": "set", "value": value}
                elif key_type == "list":
                    value = await redis.lrange(key, 0, -1)
                    export_data["other_keys"][key] = {"type": "list", "value": value}
                
                if key in export_data["other_keys"]:
                    print(f"  [OTHER] {key} ({key_type})")
        
        except Exception as e:
            print(f"  [ERROR] {key}: {e}")
    
    await redis.close()
    
    # Summary
    print("\n" + "="*60)
    print("EXPORT SUMMARY")
    print("="*60)
    print(f"Users:              {len(export_data['users'])}")
    print(f"Pending Buys:       {len(export_data['pending_buys'])}")
    print(f"Tokens:             {len(export_data['tokens'])}")
    print(f"Enriched Users:     {len(export_data['enriched_users'])}")
    print(f"MOLT Agents:        {len(export_data['molt_agents'])}")
    print(f"Pools:              {len(export_data['pools'])}")
    print(f"Pre-Clawnch:        {len(export_data['preclawnch'])}")
    print(f"PRE-BANKR Data:     {len(export_data['prebankr_data'])}")
    print(f"Molttracks:         {len(export_data['molttracks'])}")
    print(f"Blacklist:          {len(export_data['prebankr_blacklist'])}")
    print(f"Word Blacklist:     {len(export_data['prebankr_word_blacklist'])}")
    print(f"Seen Interfaces:    {len(export_data['seen_interfaces'])}")
    print(f"Notified Contracts: {len(export_data['notified_contracts'])}")
    print(f"Other Keys:         {len(export_data['other_keys'])}")
    print("="*60)
    
    # Save to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"redis_backup_{timestamp}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nExported to: {filename}")
    return filename


if __name__ == "__main__":
    asyncio.run(export_all())
