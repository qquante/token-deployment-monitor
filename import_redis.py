"""
Import Redis data from JSON backup.

Usage:
    python import_redis.py redis_backup_20260205_023712.json
    python import_redis.py backup.json --users-only
    python import_redis.py backup.json --skip-cache
    python import_redis.py backup.json --clear-first
"""
import asyncio
import json
import sys
import argparse
from datetime import datetime
from shared.redis_client import RedisClient


async def import_all(
    filename: str,
    users_only: bool = False,
    skip_cache: bool = False,
    clear_first: bool = False,
    dry_run: bool = False,
):
    """Import Redis data from JSON backup."""
    
    print(f"Loading backup from: {filename}")
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    print(f"Backup from: {data.get('exported_at', 'unknown')}")
    print()
    
    redis = RedisClient(host="localhost", port=6379)
    await redis.connect()
    
    if clear_first and not dry_run:
        print("⚠️  CLEARING ALL REDIS DATA...")
        await redis._client.flushdb()
        print("✅ Redis cleared")
        print()
    
    stats = {
        "users": 0,
        "pending_buys": 0,
        "tokens": 0,
        "enriched_users": 0,
        "molt_agents": 0,
        "pools": 0,
        "preclawnch": 0,
        "prebankr_data": 0,
        "molttracks": 0,
        "blacklist": 0,
        "word_blacklist": 0,
        "seen_interfaces": 0,
        "notified_contracts": 0,
        "likes": 0,
        "indexes_rebuilt": 0,
    }
    
    # =========================================================================
    # 1. USERS (CRITICAL)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING USERS (critical)")
    print("=" * 60)
    
    for key, user_data in data.get("users", {}).items():
        user_id = user_data.get("user_id", "")
        username = user_data.get("username", "")
        auto_buy = user_data.get("auto_buy") == "1"
        sniper_handles = user_data.get("sniper_handles", "")
        
        print(f"  [{user_id}] @{username} auto_buy={auto_buy}")
        
        if not dry_run:
            await redis.hset(key, user_data)
            
            # Rebuild indexes
            if auto_buy:
                await redis.sadd("auto_buy_users", user_id)
            
            # Sniper handles index
            if sniper_handles:
                for handle in sniper_handles.split(","):
                    handle = handle.strip().lower()
                    if handle:
                        await redis.sadd(f"sniper_handle:{handle}", user_id)
            
            # MoltX trigger index
            if user_data.get("moltx_trigger_enabled") == "1":
                await redis.sadd("moltx_trigger_users", user_id)
                handles = user_data.get("moltx_trigger_handles", "")
                for handle in handles.split(","):
                    handle = handle.strip().lower()
                    if handle:
                        await redis.sadd(f"moltx_trigger_handle:{handle}", user_id)
            
            # Moltbook trigger index
            if user_data.get("moltbook_trigger_enabled") == "1":
                await redis.sadd("moltbook_trigger_users", user_id)
                handles = user_data.get("moltbook_trigger_handles", "")
                for handle in handles.split(","):
                    handle = handle.strip().lower()
                    if handle:
                        await redis.sadd(f"moltbook_trigger_handle:{handle}", user_id)
        
        stats["users"] += 1
    
    print(f"✅ Imported {stats['users']} users")
    print()
    
    if users_only:
        print("--users-only flag set, skipping other data")
        await redis.close()
        return stats
    
    # =========================================================================
    # 2. PENDING BUYS (CRITICAL)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING PENDING BUYS")
    print("=" * 60)
    
    for key, buy_data in data.get("pending_buys", {}).items():
        user_id = buy_data.get("user_id", "")
        tweet_id = buy_data.get("tweet_id", "")
        status = buy_data.get("status", "")
        token_symbol = buy_data.get("token_symbol", "")
        target_block = buy_data.get("target_block", "0")
        token_address = buy_data.get("token_address", "")
        
        if not dry_run:
            await redis.hset(key, buy_data)
            
            # Rebuild indexes
            buy_key = f"{user_id}:{tweet_id}"
            await redis.sadd(f"buys_by_user:{user_id}", buy_key)
            await redis.sadd(f"buys_by_status:{status}", buy_key)
            await redis.sadd(f"buys_by_tweet:{tweet_id}", buy_key)
            
            if target_block and int(target_block) > 0:
                await redis.sadd(f"buys_by_target_block:{target_block}", buy_key)
            
            if token_address:
                await redis.sadd(f"buys_by_token:{token_address.lower()}", buy_key)
        
        stats["pending_buys"] += 1
    
    print(f"✅ Imported {stats['pending_buys']} pending buys")
    print()
    
    # =========================================================================
    # 3. MOLTTRACKS (CRITICAL)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING MOLTTRACKS")
    print("=" * 60)
    
    molttracks = data.get("molttracks", [])
    if molttracks:
        max_id = 0
        for track in molttracks:
            track_id = track["id"]
            track_type = track["type"]
            value = track["value"]
            
            print(f"  [{track_id}] {track_type}={value}")
            
            if not dry_run:
                track_json = json.dumps({"type": track_type, "value": value})
                await redis.hset("molttrack:list", {str(track_id): track_json})
                await redis.set(f"molttrack:{track_type}:{value}", str(track_id))
            
            max_id = max(max_id, track_id)
            stats["molttracks"] += 1
        
        if not dry_run and max_id > 0:
            await redis.set("molttrack:next_id", str(max_id))
    
    print(f"✅ Imported {stats['molttracks']} molttracks")
    print()
    
    # =========================================================================
    # 4. BLACKLISTS (CRITICAL)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING BLACKLISTS")
    print("=" * 60)
    
    # Handle blacklist
    blacklist = data.get("prebankr_blacklist", [])
    if blacklist:
        max_id = 0
        for entry in blacklist:
            bl_id = entry["id"]
            handle = entry["handle"]
            
            print(f"  [{bl_id}] @{handle}")
            
            if not dry_run:
                await redis.hset("prebankr:blacklist:list", {str(bl_id): handle})
                await redis.set(f"prebankr:blacklist:handle:{handle}", str(bl_id))
            
            max_id = max(max_id, bl_id)
            stats["blacklist"] += 1
        
        if not dry_run and max_id > 0:
            await redis.set("prebankr:blacklist:next_id", str(max_id))
    
    print(f"✅ Imported {stats['blacklist']} blacklisted handles")
    
    # Word blacklist
    word_blacklist = data.get("prebankr_word_blacklist", [])
    if word_blacklist:
        max_id = 0
        for entry in word_blacklist:
            bl_id = entry["id"]
            word = entry["word"]
            
            print(f"  [{bl_id}] \"{word}\"")
            
            if not dry_run:
                await redis.hset("prebankr:wordblacklist:list", {str(bl_id): word})
                await redis.set(f"prebankr:wordblacklist:word:{word}", str(bl_id))
            
            max_id = max(max_id, bl_id)
            stats["word_blacklist"] += 1
        
        if not dry_run and max_id > 0:
            await redis.set("prebankr:wordblacklist:next_id", str(max_id))
    
    print(f"✅ Imported {stats['word_blacklist']} blacklisted words")
    print()
    
    # =========================================================================
    # 5. LIKES
    # =========================================================================
    print("=" * 60)
    print("IMPORTING LIKES")
    print("=" * 60)
    
    other_keys = data.get("other_keys", {})
    for key, info in other_keys.items():
        if key.startswith("likes:") and info.get("type") == "set":
            if not dry_run:
                for member in info.get("value", []):
                    await redis.sadd(key, member)
            stats["likes"] += 1
    
    print(f"✅ Imported {stats['likes']} like sets")
    print()
    
    # =========================================================================
    # CACHE DATA (skip if --skip-cache)
    # =========================================================================
    
    if skip_cache:
        print("--skip-cache flag set, skipping cache data")
        print("(pools, enriched_users, molt_agents, tokens will regenerate)")
        await redis.close()
        return stats
    
    # =========================================================================
    # 6. TOKENS (cache)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING TOKENS (cache)")
    print("=" * 60)
    
    for key, token_data in data.get("tokens", {}).items():
        if not dry_run:
            await redis.hset(key, token_data)
            await redis.expire(key, 86400)  # 24h TTL
            
            # Index
            token_address = token_data.get("token_address", "")
            tweet_id = token_data.get("tweet_id", "")
            if token_address and tweet_id:
                await redis.set(f"token_by_address:{token_address.lower()}", tweet_id, ex=86400)
        
        stats["tokens"] += 1
    
    print(f"✅ Imported {stats['tokens']} tokens")
    print()
    
    # =========================================================================
    # 7. ENRICHED USERS (cache)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING ENRICHED USERS (cache)")
    print("=" * 60)
    
    for key, user_data in data.get("enriched_users", {}).items():
        if not dry_run:
            await redis.hset(key, user_data)
            await redis.expire(key, 86400)  # 24h TTL
        
        stats["enriched_users"] += 1
    
    print(f"✅ Imported {stats['enriched_users']} enriched users")
    print()
    
    # =========================================================================
    # 8. MOLT AGENTS (cache)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING MOLT AGENTS (cache)")
    print("=" * 60)
    
    for key, agent_data in data.get("molt_agents", {}).items():
        x_handle = agent_data.get("x_handle", "")
        agent_name = agent_data.get("agent_name", "")
        agent_id = agent_data.get("agent_id", "")
        
        if not dry_run:
            await redis.hset(key, agent_data)
            await redis.expire(key, 86400)  # 24h TTL
            
            # Indexes
            await redis.sadd("molt_agents_index", x_handle)
            if agent_name:
                await redis.set(f"molt_name_to_handle:{agent_name.lower()}", x_handle, ex=86400)
            if agent_id:
                await redis.set(f"molt_id_to_handle:{agent_id}", x_handle, ex=86400)
        
        stats["molt_agents"] += 1
    
    print(f"✅ Imported {stats['molt_agents']} molt agents")
    print()
    
    # =========================================================================
    # 9. POOLS (cache)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING POOLS (cache)")
    print("=" * 60)
    
    for key, pool_data in data.get("pools", {}).items():
        if not dry_run:
            await redis.hset(key, pool_data)
            await redis.expire(key, 86400 * 7)  # 7 days TTL
        
        stats["pools"] += 1
    
    print(f"✅ Imported {stats['pools']} pools")
    print()
    
    # =========================================================================
    # 10. PRE-CLAWNCH (temporary)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING PRE-CLAWNCH")
    print("=" * 60)
    
    for key, preclawnch_data in data.get("preclawnch", {}).items():
        if not dry_run:
            await redis.hset(key, preclawnch_data)
            await redis.expire(key, 86400 * 30)  # 30 days TTL
            
            # Indexes
            token_wallet = preclawnch_data.get("token_wallet", "")
            token_symbol = preclawnch_data.get("token_symbol", "")
            post_id = preclawnch_data.get("post_id", "")
            
            if token_wallet:
                prefix = token_wallet[:16].lower()
                await redis.set(f"preclawnch_by_wallet:{prefix}", post_id, ex=86400*30)
            if token_symbol:
                await redis.set(f"preclawnch_by_symbol:{token_symbol.upper()}", post_id, ex=86400*30)
        
        stats["preclawnch"] += 1
    
    print(f"✅ Imported {stats['preclawnch']} pre-clawnch tokens")
    print()
    
    # =========================================================================
    # 11. PRE-BANKR DATA (temporary)
    # =========================================================================
    print("=" * 60)
    print("IMPORTING PRE-BANKR DATA")
    print("=" * 60)
    
    for key, prebankr_data in data.get("prebankr_data", {}).items():
        if not dry_run:
            await redis.hset(key, prebankr_data)
            await redis.expire(key, 10800)  # 3h TTL
        
        stats["prebankr_data"] += 1
    
    print(f"✅ Imported {stats['prebankr_data']} pre-bankr data")
    print()
    
    # =========================================================================
    # 12. SEEN INTERFACES
    # =========================================================================
    print("=" * 60)
    print("IMPORTING SEEN INTERFACES")
    print("=" * 60)
    
    for interface in data.get("seen_interfaces", []):
        if not dry_run:
            await redis.sadd("seen_interfaces", interface)
        stats["seen_interfaces"] += 1
    
    print(f"✅ Imported {stats['seen_interfaces']} seen interfaces")
    print()
    
    # =========================================================================
    # 13. NOTIFIED CONTRACTS
    # =========================================================================
    print("=" * 60)
    print("IMPORTING NOTIFIED CONTRACTS")
    print("=" * 60)
    
    for contract in data.get("notified_contracts", []):
        if not dry_run:
            await redis.sadd("notified_contracts", contract)
        stats["notified_contracts"] += 1
    
    print(f"✅ Imported {stats['notified_contracts']} notified contracts")
    print()
    
    await redis.close()
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("=" * 60)
    print("IMPORT COMPLETE" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 60)
    print(f"Users:              {stats['users']}")
    print(f"Pending Buys:       {stats['pending_buys']}")
    print(f"Molttracks:         {stats['molttracks']}")
    print(f"Blacklist:          {stats['blacklist']}")
    print(f"Word Blacklist:     {stats['word_blacklist']}")
    print(f"Likes:              {stats['likes']}")
    print(f"Tokens:             {stats['tokens']}")
    print(f"Enriched Users:     {stats['enriched_users']}")
    print(f"MOLT Agents:        {stats['molt_agents']}")
    print(f"Pools:              {stats['pools']}")
    print(f"Pre-Clawnch:        {stats['preclawnch']}")
    print(f"PRE-BANKR Data:     {stats['prebankr_data']}")
    print(f"Seen Interfaces:    {stats['seen_interfaces']}")
    print(f"Notified Contracts: {stats['notified_contracts']}")
    print("=" * 60)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Import Redis data from JSON backup")
    parser.add_argument("filename", help="JSON backup file to import")
    parser.add_argument("--users-only", action="store_true", help="Only import user configs")
    parser.add_argument("--skip-cache", action="store_true", help="Skip cache data (pools, enriched users, etc)")
    parser.add_argument("--clear-first", action="store_true", help="Clear Redis before importing")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be imported without actually importing")
    
    args = parser.parse_args()
    
    asyncio.run(import_all(
        args.filename,
        users_only=args.users_only,
        skip_cache=args.skip_cache,
        clear_first=args.clear_first,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
