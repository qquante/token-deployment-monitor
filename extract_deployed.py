#!/usr/bin/env python3
"""
Extract ALL deployed tweets from SQLite database.

Pattern: "@username deployed TokenName contract address is 0x..."
"""

import sys
import json
import re
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.database.models import Database


def parse_deployed_tweet(text: str) -> dict:
    """Parse a deployed tweet to extract token info."""
    result = {
        "requester_username": "",
        "token_name": "",
        "contract_address": "",
        "clanker_url": ""
    }
    
    # Pattern: @username [optional: @user2] deployed TokenName contract address is 0x...
    # Example: "@sekoweed deployed clawdius contract address is 0x4be0A28749503D23c444ce721C2c44dEF7838b07"
    
    # Extract requester (first @mention)
    username_match = re.match(r'^@(\w+)', text)
    if username_match:
        result["requester_username"] = username_match.group(1)
    
    # Extract token name (between "deployed" and "contract address is")
    token_match = re.search(r'deployed\s+(.+?)\s+contract address is', text, re.IGNORECASE)
    if token_match:
        result["token_name"] = token_match.group(1).strip()
    
    # Extract contract address
    contract_match = re.search(r'contract address is\s+(0x[a-fA-F0-9]{40})', text)
    if contract_match:
        result["contract_address"] = contract_match.group(1).lower()
    
    # Extract clanker URL
    url_match = re.search(r'(https://\S+)', text)
    if url_match:
        result["clanker_url"] = url_match.group(1)
    
    return result


def main():
    print("=" * 60)
    print("EXTRACTING ALL DEPLOYED TWEETS")
    print("=" * 60)
    
    db = Database()
    db.connect()
    cursor = db.conn.cursor()
    
    # Get ALL deployed tweets from bankrbot
    cursor.execute("""
        SELECT id, text, created_at, in_reply_to_username, raw_json
        FROM tweets 
        WHERE LOWER(author_username) = 'bankrbot' 
        AND LOWER(text) LIKE '%deployed%'
        AND LOWER(text) LIKE '%contract address is%'
        ORDER BY created_at DESC
    """)
    
    rows = cursor.fetchall()
    print(f"\n[RESULT] Found {len(rows)} DEPLOYED tweets with contract address")
    
    deployed_tweets = []
    contracts_found = set()
    
    for row in rows:
        tweet_id, text, created_at, in_reply_to, raw_json = row
        
        parsed = parse_deployed_tweet(text)
        
        if parsed["contract_address"]:
            contracts_found.add(parsed["contract_address"])
        
        deployed_tweets.append({
            "tweet_id": tweet_id,
            "created_at": created_at,
            "in_reply_to_username": in_reply_to,
            "requester_username": parsed["requester_username"],
            "token_name": parsed["token_name"],
            "contract_address": parsed["contract_address"],
            "clanker_url": parsed["clanker_url"],
            "full_text": text
        })
    
    print(f"[CONTRACTS] {len(contracts_found)} unique contract addresses")
    
    # Save to JSON
    output_data = {
        "total_deployed_tweets": len(deployed_tweets),
        "unique_contracts": len(contracts_found),
        "extracted_at": str(Path(__file__)),
        "tweets": deployed_tweets
    }
    
    output_path = project_root / "data" / "deployed_tweets_all.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[SAVED] {output_path}")
    print(f"  Total tweets: {len(deployed_tweets)}")
    
    # Print sample
    print(f"\n=== SAMPLE (first 30) ===\n")
    for i, tweet in enumerate(deployed_tweets[:30]):
        print(f"[{i+1}] @{tweet['requester_username']}")
        print(f"    Token: {tweet['token_name']}")
        print(f"    Contract: {tweet['contract_address']}")
        print(f"    Time: {tweet['created_at']}")
        print()
    
    db.close()
    print("\n[DONE]")


if __name__ == "__main__":
    main()
