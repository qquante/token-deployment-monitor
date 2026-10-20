"""
Test script to verify Twitter and Moltbook API verification fields.

Tests:
1. Twitter get_user_info - isBlueVerified
2. Twitter get_user_about - affiliate_label (company badge)
3. Moltbook agent API - verified field
"""

import asyncio
import aiohttp
import os
import json
from dotenv import load_dotenv

load_dotenv()

# TWITTER API TEST

TWITTER_API_KEY = os.getenv("TWITTER_API_KEY", "")
TWITTER_BASE_URL = "https://api.twitterapi.io"


async def test_twitter_user_info(username: str):
    """Test get_user_info endpoint for isBlueVerified."""
    print(f"\n{'='*60}")
    print(f"🐦 Twitter get_user_info: @{username}")
    print(f"{'='*60}")
    
    headers = {
        "X-API-Key": TWITTER_API_KEY,
        "Content-Type": "application/json",
    }
    
    async with aiohttp.ClientSession() as session:
        url = f"{TWITTER_BASE_URL}/twitter/user/info"
        async with session.get(url, params={"userName": username}, headers=headers) as resp:
            print(f"Status: {resp.status}")
            result = await resp.json()
            
            if result.get("status") == "success" and result.get("data"):
                data = result["data"]
                print(f"\n✅ SUCCESS")
                print(f"   Name: {data.get('name')}")
                print(f"   Followers: {data.get('followers', 0):,}")
                print(f"   isBlueVerified: {data.get('isBlueVerified', False)}")
                print(f"   Description: {data.get('description', '')[:80]}...")
                print(f"   Created: {data.get('createdAt', '')}")
                
                # Show ALL fields related to verification
                print(f"\n📋 All verification-related fields:")
                for key in sorted(data.keys()):
                    if 'verif' in key.lower() or 'blue' in key.lower() or 'badge' in key.lower():
                        print(f"   {key}: {data[key]}")
                
                return data
            else:
                print(f"❌ FAILED: {result.get('msg', 'Unknown error')}")
                return None


async def test_twitter_user_about(username: str):
    """Test get_user_about endpoint for affiliate_label (company badge)."""
    print(f"\n{'='*60}")
    print(f"🐦 Twitter get_user_about: @{username}")
    print(f"{'='*60}")
    
    headers = {
        "X-API-Key": TWITTER_API_KEY,
        "Content-Type": "application/json",
    }
    
    async with aiohttp.ClientSession() as session:
        url = f"{TWITTER_BASE_URL}/twitter/user_about"
        async with session.get(url, params={"userName": username}, headers=headers) as resp:
            print(f"Status: {resp.status}")
            result = await resp.json()
            
            if result.get("status") == "success" and result.get("data"):
                data = result["data"]
                print(f"\n✅ SUCCESS")
                
                # Extract affiliate label
                affiliate_label = None
                affiliates = data.get("affiliates_highlighted_label", {})
                if affiliates:
                    label = affiliates.get("label", {})
                    affiliate_label = label.get("description")
                    print(f"   affiliates_highlighted_label: {affiliate_label}")
                
                # Also check identity_profile_labels
                identity = data.get("identity_profile_labels_highlighted_label", {})
                if identity:
                    label = identity.get("label", {})
                    identity_label = label.get("description")
                    print(f"   identity_profile_labels: {identity_label}")
                
                print(f"   isBlueVerified: {data.get('isBlueVerified', False)}")
                
                # Show full structure for debugging
                print(f"\n📋 Full response structure (keys):")
                for key in sorted(data.keys()):
                    val = data[key]
                    if isinstance(val, dict):
                        print(f"   {key}: {json.dumps(val, indent=6)[:200]}")
                    else:
                        print(f"   {key}: {val}")
                
                return {
                    "affiliate_label": affiliate_label,
                    "is_blue_verified": data.get("isBlueVerified", False),
                }
            else:
                print(f"❌ FAILED: {result.get('msg', 'Unknown error')}")
                return None


# MOLTBOOK API TEST

MOLTBOOK_API = "https://www.moltbook.com/api/v1/agents/profile"


async def test_moltbook_agent(agent_name: str):
    """Test Moltbook agent API to find verified field."""
    print(f"\n{'='*60}")
    print(f"📖 Moltbook Agent API: {agent_name}")
    print(f"{'='*60}")
    
    headers = {
        "accept": "*/*",
        "referer": f"https://www.moltbook.com/u/{agent_name}",
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(MOLTBOOK_API, params={"name": agent_name}, headers=headers) as resp:
            print(f"Status: {resp.status}")
            result = await resp.json()
            
            if result.get("success"):
                agent = result.get("agent", {})
                owner = agent.get("owner", {})
                
                print(f"\n✅ SUCCESS")
                print(f"\n📋 Agent fields:")
                for key in sorted(agent.keys()):
                    val = agent[key]
                    if key == "owner":
                        continue  # Print owner separately
                    if isinstance(val, dict):
                        print(f"   {key}: {json.dumps(val)[:100]}")
                    elif isinstance(val, str) and len(str(val)) > 50:
                        print(f"   {key}: {str(val)[:50]}...")
                    else:
                        print(f"   {key}: {val}")
                
                print(f"\n📋 Owner fields:")
                for key in sorted(owner.keys()):
                    val = owner[key]
                    if isinstance(val, str) and len(str(val)) > 50:
                        print(f"   {key}: {str(val)[:50]}...")
                    else:
                        print(f"   {key}: {val}")
                
                # Look for verification fields
                print(f"\n🔍 Looking for 'verified' fields:")
                for key in agent.keys():
                    if 'verif' in key.lower():
                        print(f"   agent.{key}: {agent[key]}")
                for key in owner.keys():
                    if 'verif' in key.lower():
                        print(f"   owner.{key}: {owner[key]}")
                
                return {"agent": agent, "owner": owner}
            else:
                print(f"❌ FAILED: {result}")
                return None


# MAIN

async def main():
    print("\n" + "="*70)
    print("🧪 VERIFICATION API TEST")
    print("="*70)
    
    # Test Twitter with known verified accounts
    print("\n\n" + "#"*70)
    print("# TWITTER TESTS")
    print("#"*70)
    
    # Test 1: Normal verified user (blue check)
    await test_twitter_user_info("elonmusk")
    await test_twitter_user_about("elonmusk")
    
    # Test 2: Company/Organization account
    await test_twitter_user_info("xlossprevention")  # X company account
    await test_twitter_user_about("xlossprevention")
    
    # Test 3: Regular user (maybe not verified)
    await test_twitter_user_info("sekoweed")
    await test_twitter_user_about("sekoweed")
    
    # Test Moltbook
    print("\n\n" + "#"*70)
    print("# MOLTBOOK TESTS")
    print("#"*70)
    
    await test_moltbook_agent("ClawCredit")
    await test_moltbook_agent("MoltChaser")
    await test_moltbook_agent("chemexpert")
    
    print("\n\n✅ All tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
