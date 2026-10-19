"""
Test script for Clawnch API integration
Tests: API fetch, JSON extraction (NO REDIS - just API testing)
"""
import asyncio
import aiohttp
import re
import json
import sys

# Fix encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

# API Configuration
CLAWNCH_API = "https://www.moltbook.com/api/v1/submolts/clawnch"

# Regex to extract JSON from !clawnch posts
CLAWNCH_PATTERN = re.compile(
    r'!clawnch\s*\n```json\s*\n(.*?)\n```',
    re.DOTALL | re.IGNORECASE
)

def print_section(title):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

async def test_api_fetch():
    """Test 1: Fetch API with different header configurations."""
    print_section("TEST 1: API FETCH")
    
    # Test with minimal headers (current worker config)
    print("📡 Testing with MINIMAL headers (current worker)...\n")
    
    headers_minimal = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                CLAWNCH_API,
                params={"sort": "new"},
                headers=headers_minimal,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                print(f"Status: {resp.status}")
                print(f"Headers: {dict(resp.headers)}\n")
                
                if resp.status == 200:
                    data = await resp.json()
                    print(f"✅ SUCCESS!")
                    print(f"   Posts returned: {len(data.get('posts', []))}")
                    print(f"   Submolt: {data.get('submolt', {}).get('name', 'N/A')}")
                    return data
                else:
                    print(f"❌ FAILED: HTTP {resp.status}")
                    text = await resp.text()
                    print(f"   Response: {text[:200]}")
                    return None
        except Exception as e:
            print(f"❌ ERROR: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # Test with full browser headers
    print("\n📡 Testing with FULL browser headers...\n")
    
    headers_full = {
        "accept": "application/json",
        "referer": "https://www.moltbook.com/m/clawnch",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                CLAWNCH_API,
                params={"sort": "new"},
                headers=headers_full,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                print(f"Status: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    print(f"✅ SUCCESS!")
                    print(f"   Posts returned: {len(data.get('posts', []))}")
                    return data
                else:
                    print(f"❌ FAILED: HTTP {resp.status}")
                    return None
        except Exception as e:
            print(f"❌ ERROR: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None

async def test_json_extraction(data):
    """Test 2: Extract token JSON from posts."""
    print_section("TEST 2: JSON EXTRACTION")
    
    if not data:
        print("❌ No data to test")
        return []
    
    posts = data.get("posts", [])
    print(f"Total posts: {len(posts)}\n")
    
    tokens_found = []
    
    for i, post in enumerate(posts[:5], 1):  # Test first 5
        post_id = post.get("id", "N/A")
        author_name = post.get("author", {}).get("name", "N/A")
        content = post.get("content", "")
        
        print(f"Post #{i}:")
        print(f"  ID: {post_id[:20]}...")
        print(f"  Author: {author_name}")
        
        # Try to extract JSON
        match = CLAWNCH_PATTERN.search(content)
        if match:
            try:
                token_json = json.loads(match.group(1))
                print(f"  ✅ Token found: {token_json.get('name')} (${token_json.get('symbol')})")
                print(f"     Wallet: {token_json.get('wallet', 'N/A')[:20]}...")
                
                tokens_found.append({
                    "post_id": post_id,
                    "author": post.get("author"),
                    "token": token_json
                })
            except json.JSONDecodeError as e:
                print(f"  ❌ Invalid JSON: {e}")
        else:
            print(f"  ⚠️  No !clawnch JSON found")
        
        print()
    
    print(f"📊 Summary: {len(tokens_found)} valid tokens extracted from {len(posts[:5])} posts\n")
    return tokens_found

async def test_molt_linking(tokens_found):
    """Test 3: Show what authors need MOLT linking (no Redis needed)."""
    print_section("TEST 3: MOLT AGENT LINKING SIMULATION")
    
    if not tokens_found:
        print("❌ No tokens to test linking")
        return
    
    print("📋 Authors that will be searched in Redis:\n")
    
    for i, item in enumerate(tokens_found, 1):
        author = item["author"]
        author_name = author.get("name", "N/A")
        author_id = author.get("id", "N/A")
        token = item["token"]
        
        print(f"Token #{i}: {token.get('name')} by @{author_name}")
        print(f"  🔍 Will search Redis for:")
        print(f"     Key 1: molt_name_to_handle:{author_name.lower()}")
        print(f"     Key 2: molt_id_to_handle:{author_id}")
        print(f"  📊 Moltbook Data:")
        print(f"     Karma: {author.get('karma', 0)}")
        print(f"     Followers: {author.get('follower_count', 0)}")
        print(f"     Description: {author.get('description', 'N/A')[:50]}...")
        print()

async def test_message_format(tokens_found):
    """Test 4: Format Telegram message."""
    print_section("TEST 4: TELEGRAM MESSAGE FORMAT")
    
    if not tokens_found:
        print("❌ No tokens to format")
        return
    
    # Simulate a token with MOLT data
    sample = tokens_found[0]
    token = sample["token"]
    author = sample["author"]
    
    # Mock MOLT data
    molt_data = {
        "x_handle": "example_handle",
        "x_name": "Example User",
        "is_verified": True,
        "followers": 15000,
        "vip_followers": ["elonmusk", "vitalikbuterin"],
        "passes_kol_filter": True,
        "agent_name": author.get("name")
    }
    
    message = f"""🦞 <b>NEW TOKEN LAUNCH</b> 🦞

<b>{token.get('name', 'N/A')}</b> (${token.get('symbol', 'N/A')})

{token.get('description', 'No description')}

━━━━━━━━━━━━━━━━━━
<b>FROM (Moltbook Agent)</b>
━━━━━━━━━━━━━━━━━━

🤖 Agent: <b>@{author.get('name')}</b>
⭐ Karma: {author.get('karma', 0)}
👥 Followers: {author.get('follower_count', 0)}

━━━━━━━━━━━━━━━━━━
<b>OWNER (Twitter)</b>
━━━━━━━━━━━━━━━━━━

🐦 Twitter: <b>@{molt_data['x_handle']}</b>
{'✅ Verified' if molt_data['is_verified'] else ''}
👥 Followers: {molt_data['followers']:,}
{'🔥 <b>KOL DETECTED</b>' if molt_data['passes_kol_filter'] else ''}

VIP Followers: {', '.join(f"@{v}" for v in molt_data['vip_followers'][:3])}

━━━━━━━━━━━━━━━━━━

💰 Wallet: <code>{token.get('wallet', 'N/A')}</code>
"""
    
    print("📱 Sample Telegram Message:\n")
    print(message)
    
    print("\n🔘 Inline Buttons:")
    print("  [Molt profile] [Twitter Owner]")
    print("  [❤️ Like]      [💰 Buy]")

async def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("  CLAWNCH API INTEGRATION TEST")
    print("="*70)
    
    # Test 1: API Fetch
    data = await test_api_fetch()
    
    if not data:
        print("\n❌ API fetch failed. Cannot continue tests.")
        return
    
    # Test 2: JSON Extraction
    tokens_found = await test_json_extraction(data)
    
    # Test 3: MOLT Linking
    await test_molt_linking(tokens_found)
    
    # Test 4: Message Format
    await test_message_format(tokens_found)
    
    print_section("TEST COMPLETE")
    print("✅ All tests finished!\n")

if __name__ == "__main__":
    asyncio.run(main())
