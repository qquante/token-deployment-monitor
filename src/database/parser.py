"""
Parser for extracting deployment info from Bankr tweets.

Parses tweets from @bankrbot to extract:
- Token name / symbol
- Contract address
- FROM: Requester (who asked for the deploy)
- TO: Recipients (fee routing, mentions)
"""

import re
from typing import Optional, Dict, Any, List, Tuple


# REGEX PATTERNS

PATTERNS = {
    # Pattern: "@username deployed TOKEN_NAME contract address is 0x..."
    "deployment": re.compile(
        r"@(\w+)\s+deployed\s+(.+?)\s+contract\s+address\s+is\s+(0x[a-fA-F0-9]{40})",
        re.IGNORECASE
    ),
    
    # Pattern: "see more: https://..."  (clanker URL)
    "clanker_url": re.compile(
        r"see\s+more:\s*(https?://\S+)",
        re.IGNORECASE
    ),
    
    # FEE/REWARD ROUTING PATTERNS (Dynamic - multiple variations)
    
    # Pattern 1: "fees routed to @username (0x...)" 
    "fee_routing": re.compile(
        r"fees?\s+(?:routed?|directed?|redirect)\s+to\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE
    ),
    
    # Pattern 2: "fees routed to @username" (without wallet - extract username only)
    "fee_routing_simple": re.compile(
        r"fees?\s+(?:routed?|directed?|redirect)\s+to\s+@(\w+)",
        re.IGNORECASE
    ),
    
    # Pattern 3: "Rewards routing to @username (0x...)"
    "rewards_routing": re.compile(
        r"rewards?\s+(?:routing|routed?)\s+to\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE
    ),
    
    # Pattern 4: "reward recipient updated to 0x..." (wallet only, no username)
    "reward_recipient_wallet_only": re.compile(
        r"reward\s+recipient\s+(?:updated?|set)\s+to\s+(0x[a-fA-F0-9]{40})",
        re.IGNORECASE
    ),
    
    # Pattern 5: "reward recipient to @username (0x...)" 
    "reward_recipient": re.compile(
        r"reward\s+recipient\s+(?:to|for)\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE
    ),
    
    # Pattern 6: "set you as the reward recipient" + find @username and (0xwallet) nearby
    "set_reward_recipient": re.compile(
        r"(?:set|configured?)\s+(?:you\s+as\s+)?(?:the\s+)?reward\s+recipient.*?@(\w+).*?\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE | re.DOTALL
    ),
    
    # Pattern 7: "I've set you as the reward recipient for this token" with wallet later
    "set_you_reward_recipient": re.compile(
        r"(?:i'?ve\s+)?set\s+you\s+as\s+the\s+reward\s+recipient.*?wallet\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE | re.DOTALL
    ),
    
    # Pattern 8: "fee rewards to be directed to @username (0x...)" / "configured the fee rewards to be directed to"
    "fee_rewards_directed": re.compile(
        r"fee\s+rewards?\s+(?:to\s+be\s+)?directed\s+to\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE
    ),
    
    # Pattern 9: "fee recipient update to @username (0x...) requires" (failed - needs access)
    "fee_recipient_requires_access": re.compile(
        r"fee\s+recipient\s+update?\s+to\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)\s*requires",
        re.IGNORECASE
    ),
    
    # Pattern 10: "100% of the fees are routed directly to ... wallet (0x...)"
    "fees_to_wallet_direct": re.compile(
        r"(\d+)%\s+(?:of\s+)?(?:the\s+)?fees?\s+(?:are\s+)?routed\s+(?:directly\s+)?to\s+.*?wallet\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE | re.DOTALL
    ),
    
    # Pattern 11: "fees redirect to @username attempted"
    "fee_attempted": re.compile(
        r"fees?\s+redirect\s+to\s+@(\w+)\s+attempted",
        re.IGNORECASE
    ),
    
    # Pattern 12: "Rewards routing to @username ... failed"
    "rewards_failed": re.compile(
        r"rewards?\s+(?:routing|routed?)\s+to\s+@(\w+)\s*\((0x[a-fA-F0-9]{40})\)\s*failed",
        re.IGNORECASE
    ),
    
    # Pattern 13: Generic @username (0xwallet) extraction - catches any mention with wallet
    "mention_with_wallet": re.compile(
        r"@(\w+)\s*\((0x[a-fA-F0-9]{40})\)",
        re.IGNORECASE
    ),
    
    # Pattern 14: Extract standalone wallet addresses
    "wallet_address": re.compile(
        r"(0x[a-fA-F0-9]{40})",
        re.IGNORECASE
    ),
    
    # Pattern 15: "(\d+)% recipient share" or "(\d+)% fee share"
    "fee_percentage": re.compile(
        r"(\d+)%\s+(?:recipient\s+)?(?:fee\s+)?share",
        re.IGNORECASE
    ),
    
    # OTHER PATTERNS
    
    # Pattern: Extract contract address from URL
    "contract_in_url": re.compile(
        r"clanker\.world/clanker/(0x[a-fA-F0-9]{40})",
        re.IGNORECASE
    ),
    
    # Pattern: "swapped X TOKEN to Y TOKEN"
    "swap": re.compile(
        r"swapped\s+([\d,.]+)\s+(\w+)\s+to\s+([\d,.]+)\s+(\w+)",
        re.IGNORECASE
    ),
    
    # Pattern: "sent X TOKEN to @username"
    "transfer": re.compile(
        r"sent\s+([\d,.]+)\s+(\w+)\s+to\s+@(\w+)",
        re.IGNORECASE
    ),
    
    # Deployment failed patterns
    "deployment_failed": re.compile(
        r"deployment\s+(?:of\s+)?(.+?)\s+(?:failed|hit\s+a\s+wall)",
        re.IGNORECASE
    ),
    
    # Hit limit pattern
    "hit_limit": re.compile(
        r"(?:hit|reached)\s+(?:your\s+)?(?:max\s+)?(?:deployment\s+)?limit",
        re.IGNORECASE
    ),
    
    # Token name with ticker: "TokenName ($TICKER)"
    "token_ticker": re.compile(
        r"([A-Za-z0-9\s]+?)\s*\(\$?([A-Z0-9]+)\)",
        re.IGNORECASE
    ),
}


# FEE/REWARD ROUTING EXTRACTION (Dynamic)

def extract_fee_routing(text: str) -> Dict[str, Any]:
    """
    Dynamically extract fee/reward routing info from tweet text.
    
    Tries multiple patterns and returns the best match.
    
    Returns dict with:
    - username: fee recipient username (if found)
    - wallet: fee recipient wallet address (if found)
    - status: 'success' | 'failed' | 'requires_access' | 'attempted' | None
    - percentage: fee percentage (if found)
    - raw_match: the matched text
    """
    result = {
        "username": None,
        "wallet": None,
        "status": None,
        "percentage": None,
        "raw_match": None,
    }
    
    # Try patterns in order of specificity (most specific first)
    
    # Check for failed/requires access first (before success patterns)
    failed_match = PATTERNS["rewards_failed"].search(text)
    if failed_match:
        result["username"] = failed_match.group(1)
        result["wallet"] = failed_match.group(2)
        result["status"] = "failed"
        result["raw_match"] = failed_match.group(0)
        return result
    
    requires_access_match = PATTERNS["fee_recipient_requires_access"].search(text)
    if requires_access_match:
        result["username"] = requires_access_match.group(1)
        result["wallet"] = requires_access_match.group(2)
        result["status"] = "requires_access"
        result["raw_match"] = requires_access_match.group(0)
        return result
    
    # Success patterns with username AND wallet
    success_patterns = [
        ("fee_routing", 1, 2),                # @username (0xwallet)
        ("rewards_routing", 1, 2),            # rewards routing to @username (0xwallet)
        ("reward_recipient", 1, 2),           # reward recipient to @username (0xwallet)
        ("set_reward_recipient", 1, 2),       # set reward recipient @username (0xwallet)
        ("fee_rewards_directed", 1, 2),       # fee rewards directed to @username (0xwallet)
    ]
    
    for pattern_name, user_group, wallet_group in success_patterns:
        match = PATTERNS[pattern_name].search(text)
        if match:
            result["username"] = match.group(user_group)
            result["wallet"] = match.group(wallet_group)
            result["status"] = "success"
            result["raw_match"] = match.group(0)
            
            # Check for percentage
            pct_match = PATTERNS["fee_percentage"].search(text)
            if pct_match:
                result["percentage"] = int(pct_match.group(1))
            
            return result
    
    # Wallet-only patterns (no username in pattern)
    wallet_only_match = PATTERNS["reward_recipient_wallet_only"].search(text)
    if wallet_only_match:
        result["wallet"] = wallet_only_match.group(1)
        result["status"] = "success"
        result["raw_match"] = wallet_only_match.group(0)
        return result
    
    # Direct wallet pattern with percentage
    direct_wallet_match = PATTERNS["fees_to_wallet_direct"].search(text)
    if direct_wallet_match:
        result["percentage"] = int(direct_wallet_match.group(1))
        result["wallet"] = direct_wallet_match.group(2)
        result["status"] = "success"
        result["raw_match"] = direct_wallet_match.group(0)
        return result
    
    # "Set you as reward recipient" pattern (username from context)
    set_you_match = PATTERNS["set_you_reward_recipient"].search(text)
    if set_you_match:
        result["wallet"] = set_you_match.group(1)
        result["status"] = "success"
        result["raw_match"] = set_you_match.group(0)
        # Try to find username nearby
        nearby_mention = re.search(r"@(\w+)", text)
        if nearby_mention:
            result["username"] = nearby_mention.group(1)
        return result
    
    # Simple fee routing (username only, no wallet)
    simple_match = PATTERNS["fee_routing_simple"].search(text)
    if simple_match:
        result["username"] = simple_match.group(1)
        result["status"] = "success"
        result["raw_match"] = simple_match.group(0)
        
        # Try to find wallet in nearby text using generic pattern
        wallet_match = PATTERNS["mention_with_wallet"].search(text)
        if wallet_match and wallet_match.group(1).lower() == result["username"].lower():
            result["wallet"] = wallet_match.group(2)
        
        return result
    
    # Attempted pattern (no wallet)
    attempted_match = PATTERNS["fee_attempted"].search(text)
    if attempted_match:
        result["username"] = attempted_match.group(1)
        result["status"] = "attempted"
        result["raw_match"] = attempted_match.group(0)
        return result
    
    return result


def extract_all_mentions_with_wallets(text: str) -> List[Dict[str, Any]]:
    """
    Extract all @username (0xwallet) pairs from text dynamically.
    
    Returns list of dicts with username and wallet.
    """
    results = []
    seen = set()
    
    for match in PATTERNS["mention_with_wallet"].finditer(text):
        username = match.group(1)
        wallet = match.group(2)
        key = (username.lower(), wallet.lower())
        
        if key not in seen:
            seen.add(key)
            results.append({
                "username": username,
                "wallet": wallet,
                "position": match.start(),
            })
    
    return results


# MENTION EXTRACTION

def extract_all_mentions(tweet: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract all @mentions from a tweet with their full info.
    
    Returns list of mention dicts with:
    - username (screen_name)
    - user_id
    - name
    - indices (position in text)
    """
    mentions = []
    entities = tweet.get("entities", {})
    user_mentions = entities.get("user_mentions", [])
    
    for mention in user_mentions:
        mentions.append({
            "username": mention.get("screen_name", ""),
            "user_id": mention.get("id_str", ""),
            "name": mention.get("name", ""),
            "indices": mention.get("indices", []),
        })
    
    return mentions


def identify_from_to(tweet: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Identify FROM (requester) and TO (recipients) from a deployment tweet.
    
    Rules:
    1. FROM = First @mention in the tweet (the person bankrbot is replying to)
    2. FROM can also be verified by inReplyToUsername
    3. TO = Any mention in "fees routed to @username" pattern
    4. TO = Other mentions that aren't the requester
    
    Returns:
        (from_user, list_of_to_users)
    """
    text = tweet.get("text", "")
    mentions = extract_all_mentions(tweet)
    
    from_user = {}
    to_users = []
    
    if not mentions:
        return from_user, to_users
    
    # First mention is typically the requester (FROM)
    first_mention = mentions[0]
    from_user = {
        "username": first_mention.get("username"),
        "user_id": first_mention.get("user_id"),
        "name": first_mention.get("name"),
        "role": "requester",
    }
    
    # Verify with inReplyToUsername if available
    in_reply_to = tweet.get("inReplyToUsername", "")
    if in_reply_to and in_reply_to.lower() != from_user.get("username", "").lower():
        # inReplyToUsername might be different, check if it's in mentions
        for mention in mentions:
            if mention.get("username", "").lower() == in_reply_to.lower():
                from_user = {
                    "username": mention.get("username"),
                    "user_id": mention.get("user_id"),
                    "name": mention.get("name"),
                    "role": "requester",
                }
                break
    
    # Check for explicit fee routing DYNAMICALLY
    fee_routing = extract_fee_routing(text)
    fee_recipient_username = fee_routing.get("username")
    fee_recipient_wallet = fee_routing.get("wallet")
    
    # Build TO list (deduplicated by user_id)
    seen_user_ids = set()
    for mention in mentions[1:]:  # Skip first mention (FROM)
        username = mention.get("username", "")
        user_id = mention.get("user_id", "")
        
        # Skip bankrbot itself
        if username.lower() == "bankrbot":
            continue
        
        # Skip duplicates
        if user_id and user_id in seen_user_ids:
            continue
        if user_id:
            seen_user_ids.add(user_id)
        
        to_entry = {
            "username": username,
            "user_id": user_id,
            "name": mention.get("name"),
            "role": "mentioned",
        }
        
        # Mark if this is the fee recipient
        if fee_recipient_username and username.lower() == fee_recipient_username.lower():
            to_entry["role"] = "fee_recipient"
        
        to_users.append(to_entry)
    
    # If fee recipient not in mentions, add them
    if fee_recipient_username:
        found = False
        for u in to_users:
            if u.get("username", "").lower() == fee_recipient_username.lower():
                u["role"] = "fee_recipient"
                if fee_recipient_wallet:
                    u["wallet"] = fee_recipient_wallet
                found = True
                break
        
        if not found:
            to_users.append({
                "username": fee_recipient_username,
                "wallet": fee_recipient_wallet,
                "role": "fee_recipient",
            })
    
    return from_user, to_users


# DEPLOYMENT PARSING

def parse_deployment(tweet_text: str, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a deployment confirmation from bankrbot.
    
    Returns deployment info dict if successful, None if not a deployment.
    """
    # Check if it's a deployment confirmation
    match = PATTERNS["deployment"].search(tweet_text)
    if not match:
        return None
    
    requester_username = match.group(1)
    token_raw = match.group(2).strip()
    contract_address = match.group(3)
    
    # Try to extract token name and symbol
    token_name = token_raw
    token_symbol = None
    
    ticker_match = PATTERNS["token_ticker"].search(token_raw)
    if ticker_match:
        token_name = ticker_match.group(1).strip()
        token_symbol = ticker_match.group(2).upper()
    else:
        # Use token name as symbol (common for memecoins)
        token_symbol = token_raw.upper().replace(" ", "")[:10]
    
    # Identify FROM and TO
    from_user, to_users = identify_from_to(tweet_data)
    
    # Extract additional info
    result = {
        "type": "deployment",
        "token_name": token_name,
        "token_symbol": token_symbol,
        "contract_address": contract_address,
        "chain": "base",
        
        # FROM (requester)
        "from_user": from_user,
        "requester_username": from_user.get("username", requester_username),
        "requester_user_id": from_user.get("user_id"),
        "requester_name": from_user.get("name"),
        
        # TO (recipients)
        "to_users": to_users,
        
        # URLs
        "clanker_url": None,
        "tweet_url": tweet_data.get("url"),
        
        # Fee routing
        "fee_recipient_username": None,
        "fee_recipient_address": None,
        
        # Timestamps
        "deployed_at": tweet_data.get("createdAt"),
        "tweet_id": tweet_data.get("id"),
        
        # Raw
        "raw_text": tweet_text,
        "all_mentions": extract_all_mentions(tweet_data),
    }
    
    # Extract clanker URL from entities
    entities = tweet_data.get("entities", {})
    urls = entities.get("urls", [])
    for url_info in urls:
        expanded = url_info.get("expanded_url", "")
        if "clanker.world" in expanded:
            result["clanker_url"] = expanded
            break
    
    # Extract fee/rewards routing info DYNAMICALLY
    fee_routing = extract_fee_routing(tweet_text)
    
    result["fee_recipient_username"] = fee_routing.get("username")
    result["fee_recipient_address"] = fee_routing.get("wallet")
    result["fee_routing_status"] = fee_routing.get("status")
    result["fee_percentage"] = fee_routing.get("percentage")
    result["fee_routing_raw"] = fee_routing.get("raw_match")
    
    # Also extract ALL @username (0xwallet) pairs for reference
    result["all_mentions_with_wallets"] = extract_all_mentions_with_wallets(tweet_text)
    
    # Update TO users with fee recipient if found
    if fee_routing.get("username"):
        fee_username = fee_routing["username"]
        # Check if fee recipient is already in to_users
        found = False
        for to_user in result["to_users"]:
            if to_user.get("username", "").lower() == fee_username.lower():
                to_user["role"] = "fee_recipient"
                if fee_routing.get("wallet"):
                    to_user["wallet"] = fee_routing["wallet"]
                found = True
                break
        
        # If not found, add to to_users
        if not found:
            result["to_users"].append({
                "username": fee_username,
                "wallet": fee_routing.get("wallet"),
                "role": "fee_recipient",
            })
    
    return result


def parse_swap(tweet_text: str, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a swap confirmation."""
    match = PATTERNS["swap"].search(tweet_text)
    if not match:
        return None
    
    return {
        "type": "swap",
        "from_amount": match.group(1).replace(",", ""),
        "from_token": match.group(2),
        "to_amount": match.group(3).replace(",", ""),
        "to_token": match.group(4),
        "tweet_id": tweet_data.get("id"),
        "timestamp": tweet_data.get("createdAt"),
    }


def parse_transfer(tweet_text: str, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a transfer confirmation."""
    match = PATTERNS["transfer"].search(tweet_text)
    if not match:
        return None
    
    return {
        "type": "transfer",
        "amount": match.group(1).replace(",", ""),
        "token": match.group(2),
        "recipient_username": match.group(3),
        "tweet_id": tweet_data.get("id"),
        "timestamp": tweet_data.get("createdAt"),
    }


def parse_failed_deployment(tweet_text: str, tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse a failed deployment."""
    if PATTERNS["hit_limit"].search(tweet_text):
        failed_match = PATTERNS["deployment_failed"].search(tweet_text)
        token_name = failed_match.group(1) if failed_match else "Unknown"
        
        from_user, to_users = identify_from_to(tweet_data)
        
        return {
            "type": "deployment_failed",
            "reason": "deployment_limit",
            "token_name": token_name,
            "from_user": from_user,
            "to_users": to_users,
            "tweet_id": tweet_data.get("id"),
            "timestamp": tweet_data.get("createdAt"),
        }
    
    return None


def classify_bankr_tweet(tweet_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify a tweet from bankrbot and extract relevant info.
    
    Returns a dict with 'type' and parsed data.
    """
    text = tweet_data.get("text", "")
    
    # Try parsing as deployment
    deployment = parse_deployment(text, tweet_data)
    if deployment:
        return deployment
    
    # Try parsing as failed deployment
    failed = parse_failed_deployment(text, tweet_data)
    if failed:
        return failed
    
    # Try parsing as swap
    swap = parse_swap(text, tweet_data)
    if swap:
        return swap
    
    # Try parsing as transfer
    transfer = parse_transfer(text, tweet_data)
    if transfer:
        return transfer
    
    # Unknown/other type
    from_user, to_users = identify_from_to(tweet_data)
    
    return {
        "type": "other",
        "tweet_id": tweet_data.get("id"),
        "text": text[:200],
        "from_user": from_user,
        "to_users": to_users,
        "all_mentions": extract_all_mentions(tweet_data),
    }


def extract_deployments_from_tweets(tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract all deployments from a list of tweets."""
    deployments = []
    for tweet in tweets:
        result = classify_bankr_tweet(tweet)
        if result.get("type") == "deployment":
            deployments.append(result)
    return deployments


# PRE-BANKR: Deployment Request Detection

# Keywords that indicate a deployment request
DEPLOY_KEYWORDS = [
    "deploy", "create", "launch", "mint", "make", "build",
    "token", "clanker", "coin", "memecoin", "meme",
    "name=", "symbol=", "ticker=",
]

# Patterns for extracting token info from requests
PRE_BANKR_PATTERNS = {
    # "deploy TOKEN_NAME symbol SYMBOL"
    "deploy_with_symbol": re.compile(
        r"(?:deploy|create|launch|make)\s+(?:a\s+)?(?:token\s+)?(?:called\s+)?['\"]?(\w+)['\"]?\s+(?:with\s+)?symbol\s+['\"]?(\w+)['\"]?",
        re.IGNORECASE
    ),
    
    # "name=TOKEN symbol=SYMBOL"
    "name_symbol_params": re.compile(
        r"name\s*[=:]\s*['\"]?(\w+)['\"]?.*?symbol\s*[=:]\s*['\"]?(\w+)['\"]?",
        re.IGNORECASE | re.DOTALL
    ),
    
    # "deploy $TICKER"
    "deploy_ticker": re.compile(
        r"(?:deploy|create|launch)\s+\$(\w+)",
        re.IGNORECASE
    ),
    
    # Generic token name after deploy keyword
    "deploy_generic": re.compile(
        r"(?:deploy|create|launch|make)\s+(?:a\s+)?(?:token\s+)?(?:called\s+)?['\"]?([A-Za-z][A-Za-z0-9\s]{1,20})['\"]?",
        re.IGNORECASE
    ),
}


def is_prebankr_request(tweet_data: Dict[str, Any]) -> bool:
    """
    Check if a tweet is a PRE-BANKR deployment request.
    
    Criteria:
    1. Mentions @bankrbot
    2. Author is NOT bankrbot
    3. Contains deployment keywords
    
    Returns:
        True if this is a deployment request
    """
    text = tweet_data.get("text", "").lower()
    author = tweet_data.get("author", {})
    author_username = author.get("userName", "").lower()
    
    # Must NOT be from bankrbot
    if author_username == "bankrbot":
        return False
    
    # Must mention bankrbot
    if "@bankrbot" not in text:
        return False
    
    # Must contain at least one deploy keyword
    for keyword in DEPLOY_KEYWORDS:
        if keyword.lower() in text:
            return True
    
    return False


def extract_token_from_request(text: str) -> Dict[str, Optional[str]]:
    """
    Try to extract token name and symbol from a deployment request.
    
    Returns:
        Dict with 'name' and 'symbol' (may be None)
    """
    result = {"name": None, "symbol": None}
    
    # Try each pattern in order of specificity
    
    # Pattern 1: "deploy TOKEN symbol SYMBOL"
    match = PRE_BANKR_PATTERNS["deploy_with_symbol"].search(text)
    if match:
        result["name"] = match.group(1).strip()
        result["symbol"] = match.group(2).strip().upper()
        return result
    
    # Pattern 2: "name=X symbol=Y"
    match = PRE_BANKR_PATTERNS["name_symbol_params"].search(text)
    if match:
        result["name"] = match.group(1).strip()
        result["symbol"] = match.group(2).strip().upper()
        return result
    
    # Pattern 3: "deploy $TICKER"
    match = PRE_BANKR_PATTERNS["deploy_ticker"].search(text)
    if match:
        ticker = match.group(1).strip().upper()
        result["name"] = ticker
        result["symbol"] = ticker
        return result
    
    # Pattern 4: Generic "deploy TOKENNAME"
    match = PRE_BANKR_PATTERNS["deploy_generic"].search(text)
    if match:
        name = match.group(1).strip()
        # Filter out common false positives
        if name.lower() not in ["a", "the", "my", "this", "token", "coin"]:
            result["name"] = name
            # Generate symbol from name (first letters or truncate)
            result["symbol"] = name.upper().replace(" ", "")[:6]
            return result
    
    return result


def parse_prebankr_request(tweet_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a PRE-BANKR deployment request.
    
    Returns:
        Parsed request info or None if not a valid request
    """
    if not is_prebankr_request(tweet_data):
        return None
    
    text = tweet_data.get("text", "")
    author = tweet_data.get("author", {})
    mentions = extract_all_mentions(tweet_data)
    
    # FROM = author of the tweet
    from_user = {
        "username": author.get("userName", ""),
        "user_id": author.get("id", ""),
        "name": author.get("name", ""),
        "followers": author.get("followers", 0),
        "is_blue_verified": author.get("isBlueVerified", False),
        "role": "requester",
    }
    
    # TO = all mentions EXCEPT bankrbot
    to_users = []
    for mention in mentions:
        if mention.get("username", "").lower() != "bankrbot":
            to_users.append({
                "username": mention.get("username", ""),
                "user_id": mention.get("user_id", ""),
                "name": mention.get("name", ""),
                "role": "mentioned",
            })
    
    # Extract token info from text
    token_info = extract_token_from_request(text)
    
    # Extract any @mention (wallet) pairs
    mentions_with_wallets = extract_all_mentions_with_wallets(text)
    
    return {
        "type": "prebankr_request",
        "tweet_id": tweet_data.get("id"),
        "tweet_url": tweet_data.get("url"),
        "raw_text": text,
        "from_user": from_user,
        "to_users": to_users,
        "token_name": token_info.get("name"),
        "token_symbol": token_info.get("symbol"),
        "all_mentions": mentions,
        "all_mentions_with_wallets": mentions_with_wallets,
        "timestamp": tweet_data.get("createdAt"),
    }


def classify_any_tweet(tweet_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify any tweet - either from bankrbot or TO bankrbot.
    
    Returns:
        Dict with 'type' and parsed data
    """
    author = tweet_data.get("author", {})
    author_username = author.get("userName", "").lower()
    
    # If from bankrbot, use existing classifier
    if author_username == "bankrbot":
        return classify_bankr_tweet(tweet_data)
    
    # Check if it's a PRE-BANKR request
    prebankr = parse_prebankr_request(tweet_data)
    if prebankr:
        return prebankr
    
    # Otherwise, just a random mention
    return {
        "type": "mention",
        "tweet_id": tweet_data.get("id"),
        "from_user": {
            "username": author.get("userName", ""),
            "user_id": author.get("id", ""),
        },
        "text": tweet_data.get("text", "")[:200],
    }
