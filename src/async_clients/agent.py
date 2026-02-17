"""
Async LLM Agent for tweet classification using Google Gemini API directly.

Uses Gemini 2.0 Flash to classify tweets and extract:
- Is it a deploy request?
- FROM/TO users
- Token info
- Fee info
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Google AI API (direct)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

# Fallback to OpenRouter if no Google key
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# =============================================================================
# STRICT CLASSIFICATION PROMPT
# =============================================================================

CLASSIFICATION_PROMPT = """Analyze this tweet to @bankrbot (token deployment bot on Base).

Tweet from @{author}:
"{text}"

=== DEPLOY DETECTION (INTELLIGENT ANALYSIS) ===

is_deploy=true if the user is REQUESTING @bankrbot to create/deploy a NEW token.

ACCEPT these as deploy requests (various forms):
- "deploy X" / "deploying X" / "let's deploy X"
- "launch X" / "launching X" 
- "create X" / "creating X" / "create a token"
- "make X" / "make a token called X"
- "spin up X" / "build X" / "mint X"
- Any creative way of asking to CREATE a new token

REJECT these (NOT deploy requests):
- Past tense: "deployed!" / "just deployed" / "already launched" (done already)
- Existing tokens: "this is a token" / "here's the contract" / "token for you"
- No creation intent: links alone, questions, thank you, swap/buy/sell
- Comments/hype: "nice!" / "lets go!" / "amazing!"

KEY: Analyze INTENT, not just keywords. Is the user ASKING @bankrbot to CREATE something NEW?

=== FEE RECIPIENTS (can be multiple!) ===

Extract ALL fee recipients from phrases like:
- "fees to @A" / "send fees @A" → recipients: ["A"]
- "split fees with @A and @B" → recipients: ["A", "B"]  
- "50% fees @A 50% fees @B" → recipients: ["A", "B"]
- "all fees go to @X" → recipients: ["X"]

If NO explicit fee recipient → recipients: []
NEVER include @bankrbot as recipient!

=== TOKEN EXTRACTION ===

Extract token name and symbol from context:
- "$SYMBOL" or "ticker SYMBOL" → symbol
- "called NAME" or "name NAME" → name
- If same word used for both, use it for both

Return ONLY JSON:
{{
  "is_deploy": boolean,
  "type": "deploy|swap|buy|sell|claim|question|thank_you|hype|comment|other",
  "confidence": 0-100,
  "from": "{author}",
  "token": {{"name": str|null, "symbol": str|null}},
  "fee": {{"recipients": ["username1", "username2"], "split": boolean}},
  "wallets": ["0x..."],
  "reason": "1 sentence"
}}"""


# =============================================================================
# ASYNC AGENT CLIENT
# =============================================================================

class AsyncAgentClient:
    """
    Async LLM agent for tweet classification.
    
    Uses Google Gemini API directly (faster, better rate limits).
    Falls back to OpenRouter if Google API key not set.
    """
    
    def __init__(self, google_api_key: Optional[str] = None, model: Optional[str] = None):
        self.google_api_key = google_api_key or GOOGLE_API_KEY
        self.openrouter_api_key = OPENROUTER_API_KEY
        self.model = model or GEMINI_MODEL
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Determine which API to use
        self.use_google = bool(self.google_api_key)
        if self.use_google:
            logger.info(f"[AGENT] Using Google Gemini API directly (model: {self.model})")
        elif self.openrouter_api_key:
            logger.info(f"[AGENT] Using OpenRouter fallback (model: {OPENROUTER_MODEL})")
        else:
            logger.warning("[AGENT] No API key configured!")
    
    async def __aenter__(self):
        await self._ensure_session()
        return self
    
    async def __aexit__(self, *args):
        await self.close()
    
    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def classify(self, author: str, text: str) -> Dict[str, Any]:
        """
        Classify a tweet using LLM.
        
        Args:
            author: Tweet author username
            text: Tweet text
            
        Returns:
            Classification result dict
        """
        if self.use_google:
            return await self._classify_google(author, text)
        elif self.openrouter_api_key:
            return await self._classify_openrouter(author, text)
        else:
            return {"error": "No API key configured", "is_deploy": False}
    
    async def _classify_google(self, author: str, text: str) -> Dict[str, Any]:
        """Classify using Google Gemini API directly."""
        await self._ensure_session()
        
        prompt = CLASSIFICATION_PROMPT.format(author=author, text=text)
        
        # Google AI API endpoint
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.google_api_key}"
        
        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 1000
            }
        }
        
        try:
            async with self._session.post(url, json=payload, headers={"Content-Type": "application/json"}) as response:
                if response.status != 200:
                    error = await response.text()
                    # Parse error for better message
                    try:
                        error_data = json.loads(error)
                        error_msg = error_data.get("error", {}).get("message", error[:100])
                    except:
                        error_msg = error[:100]
                    return {"error": f"API {response.status}: {error_msg}", "is_deploy": False}
                
                result = await response.json()
                
                # Extract text from Google AI response format
                content = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                
                # Parse JSON from response
                return self._parse_json_response(content)
                    
        except asyncio.TimeoutError:
            return {"error": "Timeout", "is_deploy": False}
        except Exception as e:
            return {"error": str(e), "is_deploy": False}
    
    async def _classify_openrouter(self, author: str, text: str) -> Dict[str, Any]:
        """Classify using OpenRouter API (fallback)."""
        await self._ensure_session()
        
        prompt = CLASSIFICATION_PROMPT.format(author=author, text=text)
        
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://bankr-monitor.local",
            "X-Title": "Bankr Monitor"
        }
        
        payload = {
            "model": OPENROUTER_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 1000,
        }
        
        try:
            async with self._session.post(OPENROUTER_URL, headers=headers, json=payload) as response:
                if response.status != 200:
                    error = await response.text()
                    return {"error": f"API {response.status}: {error[:100]}", "is_deploy": False}
                
                result = await response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                return self._parse_json_response(content)
                    
        except asyncio.TimeoutError:
            return {"error": "Timeout", "is_deploy": False}
        except Exception as e:
            return {"error": str(e), "is_deploy": False}
    
    def _parse_json_response(self, content: str) -> Dict[str, Any]:
        """Parse JSON from LLM response, handling markdown code blocks."""
        content = content.strip()
        
        # Remove markdown code blocks
        if content.startswith("```"):
            # Remove first line (```json or ```)
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()
        
        try:
            parsed = json.loads(content)
            return parsed
        except json.JSONDecodeError:
            return {"error": "JSON parse failed", "is_deploy": False, "_raw": content[:200]}


def format_agent_result_compact(result: Dict[str, Any], author: str = "") -> str:
    """Format agent result - professional, compact."""
    if "error" in result:
        return f"[ERROR] {result['error']}"
    
    is_deploy = result.get("is_deploy", False)
    req_type = result.get("type", "?").upper()
    confidence = result.get("confidence", 0)
    
    status = "DEPLOY" if is_deploy else "NO"
    from_user = result.get("from") or author
    
    # Token
    token = result.get("token") or {}
    token_str = ""
    if token.get("name") or token.get("symbol"):
        name = token.get("name", "")
        symbol = token.get("symbol", "")
        if name and symbol:
            token_str = f" | {name} (${symbol})"
        elif name:
            token_str = f" | {name}"
        elif symbol:
            token_str = f" | ${symbol}"
    
    # Fee recipients (can be multiple)
    fee = result.get("fee") or {}
    fee_str = ""
    recipients = fee.get("recipients", []) or []
    # Handle legacy single recipient format
    if not recipients and fee.get("recipient"):
        recipients = [fee.get("recipient")]
    
    if recipients:
        recipients_clean = [str(r).lstrip("@") for r in recipients if r]
        recipients_str = ", @".join(recipients_clean)
        if fee.get("split"):
            fee_str = f" | Fee: SPLIT -> @{recipients_str}"
        else:
            fee_str = f" | Fee -> @{recipients_str}"
    
    # Wallets
    wallets = result.get("wallets") or []
    wallet_str = f" | {len(wallets)}w" if wallets else ""
    
    # Build output
    line = f"[{status}] {req_type} ({confidence}%) | @{from_user}{token_str}{fee_str}{wallet_str}"
    
    reason = result.get("reason", "")
    if reason:
        return f"{line}\n   {reason}"
    return line


# =============================================================================
# Module-level convenience
# =============================================================================

_client: Optional[AsyncAgentClient] = None


async def get_client() -> AsyncAgentClient:
    """Get or create singleton agent client."""
    global _client
    if _client is None:
        _client = AsyncAgentClient()
    await _client._ensure_session()
    return _client


async def classify_tweet(author: str, text: str) -> Dict[str, Any]:
    """Classify a tweet using the agent."""
    client = await get_client()
    return await client.classify(author, text)
