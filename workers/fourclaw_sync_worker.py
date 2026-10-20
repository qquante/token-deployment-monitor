"""
4claw Board Sync Worker

Monitors token launches on 4claw.org crypto board.

Responsibilities:
- Periodically fetch threads from 4claw.org/b/crypto
- Parse HTML to extract !clawnch commands
- Extract token data from thread posts
- Send enriched notifications to Telegram
- Dedupe processed threads

Output Channels:
- telegram:send - Pre-Clawnch notifications
"""

import os
import re
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime
import html
from html.parser import HTMLParser

from workers.base import BaseWorker
from shared.pubsub import Channels
from src.async_clients.alphagate import AsyncAlphaGateClient
from src.async_clients.twitter import AsyncTwitterClient


# HTML PARSER FOR 4CLAW BOARD

class FourClawThreadParser(HTMLParser):
    """
    Parse 4claw HTML board to extract threads with !clawnch commands.
    
    HTML Structure:
    <div class="claw-thread">
        <div class="flex flex-wrap items-baseline gap-x-2 gap-y-1 text-xs mb-2">
            <a href="/t/{uuid}" class="font-bold">🦞 TokenName (SYMBOL)</a>
            <span class="text-zinc-700">
                replies <span class="font-mono">0</span>
                · bump <span class="font-mono"><time dateTime="...">...</time></span>
            </span>
        </div>
        <div class="claw-post op">
            <div class="claw-post-header">
                <span class="claw-post-name">
                    <span class="inline-flex items-baseline gap-2">
                        <span>AgentName</span>
                    </span>
                </span>
                <span class="claw-post-date">
                    <time dateTime="2026-02-02T00:53:53.645Z">...</time>
                </span>
            </div>
            <div class="claw-post-body mt-2">
                <span>!clawnch<br/></span>
                <span>name: TokenName<br/></span>
                <span>symbol: SYMBOL<br/></span>
                <span>wallet: 0x...<br/></span>
                <span>description: ...<br/></span>
                <span>image: https://...<br/></span>
                <span>website: https://...<br/></span>
                <span>twitter: @handle</span>
            </div>
        </div>
    </div>
    """
    
    def __init__(self):
        super().__init__()
        self.threads = []
        self.current_thread = None
        self.in_thread = False
        self.in_post_header = False
        self.in_post_name = False
        self.in_post_date = False
        self.in_post_body = False
        self.in_thread_link = False
        self.content_buffer = []
        self.current_time_attr = None
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        class_name = attrs_dict.get('class', '')
        
        # Start of thread
        if tag == 'div' and 'claw-thread' in class_name:
            self.current_thread = {
                'id': '',  # Thread UUID
                'title': '',  # Thread title (e.g. "🦞 OpenYoda (OPENYODA)")
                'author_name': '',
                'created_at': '',
                'content': '',
            }
            self.in_thread = True
        
        # Thread link (contains UUID)
        elif self.in_thread and tag == 'a' and 'font-bold' in class_name:
            href = attrs_dict.get('href', '')
            if href.startswith('/t/'):
                self.current_thread['id'] = href[3:]  # Remove '/t/' prefix
            self.in_thread_link = True
        
        # Post header
        elif self.in_thread and tag == 'div' and 'claw-post-header' in class_name:
            self.in_post_header = True
        
        # Post name
        elif self.in_post_header and tag == 'span' and 'claw-post-name' in class_name:
            self.in_post_name = True
        
        # Post date
        elif self.in_post_header and tag == 'span' and 'claw-post-date' in class_name:
            self.in_post_date = True
        
        # Time element (get dateTime attribute)
        elif tag == 'time' and self.in_post_date:
            self.current_time_attr = attrs_dict.get('datetime', '')
        
        # Post body
        elif self.in_thread and tag == 'div' and 'claw-post-body' in class_name:
            self.in_post_body = True
            self.content_buffer = []
    
    def handle_endtag(self, tag):
        # End of thread
        if tag == 'div' and self.in_thread and self.current_thread:
            # Check if we have content (meaning post body ended)
            if self.content_buffer:
                self.current_thread['content'] = ''.join(self.content_buffer).strip()
                self.content_buffer = []
            
            # Only add threads with actual !clawnch content
            if self.current_thread['id'] and '!clawnch' in self.current_thread.get('content', '').lower():
                self.threads.append(self.current_thread)
                self.current_thread = {
                    'id': '',
                    'title': '',
                    'author_name': '',
                    'created_at': '',
                    'content': '',
                }
        
        # End of post body
        elif tag == 'div' and self.in_post_body:
            self.in_post_body = False
            if self.current_thread:
                self.current_thread['content'] = ''.join(self.content_buffer).strip()
        
        # End of post header
        elif tag == 'div' and self.in_post_header:
            self.in_post_header = False
        
        # End of post name
        elif tag == 'span' and self.in_post_name:
            self.in_post_name = False
        
        # End of post date
        elif tag == 'span' and self.in_post_date:
            self.in_post_date = False
        
        # End of thread link
        elif tag == 'a' and self.in_thread_link:
            self.in_thread_link = False
        
        # End of time element
        elif tag == 'time' and self.current_time_attr:
            if self.current_thread and self.in_post_date:
                self.current_thread['created_at'] = self.current_time_attr
            self.current_time_attr = None
    
    def handle_data(self, data):
        if not self.current_thread:
            return
        
        # Thread title
        if self.in_thread_link:
            self.current_thread['title'] = data.strip()
        
        # Author name (innermost span in claw-post-name)
        elif self.in_post_name:
            # Only capture non-empty text
            text = data.strip()
            if text and not self.current_thread['author_name']:
                self.current_thread['author_name'] = text
        
        # Post body content
        elif self.in_post_body:
            self.content_buffer.append(data)
    
    def handle_startendtag(self, tag, attrs):
        # Handle <br/> tags in post body
        if tag == 'br' and self.in_post_body:
            self.content_buffer.append('\n')


class FourClawSyncWorker(BaseWorker):
    """
    Monitors Pre-Clawnch token launches via 4claw.org crypto board.
    
    Features:
    - Poll 4claw.org/b/crypto for threads
    - Parse HTML to extract !clawnch commands
    - Send formatted Telegram notifications
    """
    
    WORKER_NAME = "4claw-sync"
    
    # API
    FOURCLAW_BOARD_URL = "https://www.4claw.org/b/crypto"
    
    # Polling interval (seconds)
    POLL_INTERVAL = 10
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 10
    
    # Regex patterns to extract token data from !clawnch content
    CLAWNCH_PATTERNS = {
        'name': re.compile(r'name:\s*(.+?)(?:\n|$)', re.IGNORECASE),
        'symbol': re.compile(r'symbol:\s*(.+?)(?:\n|$)', re.IGNORECASE),
        'wallet': re.compile(r'wallet:\s*(0x[a-fA-F0-9]+)', re.IGNORECASE),
        'description': re.compile(r'description:\s*(.+?)(?:\n|$)', re.IGNORECASE),
        'image': re.compile(r'image:\s*(https?://\S+)', re.IGNORECASE),
        'website': re.compile(r'website:\s*(https?://\S+)', re.IGNORECASE),
        'twitter': re.compile(r'twitter:\s*(@?\S+)', re.IGNORECASE),
    }
    
    def __init__(self):
        super().__init__()
        
        # Telegram channel for Pre-Clawnch notifications
        self.telegram_chat_preclawnch = os.getenv("TELEGRAM_CHAT_ID_PRECLAWNCH", "")
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._alphagate: Optional[AsyncAlphaGateClient] = None
        
        # Track processed threads
        self._processed_threads: set = set()
        
        # Stats
        self.stats.update({
            "threads_processed": 0,
            "tokens_found": 0,
            "api_calls": 0,
            "alphagate_lookups": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
        })
        
        # Initialize AlphaGate client
        self._alphagate = AsyncAlphaGateClient()
        await self._alphagate._ensure_session()
        
        self.logger.info(f"[4CLAW] Initialized")
        self.logger.info(f"[4CLAW] Board URL: {self.FOURCLAW_BOARD_URL}")
        self.logger.info(f"[4CLAW] Telegram Channel: {self.telegram_chat_preclawnch}")
        self.logger.info(f"[4CLAW] Poll interval: {self.POLL_INTERVAL}s")
    
    async def teardown(self):
        """Close HTTP session and AlphaGate client."""
        if self._session:
            await self._session.close()
        if self._alphagate:
            await self._alphagate.close()
        await super().teardown()
    
    async def run(self):
        """Main loop - poll 4claw board."""
        self.logger.info("Starting 4claw board sync loop...")
        
        while self._running:
            try:
                await self._sync_board()
            except Exception as e:
                self.logger.error(f"[4CLAW] Sync error: {e}", exc_info=True)
                self.stats["errors"] += 1
            
            await asyncio.sleep(self.POLL_INTERVAL)
    
    # BOARD SYNC
    
    async def _sync_board(self):
        """Fetch and process threads from 4claw crypto board."""
        self.logger.debug("[4CLAW] Fetching board...")
        
        html_content = None
        
        # Retry with exponential backoff
        for attempt in range(self.MAX_RETRIES):
            try:
                self.stats["api_calls"] += 1
                
                async with self._session.get(self.FOURCLAW_BOARD_URL) as resp:
                    if resp.status != 200:
                        self.logger.warning(f"[4CLAW] HTTP {resp.status} (attempt {attempt + 1}/{self.MAX_RETRIES})")
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_DELAY * (2 ** attempt))
                            continue
                        return
                    
                    html_content = await resp.text()
                    break
                    
            except Exception as e:
                self.logger.error(f"[4CLAW] HTTP error (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY * (2 ** attempt))
                    continue
                return
        
        if not html_content:
            return
        
        # Parse HTML
        parser = FourClawThreadParser()
        try:
            parser.feed(html_content)
        except Exception as e:
            self.logger.error(f"[4CLAW] HTML parse error: {e}")
            return
        
        threads = parser.threads
        self.logger.debug(f"[4CLAW] Found {len(threads)} !clawnch threads")
        
        # Process new threads
        new_threads = 0
        for thread in threads:
            thread_id = thread.get('id', '')
            
            if not thread_id:
                continue
            
            # Check Redis for already processed
            if await self._is_thread_processed(thread_id):
                continue
            
            # Check local cache
            if thread_id in self._processed_threads:
                continue
            
            # Extract token data
            token = self._parse_clawnch_content(thread.get('content', ''))
            
            if not token.get('name') or not token.get('symbol'):
                self.logger.debug(f"[4CLAW] Skipping thread {thread_id[:16]}... - incomplete token data")
                continue
            
            # Mark as processed
            await self._mark_thread_processed(thread_id)
            self._processed_threads.add(thread_id)
            
            self.logger.info(f"[4CLAW] New token: {token.get('name')} (${token.get('symbol')})")
            
            # Process token launch
            await self._process_token_launch(thread, token)
            
            new_threads += 1
            self.stats["threads_processed"] += 1
            self.stats["tokens_found"] += 1
        
        if new_threads > 0:
            self.logger.info(f"[4CLAW] Processed {new_threads} new tokens")
    
    def _parse_clawnch_content(self, content: str) -> Dict[str, str]:
        """
        Parse !clawnch command content to extract token data.
        
        Format:
        !clawnch
        name: TokenName
        symbol: SYMBOL
        wallet: 0x...
        description: ...
        image: https://...
        website: https://...
        twitter: @handle OR https://x.com/handle OR https://twitter.com/handle
        """
        token = {}
        
        for field, pattern in self.CLAWNCH_PATTERNS.items():
            match = pattern.search(content)
            if match:
                value = match.group(1).strip()
                token[field] = value
        
        # Also store extracted twitter handle (for storage linking)
        if token.get('twitter'):
            token['twitter_handle'] = self._extract_twitter_handle(token['twitter'])
        
        return token
    
    def _extract_twitter_handle(self, value: str) -> str:
        """
        Extract clean Twitter handle from various formats:
        - @handle -> handle
        - https://x.com/handle -> handle
        - https://twitter.com/handle -> handle
        - https://x.com/i/communities/123 -> '' (not a handle)
        """
        import re
        
        # Remove @ prefix
        if value.startswith('@'):
            return value[1:]
        
        # Extract from x.com/username or twitter.com/username
        url_match = re.search(r'(?:x\.com|twitter\.com)/([a-zA-Z0-9_]+)(?:\?|$|/(?!i/))', value)
        if url_match:
            handle = url_match.group(1)
            # Ignore special pages like 'i' (for communities, etc)
            if handle.lower() not in ('i', 'home', 'explore', 'search'):
                return handle
        
        # If it's not a URL, return as-is
        if not value.startswith('http'):
            return value
        
        return ''  # URL but not a valid handle
    
    # ALPHAGATE DATA
    
    async def _get_alphagate_data(self, x_handle: str) -> Dict[str, Any]:
        """Get AlphaGate data (R, C, SF, followers, bio, created_at) for a Twitter handle."""
        if not x_handle:
            return {}
        
        x_handle = x_handle.lower().lstrip("@")
        
        # Try cache first
        try:
            cached = await self.storage.get_alphagate_data(x_handle)
            if cached:
                self.logger.debug(f"[4CLAW] AlphaGate CACHE HIT @{x_handle}")
                return cached
        except Exception as e:
            self.logger.debug(f"[4CLAW] AlphaGate cache error: {e}")
        
        # Fetch from API
        try:
            self.stats["alphagate_lookups"] += 1
            self.logger.info(f"[4CLAW] AlphaGate calling API for @{x_handle}...")
            result = await self._alphagate.get_child_data(x_handle)
            if result:
                r_count = result.get("prev_usernames_count", 0)
                c_count = result.get("contracts_count", 0)
                sf_count = result.get("smart_followers", 0)
                followers = result.get("followers_count", 0)
                bio = result.get("description", "")
                created_at = result.get("created_at", "")
                
                self.logger.info(f"[4CLAW] AlphaGate @{x_handle}: R={r_count} C={c_count} SF={sf_count} followers={followers}")
                
                # Cache for future use
                try:
                    await self.storage.save_alphagate_data(x_handle, r_count, c_count, sf_count)
                except:
                    pass
                
                return {
                    "prev_usernames_count": r_count,
                    "contracts_count": c_count,
                    "smart_followers": sf_count,
                    "followers_count": followers,
                    "bio": bio,
                    "created_at": created_at,
                }
            else:
                self.logger.info(f"[4CLAW] AlphaGate @{x_handle}: NO DATA (result={result})")
        except Exception as e:
            self.logger.info(f"[4CLAW] AlphaGate API error @{x_handle}: {e}")
        
        return {}
    
    async def _get_twitter_data(self, x_handle: str) -> Dict[str, Any]:
        """Get Twitter profile data (bio, followers, created_at, verified, affiliate_label)."""
        if not x_handle:
            return {}
        
        x_handle = x_handle.lower().lstrip("@")
        
        # CHECK CACHE FIRST (24h TTL) - GLOBAL CACHE SHARED WITH ALL WORKERS
        try:
            cached = await self.storage.get_twitter_user(x_handle)
            if cached:
                self.logger.debug(f"[4CLAW] Twitter CACHE HIT @{x_handle}")
                return {
                    "followers_count": cached.get("followers", 0),
                    "description": cached.get("bio", ""),
                    "created_at": cached.get("created_at", ""),
                    "is_blue_verified": cached.get("is_blue_verified", False),
                    "name": cached.get("name", ""),
                    "affiliate_label": cached.get("affiliate_label", ""),
                }
        except Exception as e:
            self.logger.debug(f"[4CLAW] Twitter cache error: {e}")
        
        # CALL API ONLY IF NOT IN CACHE
        try:
            async with AsyncTwitterClient() as client:
                # Get user info (followers, bio, verified)
                user_info = await client.get_user_info(x_handle)
                
                # Get user about (affiliate label - company badge)
                user_about = await client.get_user_about(x_handle)
                affiliate_label = None
                if user_about:
                    affiliate_label = user_about.get("affiliate_label")
                
                if user_info:
                    self.logger.info(f"[4CLAW] Twitter API @{x_handle}: followers={user_info.get('followers', 0)}, verified={user_info.get('isBlueVerified', False)}, affiliate={affiliate_label}")
                    return {
                        "followers_count": user_info.get("followers", 0),
                        "description": user_info.get("description", ""),
                        "created_at": user_info.get("createdAt", ""),
                        "is_blue_verified": user_info.get("isBlueVerified", False),
                        "name": user_info.get("name", ""),
                        "affiliate_label": affiliate_label,  # Company badge (X, xAI, APPLE, etc)
                    }
        except Exception as e:
            self.logger.debug(f"[4CLAW] Twitter API error @{x_handle}: {e}")
        
        return {}

    # PROCESS TOKEN LAUNCH
    
    async def _process_token_launch(self, thread: Dict[str, Any], token: Dict[str, str]):
        """Process token launch and send notification."""
        thread_id = thread.get('id', '')
        author_name = thread.get('author_name', '')
        
        try:
            # Save Pre-Clawnch token for linking
            token_wallet = token.get('wallet', '')
            twitter_raw = token.get('twitter', '')
            x_handle = token.get('twitter_handle', '')  # Use extracted handle, not raw value
            
            # Debug log
            if twitter_raw:
                self.logger.info(f"[4CLAW] Twitter raw: {twitter_raw[:50]} -> handle: {x_handle}")
            
            # Get AlphaGate data if we have twitter handle
            alphagate_data = {}
            twitter_data = {}
            if x_handle:
                alphagate_data = await self._get_alphagate_data(x_handle)
                twitter_data = await self._get_twitter_data(x_handle)
                if not alphagate_data:
                    self.logger.debug(f"[4CLAW] AlphaGate returned empty for @{x_handle}")
            
            # Format post_id as "thread:{uuid}" for 4claw
            post_id = f"thread:{thread_id}"
            
            await self.storage.save_preclawnch_token(
                post_id=post_id,
                platform="4claw",
                token_name=token.get('name', ''),
                token_symbol=token.get('symbol', ''),
                token_wallet=token_wallet,
                agent_name=author_name,
                x_handle=x_handle,
            )
            
            # Save twitter_data to cache for DEPLOYED CLAWNCH
            if twitter_data and x_handle:
                try:
                    await self.storage.save_twitter_user(
                        username=x_handle,
                        user_id="",
                        name=twitter_data.get("name", ""),
                        followers=twitter_data.get("followers_count", 0),
                        following=0,
                        is_blue_verified=twitter_data.get("is_blue_verified", False),
                        verified_type="",
                        created_at=twitter_data.get("created_at", ""),
                        bio=twitter_data.get("description", ""),
                        affiliate_label=twitter_data.get("affiliate_label", ""),
                    )
                except Exception as e:
                    self.logger.debug(f"[4CLAW] Twitter cache save error: {e}")
            
            # Format message with AlphaGate and Twitter data
            message = self._format_message(thread, token, alphagate_data, twitter_data)
            
            # Create inline buttons
            buttons = self._create_inline_buttons(thread, token)
            
            # Send to Telegram
            await self._send_telegram_notification(message, buttons)
            
            self.logger.info(f"[PRECLAWNCH] Sent Telegram notification for {token.get('symbol')} (thread_id={thread_id[:16]}...)")
        
        except Exception as e:
            self.logger.error(f"[PRECLAWNCH] Error processing thread {thread_id}: {e}", exc_info=True)
    
    def _format_message(
        self,
        thread: Dict[str, Any],
        token: Dict[str, str],
        alphagate_data: Dict[str, Any] = None,
        twitter_data: Dict[str, Any] = None,
    ) -> str:
        """Format Pre-Clawnch message for Telegram (HTML)."""
        alphagate_data = alphagate_data or {}
        twitter_data = twitter_data or {}
        
        def esc(text):
            return html.escape(str(text)) if text else ""
        
        def fmt_date(date_str):
            if not date_str:
                return ""
            try:
                # Twitter format: "Tue Mar 15 00:00:00 +0000 2022" or ISO
                if "T" in str(date_str):
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
                return dt.strftime("%d %b %Y")
            except:
                try:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    return dt.strftime("%d %b %Y")
                except:
                    return date_str[:10] if len(date_str) > 10 else date_str
        
        def fmt_number(n):
            try:
                n = int(n)
                if n >= 1_000_000:
                    return f"{n/1_000_000:.1f}M"
                elif n >= 1_000:
                    return f"{n/1_000:.1f}K"
                return str(n)
            except:
                return str(n)
        
        lines = []
        
        # Get data
        author_name = thread.get('author_name', 'Unknown')
        thread_id = thread.get('id', '')
        thread_url = f"https://4claw.org/t/{thread_id}"
        
        token_name = token.get('name', 'Unknown')
        token_symbol = token.get('symbol', '???')
        token_wallet = token.get('wallet', '')
        token_desc = token.get('description', '')
        token_image = token.get('image', '')
        token_website = token.get('website', '')
        
        x_handle = token.get('twitter_handle', '') or token.get('twitter', '').lstrip('@')
        if x_handle.startswith('http'):
            x_handle = ''  # Not a clean handle
        
        # AlphaGate data
        r_count = alphagate_data.get("prev_usernames_count", 0)
        c_count = alphagate_data.get("contracts_count", 0)
        sf_count = alphagate_data.get("smart_followers", 0)
        
        # Twitter API data (bio, followers, created_at, verified, affiliate)
        twitter_followers = twitter_data.get("followers_count", 0)
        twitter_bio = twitter_data.get("description", "")
        twitter_created = twitter_data.get("created_at", "")
        is_verified = twitter_data.get("is_blue_verified", False)
        affiliate_label = twitter_data.get("affiliate_label", "")  # Company badge (X, xAI, etc)
        
        # Header with monospace token name
        lines.append("🟠 <b>NEW PRE CLAWNCH</b>")
        lines.append("")
        lines.append(f"<code>{esc(token_name)} (${esc(token_symbol)})</code> by 🦀 <b>4CLAW</b>")
        lines.append("")
        
        # ==================== AGENT SECTION ====================
        lines.append("━━━━━━━🤖 <b>AGENT</b>━━━━━━━")
        lines.append("")
        
        # Agent name with link (4claw is anonymous)
        if thread_url:
            lines.append(f'🤖 <b>Agent:</b> <a href="{thread_url}">{esc(author_name)}</a>')
        else:
            lines.append(f'🤖 <b>Agent:</b> {esc(author_name)}')
        
        # Note: 4claw is anonymous, no karma/followers
        lines.append("⭐️ <b>Karma:</b> N/A (anônimo)")
        lines.append("👥 <b>4Claw Followers:</b> N/A (anônimo)")
        lines.append("")
        
        # ==================== OWNER SECTION ====================
        if x_handle:
            lines.append("━━━━━━━👨🏻 <b>OWNER</b>━━━━━━━")
            lines.append("")
            
            # Twitter handle with R/C and verification + company badge
            verified_icon = ""
            if is_verified:
                verified_icon = " ✅"
            if affiliate_label:
                verified_icon += f" 🏢{affiliate_label}"
            
            lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{x_handle}">@{esc(x_handle)}</a> (R:{r_count} | C:{c_count}){verified_icon}')
            
            # Twitter followers
            if twitter_followers:
                lines.append(f"👤 <b>Followers:</b> {fmt_number(twitter_followers)}")
            
            # Smart followers with alert (HIGH = positive ⛔️⚠️)
            sf_alert = " ⛔️⚠️⛔️⚠️⛔️⚠️" if sf_count >= 15 else ""
            lines.append(f"⭐️ <b>Smart Followers:</b> {sf_count}{sf_alert}")
            
            # Created at
            if twitter_created:
                lines.append(f"📅 <b>Criado em:</b> {fmt_date(twitter_created)}")
            
            # Bio
            if twitter_bio:
                bio_display = twitter_bio[:100] + "..." if len(twitter_bio) > 100 else twitter_bio
                lines.append(f"📕 <b>Bio:</b> {esc(bio_display)}")
            
            lines.append("")
        
        # ==================== TOKEN SECTION ====================
        lines.append("━━━━━━━🪙 <b>TOKEN</b>━━━━━━━")
        lines.append("")
        
        lines.append(f"📛 <b>Name:</b> {esc(token_name)} (${esc(token_symbol)})")
        
        if token_wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{token_wallet}</code>")
        
        if token_website:
            lines.append(f'🌐 <b>Web:</b> <a href="{token_website}">{esc(token_website[:40])}...</a>' if len(token_website) > 40 else f'🌐 <b>Web:</b> <a href="{token_website}">{esc(token_website)}</a>')
        
        if token_image:
            lines.append(f'🖼 <b>Image:</b> <a href="{token_image}">Link</a>')
        
        if token_desc:
            desc_display = token_desc[:200] + "..." if len(token_desc) > 200 else token_desc
            lines.append(f"📕 <b>Description:</b> {esc(desc_display)}")
        
        # Footer with monospace token name
        lines.append("")
        lines.append(f"<code>{esc(token_name)} (${esc(token_symbol)})</code> by 🦀 <b>4CLAW</b>")
        
        return "\n".join(lines)
    
    def _create_inline_buttons(
        self,
        thread: Dict[str, Any],
        token: Dict[str, str],
    ) -> List[List[Dict[str, str]]]:
        """Create inline keyboard buttons for Telegram.
        
        Layout:
        Row 1: [Agent] [Thread] [Owner] (external links)
        Row 2: [Like] [Buy] (actions with post_id callbacks)
        """
        keyboard = []
        
        thread_id = thread.get('id', '')
        thread_url = f"https://4claw.org/t/{thread_id}"
        author_name = thread.get('author_name', '')
        
        x_handle = token.get('twitter_handle', '') or token.get('twitter', '').lstrip('@')
        if x_handle.startswith('http'):
            x_handle = ''  # Not a clean handle
        
        # post_id for 4claw uses format "thread:{uuid}"
        post_id = f"thread:{thread_id}" if thread_id else ""
        
        # Row 1: External links [Agent] [Thread] [Owner]
        row1 = []
        
        # "Agent" (just links to thread since 4claw is anonymous)
        if thread_id:
            row1.append({
                "text": "👤 Agent",
                "url": thread_url
            })
        
        # 4claw Thread
        if thread_id:
            row1.append({
                "text": "📄 Thread",
                "url": thread_url
            })
        
        # Owner Twitter
        if x_handle:
            row1.append({
                "text": "🐦 Owner",
                "url": f"https://x.com/{x_handle}"
            })
        
        if row1:
            keyboard.append(row1)
        
        # Row 2: Actions [Like] [Buy] with post_id
        if post_id:
            keyboard.append([
                {
                    "text": "❤️ Like",
                    "callback_data": f"like:{post_id}"
                },
                {
                    "text": "💰 Buy",
                    "callback_data": f"buy:{post_id}"
                }
            ])
        
        return keyboard
    
    async def _send_telegram_notification(
        self,
        message: str,
        buttons: List[List[Dict[str, str]]]
    ):
        """Send notification to Telegram via pub/sub."""
        try:
            payload = {
                "chat_id": self.telegram_chat_preclawnch,
                "text": message,
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": buttons},
                "disable_web_page_preview": True,
                "channel": "preclawnch",  # Required for telegram-sender routing
            }
            
            await self.publish(Channels.TELEGRAM_SEND, payload)
            
        except Exception as e:
            self.logger.error(f"[4CLAW] Telegram send error: {e}")
            self.stats["errors"] += 1

    # DEDUPE

    async def _is_thread_processed(self, thread_id: str) -> bool:
        """Check if thread was already processed."""
        return await self.redis.sismember("4claw_threads_processed", thread_id)

    async def _mark_thread_processed(self, thread_id: str):
        """Mark thread as processed."""
        await self.redis.sadd("4claw_threads_processed", thread_id)
        # Keep for 30 days
        await self.redis.expire("4claw_threads_processed", 86400 * 30)


# MODULE ENTRY POINT

if __name__ == "__main__":
    FourClawSyncWorker.main()

