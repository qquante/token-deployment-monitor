"""
Webhook server for receiving TwitterAPI.io notifications.

This module provides a lightweight HTTP server that receives
webhook POST requests from TwitterAPI.io when tweets match
the configured filter rules.
"""

import os
import json
import logging
from typing import Optional, Callable, List
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
import socketserver

try:
    from src.twitter.models import WebhookPayload
    from src.webhook.handlers import BankrDeploymentHandler, BankrDeploymentSignal
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.twitter.models import WebhookPayload
    from src.webhook.handlers import BankrDeploymentHandler, BankrDeploymentSignal

logger = logging.getLogger(__name__)


class WebhookRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for webhook endpoints."""
    
    # Class-level handlers and API key
    handlers: List[BankrDeploymentHandler] = []
    expected_api_key: Optional[str] = None
    payload_callbacks: List[Callable[[WebhookPayload], None]] = []
    
    def log_message(self, format, *args):
        """Override to use Python logging."""
        logger.debug(f"HTTP: {format % args}")
    
    def do_GET(self):
        """Handle GET requests (health check)."""
        if self.path == '/health':
            self._send_response(200, {'status': 'ok'})
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def do_POST(self):
        """Handle POST requests (webhook)."""
        # Verify API key if configured
        if self.expected_api_key:
            received_key = self.headers.get('X-API-Key', '')
            if received_key != self.expected_api_key:
                logger.warning("Unauthorized webhook request")
                self._send_response(401, {'error': 'Unauthorized'})
                return
        
        # Read body
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            self._send_response(400, {'error': 'Empty body'})
            return
        
        try:
            body = self.rfile.read(content_length)
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            self._send_response(400, {'error': 'Invalid JSON'})
            return
        
        # Parse webhook payload
        try:
            payload = WebhookPayload.from_dict(data)
            logger.info(
                f"Received webhook: rule={payload.rule_tag}, "
                f"tweets={len(payload.tweets)}"
            )
            
            # Notify raw payload callbacks
            for callback in self.payload_callbacks:
                try:
                    callback(payload)
                except Exception as e:
                    logger.error(f"Payload callback error: {e}")
            
            # Process with handlers
            for handler in self.handlers:
                try:
                    signals = handler.process(payload)
                    if signals:
                        logger.info(f"Detected {len(signals)} deployment signals")
                except Exception as e:
                    logger.error(f"Handler error: {e}")
            
            self._send_response(200, {'status': 'ok', 'processed': len(payload.tweets)})
            
        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            self._send_response(500, {'error': str(e)})
    
    def _send_response(self, status_code: int, data: dict):
        """Send JSON response."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))


class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """Threaded HTTP server for concurrent request handling."""
    allow_reuse_address = True


@dataclass
class WebhookServer:
    """
    Webhook server for TwitterAPI.io notifications.
    
    Runs an HTTP server that receives webhook POST requests
    and processes them with registered handlers.
    
    Example:
        server = WebhookServer(port=8080)
        
        # Register deployment handler
        handler = BankrDeploymentHandler()
        handler.on_deployment(lambda s: print(f"New deployment: {s.tweet_url}"))
        server.add_handler(handler)
        
        # Start server
        server.start()  # Runs in background thread
    """
    
    host: str = "0.0.0.0"
    port: int = 8080
    api_key: Optional[str] = None
    handlers: List[BankrDeploymentHandler] = field(default_factory=list)
    _server: Optional[ThreadedHTTPServer] = field(default=None, init=False)
    _thread: Optional[Thread] = field(default=None, init=False)
    
    def __post_init__(self):
        """Initialize with API key from env if not provided."""
        if self.api_key is None:
            self.api_key = os.getenv("TWITTER_API_KEY")
    
    def add_handler(self, handler: BankrDeploymentHandler) -> None:
        """Add a deployment handler."""
        self.handlers.append(handler)
    
    def on_payload(self, callback: Callable[[WebhookPayload], None]) -> None:
        """Register callback for raw webhook payloads."""
        WebhookRequestHandler.payload_callbacks.append(callback)
    
    def start(self, blocking: bool = False) -> None:
        """
        Start the webhook server.
        
        Args:
            blocking: If True, run in foreground. If False, run in background thread.
        """
        # Configure handler class
        WebhookRequestHandler.handlers = self.handlers
        WebhookRequestHandler.expected_api_key = self.api_key
        
        # Create server
        self._server = ThreadedHTTPServer(
            (self.host, self.port),
            WebhookRequestHandler
        )
        
        logger.info(f"Webhook server starting on {self.host}:{self.port}")
        
        if blocking:
            self._server.serve_forever()
        else:
            self._thread = Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            logger.info("Webhook server running in background")
    
    def stop(self) -> None:
        """Stop the webhook server."""
        if self._server:
            self._server.shutdown()
            logger.info("Webhook server stopped")
    
    @property
    def url(self) -> str:
        """Get the webhook URL."""
        return f"http://{self.host}:{self.port}/"


def create_app(
    port: int = 8080,
    api_key: Optional[str] = None
) -> WebhookServer:
    """
    Factory function to create configured webhook server.
    
    Args:
        port: Server port
        api_key: API key for verification
        
    Returns:
        Configured WebhookServer with default handler
    """
    server = WebhookServer(port=port, api_key=api_key)
    
    # Add default Bankr handler
    handler = BankrDeploymentHandler()
    server.add_handler(handler)
    
    return server
