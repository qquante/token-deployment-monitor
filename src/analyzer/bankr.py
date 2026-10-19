"""
Bankr deployment analyzer.

This module provides filtering and analysis capabilities
specifically for Bankr token deployments. It identifies
Bankr deployments by checking the tokenContext interface
field and provides statistical analysis.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from collections import Counter

try:
    from config import bankr_config
    from src.decoder import DecodedTokenEvent
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from config import bankr_config
    from src.decoder import DecodedTokenEvent

logger = logging.getLogger(__name__)


@dataclass
class BankrStats:
    """
    Statistical summary of Bankr deployments.
    
    Attributes:
        total_events: Total TokenCreated events analyzed
        bankr_count: Number of Bankr deployments
        other_count: Number of non-Bankr deployments
        platforms: Count by platform (farcaster, X, etc.)
        descriptions: Count by description pattern
        deployers: Count by msg_sender address
        interfaces: Count by interface (non-Bankr)
        unique_ids: Count of unique tokenContext IDs
        with_image: Count with token image
        with_extensions: Count with extensions
    """
    
    total_events: int = 0
    bankr_count: int = 0
    other_count: int = 0
    platforms: Dict[str, int] = field(default_factory=dict)
    descriptions: Dict[str, int] = field(default_factory=dict)
    deployers: Dict[str, int] = field(default_factory=dict)
    interfaces: Dict[str, int] = field(default_factory=dict)
    unique_ids: int = 0
    with_image: int = 0
    with_extensions: int = 0
    block_range: Tuple[int, int] = (0, 0)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'total_events': self.total_events,
            'bankr_count': self.bankr_count,
            'other_count': self.other_count,
            'platforms': self.platforms,
            'descriptions': self.descriptions,
            'deployers': dict(list(self.deployers.items())[:10]),  # Top 10
            'interfaces': self.interfaces,
            'unique_ids': self.unique_ids,
            'with_image': self.with_image,
            'with_extensions': self.with_extensions,
            'block_range': self.block_range,
        }


class BankrAnalyzer:
    """
    Analyzer for identifying and filtering Bankr deployments.
    
    Provides methods to:
    - Filter Bankr deployments from all TokenCreated events
    - Generate statistical summaries
    - Identify deployment patterns
    
    Example:
        analyzer = BankrAnalyzer()
        bankr, others = analyzer.filter_bankr(all_events)
        stats = analyzer.compute_stats(all_events, bankr)
    """
    
    def __init__(self, interface_name: Optional[str] = None):
        """
        Initialize analyzer.
        
        Args:
            interface_name: Interface to filter for (default: "Bankr")
        """
        self.interface_name = interface_name or bankr_config.INTERFACE_NAME
    
    def is_bankr_deployment(self, event: DecodedTokenEvent) -> bool:
        """
        Check if event is a Bankr deployment.
        
        Identifies Bankr deployments by checking the 'interface'
        field in tokenContext JSON.
        
        Args:
            event: Decoded token event
            
        Returns:
            True if this is a Bankr deployment
        """
        interface = event.context_parsed.get('interface', '')
        return interface == self.interface_name
    
    def filter_bankr(
        self,
        events: List[DecodedTokenEvent]
    ) -> Tuple[List[DecodedTokenEvent], List[DecodedTokenEvent]]:
        """
        Separate Bankr and non-Bankr deployments.
        
        Args:
            events: List of all decoded token events
            
        Returns:
            Tuple of (bankr_events, other_events)
        """
        bankr_events = []
        other_events = []
        
        for event in events:
            if self.is_bankr_deployment(event):
                bankr_events.append(event)
            else:
                other_events.append(event)
        
        logger.info(
            f"Filtered {len(bankr_events)} Bankr deployments "
            f"from {len(events)} total events"
        )
        
        return bankr_events, other_events
    
    def compute_stats(
        self,
        all_events: List[DecodedTokenEvent],
        bankr_events: List[DecodedTokenEvent]
    ) -> BankrStats:
        """
        Compute comprehensive statistics.
        
        Args:
            all_events: All TokenCreated events
            bankr_events: Filtered Bankr events
            
        Returns:
            BankrStats with all computed metrics
        """
        stats = BankrStats()
        stats.total_events = len(all_events)
        stats.bankr_count = len(bankr_events)
        stats.other_count = len(all_events) - len(bankr_events)
        
        # Platform stats
        platform_counter = Counter()
        for event in bankr_events:
            platform = event.context_parsed.get('platform', 'unknown')
            platform_counter[platform] += 1
        stats.platforms = dict(platform_counter.most_common())
        
        # Description stats
        desc_counter = Counter()
        for event in bankr_events:
            desc = event.metadata_parsed.get('description', '')
            desc_counter[desc] += 1
        stats.descriptions = dict(desc_counter.most_common())
        
        # Deployer stats
        deployer_counter = Counter()
        for event in bankr_events:
            deployer_counter[event.msg_sender] += 1
        stats.deployers = dict(deployer_counter.most_common())
        
        # Interface stats (non-Bankr)
        interface_counter = Counter()
        for event in all_events:
            if not self.is_bankr_deployment(event):
                interface = event.context_parsed.get('interface', 'unknown')
                interface_counter[interface] += 1
        stats.interfaces = dict(interface_counter.most_common())
        
        # Unique IDs
        unique_ids = set(
            event.context_parsed.get('id', '')
            for event in bankr_events
        )
        stats.unique_ids = len(unique_ids)
        
        # Image and extensions
        stats.with_image = sum(1 for e in bankr_events if e.token_image)
        stats.with_extensions = sum(1 for e in bankr_events if e.extensions)
        
        # Block range
        if bankr_events:
            blocks = [e.block_number for e in bankr_events]
            stats.block_range = (min(blocks), max(blocks))
        
        return stats
    
    def extract_twitter_urls(
        self,
        events: List[DecodedTokenEvent]
    ) -> List[Dict[str, Any]]:
        """
        Extract Twitter/X URLs from Bankr deployments.
        
        Args:
            events: List of Bankr events
            
        Returns:
            List of dicts with URL and token info
        """
        urls = []
        for event in events:
            msg_id = event.context_parsed.get('messageId', '')
            if msg_id and msg_id.startswith('http'):
                urls.append({
                    'url': msg_id,
                    'token_name': event.token_name,
                    'token_symbol': event.token_symbol,
                    'token_address': event.token_address,
                    'user_id': event.context_parsed.get('id', ''),
                    'block': event.block_number,
                    'tx_hash': event.tx_hash,
                })
        return urls
    
    def extract_images(
        self,
        events: List[DecodedTokenEvent]
    ) -> List[Dict[str, str]]:
        """
        Extract token images from deployments.
        
        Args:
            events: List of events
            
        Returns:
            List of dicts with image URL and token info
        """
        images = []
        for event in events:
            if event.token_image:
                images.append({
                    'image_url': event.token_image,
                    'token_name': event.token_name,
                    'token_address': event.token_address,
                })
        return images
