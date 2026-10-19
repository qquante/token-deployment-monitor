"""
JSON exporter for Bankr deployment data.

This module handles the serialization and export of
analyzed data to JSON files for further processing
or archival purposes.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

try:
    from src.decoder import DecodedTokenEvent
    from src.analyzer import BankrStats
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.decoder import DecodedTokenEvent
    from src.analyzer import BankrStats

logger = logging.getLogger(__name__)


class JSONExporter:
    """
    JSON file exporter for deployment data.
    
    Handles serialization of decoded events and statistics
    to JSON files with proper encoding and formatting.
    
    Attributes:
        output_dir: Directory for output files
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize exporter.
        
        Args:
            output_dir: Output directory path (default: ./data)
        """
        self.output_dir = Path(output_dir or "data")
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_deployments(
        self,
        events: List[DecodedTokenEvent],
        filename: str = "bankr_deployments.json"
    ) -> Path:
        """
        Export deployment events to JSON.
        
        Args:
            events: List of decoded events
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        data = [event.to_dict() for event in events]
        
        self._write_json(filepath, data)
        logger.info(f"Exported {len(events)} deployments to {filepath}")
        
        return filepath
    
    def export_stats(
        self,
        stats: BankrStats,
        filename: str = "summary.json"
    ) -> Path:
        """
        Export statistics summary to JSON.
        
        Args:
            stats: BankrStats object
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        data = stats.to_dict()
        data['exported_at'] = datetime.utcnow().isoformat()
        
        self._write_json(filepath, data)
        logger.info(f"Exported stats to {filepath}")
        
        return filepath
    
    def export_twitter_urls(
        self,
        urls: List[Dict[str, Any]],
        filename: str = "twitter_urls.json"
    ) -> Path:
        """
        Export Twitter URLs to JSON.
        
        Args:
            urls: List of URL data dicts
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        
        self._write_json(filepath, urls)
        logger.info(f"Exported {len(urls)} Twitter URLs to {filepath}")
        
        return filepath
    
    def export_images(
        self,
        images: List[Dict[str, str]],
        filename: str = "token_images.json"
    ) -> Path:
        """
        Export token images to JSON.
        
        Args:
            images: List of image data dicts
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        
        self._write_json(filepath, images)
        logger.info(f"Exported {len(images)} token images to {filepath}")
        
        return filepath
    
    def export_full_report(
        self,
        bankr_events: List[DecodedTokenEvent],
        stats: BankrStats,
        twitter_urls: List[Dict[str, Any]],
        images: List[Dict[str, str]]
    ) -> Dict[str, Path]:
        """
        Export all data files at once.
        
        Args:
            bankr_events: Bankr deployment events
            stats: Statistics summary
            twitter_urls: Extracted Twitter URLs
            images: Extracted token images
            
        Returns:
            Dict mapping data type to file path
        """
        return {
            'deployments': self.export_deployments(bankr_events),
            'stats': self.export_stats(stats),
            'twitter_urls': self.export_twitter_urls(twitter_urls),
            'images': self.export_images(images),
        }
    
    @staticmethod
    def _write_json(filepath: Path, data: Any) -> None:
        """Write data to JSON file with proper encoding."""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
