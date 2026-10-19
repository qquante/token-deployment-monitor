"""
Tests for event decoder module.

These tests verify the correct decoding of TokenCreated
events from raw blockchain logs.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.decoder import TokenCreatedDecoder, DecodedTokenEvent


class TestDecodedTokenEvent:
    """Tests for DecodedTokenEvent dataclass."""
    
    def test_to_dict(self):
        """Test serialization to dictionary."""
        event = DecodedTokenEvent(
            token_address="0x1234567890123456789012345678901234567890",
            token_admin="0x0987654321098765432109876543210987654321",
            msg_sender="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_image="https://example.com/image.png",
            token_name="Test Token",
            token_symbol="TEST",
            token_metadata='{"description":"Test"}',
            token_context='{"interface":"Bankr"}',
            starting_tick=-230400,
            pool_hook="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            pool_id="abcd1234" * 8,
            paired_token="0x4200000000000000000000000000000000000006",
            locker="0xcccccccccccccccccccccccccccccccccccccccc",
            mev_module="0xdddddddddddddddddddddddddddddddddddddddd",
            extensions_supply=0,
            extensions=[],
            block_number=12345678,
            tx_hash="ef" * 32,
            context_parsed={"interface": "Bankr"},
            metadata_parsed={"description": "Test"},
        )
        
        d = event.to_dict()
        
        assert d['token_name'] == "Test Token"
        assert d['token_symbol'] == "TEST"
        assert d['block_number'] == 12345678
        assert d['context_parsed']['interface'] == "Bankr"
    
    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            'token_address': "0x1234567890123456789012345678901234567890",
            'token_admin': "0x0987654321098765432109876543210987654321",
            'msg_sender': "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            'token_image': "",
            'token_name': "From Dict",
            'token_symbol': "FD",
            'token_metadata': '{}',
            'token_context': '{}',
            'starting_tick': -230400,
            'pool_hook': "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            'pool_id': "00" * 32,
            'paired_token': "0x4200000000000000000000000000000000000006",
            'locker': "0xcccccccccccccccccccccccccccccccccccccccc",
            'mev_module': "0xdddddddddddddddddddddddddddddddddddddddd",
            'extensions_supply': 0,
            'extensions': [],
            'block_number': 99999999,
            'tx_hash': "aa" * 32,
            'context_parsed': {},
            'metadata_parsed': {},
        }
        
        event = DecodedTokenEvent.from_dict(data)
        
        assert event.token_name == "From Dict"
        assert event.block_number == 99999999


class TestTokenCreatedDecoder:
    """Tests for TokenCreatedDecoder class."""
    
    def test_extract_address_from_topic(self):
        """Test address extraction from 32-byte topic."""
        # 32-byte topic with address in last 20 bytes
        topic = bytes.fromhex(
            "0000000000000000000000001234567890abcdef1234567890abcdef12345678"
        )
        
        address = TokenCreatedDecoder._extract_address_from_topic(topic)
        
        assert address == "0x1234567890abcdef1234567890abcdef12345678"
    
    def test_parse_json_valid(self):
        """Test JSON parsing with valid input."""
        json_str = '{"interface":"Bankr","platform":"X"}'
        
        result = TokenCreatedDecoder._parse_json(json_str)
        
        assert result['interface'] == "Bankr"
        assert result['platform'] == "X"
    
    def test_parse_json_invalid(self):
        """Test JSON parsing with invalid input."""
        invalid_json = "not valid json {"
        
        result = TokenCreatedDecoder._parse_json(invalid_json)
        
        assert result == {}
    
    def test_parse_json_empty(self):
        """Test JSON parsing with empty string."""
        result = TokenCreatedDecoder._parse_json("")
        
        assert result == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
