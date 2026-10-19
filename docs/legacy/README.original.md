> Historical project notes. Paths, operational status, badges and test claims below are retained as archival reference and have not all been revalidated. Use the root README and docs/REVIEW.md for this review snapshot.

# Bankr Monitor

Forensic analysis tool for monitoring and analyzing Bankr token deployments on the Base blockchain.

## Overview

Bankr Monitor queries the Clanker v4.0.0 smart contract on Base to extract and analyze token deployment events. It specifically filters for deployments made through the Bankr interface, providing comprehensive data extraction and statistical analysis.

## Features

- **RPC Client**: Paginated log queries with automatic retry and rate limiting
- **Event Decoder**: ABI decoding of TokenCreated events
- **Bankr Filter**: Identifies Bankr deployments by interface signature
- **Data Export**: JSON export of deployments, stats, Twitter URLs, and images
- **Modular Architecture**: Clean separation of concerns, max 800 lines per module

## Quick Start

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/bankr-monitor.git
cd bankr-monitor

# Create virtual environment
python -m venv venv

# Activate (Windows)
.\venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Usage

```bash
# Run analysis (default: last 100k blocks)
python scripts/run_analysis.py

# Specify block range
python scripts/run_analysis.py --blocks 50000

# Custom output directory
python scripts/run_analysis.py --output ./my_data
```

### Docker

```bash
# Build and run
docker-compose up bankr-monitor

# Run with custom blocks
docker-compose run bankr-monitor --blocks 50000
```

## Project Structure

```
bankr-monitor/
├── config/
│   ├── __init__.py
│   └── settings.py          # Configuration (contracts, RPC, events)
├── src/
│   ├── rpc/
│   │   └── client.py        # Base RPC client with pagination
│   ├── decoder/
│   │   └── events.py        # TokenCreated event decoder
│   ├── analyzer/
│   │   └── bankr.py         # Bankr filtering and stats
│   └── exporter/
│       └── json_export.py   # JSON file export
├── scripts/
│   └── run_analysis.py      # Main analysis script
├── tests/
│   └── test_decoder.py      # Unit tests
├── data/                    # Output directory
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── FORENSIC_REPORT.md       # Detailed forensic documentation
└── README.md
```

## Output Files

| File | Description |
|------|-------------|
| `bankr_deployments.json` | All Bankr token deployments |
| `summary.json` | Statistical summary |
| `twitter_urls.json` | Extracted Twitter/X URLs |
| `token_images.json` | Token image URLs |

## Configuration

Configuration is centralized in `config/settings.py`:

- **RPCConfig**: RPC endpoints and connection settings
- **ContractConfig**: Smart contract addresses
- **EventConfig**: Event signatures and ABI types
- **QueryConfig**: Query parameters (chunk size, rate limits)
- **BankrConfig**: Bankr identification patterns

Override via environment variables:

```bash
export BASE_RPC="https://your-rpc-endpoint.com"
```

## Key Contracts

| Contract | Address |
|----------|---------|
| Clanker v4.0.0 | `0xE85A59c628F7d27878ACeB4bf3b35733630083a9` |
| LP Locker | `0x63D2DfEA64b3433F4071A98665bcD7Ca14d93496` |
| Pool Hook | `0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC` |
| MEV Module | `0xebB25BB797D82CB78E1bc70406b13233c0854413` |

## API Reference

### BaseRPCClient

```python
from src.rpc import BaseRPCClient

client = BaseRPCClient()
logs = client.get_token_created_logs(num_blocks=10000)
```

### TokenCreatedDecoder

```python
from src.decoder import TokenCreatedDecoder

decoder = TokenCreatedDecoder()
events = decoder.decode_many(logs)
```

### BankrAnalyzer

```python
from src.analyzer import BankrAnalyzer

analyzer = BankrAnalyzer()
bankr_events, other_events = analyzer.filter_bankr(events)
stats = analyzer.compute_stats(events, bankr_events)
```

### JSONExporter

```python
from src.exporter import JSONExporter

exporter = JSONExporter("./data")
exporter.export_deployments(bankr_events)
```

## Testing

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_decoder.py
```

## License

MIT

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open Pull Request
