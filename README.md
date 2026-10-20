# Token Deployment Monitor

A Python event-processing toolkit for inspecting token deployments, enriching observations and distributing alerts.

## Features

- Event decoding and block-range collection identify deployment observations.
- Worker modules enrich token and social metadata.
- Messaging modules route summarized observations to configured consumers.

## Stack

Python · Web3.py · eth-abi · aiohttp · Redis · SQLite paths · Docker

Redis channels/caches, local storage modules and JSON exports are present. Runtime datasets and downloaded protocol SDK copies are excluded.

## Getting started

Install the declared Python requirements in an isolated environment. Review individual worker entrypoints and compose services before execution; do not start the whole stack as a demo.

## How it works

```text
On-chain event → decoder and filtering → Redis worker/enrichment pipeline → local persistence and alerts
```

## Current limitations

The source also contains a trading executor; the complete stack is not universally read-only. No worker, transaction or live messaging flow was run. Embedded credential fallbacks were removed in the preparation copy.

## License

Project-authored source code is licensed under the [MIT License](LICENSE). Third-party components and datasets retain their respective rights and licenses.
