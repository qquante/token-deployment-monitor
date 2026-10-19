# Token Deployment Monitor

> **Local private review:** publication is paused. See [security review and remaining release conditions](docs/SECURITY_REVIEW.md). Local fixes have not been pushed.


A Python event-processing toolkit for inspecting token deployments, enriching observations and distributing alerts.

![review](https://img.shields.io/badge/review-private-374151) ![primary](https://img.shields.io/badge/primary-Python-2563EB) [![LinkedIn](https://img.shields.io/badge/LinkedIn-Douglas%20Silva-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/douglas-silva-data-analyst/)

[Overview](#overview) · [Architecture](#architecture) · [Setup](#setup) · [Validation](#validation) · [Roadmap](#roadmap) · [Review notes](docs/REVIEW.md)

## Overview

- Event decoding and block-range collection identify deployment observations.
- Worker modules enrich token and social metadata.
- Messaging modules route summarized observations to configured consumers.

**Technology:** Python · Web3.py · eth-abi · aiohttp · Redis · SQLite paths · Docker

**Persistence and data:** Redis channels/caches, local storage modules and JSON exports are present. Runtime datasets and downloaded protocol SDK copies are excluded.

## Architecture

```text
On-chain event → decoder and filtering → Redis worker/enrichment pipeline → local persistence and alerts
```

| Location | Review focus |
| --- | --- |
| [`src/onchain/listener.py`](src/onchain/listener.py) | Source/configuration inspected for scope and structure |
| [`workers/onchain_listener.py`](workers/onchain_listener.py) | Source/configuration inspected for scope and structure |
| [`shared/models.py`](shared/models.py) | Source/configuration inspected for scope and structure |
| [`workers/trading_executor.py`](workers/trading_executor.py) | Source/configuration inspected for scope and structure |
| [`requirements.txt`](requirements.txt) | Source/configuration inspected for scope and structure |

Internal package and module names are retained where changing them would affect imports or compatibility. This descriptive repository name does not imply a completed package/API migration.

## Setup

Install the declared Python requirements in an isolated environment. Review individual worker entrypoints and compose services before execution; do not start the whole stack as a demo.

Start from an isolated environment. Use `.env.example` when present and inspect the configuration loader: an example file is not necessarily loaded automatically. Runtime databases, real account sessions and private credentials are not bundled. Live integration prerequisites are separate from local tests.

## Validation

Static parsing checked 85 Python files and 0 strict JSON files. Python syntax errors: 0; JSON errors: 0. No functional test/build result is claimed for this preparation.

Available test scripts are inventoried in [the review notes](docs/REVIEW.md). Choose offline checks before operating any integration.

These results apply to the selected checks on the preparation copy. They do not establish deployment readiness, full coverage, live provider availability or measured performance. CI definitions, when present under `docs/ci-templates/`, are inactive review templates; no remote CI badge is claimed.

## Limits and current status

The source also contains a trading executor; the complete stack is not universally read-only. No worker, transaction or live messaging flow was run. Embedded credential fallbacks were removed in the preparation copy.

This private snapshot is for code and portfolio review. Generated runtime material and unreviewed binary assets may be excluded. [Review notes](docs/REVIEW.md) record preparation scope, findings and outstanding validation.

## Roadmap

- [ ] Separate monitoring-only services from transaction-capable workers.
- [ ] Run decoder tests against fixed public receipts.
- [ ] Review worker retries, idempotency and configuration before any deployment.

## History and attribution

No recoverable original Git history was verified for this snapshot. Import and preparation commits use their actual current dates; filesystem timestamps are not treated as development dates.

No open-source license is granted by this preparation. A license and attribution review is required before public redistribution.

## Maintainer and feedback

Douglas de Oliveira Silva · [GitHub](https://github.com/qquante) · [LinkedIn](https://www.linkedin.com/in/douglas-silva-data-analyst/)

For this private review, report reproducible issues with a minimal fixture and the exact check output. Keep account data, credentials and generated sensitive reports out of issues and commits. Existing contribution guidelines apply where present.

**Topics:** `python`, `web3`, `redis`, `blockchain`, `event-processing`, `data-pipeline`
