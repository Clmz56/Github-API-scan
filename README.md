# Github-API-Scan

Automated GitHub secret scanning and validation engine.
Designed for security research and red teaming operations.

## Overview

High-performance asynchronous scanner that detects sensitive keys in real-time via GitHub API.
Implements deep validity checking with context-aware base URL extraction to reduce false positives.

## Core Architecture

- **Async I/O**: Built on `asyncio` + `aiohttp` for high concurrency.
- **Circuit Breaker**: Domain-level protection to prevent IP bans and reduce network-related errors.
- **Deep Inspection**:
    - Entropy analysis (Shannon entropy > 3.8).
    - Context-aware endpoint extraction (Azure/Custom relays).
    - Active probing (Model tier detection/Balance check/RPM analysis).

## Features

- **Multi-Platform**: Support for OpenAI, Azure, Anthropic, and Gemini.
- **Heuristic Filtering**: 
    - Excludes test/example keys via regex and entropy thresholds.
    - Blacklists common false-positive paths (`/test/`, `/mock/`, `localhost`).
- **Persistence**: SQLite storage with SHA-based deduplication to prevent scanning the same file twice.

## Installation

Python >= 3.9 required.

```bash
pip install -r requirements.txt
```

## Usage

### Configuration

Set proxy (if required):
```bash
# Environment variable
set PROXY_URL=http://127.0.0.1:7890

# Or CLI argument
python main.py --proxy http://127.0.0.1:7890
```

Configure GitHub Tokens:
```bash
# Comma-separated list of PATs
set GITHUB_TOKENS=ghp_xxx,ghp_yyy
```

### Running

Start scanning with TUI dashboard:
```bash
python main.py
```

### Data Export

Export valid keys to text:
```bash
python main.py --export output.txt
```

Export detailed CSV (includes balance/RPM data):
```bash
python main.py --export-csv results.csv
```

View database statistics:
```bash
python main.py --stats
```

## Disclaimer

This tool is for authorized security testing and educational purposes only.
Users are responsible for ensuring compliance with applicable laws and regulations.
