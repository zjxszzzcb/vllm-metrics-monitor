# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

vLLM Metrics Monitor (`vmm`) — a lightweight, real-time monitoring dashboard for vLLM inference servers. Scrapes Prometheus metrics from vLLM, stores them in SQLite, and serves a web-based dashboard with live charts.

**Zero external dependencies** — uses only Python standard library.

## Common Commands

```bash
# Install for development
uv venv && uv pip install -e .

# Run locally
uv run vmm http://localhost:8000/metrics --debug

# Run with custom options
uv run vmm http://vllm:8000/metrics -p 9090 -i 5 --retention 72

# Build package
uv build
```

No test suite exists yet. Manual testing via the web UI at `http://localhost:8080` or API endpoints (`/api/current`, `/api/history?minutes=120`).

## Architecture

```
src/vllm_metrics_monitor/
├── cli.py        # Entry point, argument parsing, orchestrator
├── collector.py  # Prometheus scraper + SQLite storage + downsampling
├── server.py     # HTTP server + JSON API endpoints
└── static/
    ├── index.html    # Self-contained dashboard (HTML/CSS/JS + Chart.js CDN)
    ├── favicon.svg   # SVG favicon (primary, used by modern browsers)
    └── favicon.png   # PNG favicon (fallback)
```

**Dual-threaded design:**
- Background daemon thread: scrapes vLLM Prometheus endpoint every N seconds
- Main thread: runs HTTP server serving the dashboard and API
- Separate cleanup daemon thread: hourly removes data beyond retention period

**Data flow:** vLLM `/metrics` → regex parser → SQLite (WAL mode) → rate calculation → downsampling → JSON API → Chart.js dashboard (3s polling)

## Key Patterns

- **SQLite with WAL mode** for concurrent reads during writes; two tables (`metrics` for global, `engine_metrics` per-engine)
- **Rate calculations** are computed at query time (delta / time_delta), not stored
- **Server-side downsampling** (`_downsample` in collector.py): when raw points exceed `max_points` (default 300), groups consecutive points into bins and computes mean + max per bin; returns `downsampled: true` and `max` dict in response
- **Default retention** is 720 hours (30 days); default scrape interval is 3 seconds
- **Graceful degradation** on scrape failure — returns last known snapshot with gauges zeroed
- **Path traversal protection** in static file serving
- **Frontend** is a single `index.html` with vanilla JS, no build step; uses Chart.js 4.4.7 from CDN
- **Time ranges**: 30m / 2h / 8h / 24h / 3d / 7d / 30d with smooth toggle (Chart.js tension)

## Build & Packaging

- Build backend: **Hatchling** (`pyproject.toml`)
- CLI entry point: `vmm = "vllm_metrics_monitor.cli:main"`
- Python ≥ 3.10 required
