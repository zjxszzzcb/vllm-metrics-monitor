# AGENTS.md

Guidance for AI coding agents working in this repository. Assumes no prior knowledge of the project.

## Project Overview

**vLLM Metrics Monitor (`vmm`)** is a lightweight, real-time monitoring dashboard for vLLM inference servers. It scrapes Prometheus metrics from a vLLM `/metrics` endpoint, persists them to SQLite, and serves a web dashboard with live charts.

**Zero external runtime dependencies** — the backend uses only the Python standard library (the frontend loads Chart.js from a CDN). Keep it that way; do not add dependencies unless truly necessary.

- Package name: `vllm-metrics-monitor`, current version `0.4.0` (bump in `pyproject.toml` when releasing)
- Python ≥ 3.10, build backend: Hatchling, `src/` layout
- CLI entry point: `vmm = "vllm_metrics_monitor.cli:main"`
- Package manager: **uv** (see `uv.lock`)

## Commands

```bash
# Install for development
uv venv && uv pip install -e .

# Run locally
uv run vmm http://localhost:8000/metrics --debug

# Run with custom options
uv run vmm http://vllm:8000/metrics -p 9090 -i 5 --retention 72

# Build package (output to dist/)
uv build

# Docker (image installs the newest wheel from dist/)
uv build && docker build -t vmm .
docker run -d --network host vmm http://localhost:8000/metrics
METRICS_URLS="http://<host1>:8000/metrics http://<host2>:8000/metrics" docker compose up -d
```

**There is no test suite.** Manual testing is done via the web UI at `http://localhost:8080` and the JSON API endpoints (`/api/current`, `/api/history?minutes=120`). When you change behavior, verify it by running `uv run vmm` against a live (or mocked) metrics endpoint and checking the dashboard/API responses.

## CLI Options

```
vmm [URL...]                   vLLM Prometheus metrics endpoint(s); multiple URLs are
                               scraped concurrently (default: http://localhost:8000/metrics)
-p, --port PORT                Dashboard HTTP port (default: 8080)
-i, --interval SEC             Scrape interval in seconds (default: 3)
--retention HOURS              Data retention (default: 720, i.e. 30 days)
--db PATH                      SQLite path (default: ~/.vmm/data.db)
--reset                        Delete existing DB and start fresh
--debug                        Enable debug logging
-v, --version                  Show version and exit
```

## Code Organization

```
src/vllm_metrics_monitor/
├── __init__.py   # __version__ read from installed package metadata
├── cli.py        # argparse entry point; wires CLI args into collector module globals, starts threads
├── collector.py  # Prometheus text parser, SQLite storage, scraper loop, query/downsampling helpers
├── server.py     # ThreadingHTTPServer + request handler: static files and JSON API
└── static/
    ├── index.html    # Self-contained dashboard (HTML/CSS/JS, ~800 lines), Chart.js 4.4.7 via CDN
    ├── favicon.svg   # Primary favicon
    └── favicon.png   # Fallback favicon
```

## Runtime Architecture

**Three+ threads, no frameworks:**

- One scraper daemon thread per configured metrics URL (`collector.scraper_loop` with a source id): fetches and parses its vLLM endpoint every `scrape_interval` seconds, writes a snapshot to SQLite tagged with its `source_id`.
- Cleanup daemon thread: every hour deletes rows older than `retention_hours` (across all sources).
- Main thread: runs `ThreadingHTTPServer` (one thread per request, 30s socket timeout) serving the dashboard and API.

**Data flow:** vLLM `/metrics` (×N sources) → regex parser → SQLite (WAL mode) → rate computation at query time → optional downsampling → JSON API → Chart.js dashboard (browser polls every ~3s). The dashboard header has a source dropdown (shown only when multiple sources exist) backed by `/api/sources`.

## Key Patterns and Conventions

- **Module-level configuration globals** in `collector.py` (`metrics_urls`, `scrape_interval`, `retention_hours`, `db_path`) are set directly from `cli.py`. Follow this pattern rather than introducing config classes.
- **Multi-source model**: a `sources` table maps `id → (label, url, model_name)`. Labels are auto-derived from the URL's `host:port` (deduplicated with ` (n)` suffixes); `model_name` is discovered at scrape time from the `model_name` label on core vLLM metrics and updated in the DB when it changes. The in-memory `collector.sources` list is the source of truth for the API.
- **SQLite in WAL mode**, one connection per call (opened/closed inside each function). The `journal_mode=WAL` PRAGMA is persistent, so it is set once in `init_db` — do NOT re-run it on every connect; under concurrent scraper threads that wedges all DB access. Three tables: `sources`, `metrics` (one row per timestamp per source, PK `(timestamp, source_id)`), and `engine_metrics` (per engine per timestamp per source, PK `(timestamp, engine_id, source_id)`). All metric queries filter by `source_id`. Schema evolution uses two mechanisms: additive `ALTER TABLE` via `_migrate_db` (silently passes if the column exists), and a rebuild migration (`_rebuild_with_source_id`) for PK changes — pre-multi-source databases are rebuilt with old rows attributed to the first configured URL.
- **Per-source state is keyed by source id** (`_last_snapshots` for fallback baselines and histogram bucket state). Anything that used to be a single global snapshot must stay per-source.
- **Rates are computed at query time** (delta / time_delta between consecutive rows), never stored. Counter resets (vLLM restart) are detected by checking for negative deltas and treated as a restart from zero — keep this logic consistent between `query_history` and `query_current`.
- **Server-side downsampling** (`collector._downsample`): when points exceed `max_points` (default 180 from the API handler), consecutive points are binned; each bin yields a mean value and a max value. Responses set `downsampled: true` and include a `max` dict.
- **Graceful degradation** on scrape failure: if a previous snapshot exists, store a fallback with gauges zeroed and counters carried over; if no baseline exists yet, skip the sample entirely (storing zero counters would create a fake counter reset and a huge spike on recovery).
- **Metrics are summed across vLLM engines** (grouped by the `engine` label); per-engine running/waiting/KV values go to `engine_metrics`. KV cache usage is reported as the **max** across engines.
- **Request counts exclude aborts**: `request_success_total` with `finished_reason == "abort"` goes to `abort_count`, `"error"` to `error_count`; failure rate = (abort + error) / total per interval.
- **Histogram maxes** (prompt/generation length) are estimated by finding the highest bucket whose count increased since the previous scrape; bucket baselines are dropped when the corresponding counter resets.

## HTTP Server and Security

- Endpoints: `/` and `/index.html` (version-stamped via `{{ VERSION }}` template substitution), `/api/sources`, `/api/current?source=N`, `/api/history?minutes=N&max_points=M&source=N`, and any other path as a static file. The `source` param defaults to the first source when omitted or invalid.
- **Path traversal protection**: static paths are resolved with `os.path.realpath` and rejected unless they stay under `STATIC_DIR`. Preserve this when touching `server.py`.
- Query parameters are parsed by hand with strict `int()` conversion and safe fallbacks — no user input ever reaches SQL as a string (all queries are parameterized). Keep it that way.
- The server binds to `0.0.0.0` and has no authentication — it is intended for trusted networks.
- Client disconnects (broken pipe, reset, timeout) are expected noise and logged at debug level, not as errors.
- All responses use `Cache-Control: no-cache` so dashboard upgrades show up immediately.

## Frontend

- A single self-contained `index.html` — vanilla JS, no build step, no framework. The only external resources are Chart.js 4.4.7 and chartjs-adapter-date-fns 3.0.0 from jsDelivr CDN.
- Time ranges: 30m / 2h / 8h / 24h / 3d / 7d / 30d.
- The version is injected server-side by replacing `{{ VERSION }}` in the HTML.

## Style Guidelines

- Match existing code: standard library only, `logging` module for output, type hints on function signatures, docstrings on public functions, comments that explain *why* (e.g. counter-reset handling, fallback semantics).
- Keep the zero-dependency and no-build-step properties; simplicity is a deliberate design goal.

## Deployment

- `Dockerfile`: `python:3.12-slim`, installs the newest wheel from `dist/`, entrypoint `vmm`, exposes 8080. You must run `uv build` before `docker build`.
- `docker-compose.yml`: host network mode, `METRICS_URLS` env var for the target endpoint(s) (space-separated), persistent `vmm-data` volume mounted at `/root/.vmm`.
