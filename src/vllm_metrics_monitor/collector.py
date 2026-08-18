"""Metrics collector: Prometheus parser, SQLite storage, scraper loop."""

import logging
import os
import re
import sqlite3
import threading
import time
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# --- Configuration (set by CLI) ---
metrics_urls: list[str] = ["http://localhost:8000/metrics"]
scrape_interval: int = 1
retention_hours: int = 24
db_path: str = "data.db"

# Registered sources, filled by init_db():
# [{"id": int, "label": str, "url": str, "model_name": str | None}]
sources: list[dict] = []


def derive_label(url: str) -> str:
    """Derive a short display label (host:port) from a metrics URL."""
    try:
        netloc = urlparse(url).netloc
    except ValueError:
        netloc = ""
    return netloc or url


def _source_by_id(source_id: int | None) -> dict:
    """Look up a source by id, falling back to the first source."""
    for s in sources:
        if s["id"] == source_id:
            return s
    return sources[0]


# --- Database ---


def _get_db():
    # WAL mode is persistent once set (init_db enables it), so connections
    # only need the row factory here — re-running the journal_mode PRAGMA on
    # every open adds lock contention with multiple scraper threads.
    # timeout=30: with several writer threads plus the hourly cleanup, a
    # writer may legitimately wait longer than the 5s default for the WAL
    # write lock; the scraper must not die on a transient "database is
    # locked".
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_db(conn):
    """Add missing columns to existing tables."""
    migrations = [
        ("metrics", "prompt_len_sum", "REAL"),
        ("metrics", "prompt_len_count", "REAL"),
        ("metrics", "gen_len_sum", "REAL"),
        ("metrics", "gen_len_count", "REAL"),
        ("metrics", "prompt_len_max", "REAL"),
        ("metrics", "gen_len_max", "REAL"),
        ("metrics", "abort_count", "REAL"),
        ("metrics", "error_count", "REAL"),
        ("metrics", "queue_time_sum", "REAL"),
        ("metrics", "queue_time_count", "REAL"),
    ]
    for table, col, dtype in migrations:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
        except sqlite3.OperationalError:
            pass  # column already exists


# Columns of the metrics table, excluding timestamp/source_id (the PK).
# Used by CREATE TABLE, INSERT, and the source_id rebuild migration so the
# explicit column lists can never drift apart.
_METRICS_COLS = [
    "num_requests_running", "num_requests_waiting", "kv_cache_usage_perc",
    "prompt_tokens_total", "generation_tokens_total", "request_success_total",
    "prompt_tokens_cached_total", "ttft_sum", "ttft_count", "itl_sum",
    "itl_count", "e2e_sum", "e2e_count", "prompt_len_sum", "prompt_len_count",
    "gen_len_sum", "gen_len_count", "prompt_len_max", "gen_len_max",
    "abort_count", "error_count", "queue_time_sum", "queue_time_count",
]

_ENGINE_COLS = [
    "num_requests_running", "num_requests_waiting", "kv_cache_usage_perc",
    "prompt_tokens_total", "generation_tokens_total",
]

_METRICS_SCHEMA = """
    timestamp REAL,
    source_id INTEGER NOT NULL DEFAULT 0,
    %s,
    PRIMARY KEY (timestamp, source_id)
""" % ", ".join(f"{c} REAL" for c in _METRICS_COLS)

_ENGINE_SCHEMA = """
    timestamp REAL,
    engine_id INTEGER,
    source_id INTEGER NOT NULL DEFAULT 0,
    %s,
    PRIMARY KEY (timestamp, engine_id, source_id)
""" % ", ".join(f"{c} REAL" for c in _ENGINE_COLS)


def _has_column(conn, table: str, column: str) -> bool:
    return any(
        row["name"] == column for row in conn.execute(f"PRAGMA table_info({table})")
    )


def _rebuild_with_source_id(conn, table: str, key_cols: list[str], cols: list[str],
                            schema: str, first_source_id: int):
    """Rebuild a pre-multi-source table, attributing old rows to the first source.

    SQLite cannot alter a PRIMARY KEY, so adding source_id to the PK requires
    creating a new table, copying rows, and renaming.
    """
    all_cols = key_cols + cols
    col_list = ", ".join(all_cols)
    row_count = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
    conn.execute(f"CREATE TABLE {table}_new ({schema})")
    conn.execute(
        f"INSERT INTO {table}_new ({col_list}, source_id) "
        f"SELECT {col_list}, ? FROM {table}",
        (first_source_id,),
    )
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE {table}_new RENAME TO {table}")
    logger.info("Migrated table %s to multi-source schema (%d rows kept)",
                table, row_count)


def _seed_sources(conn):
    """Register configured URLs in the sources table and load them globally."""
    existing = {
        row["url"]: row["label"]
        for row in conn.execute("SELECT url, label FROM sources")
    }
    used_labels = set(existing.values())
    for url in metrics_urls:
        if url in existing:
            continue
        label = derive_label(url)
        # Disambiguate duplicate host:port labels (e.g. same URL entered twice
        # with different paths, or two URLs sharing a netloc).
        n = 2
        unique = label
        while unique in used_labels:
            unique = f"{label} ({n})"
            n += 1
        used_labels.add(unique)
        conn.execute(
            "INSERT INTO sources (label, url) VALUES (?, ?)", (unique, url)
        )
    conn.commit()

    global sources
    sources = [
        {"id": row["id"], "label": row["label"], "url": row["url"],
         "model_name": row["model_name"]}
        for row in conn.execute("SELECT * FROM sources ORDER BY id")
    ]


def init_db():
    conn = _get_db()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            url TEXT UNIQUE,
            model_name TEXT
        );
        CREATE TABLE IF NOT EXISTS metrics ({_METRICS_SCHEMA});
        CREATE TABLE IF NOT EXISTS engine_metrics ({_ENGINE_SCHEMA});
    """)
    # Additive column migrations for pre-multi-source databases; must run
    # before the rebuild so the old table has every column we copy.
    _migrate_db(conn)
    # Sources must be seeded before the rebuild: old rows are attributed to
    # the first configured URL.
    _seed_sources(conn)
    if not _has_column(conn, "metrics", "source_id"):
        first_id = sources[0]["id"]
        _rebuild_with_source_id(conn, "metrics", ["timestamp"], _METRICS_COLS,
                                _METRICS_SCHEMA, first_id)
        _rebuild_with_source_id(conn, "engine_metrics",
                                ["timestamp", "engine_id"], _ENGINE_COLS,
                                _ENGINE_SCHEMA, first_id)
    conn.commit()
    conn.close()


def cleanup_db():
    """Remove data older than retention_hours."""
    cutoff = time.time() - retention_hours * 3600
    conn = _get_db()
    conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff,))
    conn.execute("DELETE FROM engine_metrics WHERE timestamp < ?", (cutoff,))
    conn.commit()
    conn.close()
    logger.info("Database cleanup completed")


# --- Prometheus Parser ---


def parse_prometheus(text: str) -> dict:
    """Parse Prometheus text format into {metric_name: [{labels, value}]}."""
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r'^([\w:]+)(?:\{([^}]*)\})?\s+([\d.eE+-]+|NaN)$', line)
        if not m:
            continue
        name, labels_str, value_str = m.group(1), m.group(2), m.group(3)
        labels = {}
        if labels_str:
            for pair in labels_str.split(","):
                k, v = pair.split("=", 1)
                labels[k.strip()] = v.strip('"')
        try:
            value = float(value_str)
        except ValueError:
            continue
        result.setdefault(name, []).append({"labels": labels, "value": value})
    return result


def _sum_by_label(entries: list, label: str) -> dict:
    """Sum values grouped by a label."""
    groups = {}
    for e in entries:
        key = e["labels"].get(label, "")
        groups[key] = groups.get(key, 0) + e["value"]
    return groups


def _get_total(parsed: dict, metric: str) -> float:
    """Sum all values of a metric."""
    return sum(e["value"] for e in parsed.get(metric, []))


def _parse_histogram_buckets(parsed: dict, metric: str) -> dict:
    """Parse histogram buckets into {le_value: total_count_across_engines}."""
    buckets = {}
    for e in parsed.get(metric + "_bucket", []):
        le = e["labels"].get("le", "")
        if le == "+Inf":
            continue
        try:
            le_val = float(le)
        except ValueError:
            continue
        buckets[le_val] = buckets.get(le_val, 0) + e["value"]
    return buckets


def _estimate_histogram_max(current_buckets: dict, prev_buckets: dict | None) -> float:
    """Find highest bucket whose count increased since last scrape."""
    if not prev_buckets:
        return 0
    max_val = 0
    for le_val, count in current_buckets.items():
        prev_count = prev_buckets.get(le_val, 0)
        if count > prev_count:
            max_val = max(max_val, le_val)
    return max_val


# --- Collect & Store ---


# Per-source last successful snapshot (fallback baseline), keyed by source id.
_last_snapshots: dict[int, dict] = {}


def _extract_model_name(parsed: dict) -> str | None:
    """Extract the model name from core vLLM metrics' model_name label."""
    for metric in ("vllm:num_requests_running", "vllm:prompt_tokens_total",
                   "vllm:generation_tokens_total"):
        for e in parsed.get(metric, []):
            name = e["labels"].get("model_name")
            if name:
                return name
    return None


def _update_model_name(source: dict, model_name: str):
    """Persist a newly discovered (or changed) model name for a source."""
    source["model_name"] = model_name
    conn = _get_db()
    conn.execute("UPDATE sources SET model_name = ? WHERE id = ?",
                 (model_name, source["id"]))
    conn.commit()
    conn.close()
    logger.info("Source %s serves model %s", source["label"], model_name)


def _fallback_snapshot(source_id: int) -> dict:
    """Return snapshot with gauges=0 and counters carried from last sample."""
    prev = _last_snapshots.get(source_id)
    return {
        "timestamp": time.time(),
        "num_requests_running": 0,
        "num_requests_waiting": 0,
        "kv_cache_usage_perc": 0,
        "prompt_tokens_total": prev["prompt_tokens_total"] if prev else 0,
        "generation_tokens_total": prev["generation_tokens_total"] if prev else 0,
        "request_success_total": prev["request_success_total"] if prev else 0,
        "prompt_tokens_cached_total": prev["prompt_tokens_cached_total"] if prev else 0,
        "ttft_sum": prev["ttft_sum"] if prev else 0,
        "ttft_count": prev["ttft_count"] if prev else 0,
        "itl_sum": prev["itl_sum"] if prev else 0,
        "itl_count": prev["itl_count"] if prev else 0,
        "e2e_sum": prev["e2e_sum"] if prev else 0,
        "e2e_count": prev["e2e_count"] if prev else 0,
        "prompt_len_sum": prev["prompt_len_sum"] if prev else 0,
        "prompt_len_count": prev["prompt_len_count"] if prev else 0,
        "gen_len_sum": prev["gen_len_sum"] if prev else 0,
        "gen_len_count": prev["gen_len_count"] if prev else 0,
        "prompt_len_max": prev["prompt_len_max"] if prev else 0,
        "gen_len_max": prev["gen_len_max"] if prev else 0,
        "abort_count": prev["abort_count"] if prev else 0,
        "error_count": prev["error_count"] if prev else 0,
        "queue_time_sum": prev["queue_time_sum"] if prev else 0,
        "queue_time_count": prev["queue_time_count"] if prev else 0,
        "prompt_buckets": prev.get("prompt_buckets") if prev else {},
        "gen_buckets": prev.get("gen_buckets") if prev else {},
        "uptime": 0,
        "engines": [],
    }


def collect_metrics(source_id: int) -> dict | None:
    """Fetch and parse vLLM metrics for one source. Returns fallback dict on timeout."""
    source = _source_by_id(source_id)
    last = _last_snapshots.get(source_id)
    try:
        req = Request(source["url"], headers={"Accept": "text/plain"})
        with urlopen(req, timeout=5) as resp:
            text = resp.read().decode()
    except (URLError, OSError) as e:
        logger.warning("Metrics fetch failed for %s: %s", source["label"], e)
        if last is None:
            # No baseline yet; storing a fallback with zero counters would
            # create a fake "reset" and produce a huge spike on the next
            # successful scrape.
            return None
        return _fallback_snapshot(source_id)

    parsed = parse_prometheus(text)

    model_name = _extract_model_name(parsed)
    if model_name and model_name != source.get("model_name"):
        _update_model_name(source, model_name)

    running_by_engine = _sum_by_label(
        parsed.get("vllm:num_requests_running", []), "engine"
    )
    waiting_by_engine = _sum_by_label(
        parsed.get("vllm:num_requests_waiting", []), "engine"
    )
    kv_by_engine = _sum_by_label(
        parsed.get("vllm:kv_cache_usage_perc", []), "engine"
    )

    total_running = sum(running_by_engine.values())
    total_waiting = sum(waiting_by_engine.values())
    max_kv = max(kv_by_engine.values()) if kv_by_engine else 0

    prompt_tokens = _get_total(parsed, "vllm:prompt_tokens_total")
    gen_tokens = _get_total(parsed, "vllm:generation_tokens_total")
    success_entries = parsed.get("vllm:request_success_total", [])
    total_requests = sum(
        e["value"] for e in success_entries
        if e["labels"].get("finished_reason") != "abort"
    )
    cached_tokens = _get_total(parsed, "vllm:prompt_tokens_cached_total")

    ttft_sum = _get_total(parsed, "vllm:time_to_first_token_seconds_sum")
    ttft_count = _get_total(parsed, "vllm:time_to_first_token_seconds_count")
    itl_sum = _get_total(parsed, "vllm:inter_token_latency_seconds_sum")
    itl_count = _get_total(parsed, "vllm:inter_token_latency_seconds_count")
    e2e_sum = _get_total(parsed, "vllm:e2e_request_latency_seconds_sum")
    e2e_count = _get_total(parsed, "vllm:e2e_request_latency_seconds_count")

    prompt_len_sum = _get_total(parsed, "vllm:request_prompt_tokens_sum")
    prompt_len_count = _get_total(parsed, "vllm:request_prompt_tokens_count")
    gen_len_sum = _get_total(parsed, "vllm:request_generation_tokens_sum")
    gen_len_count = _get_total(parsed, "vllm:request_generation_tokens_count")

    abort_count = sum(
        e["value"] for e in parsed.get("vllm:request_success_total", [])
        if e["labels"].get("finished_reason") == "abort"
    )
    error_count = sum(
        e["value"] for e in parsed.get("vllm:request_success_total", [])
        if e["labels"].get("finished_reason") == "error"
    )
    queue_time_sum = _get_total(parsed, "vllm:request_queue_time_seconds_sum")
    queue_time_count = _get_total(parsed, "vllm:request_queue_time_seconds_count")

    prompt_buckets = _parse_histogram_buckets(parsed, "vllm:request_prompt_tokens")
    gen_buckets = _parse_histogram_buckets(parsed, "vllm:request_generation_tokens")

    prev_prompt_buckets = last.get("prompt_buckets") if last else None
    prev_gen_buckets = last.get("gen_buckets") if last else None

    # Detect counter reset for histograms (vLLM restarted)
    if last and prompt_len_count < last.get("prompt_len_count", 0):
        prev_prompt_buckets = None
    if last and gen_len_count < last.get("gen_len_count", 0):
        prev_gen_buckets = None

    prompt_len_max = _estimate_histogram_max(prompt_buckets, prev_prompt_buckets)
    gen_len_max = _estimate_histogram_max(gen_buckets, prev_gen_buckets)

    start_time = _get_total(parsed, "process_start_time_seconds")
    now = time.time()

    data = {
        "timestamp": now,
        "num_requests_running": total_running,
        "num_requests_waiting": total_waiting,
        "kv_cache_usage_perc": max_kv,
        "prompt_tokens_total": prompt_tokens,
        "generation_tokens_total": gen_tokens,
        "request_success_total": total_requests,
        "prompt_tokens_cached_total": cached_tokens,
        "ttft_sum": ttft_sum,
        "ttft_count": ttft_count,
        "itl_sum": itl_sum,
        "itl_count": itl_count,
        "e2e_sum": e2e_sum,
        "e2e_count": e2e_count,
        "prompt_len_sum": prompt_len_sum,
        "prompt_len_count": prompt_len_count,
        "gen_len_sum": gen_len_sum,
        "gen_len_count": gen_len_count,
        "prompt_len_max": prompt_len_max,
        "gen_len_max": gen_len_max,
        "abort_count": abort_count,
        "error_count": error_count,
        "queue_time_sum": queue_time_sum,
        "queue_time_count": queue_time_count,
        "prompt_buckets": prompt_buckets,
        "gen_buckets": gen_buckets,
        "uptime": now - start_time,
        "engines": [],
    }

    for eid in sorted(running_by_engine.keys()):
        data["engines"].append({
            "id": int(eid),
            "running": running_by_engine.get(eid, 0),
            "waiting": waiting_by_engine.get(eid, 0),
            "kv_cache": kv_by_engine.get(eid, 0),
            "prompt_tokens": _sum_by_label(
                parsed.get("vllm:prompt_tokens_total", []), "engine"
            ).get(eid, 0),
            "generation_tokens": _sum_by_label(
                parsed.get("vllm:generation_tokens_total", []), "engine"
            ).get(eid, 0),
        })

    _last_snapshots[source_id] = data
    return data


def store_metrics(source_id: int, data: dict):
    """Persist metrics snapshot to SQLite."""
    conn = _get_db()
    ts = data["timestamp"]
    conn.execute(
        """INSERT OR REPLACE INTO metrics
        (timestamp, source_id, num_requests_running, num_requests_waiting, kv_cache_usage_perc,
         prompt_tokens_total, generation_tokens_total, request_success_total,
         prompt_tokens_cached_total, ttft_sum, ttft_count, itl_sum, itl_count,
         e2e_sum, e2e_count, prompt_len_sum, prompt_len_count, gen_len_sum, gen_len_count,
         prompt_len_max, gen_len_max, abort_count, error_count, queue_time_sum, queue_time_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ts, source_id, data["num_requests_running"], data["num_requests_waiting"],
         data["kv_cache_usage_perc"], data["prompt_tokens_total"],
         data["generation_tokens_total"], data["request_success_total"],
         data["prompt_tokens_cached_total"], data["ttft_sum"], data["ttft_count"],
         data["itl_sum"], data["itl_count"], data["e2e_sum"], data["e2e_count"],
         data["prompt_len_sum"], data["prompt_len_count"],
         data["gen_len_sum"], data["gen_len_count"],
         data["prompt_len_max"], data["gen_len_max"],
         data["abort_count"], data["error_count"],
         data["queue_time_sum"], data["queue_time_count"]),
    )
    for eng in data["engines"]:
        conn.execute(
            """INSERT OR REPLACE INTO engine_metrics
            (timestamp, engine_id, source_id, num_requests_running, num_requests_waiting,
             kv_cache_usage_perc, prompt_tokens_total, generation_tokens_total)
            VALUES (?,?,?,?,?,?,?,?)""",
            (ts, eng["id"], source_id, eng["running"], eng["waiting"],
             eng["kv_cache"], eng["prompt_tokens"], eng["generation_tokens"]),
        )
    conn.commit()
    conn.close()


# --- Scraper Thread ---


def scraper_loop(source_id: int):
    """Background thread: periodically scrape and store metrics for one source."""
    source = _source_by_id(source_id)
    logger.info("Scraper started, interval=%ds, url=%s",
                scrape_interval, source["url"])
    while True:
        # Never let a transient error (locked DB, disk hiccup, malformed
        # response) kill the thread — a dead scraper silently freezes that
        # source's dashboard while everything else keeps working.
        try:
            data = collect_metrics(source_id)
            if data:
                store_metrics(source_id, data)
                logger.debug(
                    "Collected [%s]: running=%.0f waiting=%.0f",
                    source["label"],
                    data["num_requests_running"], data["num_requests_waiting"],
                )
        except Exception:
            logger.exception("Scrape/store failed for %s; will retry",
                             source["label"])
        time.sleep(scrape_interval)


def start_scraper():
    """Start one scraper thread per configured source, plus the cleanup thread."""
    for source in sources:
        # Sources left in the DB from earlier runs keep their history visible
        # in the UI but are not scraped unless still configured.
        if source["url"] not in metrics_urls:
            continue
        threading.Thread(target=scraper_loop, args=(source["id"],),
                         daemon=True).start()

    def cleanup_loop():
        while True:
            time.sleep(3600)
            cleanup_db()

    threading.Thread(target=cleanup_loop, daemon=True).start()


# --- Query Helpers ---


def _downsample(timestamps, value_arrays, max_points):
    """Downsample parallel time-series arrays by binning.

    Groups consecutive points into bins of size (n / max_points).
    Returns (timestamps, mean_arrays, max_arrays).
    Returns (timestamps, value_arrays, None) if n <= max_points.
    """
    n = len(timestamps)
    if n <= max_points:
        return timestamps, value_arrays, None

    bin_size = n / max_points
    out_ts = []
    out_mean = [[] for _ in value_arrays]
    out_max = [[] for _ in value_arrays]

    i = 0
    for b in range(max_points):
        end = min(int((b + 1) * bin_size), n)
        if end <= i:
            end = i + 1
        if end > n:
            end = n

        count = end - i
        out_ts.append(sum(timestamps[i:end]) / count)
        for j, arr in enumerate(value_arrays):
            s = arr[i:end]
            out_mean[j].append(sum(s) / count)
            out_max[j].append(max(s))
        i = end

    return out_ts, out_mean, out_max


def query_history(minutes: int, max_points: int = 300,
                  source_id: int | None = None) -> dict:
    """Query historical metrics for the last N minutes for one source."""
    source = _source_by_id(source_id)
    cutoff = time.time() - minutes * 60
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM metrics WHERE timestamp >= ? AND source_id = ? "
        "ORDER BY timestamp",
        (cutoff, source["id"]),
    ).fetchall()

    timestamps = []
    running, waiting, kv = [], [], []
    req_rate, tok_rate, input_tok_rate = [], [], []
    cache_hit_rate = []
    ttft_avg, itl_avg, e2e_avg = [], [], []
    avg_prompt_length, avg_generation_length = [], []
    max_prompt_length, max_generation_length = [], []
    failure_rate = []
    queue_time_avg = []

    prev = None
    for r in rows:
        ts = r["timestamp"]
        timestamps.append(ts)
        running.append(r["num_requests_running"])
        waiting.append(r["num_requests_waiting"])
        kv.append(r["kv_cache_usage_perc"])

        if prev and (ts - prev["timestamp"]) > 0:
            dt = ts - prev["timestamp"]
            d_req = r["request_success_total"] - prev["request_success_total"]
            d_tok = r["generation_tokens_total"] - prev["generation_tokens_total"]
            d_input = r["prompt_tokens_total"] - prev["prompt_tokens_total"]

            # Detect Prometheus-style counter resets. If a counter dropped
            # significantly we treat the new value as the absolute increment
            # (the counter restarted from zero).
            if d_req < 0 and prev["request_success_total"] > 0:
                d_req = r["request_success_total"]
            if d_tok < 0 and prev["generation_tokens_total"] > 0:
                d_tok = r["generation_tokens_total"]
            if d_input < 0 and prev["prompt_tokens_total"] > 0:
                d_input = r["prompt_tokens_total"]

            req_rate.append(max(0, d_req / dt))
            tok_rate.append(max(0, d_tok / dt))
            input_tok_rate.append(max(0, d_input / dt))
            delta_prompt = r["prompt_tokens_total"] - prev["prompt_tokens_total"]
            delta_cached = r["prompt_tokens_cached_total"] - prev["prompt_tokens_cached_total"]
            # If prompt counter reset, hit-rate is undefined for this interval
            cache_hit_rate.append(
                (delta_cached / delta_prompt) if delta_prompt > 0 else 0
            )

            # Incremental average prompt / generation length
            d_prompt_len_count = (r["prompt_len_count"] or 0) - (prev["prompt_len_count"] or 0)
            d_prompt_len_sum = (r["prompt_len_sum"] or 0) - (prev["prompt_len_sum"] or 0)
            if d_prompt_len_count > 0:
                avg_prompt_length.append(d_prompt_len_sum / d_prompt_len_count)
            else:
                avg_prompt_length.append(0)

            d_gen_len_count = (r["gen_len_count"] or 0) - (prev["gen_len_count"] or 0)
            d_gen_len_sum = (r["gen_len_sum"] or 0) - (prev["gen_len_sum"] or 0)
            if d_gen_len_count > 0:
                avg_generation_length.append(d_gen_len_sum / d_gen_len_count)
            else:
                avg_generation_length.append(0)

            max_prompt_length.append(r["prompt_len_max"] or 0)
            max_generation_length.append(r["gen_len_max"] or 0)

            # Failure rate (abort + error) / total_requests in interval
            d_abort = (r["abort_count"] or 0) - (prev["abort_count"] or 0)
            d_error = (r["error_count"] or 0) - (prev["error_count"] or 0)
            d_total = d_req + d_abort  # d_req already excludes abort; add abort back
            if d_total > 0:
                failure_rate.append(((d_abort + d_error) / d_total) * 100)
            else:
                failure_rate.append(0)

            # Queue time average
            d_qt_count = (r["queue_time_count"] or 0) - (prev["queue_time_count"] or 0)
            d_qt_sum = (r["queue_time_sum"] or 0) - (prev["queue_time_sum"] or 0)
            if d_qt_count > 0:
                queue_time_avg.append(d_qt_sum / d_qt_count)
            else:
                queue_time_avg.append(0)
        else:
            req_rate.append(0)
            tok_rate.append(0)
            input_tok_rate.append(0)
            pt = r["prompt_tokens_total"]
            ct = r["prompt_tokens_cached_total"]
            cache_hit_rate.append((ct / pt) if pt > 0 else 0)
            avg_prompt_length.append(0)
            avg_generation_length.append(0)
            max_prompt_length.append(0)
            max_generation_length.append(0)
            failure_rate.append(0)
            queue_time_avg.append(0)

        ttft_avg.append(
            (r["ttft_sum"] / r["ttft_count"]) if r["ttft_count"] > 0 else 0
        )
        itl_avg.append(
            (r["itl_sum"] / r["itl_count"]) if r["itl_count"] > 0 else 0
        )
        e2e_avg.append(
            (r["e2e_sum"] / r["e2e_count"]) if r["e2e_count"] > 0 else 0
        )
        prev = r

    # Downsample main metrics if needed
    downsampled = False
    ds_max = {}
    if max_points > 0 and len(timestamps) > max_points:
        all_arrays = [running, waiting, kv, req_rate, tok_rate, input_tok_rate,
                      cache_hit_rate, ttft_avg, itl_avg, e2e_avg,
                      avg_prompt_length, avg_generation_length,
                      max_prompt_length, max_generation_length,
                      failure_rate, queue_time_avg]
        names = ["num_requests_running", "num_requests_waiting", "kv_cache_usage_perc",
                 "requests_per_second", "tokens_per_second", "input_tokens_per_second",
                 "cache_hit_rate", "ttft_avg", "itl_avg", "e2e_avg",
                 "avg_prompt_length", "avg_generation_length",
                 "max_prompt_length", "max_generation_length",
                 "failure_rate", "queue_time_avg"]
        timestamps, all_arrays, ds_max_list = _downsample(
            timestamps, all_arrays, max_points
        )
        running, waiting, kv = all_arrays[0], all_arrays[1], all_arrays[2]
        req_rate, tok_rate, input_tok_rate = all_arrays[3], all_arrays[4], all_arrays[5]
        cache_hit_rate, ttft_avg, itl_avg, e2e_avg = (
            all_arrays[6], all_arrays[7], all_arrays[8], all_arrays[9]
        )
        avg_prompt_length, avg_generation_length = all_arrays[10], all_arrays[11]
        max_prompt_length, max_generation_length = all_arrays[12], all_arrays[13]
        failure_rate, queue_time_avg = all_arrays[14], all_arrays[15]
        for idx, name in enumerate(names):
            ds_max[name] = ds_max_list[idx]
        downsampled = True

    # Per-engine history
    engine_history = {}
    eng_rows = conn.execute(
        "SELECT * FROM engine_metrics WHERE timestamp >= ? AND source_id = ? "
        "ORDER BY timestamp, engine_id",
        (cutoff, source["id"]),
    ).fetchall()
    for er in eng_rows:
        eid = str(er["engine_id"])
        if eid not in engine_history:
            engine_history[eid] = {
                "timestamps": [], "running": [], "waiting": [], "kv_cache": [],
            }
        engine_history[eid]["timestamps"].append(er["timestamp"])
        engine_history[eid]["running"].append(er["num_requests_running"])
        engine_history[eid]["waiting"].append(er["num_requests_waiting"])
        engine_history[eid]["kv_cache"].append(er["kv_cache_usage_perc"])

    # Downsample per-engine data if needed
    if max_points > 0:
        for eid in engine_history:
            eh = engine_history[eid]
            if len(eh["timestamps"]) > max_points:
                eng_arrays = [eh["running"], eh["waiting"], eh["kv_cache"]]
                eh["timestamps"], eng_arrays, eng_max = _downsample(
                    eh["timestamps"], eng_arrays, max_points
                )
                eh["running"], eh["waiting"], eh["kv_cache"] = eng_arrays
                eh["running_max"] = eng_max[0]

    conn.close()

    return {
        "timestamps": timestamps,
        "num_requests_running": running,
        "num_requests_waiting": waiting,
        "kv_cache_usage_perc": kv,
        "requests_per_second": req_rate,
        "tokens_per_second": tok_rate,
        "input_tokens_per_second": input_tok_rate,
        "cache_hit_rate": cache_hit_rate,
        "ttft_avg": ttft_avg,
        "itl_avg": itl_avg,
        "e2e_avg": e2e_avg,
        "avg_prompt_length": avg_prompt_length,
        "avg_generation_length": avg_generation_length,
        "max_prompt_length": max_prompt_length,
        "max_generation_length": max_generation_length,
        "failure_rate": failure_rate,
        "queue_time_avg": queue_time_avg,
        "engine_history": engine_history,
        "downsampled": downsampled,
        **({"max": ds_max} if downsampled else {}),
    }


def query_current(source_id: int | None = None) -> dict | None:
    """Get the most recent metrics snapshot for one source, with computed rates."""
    source = _source_by_id(source_id)
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM metrics WHERE source_id = ? "
        "ORDER BY timestamp DESC LIMIT 1",
        (source["id"],),
    ).fetchone()
    if not row:
        conn.close()
        return None

    prev_row = conn.execute(
        "SELECT * FROM metrics WHERE source_id = ? "
        "ORDER BY timestamp DESC LIMIT 1 OFFSET 1",
        (source["id"],),
    ).fetchone()
    conn.close()

    req_rate = tok_rate = input_rate = 0
    avg_prompt_len = avg_gen_len = 0
    failure_rate_val = 0
    queue_time_val = 0
    if prev_row:
        dt = row["timestamp"] - prev_row["timestamp"]
        if dt > 0:
            d_req = row["request_success_total"] - prev_row["request_success_total"]
            d_tok = row["generation_tokens_total"] - prev_row["generation_tokens_total"]
            d_input = row["prompt_tokens_total"] - prev_row["prompt_tokens_total"]
            # Handle counter reset (same logic as query_history)
            if d_req < 0 and prev_row["request_success_total"] > 0:
                d_req = row["request_success_total"]
            if d_tok < 0 and prev_row["generation_tokens_total"] > 0:
                d_tok = row["generation_tokens_total"]
            if d_input < 0 and prev_row["prompt_tokens_total"] > 0:
                d_input = row["prompt_tokens_total"]
            req_rate = max(0, d_req / dt)
            tok_rate = max(0, d_tok / dt)
            input_rate = max(0, d_input / dt)

            # Incremental average prompt / generation length
            plc = prev_row["prompt_len_count"] or 0
            pls = prev_row["prompt_len_sum"] or 0
            d_plc = (row["prompt_len_count"] or 0) - plc
            d_pls = (row["prompt_len_sum"] or 0) - pls
            if d_plc > 0:
                avg_prompt_len = d_pls / d_plc
            d_glc = (row["gen_len_count"] or 0) - (prev_row["gen_len_count"] or 0)
            d_gls = (row["gen_len_sum"] or 0) - (prev_row["gen_len_sum"] or 0)
            if d_glc > 0:
                avg_gen_len = d_gls / d_glc

            # Failure rate
            d_abort = (row["abort_count"] or 0) - (prev_row["abort_count"] or 0)
            d_error = (row["error_count"] or 0) - (prev_row["error_count"] or 0)
            d_total = d_req + d_abort
            if d_total > 0:
                failure_rate_val = ((d_abort + d_error) / d_total) * 100

            # Queue time
            d_qt_count = (row["queue_time_count"] or 0) - (prev_row["queue_time_count"] or 0)
            d_qt_sum = (row["queue_time_sum"] or 0) - (prev_row["queue_time_sum"] or 0)
            if d_qt_count > 0:
                queue_time_val = d_qt_sum / d_qt_count

    pt = row["prompt_tokens_total"]
    ct = row["prompt_tokens_cached_total"]
    hit_rate = (ct / pt * 100) if pt > 0 else 0

    # Try to get uptime from live metrics
    uptime = 0
    try:
        req = Request(source["url"], headers={"Accept": "text/plain"})
        with urlopen(req, timeout=5) as resp:
            parsed = parse_prometheus(resp.read().decode())
            start = _get_total(parsed, "process_start_time_seconds")
            uptime = time.time() - start
    except (URLError, OSError, ValueError):
        conn2 = _get_db()
        r = conn2.execute(
            "SELECT MIN(timestamp) as ts FROM metrics WHERE source_id = ?",
            (source["id"],),
        ).fetchone()
        conn2.close()
        uptime = time.time() - r["ts"] if r and r["ts"] else 0

    # Latest per-engine snapshot
    engines = []
    try:
        conn3 = _get_db()
        latest_engine_ts = conn3.execute(
            "SELECT MAX(timestamp) as ts FROM engine_metrics WHERE source_id = ?",
            (source["id"],),
        ).fetchone()["ts"]
        if latest_engine_ts:
            eng_rows = conn3.execute(
                """SELECT engine_id, num_requests_running, num_requests_waiting,
                          kv_cache_usage_perc, prompt_tokens_total, generation_tokens_total
                   FROM engine_metrics
                   WHERE timestamp = ? AND source_id = ?
                   ORDER BY engine_id""",
                (latest_engine_ts, source["id"]),
            ).fetchall()
            for er in eng_rows:
                engines.append({
                    "id": er["engine_id"],
                    "running": er["num_requests_running"],
                    "waiting": er["num_requests_waiting"],
                    "kv_cache": er["kv_cache_usage_perc"],
                    "prompt_tokens": er["prompt_tokens_total"],
                    "generation_tokens": er["generation_tokens_total"],
                })
        conn3.close()
    except Exception:
        engines = []

    return {
        "timestamp": row["timestamp"],
        "num_requests_running": row["num_requests_running"],
        "num_requests_waiting": row["num_requests_waiting"],
        "kv_cache_usage_perc": row["kv_cache_usage_perc"] * 100,
        "prompt_tokens_total": row["prompt_tokens_total"],
        "generation_tokens_total": row["generation_tokens_total"],
        "request_success_total": row["request_success_total"],
        "cache_hit_rate": hit_rate,
        "requests_per_second": req_rate,
        "tokens_per_second": tok_rate,
        "input_tokens_per_second": input_rate,
        "ttft_avg": (row["ttft_sum"] / row["ttft_count"]) if row["ttft_count"] > 0 else 0,
        "itl_avg": (row["itl_sum"] / row["itl_count"]) if row["itl_count"] > 0 else 0,
        "e2e_avg": (row["e2e_sum"] / row["e2e_count"]) if row["e2e_count"] > 0 else 0,
        "avg_prompt_length": avg_prompt_len,
        "avg_generation_length": avg_gen_len,
        "max_prompt_length": row["prompt_len_max"] or 0,
        "max_generation_length": row["gen_len_max"] or 0,
        "failure_rate": failure_rate_val,
        "queue_time_avg": queue_time_val,
        "uptime": uptime,
        "engines": engines,
    }
