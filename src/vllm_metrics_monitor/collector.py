"""Metrics collector: Prometheus parser, SQLite storage, scraper loop."""

import logging
import os
import re
import sqlite3
import threading
import time
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger(__name__)

# --- Configuration (set by CLI) ---
metrics_url: str = "http://localhost:8000/metrics"
scrape_interval: int = 1
retention_hours: int = 24
db_path: str = "data.db"


# --- Database ---


def _get_db():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS metrics (
            timestamp REAL PRIMARY KEY,
            num_requests_running REAL,
            num_requests_waiting REAL,
            kv_cache_usage_perc REAL,
            prompt_tokens_total REAL,
            generation_tokens_total REAL,
            request_success_total REAL,
            prompt_tokens_cached_total REAL,
            ttft_sum REAL, ttft_count REAL,
            itl_sum REAL, itl_count REAL,
            e2e_sum REAL, e2e_count REAL
        );
        CREATE TABLE IF NOT EXISTS engine_metrics (
            timestamp REAL,
            engine_id INTEGER,
            num_requests_running REAL,
            num_requests_waiting REAL,
            kv_cache_usage_perc REAL,
            prompt_tokens_total REAL,
            generation_tokens_total REAL,
            PRIMARY KEY (timestamp, engine_id)
        );
    """)
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


# --- Collect & Store ---


_last_snapshot: dict | None = None


def _fallback_snapshot() -> dict:
    """Return snapshot with gauges=0 and counters carried from last sample."""
    prev = _last_snapshot
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
        "uptime": 0,
        "engines": [],
    }


def collect_metrics() -> dict | None:
    """Fetch and parse vLLM metrics. Returns fallback dict on timeout."""
    global _last_snapshot
    try:
        req = Request(metrics_url, headers={"Accept": "text/plain"})
        with urlopen(req, timeout=5) as resp:
            text = resp.read().decode()
    except (URLError, OSError) as e:
        logger.warning("Metrics fetch failed: %s", e)
        if _last_snapshot is None:
            # No baseline yet; storing a fallback with zero counters would
            # create a fake "reset" and produce a huge spike on the next
            # successful scrape.
            return None
        return _fallback_snapshot()

    parsed = parse_prometheus(text)

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

    _last_snapshot = data
    return data


def store_metrics(data: dict):
    """Persist metrics snapshot to SQLite."""
    conn = _get_db()
    ts = data["timestamp"]
    conn.execute(
        """INSERT OR REPLACE INTO metrics
        (timestamp, num_requests_running, num_requests_waiting, kv_cache_usage_perc,
         prompt_tokens_total, generation_tokens_total, request_success_total,
         prompt_tokens_cached_total, ttft_sum, ttft_count, itl_sum, itl_count,
         e2e_sum, e2e_count)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ts, data["num_requests_running"], data["num_requests_waiting"],
         data["kv_cache_usage_perc"], data["prompt_tokens_total"],
         data["generation_tokens_total"], data["request_success_total"],
         data["prompt_tokens_cached_total"], data["ttft_sum"], data["ttft_count"],
         data["itl_sum"], data["itl_count"], data["e2e_sum"], data["e2e_count"]),
    )
    for eng in data["engines"]:
        conn.execute(
            """INSERT OR REPLACE INTO engine_metrics
            (timestamp, engine_id, num_requests_running, num_requests_waiting,
             kv_cache_usage_perc, prompt_tokens_total, generation_tokens_total)
            VALUES (?,?,?,?,?,?,?)""",
            (ts, eng["id"], eng["running"], eng["waiting"],
             eng["kv_cache"], eng["prompt_tokens"], eng["generation_tokens"]),
        )
    conn.commit()
    conn.close()


# --- Scraper Thread ---


def scraper_loop():
    """Background thread: periodically scrape and store metrics."""
    logger.info("Scraper started, interval=%ds, url=%s", scrape_interval, metrics_url)
    while True:
        data = collect_metrics()
        if data:
            store_metrics(data)
            logger.debug(
                "Collected: running=%.0f waiting=%.0f",
                data["num_requests_running"], data["num_requests_waiting"],
            )
        time.sleep(scrape_interval)


def start_scraper():
    """Start scraper and cleanup threads."""
    threading.Thread(target=scraper_loop, daemon=True).start()

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


def query_history(minutes: int, max_points: int = 300) -> dict:
    """Query historical metrics for the last N minutes."""
    cutoff = time.time() - minutes * 60
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM metrics WHERE timestamp >= ? ORDER BY timestamp",
        (cutoff,),
    ).fetchall()

    timestamps = []
    running, waiting, kv = [], [], []
    req_rate, tok_rate, input_tok_rate = [], [], []
    cache_hit_rate = []
    ttft_avg, itl_avg, e2e_avg = [], [], []

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
        else:
            req_rate.append(0)
            tok_rate.append(0)
            input_tok_rate.append(0)
            pt = r["prompt_tokens_total"]
            ct = r["prompt_tokens_cached_total"]
            cache_hit_rate.append((ct / pt) if pt > 0 else 0)

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
                      cache_hit_rate, ttft_avg, itl_avg, e2e_avg]
        names = ["num_requests_running", "num_requests_waiting", "kv_cache_usage_perc",
                 "requests_per_second", "tokens_per_second", "input_tokens_per_second",
                 "cache_hit_rate", "ttft_avg", "itl_avg", "e2e_avg"]
        timestamps, all_arrays, ds_max_list = _downsample(
            timestamps, all_arrays, max_points
        )
        running, waiting, kv = all_arrays[0], all_arrays[1], all_arrays[2]
        req_rate, tok_rate, input_tok_rate = all_arrays[3], all_arrays[4], all_arrays[5]
        cache_hit_rate, ttft_avg, itl_avg, e2e_avg = (
            all_arrays[6], all_arrays[7], all_arrays[8], all_arrays[9]
        )
        for idx, name in enumerate(names):
            ds_max[name] = ds_max_list[idx]
        downsampled = True

    # Per-engine history
    engine_history = {}
    eng_rows = conn.execute(
        "SELECT * FROM engine_metrics WHERE timestamp >= ? ORDER BY timestamp, engine_id",
        (cutoff,),
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
        "engine_history": engine_history,
        "downsampled": downsampled,
        **({"max": ds_max} if downsampled else {}),
    }


def query_current() -> dict | None:
    """Get the most recent metrics snapshot with computed rates."""
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    if not row:
        conn.close()
        return None

    prev_row = conn.execute(
        "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 1 OFFSET 1"
    ).fetchone()
    conn.close()

    req_rate = tok_rate = input_rate = 0
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

    pt = row["prompt_tokens_total"]
    ct = row["prompt_tokens_cached_total"]
    hit_rate = (ct / pt * 100) if pt > 0 else 0

    # Try to get uptime from live metrics
    uptime = 0
    try:
        req = Request(metrics_url, headers={"Accept": "text/plain"})
        with urlopen(req, timeout=5) as resp:
            parsed = parse_prometheus(resp.read().decode())
            start = _get_total(parsed, "process_start_time_seconds")
            uptime = time.time() - start
    except (URLError, OSError, ValueError):
        conn2 = _get_db()
        r = conn2.execute("SELECT MIN(timestamp) as ts FROM metrics").fetchone()
        conn2.close()
        uptime = time.time() - r["ts"] if r and r["ts"] else 0

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
        "uptime": uptime,
    }
