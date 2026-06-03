"""CLI entry point for vllm-metrics-monitor."""

import argparse
import logging
import os

from . import collector, server

# Default db path: ~/.vmm/data.db
DEFAULT_DB = os.path.join(os.path.expanduser("~"), ".vmm", "data.db")


def main():
    parser = argparse.ArgumentParser(
        prog="vmm",
        description="Lightweight monitoring dashboard for vLLM inference servers",
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="http://localhost:8000/metrics",
        help="vLLM Prometheus metrics endpoint (default: http://localhost:8000/metrics)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8080,
        help="Dashboard HTTP port (default: 8080)",
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=3,
        help="Metrics scrape interval in seconds (default: 3)",
    )
    parser.add_argument(
        "--retention",
        type=int,
        default=720,
        help="Data retention in hours (default: 720, i.e. 30 days)",
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"SQLite database path (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing database and start fresh",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    # Resolve db path
    db_file = args.db or DEFAULT_DB
    db_dir = os.path.dirname(db_file)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    # --reset: remove existing db
    if args.reset:
        for suffix in ("", "-shm", "-wal"):
            p = db_file + suffix
            if os.path.exists(p):
                os.remove(p)
                logger.info("Removed %s", p)

    # Configure collector
    collector.metrics_url = args.url
    collector.scrape_interval = args.interval
    collector.retention_hours = args.retention
    collector.db_path = db_file

    # Init DB and start background threads
    collector.init_db()
    collector.start_scraper()

    # Block on HTTP server
    server.run_server(args.port)


if __name__ == "__main__":
    main()
