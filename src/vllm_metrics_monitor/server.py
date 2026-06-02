"""HTTP server: static files + JSON API."""

import json
import logging
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

from . import collector

logger = logging.getLogger(__name__)

STATIC_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "static"))


class DashboardHandler(BaseHTTPRequestHandler):
    """Serve static files and JSON API."""

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._serve_file("index.html", "text/html")
        elif self.path == "/api/current":
            self._serve_json(collector.query_current())
        elif self.path.startswith("/api/history"):
            minutes = 60
            if "minutes=" in self.path:
                try:
                    minutes = int(self.path.split("minutes=")[1].split("&")[0])
                except (ValueError, IndexError):
                    pass
            self._serve_json(collector.query_history(minutes))
        else:
            self._serve_static(self.path)

    def _serve_static(self, raw_path: str):
        """Serve a static file with path traversal protection."""
        rel_path = raw_path.lstrip("/")
        filepath = os.path.realpath(os.path.join(STATIC_DIR, rel_path))
        if not filepath.startswith(STATIC_DIR + os.sep):
            self.send_error(403)
            return
        if not os.path.isfile(filepath):
            self.send_error(404)
            return
        ext = os.path.splitext(filepath)[1]
        content_types = {
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }
        self._send_file(filepath, content_types.get(ext, "application/octet-stream"))

    def _serve_file(self, filename: str, content_type: str):
        filepath = os.path.realpath(os.path.join(STATIC_DIR, filename))
        if not filepath.startswith(STATIC_DIR + os.sep) or not os.path.isfile(filepath):
            self.send_error(404)
            return
        self._send_file(filepath, content_type)

    def _send_file(self, filepath: str, content_type: str):
        with open(filepath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_json(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        logger.debug("HTTP %s", format % args)


def run_server(port: int):
    """Start the HTTP server (blocking)."""
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    logger.info("Dashboard running at http://0.0.0.0:%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.shutdown()
