#!/usr/bin/env python3
"""Serve the site directory with a WebAssembly content type."""

import http.server
import mimetypes
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "site"
PORT = 8765


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()


def main() -> None:
    os.chdir(ROOT)
    mimetypes.add_type("application/wasm", ".wasm")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Serving {ROOT} at http://127.0.0.1:{PORT}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
