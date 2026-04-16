#!/usr/bin/env python3
"""
web_server.py
index.html을 브라우저에 제공하는 경량 HTTP 서버
"""
import http.server
import socketserver
import os

PORT = 8081
DIR  = os.path.dirname(os.path.abspath(__file__))

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.address_string()} {fmt % args}")

if __name__ == "__main__":
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"[HTTP] Serving at http://0.0.0.0:{PORT}")
        httpd.serve_forever()