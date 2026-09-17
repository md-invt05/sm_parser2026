from __future__ import annotations

import http.client
import json
import os
import re
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


SOCKET_PATH = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
TARGET = os.getenv("SCANNER_CONTAINER_NAME", "evm-scanner")
REQUIRED_LABEL = "evm-parser.controlled"
CONTAINER_ROUTE = re.compile(r"^/containers/([^/]+)/(json|stats|start|stop|restart)$")


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str):
        super().__init__("localhost", timeout=20)
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


def docker_request(method: str, path: str) -> tuple[int, bytes, str]:
    connection = UnixConnection(SOCKET_PATH)
    try:
        connection.request(method, path, headers={"Host": "localhost"})
        response = connection.getresponse()
        return response.status, response.read(), response.getheader("Content-Type", "application/json")
    finally:
        connection.close()


def target_is_authorized() -> bool:
    status, body, _ = docker_request("GET", f"/containers/{TARGET}/json")
    if status != 200:
        return False
    try:
        labels = json.loads(body).get("Config", {}).get("Labels", {}) or {}
    except (ValueError, TypeError):
        return False
    return labels.get(REQUIRED_LABEL) == "true"


class GuardHandler(BaseHTTPRequestHandler):
    server_version = "evm-docker-guard/1"

    def log_message(self, fmt: str, *args: object) -> None:
        # Paths never contain secrets; retain concise access logs.
        print(f"docker-guard {self.address_string()} {fmt % args}", flush=True)

    def _reply(self, status: int, body: bytes = b"", content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _handle(self) -> None:
        parsed = urlsplit(self.path)
        match = CONTAINER_ROUTE.fullmatch(parsed.path)
        if not match:
            self._reply(404, b'{"error":"route not allowed"}')
            return
        container, action = match.groups()
        if container != TARGET:
            self._reply(403, b'{"error":"container not allowed"}')
            return
        expected_method = "GET" if action in {"json", "stats"} else "POST"
        if self.command != expected_method:
            self._reply(405, b'{"error":"method not allowed"}')
            return
        try:
            if not target_is_authorized():
                self._reply(403, b'{"error":"required label is missing"}')
                return
            upstream = f"/containers/{TARGET}/{action}"
            if action == "stats":
                upstream += "?stream=false"
            elif action in {"stop", "restart"}:
                upstream += "?t=30"
            status, body, content_type = docker_request(self.command, upstream)
            self._reply(status, body, content_type)
        except (OSError, http.client.HTTPException) as exc:
            payload = json.dumps({"error": type(exc).__name__}).encode()
            self._reply(502, payload)

    do_GET = _handle
    do_POST = _handle


def main() -> None:
    port = int(os.getenv("PORT", "2375"))
    server = ThreadingHTTPServer(("0.0.0.0", port), GuardHandler)
    print(f"docker-guard listening on {port}; fixed target={TARGET}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
