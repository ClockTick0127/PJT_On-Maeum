"""Small internal HTTP server for health, readiness, and Prometheus metrics."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, REGISTRY, generate_latest

from realtime_analysis.readiness import ReadinessProbe


logger = logging.getLogger(__name__)


class _ObservabilityHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _request_handler(
    readiness: ReadinessProbe,
    registry: CollectorRegistry,
) -> type[BaseHTTPRequestHandler]:
    class ObservabilityRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            path = urlsplit(self.path).path
            if path == "/health":
                self._write_json(200, {"status": "UP"})
                return
            if path == "/ready":
                report = readiness.evaluate()
                self._write_json(
                    200 if report.ready else 503,
                    {
                        "status": "UP" if report.ready else "DOWN",
                        "checks": report.checks,
                    },
                )
                return
            if path == "/metrics":
                payload = generate_latest(registry)
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(payload)
                return
            self._write_json(404, {"status": "NOT_FOUND"})

        def _write_json(self, status: int, body: dict[str, object]) -> None:
            payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            logger.debug("Observability HTTP: " + format, *args)

    return ObservabilityRequestHandler


class ObservabilityServer:
    """Runs the internal endpoints beside the blocking Kafka consumer loop."""

    def __init__(
        self,
        host: str,
        port: int,
        readiness: ReadinessProbe,
        registry: CollectorRegistry = REGISTRY,
    ) -> None:
        self._server = _ObservabilityHttpServer(
            (host, port),
            _request_handler(readiness, registry),
        )
        self._thread: Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("Observability HTTP server is already running")
        self._thread = Thread(
            target=self._server.serve_forever,
            name="observability-http",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Observability endpoints started: address=%s:%s",
            self._server.server_address[0],
            self.port,
        )

    def stop(self) -> None:
        if self._thread is None:
            self._server.server_close()
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        self._thread = None
        logger.info("Observability endpoints stopped")
