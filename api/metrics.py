"""Dependency-free Prometheus metrics for the reporting service."""

from __future__ import annotations

from collections import Counter
from threading import Lock
from time import perf_counter

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint


class Metrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self.requests: Counter[tuple[str, str, int]] = Counter()
        self.duration_seconds: Counter[tuple[str, str]] = Counter()

    def observe(self, method: str, route: str, status: int, duration: float) -> None:
        with self._lock:
            self.requests[(method, route, status)] += 1
            self.duration_seconds[(method, route)] += duration

    def render(self) -> str:
        lines = [
            "# HELP reporting_http_requests_total HTTP requests handled.",
            "# TYPE reporting_http_requests_total counter",
        ]
        with self._lock:
            for (method, route, status), value in sorted(self.requests.items()):
                labels = f'method="{method}",route="{route}",status="{status}"'
                lines.append(f"reporting_http_requests_total{{{labels}}} {value}")
            lines.extend((
                "# HELP reporting_http_request_duration_seconds_total Cumulative request time.",
                "# TYPE reporting_http_request_duration_seconds_total counter",
            ))
            for (method, route), value in sorted(self.duration_seconds.items()):
                labels = f'method="{method}",route="{route}"'
                lines.append(
                    f"reporting_http_request_duration_seconds_total{{{labels}}} {value:.6f}"
                )
        return "\n".join(lines) + "\n"


class MetricsMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, metrics: Metrics):
        super().__init__(app)
        self.metrics = metrics

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint):
        started = perf_counter()
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", request.url.path)
        self.metrics.observe(request.method, route, response.status_code, perf_counter() - started)
        return response


router = APIRouter()


@router.get("/metrics", response_class=PlainTextResponse, include_in_schema=False)
def metrics(request: Request) -> str:
    return request.app.state.metrics.render()
