"""Small environment-driven authentication layer for reports and dashboard."""

from __future__ import annotations

import base64
import binascii
import hmac
import os
from dataclasses import dataclass
from collections.abc import Mapping

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response


@dataclass(frozen=True, slots=True)
class AuthSettings:
    enabled: bool
    dashboard_username: str = ""
    dashboard_password: str = ""
    admin_token: str = ""
    operator_token: str = ""
    viewer_token: str = ""

    @classmethod
    def from_env(cls, environment: Mapping[str, str] | None = None) -> "AuthSettings":
        env = os.environ if environment is None else environment
        enabled = env.get("REPORTING_AUTH_ENABLED", "false").lower() in {"1", "true", "yes"}
        settings = cls(
            enabled=enabled,
            dashboard_username=env.get("REPORTING_DASHBOARD_USERNAME", "").strip(),
            dashboard_password=env.get("REPORTING_DASHBOARD_PASSWORD", ""),
            admin_token=env.get("REPORTING_ADMIN_TOKEN", ""),
            operator_token=env.get("REPORTING_OPERATOR_TOKEN", ""),
            viewer_token=env.get("REPORTING_VIEWER_TOKEN", ""),
        )
        if enabled and not all((settings.dashboard_username, settings.dashboard_password,
                                settings.admin_token, settings.operator_token,
                                settings.viewer_token)):
            raise ValueError("Reporting authentication is enabled but credentials are incomplete")
        return settings


class ReportingAuthMiddleware(BaseHTTPMiddleware):
    """Protect dashboard and API routes while leaving health and OpenAPI public."""

    def __init__(self, app, settings: AuthSettings):
        super().__init__(app)
        self.settings = settings

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if not self.settings.enabled or not (
            request.url.path == "/dashboard" or request.url.path.startswith("/api/v1")
        ):
            return await call_next(request)
        role = self._role(request.headers.get("authorization", ""))
        if role is None:
            headers = {"WWW-Authenticate": "Basic"} if request.url.path == "/dashboard" else {}
            return JSONResponse({"detail": "Authentication required"}, status_code=401, headers=headers)
        request.state.reporting_role = role
        response = await call_next(request)
        response.headers["X-Reporting-Role"] = role
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; frame-ancestors 'none'"
        )
        return response

    def _role(self, authorization: str) -> str | None:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer":
            for role, token in (("administrator", self.settings.admin_token),
                                ("operator", self.settings.operator_token),
                                ("read-only", self.settings.viewer_token)):
                if hmac.compare_digest(value, token):
                    return role
        if scheme.lower() == "basic":
            try:
                username, password = base64.b64decode(value).decode("utf-8").split(":", 1)
            except (ValueError, UnicodeDecodeError, binascii.Error):
                return None
            if (hmac.compare_digest(username, self.settings.dashboard_username)
                    and hmac.compare_digest(password, self.settings.dashboard_password)):
                return "administrator"
        return None
