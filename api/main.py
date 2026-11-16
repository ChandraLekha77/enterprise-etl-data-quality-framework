"""FastAPI entry point for read-only ETL quality reports."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.auth import AuthSettings, ReportingAuthMiddleware
from api.dashboard import router as dashboard_router
from api.metrics import Metrics, MetricsMiddleware, router as metrics_router
from api.operator import AirflowClient, router as operator_router
from api.routes.reports import router
from etl_framework.reporting import ReportingError, ReportingRepository


@asynccontextmanager
async def _lifespan(app: FastAPI):
    yield
    repository = app.state.reporting_repository
    if repository is not None and app.state.owns_reporting_repository:
        repository.close()


def create_app(
    repository: ReportingRepository | None = None,
    auth_settings: AuthSettings | None = None,
    airflow_client: AirflowClient | None = None,
) -> FastAPI:
    app = FastAPI(
        title="Enterprise ETL Quality Reports",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
        lifespan=_lifespan,
    )
    app.state.reporting_repository = repository
    app.state.owns_reporting_repository = False
    app.state.metrics = Metrics()
    app.state.airflow_client = airflow_client
    app.add_middleware(
        ReportingAuthMiddleware,
        settings=auth_settings or AuthSettings.from_env(),
    )
    app.include_router(router)
    app.include_router(dashboard_router)
    app.include_router(metrics_router)
    app.include_router(operator_router)
    app.add_middleware(MetricsMiddleware, metrics=app.state.metrics)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, _exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422, content={"detail": "Request validation failed"}
        )

    @app.exception_handler(ReportingError)
    async def reporting_error_handler(
        _request: Request, _exc: ReportingError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"detail": "Reporting service is unavailable"}
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: Request, _exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500, content={"detail": "Internal reporting error"}
        )

    return app


app = create_app()
