import time
import uuid
import structlog
from fastapi import FastAPI, Request, Response, status, Depends
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from redis import asyncio as aioredis

from app.core.config import settings
from app.core.database import get_db
from app.core.logging import configure_logging, logger
from app.core.metrics import (
    HTTP_REQUESTS_TOTAL,
    HTTP_REQUEST_LATENCY,
    get_metrics_response,
)
from app.api import auth, jobs

# Configure logging on app load
configure_logging(settings.APP_ENV)

app = FastAPI(
    title=settings.APP_NAME,
    description="A production-grade distributed asynchronous job execution platform",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routers
app.include_router(auth.router, prefix="/api/v1")
app.include_router(jobs.router, prefix="/api/v1")


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """Injects correlation ID/request ID into contextvars and logs API latency."""
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    
    # Bind request_id to structured logger
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    
    start_time = time.perf_counter()
    
    # Process request
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        # Structured log of uncaught error
        logger.error("Uncaught exception in request handler", error=str(exc), exc_info=True)
        response = JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": "An unexpected error occurred on the server.",
                    "request_id": request_id,
                }
            },
        )
    
    # Calculate latency
    duration = time.perf_counter() - start_time
    status_code = response.status_code
    
    # Record Prometheus metrics
    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        endpoint=request.url.path,
        status_code=status_code
    ).inc()
    
    HTTP_REQUEST_LATENCY.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    # Add Request ID header to response
    response.headers["X-Request-ID"] = request_id
    
    logger.info(
        "Request processed",
        method=request.method,
        path=request.url.path,
        status_code=status_code,
        duration_seconds=duration,
    )
    
    return response


# Exception Handlers
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Standardized validation error handler."""
    request_id = structlog.contextvars.get_contextvars().get("request_id")
    logger.warn("Validation error on request", errors=exc.errors())
    
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Input validation failed. Please check the request payload.",
                "request_id": request_id,
            }
        },
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Standardized generic exception handler."""
    request_id = structlog.contextvars.get_contextvars().get("request_id")
    logger.error("Exception occurred during execution", error=str(exc), exc_info=True)
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "INTERNAL_SERVER_ERROR",
                "message": "An unexpected error occurred.",
                "request_id": request_id,
            }
        },
    )


# Shallow L7 Health Check
@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    """Shallow L7 health check to verify process liveness."""
    return {"status": "ok", "timestamp": time.time()}


# Deep L7 Readiness Check
@app.get("/ready", status_code=status.HTTP_200_OK)
async def readiness_check(db_session: AsyncSession = Depends(get_db)):
    """Deep L7 readiness check verifying database and Redis connectivity."""
    status_details = {"database": "ok", "redis": "ok"}
    is_ready = True
    
    # Check PostgreSQL
    try:
        await db_session.execute(select(1))
    except Exception as exc:
        status_details["database"] = f"down: {exc}"
        is_ready = False
        
    # Check Redis
    try:
        redis_client = aioredis.from_url(settings.REDIS_URL, socket_timeout=2)
        await redis_client.ping()
        await redis_client.close()
    except Exception as exc:
        status_details["redis"] = f"down: {exc}"
        is_ready = False
        
    if not is_ready:
        logger.error("Readiness check failed", details=status_details)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unavailable", "details": status_details}
        )
        
    return {"status": "ready", "details": status_details}


# Prometheus Metrics Exposition Endpoint
@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return get_metrics_response()
