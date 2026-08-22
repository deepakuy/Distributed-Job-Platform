from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

# HTTP API Metrics
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total count of HTTP requests received",
    ["method", "endpoint", "status_code"],
)

HTTP_REQUEST_LATENCY = Histogram(
    "http_request_latency_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
)

# Job Platform Metrics
JOBS_CREATED = Counter(
    "jobs_created_total",
    "Total number of jobs created",
    ["type"],
)

JOBS_PROCESSED = Counter(
    "jobs_processed_total",
    "Total number of jobs processed to termination (succeeded, failed, cancelled, dead_lettered)",
    ["type", "status"],
)

JOB_PROCESSING_LATENCY = Histogram(
    "job_processing_latency_seconds",
    "Job execution latency in seconds",
    ["type"],
)

QUEUE_DEPTH = Gauge(
    "queue_depth",
    "Current number of jobs in specific states",
    ["status"],
)

ACTIVE_WORKERS = Gauge(
    "active_workers_count",
    "Number of active worker processes",
)


def get_metrics_response() -> Response:
    """Generate Prometheus exposition format payload."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
