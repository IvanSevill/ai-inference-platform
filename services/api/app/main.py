import json
import os
import time
import uuid
from uuid import UUID

import pika
import psycopg
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from pydantic import BaseModel

app = FastAPI(title="AI Inference Platform API")


HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "path", "status_code"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
)

JOBS_CREATED_TOTAL = Counter(
    "jobs_created_total",
    "Total number of inference jobs created",
)


class JobRequest(BaseModel):
    prompt: str


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    start_time = time.perf_counter()

    response = await call_next(request)

    duration = time.perf_counter() - start_time

    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)

    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        path=path,
        status_code=str(response.status_code),
    ).inc()

    HTTP_REQUEST_DURATION_SECONDS.labels(
        method=request.method,
        path=path,
    ).observe(duration)

    return response


def get_database_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ai_platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platformpass"),
        row_factory=dict_row,
    )


def publish_job(job):
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "platform"),
        os.getenv("RABBITMQ_PASSWORD", "platformpass"),
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=5672,
            credentials=credentials,
        )
    )

    channel = connection.channel()

    channel.queue_declare(
        queue="jobs",
        durable=True,
    )

    channel.basic_publish(
        exchange="",
        routing_key="jobs",
        body=json.dumps(job),
        properties=pika.BasicProperties(
            delivery_mode=2,
        ),
    )

    connection.close()


@app.get("/")
def root():
    return {
        "service": "api",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.post("/jobs")
def create_job(request: JobRequest):
    job_id = uuid.uuid4()

    with get_database_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO jobs (id, prompt, status)
            VALUES (%s, %s, 'PENDING')
            """,
            (
                job_id,
                request.prompt,
            ),
        )

    JOBS_CREATED_TOTAL.inc()

    job = {
        "id": str(job_id),
        "type": "inference",
        "prompt": request.prompt,
    }

    try:
        publish_job(job)

    except Exception as error:
        with get_database_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE jobs
                    SET status = 'FAILED',
                        error = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        str(error),
                        job_id,
                    ),
                )

        raise HTTPException(
            status_code=503,
            detail="Could not queue the job",
        )

    return {
        "status": "queued",
        "job": job,
    }


@app.get("/jobs/{job_id}")
def get_job(job_id: UUID):
    with get_database_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                prompt,
                status,
                result,
                error,
                created_at,
                updated_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )

        job = cursor.fetchone()

    if job is None:
        raise HTTPException(
            status_code=404,
            detail="Job not found",
        )

    return job
