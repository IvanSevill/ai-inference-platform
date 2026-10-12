import json
import os
import socket
import time

import pika
import psycopg
import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

WORKER_NAME = socket.gethostname()

OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434",
)

OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "llama3.2:1b",
)


JOBS_PROCESSED_TOTAL = Counter(
    "worker_jobs_processed_total",
    "Total number of jobs successfully processed",
    ["worker"],
)

JOBS_FAILED_TOTAL = Counter(
    "worker_jobs_failed_total",
    "Total number of jobs that failed",
    ["worker"],
)

JOBS_IN_PROGRESS = Gauge(
    "worker_jobs_in_progress",
    "Number of jobs currently being processed",
    ["worker"],
)

INFERENCE_DURATION_SECONDS = Histogram(
    "worker_inference_duration_seconds",
    "Time spent waiting for Ollama inference",
    ["worker"],
)


def get_database_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "ai_platform"),
        user=os.getenv("POSTGRES_USER", "platform"),
        password=os.getenv("POSTGRES_PASSWORD", "platformpass"),
    )


def update_job_status(job_id, status, result=None, error=None):
    with get_database_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE jobs
            SET status = %s,
                result = %s,
                error = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                status,
                result,
                error,
                job_id,
            ),
        )


def process_job(job):
    job_id = job["id"]

    print(
        f"[{WORKER_NAME}] Processing job {job_id}",
        flush=True,
    )

    JOBS_IN_PROGRESS.labels(
        worker=WORKER_NAME,
    ).inc()

    try:
        update_job_status(
            job_id,
            "PROCESSING",
        )

        inference_start = time.perf_counter()

        response = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": job["prompt"],
                "stream": False,
            },
            timeout=120,
        )

        inference_duration = time.perf_counter() - inference_start

        INFERENCE_DURATION_SECONDS.labels(
            worker=WORKER_NAME,
        ).observe(inference_duration)

        response.raise_for_status()

        result = response.json()["response"]

        update_job_status(
            job_id,
            "COMPLETED",
            result=result,
        )

        JOBS_PROCESSED_TOTAL.labels(
            worker=WORKER_NAME,
        ).inc()

        print(
            f"[{WORKER_NAME}] Prompt: {job['prompt']}",
            flush=True,
        )

        print(
            f"[{WORKER_NAME}] Response: {result}",
            flush=True,
        )

        print(
            f"[{WORKER_NAME}] Finished job {job_id}",
            flush=True,
        )

    finally:
        JOBS_IN_PROGRESS.labels(
            worker=WORKER_NAME,
        ).dec()


credentials = pika.PlainCredentials(
    os.getenv("RABBITMQ_USER", "platform"),
    os.getenv("RABBITMQ_PASSWORD", "platformpass"),
)

start_http_server(9100)

print(
    f"[{WORKER_NAME}] Metrics available on port 9100",
    flush=True,
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

channel.basic_qos(
    prefetch_count=1,
)


def callback(ch, method, properties, body):
    job = json.loads(body)

    try:
        process_job(job)

        ch.basic_ack(
            delivery_tag=method.delivery_tag,
        )

    except Exception as error:
        JOBS_FAILED_TOTAL.labels(
            worker=WORKER_NAME,
        ).inc()

        print(
            f"[{WORKER_NAME}] Job {job['id']} failed: {error}",
            flush=True,
        )

        try:
            update_job_status(
                job["id"],
                "FAILED",
                error=str(error),
            )

        except Exception as database_error:
            print(
                f"[{WORKER_NAME}] Could not update job status: {database_error}",
                flush=True,
            )

        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False,
        )


channel.basic_consume(
    queue="jobs",
    on_message_callback=callback,
)

print(
    f"[{WORKER_NAME}] Waiting for jobs...",
    flush=True,
)

channel.start_consuming()
