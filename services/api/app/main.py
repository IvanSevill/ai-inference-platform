import json
import os
import uuid
from uuid import UUID

import pika
import psycopg
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row
from pydantic import BaseModel

app = FastAPI(title="AI Inference Platform API")


class JobRequest(BaseModel):
    prompt: str


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
