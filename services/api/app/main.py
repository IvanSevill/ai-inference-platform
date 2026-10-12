import json
import os
import uuid

import pika
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="AI Inference Platform API")


class JobRequest(BaseModel):
    prompt: str


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
    job = {
        "id": str(uuid.uuid4()),
        "type": "inference",
        "prompt": request.prompt,
    }

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

    return {
        "status": "queued",
        "job": job,
    }
