import json
import os
import socket

import pika
import requests

WORKER_NAME = socket.gethostname()

OLLAMA_HOST = os.getenv(
    "OLLAMA_HOST",
    "http://ollama:11434",
)

OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "llama3.2:1b",
)


def process_job(job):
    print(
        f"[{WORKER_NAME}] Processing job {job['id']}",
        flush=True,
    )

    response = requests.post(
        f"{OLLAMA_HOST}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": job["prompt"],
            "stream": False,
        },
        timeout=120,
    )

    response.raise_for_status()

    result = response.json()

    print(
        f"[{WORKER_NAME}] Prompt: {job['prompt']}",
        flush=True,
    )

    print(
        f"[{WORKER_NAME}] Response: {result['response']}",
        flush=True,
    )

    print(
        f"[{WORKER_NAME}] Finished job {job['id']}",
        flush=True,
    )


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

channel.basic_qos(
    prefetch_count=1,
)


def callback(ch, method, properties, body):
    try:
        job = json.loads(body)

        process_job(job)

        ch.basic_ack(
            delivery_tag=method.delivery_tag,
        )

    except Exception as error:
        print(
            f"[{WORKER_NAME}] Job failed: {error}",
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
