import json
import os
import socket
import time

import pika

WORKER_NAME = socket.gethostname()


def process_job(job):
    print(
        f"[{WORKER_NAME}] Processing job {job['id']}",
        flush=True,
    )

    time.sleep(3)

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
        host=os.getenv("RABBITMQ_HOST", "ai-platform-rabbitmq"),
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

    process_job(job)

    ch.basic_ack(
        delivery_tag=method.delivery_tag,
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
