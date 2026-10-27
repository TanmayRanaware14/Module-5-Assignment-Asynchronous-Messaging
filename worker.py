#!/usr/bin/env python3
"""Order worker. Consumes the orders queue, one message at a time, then ACKs."""

import argparse
import fcntl
import json
import os
import sys
from pathlib import Path

import pika

QUEUE = "orders"
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
PROCESSED_FILE = DATA_DIR / "processed_event_ids.txt"
BUSINESS_LOG = DATA_DIR / "business_actions.log"


def connection_parameters():
    return pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST", "127.0.0.1"),
        port=int(os.environ.get("RABBITMQ_PORT", "5672")),
        credentials=pika.PlainCredentials(
            os.environ.get("RABBITMQ_USER", "guest"),
            os.environ.get("RABBITMQ_PASSWORD", "guest"),
        ),
    )


def claim_event(event_id, worker_name, order):
    """Record event_id once. Returns False when this id was already processed."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_FILE.touch(exist_ok=True)
    with PROCESSED_FILE.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        seen = {line.strip() for line in handle if line.strip()}
        if event_id in seen:
            return False
        handle.write(event_id + "\n")
        handle.flush()
        with BUSINESS_LOG.open("a", encoding="utf-8") as business:
            business.write(
                f"{worker_name} processed event_id={event_id} "
                f"order_id={order['order_id']} item_id={order['item_id']} "
                f"quantity={order['quantity']}\n"
            )
            business.flush()
        return True


def make_callback(worker_name, crash_event_id):
    def callback(channel, method, properties, body):
        try:
            order = json.loads(body.decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            print(f"[{worker_name}] invalid JSON; acknowledging so the queue can move on", flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        event_id = order.get("event_id")
        order_id = order.get("order_id")
        item_id = order.get("item_id")
        quantity = order.get("quantity")
        print(
            f"[{worker_name}] received event_id={event_id} order_id={order_id} "
            f"item_id={item_id} quantity={quantity} redelivered={method.redelivered}",
            flush=True,
        )

        if not all(isinstance(value, str) and value for value in (event_id, order_id, item_id)):
            print(f"[{worker_name}] missing order fields; acknowledging and skipping", flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 1:
            print(f"[{worker_name}] invalid quantity; acknowledging and skipping", flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if crash_event_id and event_id == crash_event_id and not method.redelivered:
            print(
                f"[{worker_name}] stopping before ACK for event_id={event_id}; "
                "business action not applied; message stays unacknowledged",
                flush=True,
            )
            os._exit(1)

        if not claim_event(event_id, worker_name, order):
            print(
                f"[{worker_name}] duplicate event_id={event_id} detected; business action skipped",
                flush=True,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[{worker_name}] ack event_id={event_id}", flush=True)
            return

        print(
            f"[{worker_name}] processing order_id={order_id} item_id={item_id} quantity={quantity}",
            flush=True,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{worker_name}] ack event_id={event_id}", flush=True)

    return callback


def main():
    parser = argparse.ArgumentParser(description="Consume order messages from RabbitMQ.")
    parser.add_argument("--name", default=os.environ.get("WORKER_NAME", "worker"))
    args = parser.parse_args()
    crash_event_id = os.environ.get("CRASH_BEFORE_ACK_EVENT_ID") or None

    connection = pika.BlockingConnection(connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=QUEUE,
        on_message_callback=make_callback(args.name, crash_event_id),
        auto_ack=False,
    )
    print(f"[{args.name}] listening on queue {QUEUE}", flush=True)
    if crash_event_id:
        print(f"[{args.name}] will exit before ACK if event_id={crash_event_id} arrives", flush=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError as exc:
        print(f"[worker] could not reach RabbitMQ: {exc}", file=sys.stderr)
        sys.exit(1)
