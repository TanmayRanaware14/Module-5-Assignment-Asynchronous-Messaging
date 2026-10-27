#!/usr/bin/env python3
"""Order producer. Publishes order messages to the RabbitMQ orders queue."""

import argparse
import json
import os
import sys

import pika

QUEUE = "orders"

SAMPLE_ORDERS = [
    {"event_id": "evt-1001", "order_id": "ord-1001", "item_id": "laptop", "quantity": 1},
    {"event_id": "evt-1002", "order_id": "ord-1002", "item_id": "keyboard", "quantity": 2},
    {"event_id": "evt-1003", "order_id": "ord-1003", "item_id": "headphones", "quantity": 1},
    {"event_id": "evt-1004", "order_id": "ord-1004", "item_id": "mouse", "quantity": 3},
]


def connection_parameters():
    return pika.ConnectionParameters(
        host=os.environ.get("RABBITMQ_HOST", "127.0.0.1"),
        port=int(os.environ.get("RABBITMQ_PORT", "5672")),
        credentials=pika.PlainCredentials(
            os.environ.get("RABBITMQ_USER", "guest"),
            os.environ.get("RABBITMQ_PASSWORD", "guest"),
        ),
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ.")
    parser.add_argument("--event-id")
    parser.add_argument("--order-id")
    parser.add_argument("--item-id")
    parser.add_argument("--quantity", type=int)
    args = parser.parse_args()
    custom = [args.event_id, args.order_id, args.item_id, args.quantity]
    if any(value is not None for value in custom) and not all(value is not None for value in custom):
        parser.error("--event-id, --order-id, --item-id, and --quantity must be used together")
    if args.quantity is not None and args.quantity < 1:
        parser.error("--quantity must be a positive integer")
    return args


def orders_to_publish(args):
    if args.event_id:
        return [
            {
                "event_id": args.event_id,
                "order_id": args.order_id,
                "item_id": args.item_id,
                "quantity": args.quantity,
            }
        ]
    return SAMPLE_ORDERS


def main():
    args = parse_args()
    orders = orders_to_publish(args)

    connection = pika.BlockingConnection(connection_parameters())
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.confirm_delivery()

    for order in orders:
        body = json.dumps(order).encode()
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(
            f"[producer] published event_id={order['event_id']} "
            f"order_id={order['order_id']} item_id={order['item_id']} "
            f"quantity={order['quantity']}",
            flush=True,
        )

    connection.close()
    print(f"[producer] {len(orders)} message(s) confirmed on queue {QUEUE}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError as exc:
        print(f"[producer] could not reach RabbitMQ: {exc}", file=sys.stderr)
        sys.exit(1)
