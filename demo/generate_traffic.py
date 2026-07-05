#!/usr/bin/env python3
"""E-commerce traffic simulator — feeds the CDC pipeline with realistic activity.

Writes to the source Postgres database (the one Debezium captures):
  - new customers and new orders with 1-4 line items
  - order status progression (pending -> confirmed -> shipped -> delivered -> completed)
  - occasional cancellations and quantity updates

Every commit becomes a CDC event: watch it flow through Kafka, aggregate in
Flink (gold.order-revenue-1m) and land in Iceberg via the batch pipeline.

Usage:
    export PGPASSWORD=<password>          # see scripts/demo.sh for k8s wiring
    python demo/generate_traffic.py --rate 30 --duration 10

    --rate      actions per minute (default 30)
    --duration  minutes to run, 0 = until Ctrl-C (default 0)
"""

# Keep imports working on Python 3.8 too — the documented pytest fallback runs
# inside apache/spark:3.5.3-python3 (see CLAUDE.md), which ships 3.8.
from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("traffic")

# Allowed status transitions with weights — pending orders mostly confirm,
# a small share cancels; everything else moves forward.
STATUS_FLOW: dict[str, list[tuple[str, float]]] = {
    "pending": [("confirmed", 0.9), ("cancelled", 0.1)],
    "confirmed": [("shipped", 0.95), ("cancelled", 0.05)],
    "shipped": [("delivered", 1.0)],
    "delivered": [("completed", 1.0)],
}
TERMINAL_STATUSES = ("completed", "cancelled")

PRODUCTS: list[tuple[int, str, float]] = [
    (101, "Mechanical keyboard", 89.90),
    (102, "4K monitor", 349.00),
    (103, "USB-C dock", 129.50),
    (104, "Noise-cancelling headset", 199.99),
    (105, "Webcam", 59.90),
    (106, "Laptop stand", 39.00),
    (107, "Ergonomic mouse", 49.90),
    (108, "Desk mat", 19.90),
    (109, "HDMI cable", 9.90),
    (110, "External SSD 1TB", 109.00),
]

FIRST_NAMES = ["Alice", "Bob", "Chloe", "David", "Emma", "Felix", "Grace", "Hugo", "Ines", "Jules"]
LAST_NAMES = ["Martin", "Dupont", "Bernard", "Petit", "Durand", "Leroy", "Moreau", "Simon", "Laurent"]


def next_status(status: str, rng: random.Random) -> str | None:
    """Return the next order status, or None if the status is terminal.

    Args:
        status: Current order status.
        rng: Random source (injected for deterministic tests).

    Returns:
        The next status drawn from the weighted transition table, or None.
    """
    transitions = STATUS_FLOW.get(status)
    if not transitions:
        return None
    choices, weights = zip(*transitions)
    return rng.choices(choices, weights=weights, k=1)[0]


def connect():
    """Open a Postgres connection from PG* environment variables."""
    # Imported lazily so unit tests can import this module without the driver.
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "lakehouse"),
        user=os.environ.get("PGUSER", "lakehouse"),
        password=os.environ.get("PGPASSWORD", "lakehouse-dev"),
    )


def create_customer(cur, rng: random.Random) -> None:
    name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    email = f"{name.split()[0].lower()}.{uuid.uuid4().hex[:8]}@example.com"
    cur.execute(
        "INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING customer_id",
        (name, email),
    )
    logger.info("new customer  #%s %s", cur.fetchone()[0], name)


def create_order(cur, rng: random.Random) -> None:
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        create_customer(cur, rng)
        cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
        row = cur.fetchone()
    customer_id = row[0]

    cur.execute(
        "INSERT INTO orders (customer_id, status, total_amount) VALUES (%s, 'pending', 0) RETURNING order_id",
        (customer_id,),
    )
    order_id = cur.fetchone()[0]

    total = 0.0
    for product_id, _, price in rng.sample(PRODUCTS, k=rng.randint(1, 4)):
        quantity = rng.randint(1, 3)
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, quantity, price),
        )
        total += quantity * price

    cur.execute(
        "UPDATE orders SET total_amount = %s, updated_at = NOW() WHERE order_id = %s",
        (round(total, 2), order_id),
    )
    logger.info("new order     #%s customer=%s total=%.2f", order_id, customer_id, total)


def progress_order(cur, rng: random.Random) -> None:
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status NOT IN %s ORDER BY random() LIMIT 1",
        (TERMINAL_STATUSES,),
    )
    row = cur.fetchone()
    if row is None:
        return
    order_id, status = row
    new = next_status(status, rng)
    if new is None:
        return
    cur.execute(
        "UPDATE orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
        (new, order_id),
    )
    logger.info("order status  #%s %s -> %s", order_id, status, new)


def update_item_quantity(cur, rng: random.Random) -> None:
    cur.execute(
        "SELECT i.item_id, i.order_id, i.unit_price FROM order_items i "
        "JOIN orders o ON o.order_id = i.order_id "
        "WHERE o.status = 'pending' ORDER BY random() LIMIT 1"
    )
    row = cur.fetchone()
    if row is None:
        return
    item_id, order_id, _ = row
    quantity = rng.randint(1, 5)
    cur.execute("UPDATE order_items SET quantity = %s WHERE item_id = %s", (quantity, item_id))
    cur.execute(
        "UPDATE orders o SET total_amount = "
        "(SELECT SUM(quantity * unit_price) FROM order_items WHERE order_id = o.order_id), "
        "updated_at = NOW() WHERE o.order_id = %s",
        (order_id,),
    )
    logger.info("item update   #%s quantity=%s (order #%s)", item_id, quantity, order_id)


# Action mix: mostly new orders and progressions — enough movement for
# 1-minute Flink windows to show live variation.
ACTIONS = [
    (create_order, 0.45),
    (progress_order, 0.35),
    (create_customer, 0.10),
    (update_item_quantity, 0.10),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate demo e-commerce traffic")
    parser.add_argument("--rate", type=float, default=30, help="actions per minute (default 30)")
    parser.add_argument("--duration", type=float, default=0, help="minutes to run, 0 = forever")
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    conn = connect()
    conn.autocommit = False
    logger.info(
        "connected to %s:%s/%s — %.0f actions/min",
        os.environ.get("PGHOST", "localhost"),
        os.environ.get("PGPORT", "5432"),
        os.environ.get("PGDATABASE", "lakehouse"),
        args.rate,
    )

    deadline = time.monotonic() + args.duration * 60 if args.duration else None
    actions, weights = zip(*ACTIONS)
    count = 0
    try:
        while deadline is None or time.monotonic() < deadline:
            action = rng.choices(actions, weights=weights, k=1)[0]
            with conn.cursor() as cur:
                action(cur, rng)
            conn.commit()  # one commit per action = one CDC transaction
            count += 1
            time.sleep(60.0 / args.rate * rng.uniform(0.5, 1.5))
    except KeyboardInterrupt:
        logger.info("interrupted")
    finally:
        conn.close()
        logger.info("done — %d actions committed", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
