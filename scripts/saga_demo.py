#!/usr/bin/env python3
"""Minimal CLI to run choreography or orchestration demo flows."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


def post(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(body, file=sys.stderr)
        raise SystemExit(e.code) from e


def get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=60) as resp:
        return json.loads(resp.read().decode())


def main() -> None:
    p = argparse.ArgumentParser(description="Saga demo CLI")
    p.add_argument(
        "--order-url",
        default="http://127.0.0.1:8000",
        help="Order service base URL",
    )
    p.add_argument(
        "--orchestrator-url",
        default="http://127.0.0.1:8003",
        help="Orchestrator base URL",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    ch = sub.add_parser("choreography", help="POST /orders (event-driven saga)")
    ch.add_argument("--amount-cents", type=int, default=1999)

    orch = sub.add_parser("orchestration", help="POST /orchestration/sagas")
    orch.add_argument("--amount-cents", type=int, default=1999)

    st = sub.add_parser("status", help="GET order by id")
    st.add_argument("order_id")
    st.add_argument(
        "--order-url",
        default="http://127.0.0.1:8000",
        help="Order service base URL",
    )

    sg = sub.add_parser("saga", help="GET orchestration saga status")
    sg.add_argument("saga_id")
    sg.add_argument(
        "--orchestrator-url",
        default="http://127.0.0.1:8003",
        help="Orchestrator base URL",
    )

    args = p.parse_args()

    if args.cmd == "choreography":
        out = post(
            f"{args.order_url.rstrip('/')}/orders",
            {
                "customer_id": "cli",
                "items": [{"sku": "SKU-DEMO", "qty": 1}],
                "amount_cents": args.amount_cents,
            },
        )
        print(json.dumps(out, indent=2))
        oid = out.get("order_id")
        if oid:
            print("\nPoll:", f"{args.order_url.rstrip('/')}/orders/{oid}")

    elif args.cmd == "orchestration":
        out = post(
            f"{args.orchestrator_url.rstrip('/')}/orchestration/sagas",
            {
                "customer_id": "cli",
                "items": [{"sku": "SKU-DEMO", "qty": 1}],
                "amount_cents": args.amount_cents,
            },
        )
        print(json.dumps(out, indent=2))
        sid = out.get("saga_id")
        if sid:
            print("\nSaga:", f"{args.orchestrator_url.rstrip('/')}/orchestration/sagas/{sid}")

    elif args.cmd == "status":
        out = get(f"{args.order_url.rstrip('/')}/orders/{args.order_id}")
        print(json.dumps(out, indent=2))

    elif args.cmd == "saga":
        out = get(f"{args.orchestrator_url.rstrip('/')}/orchestration/sagas/{args.saga_id}")
        print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
