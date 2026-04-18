from __future__ import annotations

from conftest import load_service_module


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http error {self.status_code}")

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, responses: list[tuple[str, str, dict]]):
        self._responses = responses
        self.calls: list[tuple[str, str, dict | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, json=None, headers=None):  # noqa: A002
        self.calls.append(("POST", url, json))
        method, expected_url, payload = self._responses.pop(0)
        assert method == "POST"
        assert expected_url == url
        return _FakeResponse(payload)

    def patch(self, url: str, json=None, headers=None):  # noqa: A002
        self.calls.append(("PATCH", url, json))
        method, expected_url, payload = self._responses.pop(0)
        assert method == "PATCH"
        assert expected_url == url
        return _FakeResponse(payload)


def test_orchestrator_compensates_and_cancels_order(tmp_path, monkeypatch):
    db = load_service_module("saga_orchestrator", "app.db")
    engine = load_service_module("saga_orchestrator", "app.engine")

    db_path = tmp_path / "orchestrator.db"
    monkeypatch.setattr(engine, "DATABASE_PATH", str(db_path))

    saga_id = "saga-1"
    with db.get_conn(str(db_path)) as conn:
        db.insert_saga(conn, saga_id, {"trace_id": "trace-1"})

    responses = [
        ("POST", f"{engine.ORDER_URL}/orders", {"order_id": "order-1", "correlation_id": "corr-1"}),
        ("POST", f"{engine.PAYMENT_URL}/payments/charge", {"event_type": "PAYMENT_PROCESSED"}),
        ("PATCH", f"{engine.ORDER_URL}/orders/order-1/status", {"ok": True}),
        ("POST", f"{engine.INVENTORY_URL}/inventory/reserve", {"event_type": "INVENTORY_FAILED"}),
        ("POST", f"{engine.PAYMENT_URL}/payments/refund", {"refunded": True}),
        ("PATCH", f"{engine.ORDER_URL}/orders/order-1/status", {"ok": True}),
    ]
    fake_client = _FakeClient(responses)

    class _Factory:
        def __call__(self, *args, **kwargs):
            return fake_client

    monkeypatch.setattr(engine.httpx, "Client", _Factory())

    result = engine.run_orchestrated_saga_sync(
        saga_id=saga_id,
        customer_id="cust-1",
        items=[{"sku": "SKU-DEMO", "qty": 1}],
        amount_cents=1250,
    )

    assert result["state"] == "CANCELLED"
    assert result["order_id"] == "order-1"

    with db.get_conn(str(db_path)) as conn:
        row = db.get_saga(conn, saga_id)
        assert row is not None
        assert row.state == db.SagaState.CANCELLED
        logs = db.list_saga_logs(conn, saga_id)
        assert any("compensating" in msg.lower() for msg in logs)
