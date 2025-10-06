"""
Tests for the FastAPI microservice (app.py) that exposes /transaction.
Requires: httpx (for TestClient), fastapi, pydantic.
"""

from fastapi.testclient import TestClient

# Import the FastAPI instance from app.py
# If your app file lives elsewhere (e.g., src/app.py), change the import to:
#   from src.app import app as fastapi_app
from app import app as fastapi_app

client = TestClient(fastapi_app)


def test_health():
    """Basic healthcheck should return status ok."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_config_contains_score_mapping():
    """Config endpoint should expose current rule thresholds/weights."""
    r = client.get("/config")
    assert r.status_code == 200
    payload = r.json()
    assert isinstance(payload, dict)
    assert "score_to_decision" in payload
    assert "amount_thresholds" in payload


def test_transaction_in_review_path():
    """Typical medium-risk digital transaction from NEW user at night -> IN_REVIEW."""
    body = {
        "transaction_id": 42,
        "amount_mxn": 5200.0,
        "customer_txn_30d": 1,
        "geo_state": "Nuevo León",
        "device_type": "mobile",
        "chargeback_count": 0,
        "hour": 23,
        "product_type": "digital",
        "latency_ms": 180,
        "user_reputation": "new",
        "device_fingerprint_risk": "low",
        "ip_risk": "medium",
        "email_risk": "new_domain",
        "bin_country": "MX",
        "ip_country": "MX"
    }
    r = client.post("/transaction", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["transaction_id"] == 42
    assert data["decision"] in ("ACCEPTED", "IN_REVIEW", "REJECTED")
    # With the current defaults (reject_at=10, review_at=4), this should lean to IN_REVIEW
    # If you tuned env vars REJECT_AT/REVIEW_AT, this assertion may need adjustment.
    assert data["decision"] == "IN_REVIEW"


def test_transaction_hard_block_rejection():
    """Chargebacks>=2 with ip_risk=high should trigger hard block -> REJECTED."""
    body = {
        "transaction_id": 99,
        "amount_mxn": 300.0,
        "customer_txn_30d": 0,
        "geo_state": "Nuevo León",
        "device_type": "mobile",
        "chargeback_count": 2,
        "hour": 12,
        "product_type": "digital",
        "latency_ms": 100,
        "user_reputation": "new",
        "device_fingerprint_risk": "low",
        "ip_risk": "high",
        "email_risk": "low",
        "bin_country": "MX",
        "ip_country": "MX"
    }
    r = client.post("/transaction", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["transaction_id"] == 99
    assert data["decision"] == "REJECTED"

def test_transaction_hour_out_of_range():
    """Verificar que la aplicación maneja valores de hora fuera de rango 0-23"""
    body_over = {
        "transaction_id": 101,
        "amount_mxn": 1000.0,
        "hour": 24,
        "product_type": "digital"
    }
    r = client.post("/transaction", json=body_over)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    data = r.json()
    assert data["transaction_id"] == 101

    body_under = {
        "transaction_id": 102,
        "amount_mxn": 1000.0,
        "hour": -1,
        "product_type": "digital"
    }
    r = client.post("/transaction", json=body_under)
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"


def test_transaction_bin_ip_country_mismatch():
    """Verificar comportamiento cuando bin_country != ip_country"""
    body = {
        "transaction_id": 200,
        "amount_mxn": 3000.0,
        "customer_txn_30d": 5,
        "geo_state": "CDMX",
        "device_type": "desktop",
        "chargeback_count": 0,
        "hour": 14,
        "product_type": "digital",
        "latency_ms": 150,
        "user_reputation": "trusted",
        "device_fingerprint_risk": "low",
        "ip_risk": "low",
        "email_risk": "low",
        "bin_country": "MX",
        "ip_country": "US"
    }
    r = client.post("/transaction", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["transaction_id"] == 200
    assert data["decision"] in ("ACCEPTED", "IN_REVIEW", "REJECTED")
    assert isinstance(data["risk_score"], (int, float))

def test_transaction_response_structure():
    """Verfiicar que la respuesta tenga la estructura correcta"""
    body = {
        "transaction_id": 300,
        "amount_mxn": 500.0,
        "product_type": "digital"
    }
    r = client.post("/transaction", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    
    assert "transaction_id" in data, "Response should contain transaction_id"
    assert "decision" in data, "Response should contain decision"
    assert "risk_score" in data, "Response should contain risk_score"
    assert "reasons" in data, "Response should contain reasons"

    assert isinstance(data["transaction_id"], int), "transaction_id should be int"
    assert isinstance(data["decision"], str), "decision should be string"
    assert isinstance(data["risk_score"], (int, float)), "risk_score should be numeric"
    assert isinstance(data["reasons"], str), "reasons should be string"

    assert data["transaction_id"] == 300, "transaction_id should match request"
    assert data["decision"] in ("ACCEPTED", "IN_REVIEW", "REJECTED"), \
        f"decision should be ACCEPTED, IN_REVIEW or REJECTED, got {data['decision']}"
    
    assert isinstance(data["reasons"], str), "reasons should be a string"
    assert data["risk_score"] >= 0, "risk_score should be non-negative"