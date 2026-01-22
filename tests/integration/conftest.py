"""
Integration test fixtures.

These fixtures connect to real services running in Docker.
Set environment variables to configure connections:
    REDIS_HOST, REDIS_PORT
    TRINO_HOST, TRINO_PORT
    MLFLOW_TRACKING_URI
    LAKEFS_ENDPOINT
"""

import os
import pytest
from typing import Generator, Any


# =============================================================================
# SKIP CONDITIONS
# =============================================================================

def is_docker_running() -> bool:
    """Check if Docker services are available."""
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "ps"], capture_output=True, timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def is_redis_available() -> bool:
    """Check if Redis is accessible."""
    try:
        import redis
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "16379"))
        r = redis.Redis(host=host, port=port, socket_timeout=2)
        r.ping()
        return True
    except Exception:
        return False


def is_trino_available() -> bool:
    """Check if Trino is accessible."""
    try:
        import trino
        host = os.getenv("TRINO_HOST", "localhost")
        port = int(os.getenv("TRINO_PORT", "18083"))
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            user="test",
            catalog="iceberg_dev",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True
    except Exception:
        return False


# Skip markers for missing services
requires_docker = pytest.mark.skipif(
    not is_docker_running(),
    reason="Docker is not running"
)

requires_redis = pytest.mark.skipif(
    not is_redis_available(),
    reason="Redis is not available"
)

requires_trino = pytest.mark.skipif(
    not is_trino_available(),
    reason="Trino is not available"
)


# =============================================================================
# REDIS FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def redis_client():
    """Create Redis client for integration tests."""
    import redis
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "16379"))

    client = redis.Redis(host=host, port=port, decode_responses=True)

    # Verify connection
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available")

    yield client

    # Cleanup test keys
    for key in client.scan_iter("test:*"):
        client.delete(key)


@pytest.fixture
def redis_test_prefix(redis_client) -> str:
    """Generate unique prefix for test keys."""
    import uuid
    prefix = f"test:{uuid.uuid4().hex[:8]}"
    yield prefix
    # Cleanup
    for key in redis_client.scan_iter(f"{prefix}:*"):
        redis_client.delete(key)


# =============================================================================
# TRINO FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def trino_connection():
    """Create Trino connection for integration tests."""
    import trino
    host = os.getenv("TRINO_HOST", "localhost")
    port = int(os.getenv("TRINO_PORT", "18083"))

    try:
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            user="integration_test",
            catalog="iceberg_dev",
            schema="bronze",
        )
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"Trino not available: {e}")


@pytest.fixture
def trino_cursor(trino_connection):
    """Create Trino cursor for tests."""
    cursor = trino_connection.cursor()
    yield cursor
    cursor.close()


# =============================================================================
# API CLIENT FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def fraud_api_client():
    """Create HTTP client for Fraud API integration tests."""
    import httpx

    base_url = os.getenv("FRAUD_API_URL", "http://localhost:18002")

    client = httpx.Client(base_url=base_url, timeout=30.0)

    # Check if API is available
    try:
        response = client.get("/health")
        if response.status_code != 200:
            pytest.skip("Fraud API not healthy")
    except Exception as e:
        pytest.skip(f"Fraud API not available: {e}")

    yield client
    client.close()


@pytest.fixture(scope="session")
def cv_api_client():
    """Create HTTP client for CV API integration tests."""
    import httpx

    base_url = os.getenv("CV_API_URL", "http://localhost:8002")

    client = httpx.Client(base_url=base_url, timeout=60.0)

    # Check if API is available
    try:
        response = client.get("/health")
        if response.status_code != 200:
            pytest.skip("CV API not healthy")
    except Exception as e:
        pytest.skip(f"CV API not available: {e}")

    yield client
    client.close()


# =============================================================================
# MLFLOW FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def mlflow_client():
    """Create MLflow client for integration tests."""
    import mlflow

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:15000")
    mlflow.set_tracking_uri(tracking_uri)

    try:
        # Verify connection
        mlflow.search_experiments(max_results=1)
    except Exception as e:
        pytest.skip(f"MLflow not available: {e}")

    yield mlflow.MlflowClient()


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_fraud_transaction() -> dict:
    """Generate sample transaction for fraud prediction."""
    return {
        "transaction_id": "INTG_TEST_001",
        "amount": 250.00,
        "customer_id": "CUST_TEST_001",
        "merchant_category": "electronics",
        "transaction_hour": 14,
        "is_weekend": False,
        "customer_age": 35,
        "account_age_days": 365,
        "transaction_count_1h": 2,
        "transaction_count_24h": 5,
        "avg_transaction_amount_7d": 150.00,
    }


@pytest.fixture
def sample_batch_transactions(sample_fraud_transaction) -> list:
    """Generate batch of sample transactions."""
    import copy
    transactions = []
    for i in range(10):
        txn = copy.deepcopy(sample_fraud_transaction)
        txn["transaction_id"] = f"INTG_TEST_{i:03d}"
        txn["amount"] = 100.0 + (i * 50)
        transactions.append(txn)
    return transactions
