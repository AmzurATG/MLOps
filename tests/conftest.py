"""
Shared test fixtures for MLOps Fraud Detection platform.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock dagster before importing any pipelines modules
# This avoids requiring dagster to be installed for unit tests
if 'dagster' not in sys.modules:
    sys.modules['dagster'] = MagicMock()
    sys.modules['dagster_mlflow'] = MagicMock()

# Configure pytest-asyncio if available
try:
    import pytest_asyncio
    pytest_plugins = ['pytest_asyncio']
except ImportError:
    pass  # pytest-asyncio not installed, async tests will be skipped


# =============================================================================
# ENVIRONMENT SETUP
# =============================================================================

@pytest.fixture(autouse=True)
def mock_environment(monkeypatch):
    """Set up test environment variables."""
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    monkeypatch.setenv("FEAST_REPO_PATH", "/tmp/feast_repo")
    monkeypatch.setenv("REDIS_HOST", "localhost")
    monkeypatch.setenv("REDIS_PORT", "6379")
    monkeypatch.setenv("TRINO_HOST", "localhost")
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setenv("PROMETHEUS_METRICS", "false")
    monkeypatch.setenv("AB_TEST_ENABLED", "false")
    monkeypatch.setenv("SHADOW_MODE_ENABLED", "false")


# =============================================================================
# MOCK FIXTURES
# =============================================================================

@pytest.fixture
def mock_feast_client():
    """Mock Feast feature store client."""
    mock = MagicMock()
    mock.get_online_features.return_value = MagicMock(
        to_dict=lambda: {
            "customer_id": ["CUST_123"],
            "customer_age": [35],
            "customer_tenure_days": [365],
            "customer_total_transactions": [150],
            "customer_avg_transaction": [125.50],
        }
    )
    return mock


@pytest.fixture
def mock_mlflow_model():
    """Mock MLflow model for inference."""
    mock_model = MagicMock()
    mock_model.predict.return_value = [[0.75]]  # High fraud probability
    return mock_model


@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    mock = MagicMock()
    mock.get.return_value = None
    mock.set.return_value = True
    mock.hgetall.return_value = {}
    return mock


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_transaction():
    """Sample fraud transaction data for testing."""
    return {
        "transaction_id": "TXN_TEST_001",
        "amount": 150.00,
        "customer_id": "CUST_123",
        "merchant_category": "electronics",
        "transaction_hour": 14,
        "transaction_day_of_week": 2,
        "is_weekend": False,
        "is_night": False,
        "customer_age": 35,
    }


@pytest.fixture
def sample_high_risk_transaction():
    """Sample high-risk transaction for testing."""
    return {
        "transaction_id": "TXN_HIGH_RISK_001",
        "amount": 9500.00,  # High amount
        "customer_id": "CUST_456",
        "merchant_category": "cash_advance",
        "transaction_hour": 3,  # Night time
        "transaction_day_of_week": 6,  # Weekend
        "is_weekend": True,
        "is_night": True,
        "customer_age": 22,
    }


@pytest.fixture
def sample_batch_transactions():
    """Sample batch of transactions for testing."""
    return [
        {
            "transaction_id": f"TXN_BATCH_{i:03d}",
            "amount": 100.00 + (i * 10),
            "customer_id": f"CUST_{i:03d}",
            "merchant_category": "retail",
            "transaction_hour": 10 + (i % 12),
            "transaction_day_of_week": i % 7,
            "is_weekend": (i % 7) >= 5,
            "is_night": False,
            "customer_age": 25 + i,
        }
        for i in range(5)
    ]


@pytest.fixture
def sample_prediction_result():
    """Sample prediction result."""
    return {
        "transaction_id": "TXN_TEST_001",
        "fraud_score": 0.75,
        "is_fraud": True,
        "risk_level": "high",
        "model_version": "1.0.0",
        "model_stage": "Production",
        "latency_ms": 45.5,
        "timestamp": datetime.now().isoformat(),
    }


# =============================================================================
# SHADOW MODE FIXTURES
# =============================================================================

@pytest.fixture
def shadow_mode_config():
    """Configuration for shadow mode testing."""
    return {
        "enabled": True,
        "shadow_stage": "Staging",
        "agreement_threshold": 0.9,
        "score_diff_threshold": 0.2,
    }


@pytest.fixture
def shadow_prediction_pair():
    """Pair of production and shadow predictions."""
    return {
        "production": {
            "fraud_score": 0.75,
            "is_fraud": True,
        },
        "shadow": {
            "fraud_score": 0.72,
            "is_fraud": True,
        },
        "agreement": True,
        "score_difference": 0.03,
    }


@pytest.fixture
def shadow_disagreement_pair():
    """Pair of predictions that disagree."""
    return {
        "production": {
            "fraud_score": 0.55,
            "is_fraud": True,
        },
        "shadow": {
            "fraud_score": 0.35,
            "is_fraud": False,
        },
        "agreement": False,
        "score_difference": 0.20,
    }


# =============================================================================
# PERFORMANCE TRACKER FIXTURES
# =============================================================================

@pytest.fixture
def performance_metrics():
    """Sample performance metrics."""
    return {
        "model_stage": "Production",
        "window": "24h",
        "accuracy": 0.92,
        "precision": 0.88,
        "recall": 0.85,
        "f1_score": 0.865,
        "total_predictions": 1000,
        "true_positives": 85,
        "false_positives": 12,
        "true_negatives": 835,
        "false_negatives": 15,
    }


# =============================================================================
# STREAMING FEATURE FIXTURES
# =============================================================================

@pytest.fixture
def sample_streaming_features():
    """Sample streaming features from Redis/ksqlDB."""
    return {
        "tx_count_5min": 3,
        "tx_count_1h": 12,
        "tx_count_24h": 35,
        "velocity_score": 0.6,
        "multi_country_flag": False,
        "multi_device_flag": False,
        "high_velocity_flag": False,
        "amount_anomaly_flag": False,
    }


@pytest.fixture
def high_risk_streaming_features():
    """High-risk streaming features for testing rules."""
    return {
        "tx_count_5min": 8,
        "tx_count_1h": 25,
        "tx_count_24h": 60,
        "velocity_score": 0.9,
        "multi_country_flag": True,
        "multi_device_flag": True,
        "high_velocity_flag": True,
        "amount_anomaly_flag": True,
    }


@pytest.fixture
def sample_batch_features():
    """Sample batch features from Feast."""
    return {
        "avg_amount_30d": 150.0,
        "tx_count_30d": 25,
        "total_spend_30d": 3750.0,
        "days_since_last_tx": 2.5,
        "is_new_customer": 0,
        "transactions_before": 100,
    }


# =============================================================================
# SERVING LAYER FIXTURES
# =============================================================================

@pytest.fixture
def mock_model_loader():
    """Mock ModelLoader for serving tests."""
    mock = MagicMock()
    mock.load.return_value = MagicMock(
        model=MagicMock(predict_proba=lambda x: [[0.25, 0.75]]),
        info=MagicMock(
            model_name="fraud_detection_model",
            version=1,
            stage="Production",
            run_id="test_run_123",
            contract_version="1.0.0",
        ),
        transformer=None,
        contract=None,
    )
    mock.cached_count = 1
    mock.list_models.return_value = ["fraud_detection_model"]
    return mock


@pytest.fixture
def mock_feature_service():
    """Mock FeatureService for serving tests."""
    from api.serving.features.feature_service import FeatureVector

    mock = MagicMock()
    mock.get_features.return_value = FeatureVector(
        batch_features={"avg_amount_30d": 150.0, "tx_count_30d": 25},
        streaming_features={"tx_count_5min": 3, "velocity_score": 0.5},
        batch_latency_ms=5.0,
        streaming_latency_ms=2.0,
        batch_available=True,
        streaming_available=True,
    )
    return mock


# =============================================================================
# RESOURCE CACHE MANAGEMENT
# =============================================================================

@pytest.fixture
def clear_caches():
    """Fixture to clear resource caches before and after tests."""
    # Note: clear_resource_caches was in old pipelines.core.resources
    # Now using standalone resources which don't have cache clearing
    yield
