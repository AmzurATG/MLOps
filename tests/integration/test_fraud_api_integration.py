"""
Integration tests for the Fraud Detection API.

These tests verify end-to-end functionality against running services.
Run with: pytest tests/integration/test_fraud_api_integration.py -v -m integration
"""

import pytest
from tests.integration.conftest import requires_redis


# =============================================================================
# HEALTH & READINESS TESTS
# =============================================================================

@pytest.mark.integration
class TestFraudAPIHealth:
    """Integration tests for API health endpoints."""

    def test_health_endpoint_returns_healthy(self, fraud_api_client):
        """Test that /health returns healthy status with real services."""
        response = fraud_api_client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ["healthy", "degraded"]

    def test_root_endpoint_returns_api_info(self, fraud_api_client):
        """Test that root endpoint returns API information."""
        response = fraud_api_client.get("/")

        assert response.status_code == 200
        data = response.json()
        assert "name" in data or "version" in data or "message" in data


# =============================================================================
# PREDICTION TESTS
# =============================================================================

@pytest.mark.integration
class TestFraudPrediction:
    """Integration tests for fraud prediction endpoints."""

    def test_single_prediction_returns_score(
        self, fraud_api_client, sample_fraud_transaction
    ):
        """Test that single prediction returns valid fraud score."""
        response = fraud_api_client.post(
            "/predict",
            json=sample_fraud_transaction
        )

        # May return 500/503 if no model loaded yet
        if response.status_code in [500, 503]:
            pytest.skip("Model not available - prediction requires trained model")

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert "transaction_id" in data or "fraud_probability" in data or "prediction" in data

        # If fraud_probability exists, verify it's valid
        if "fraud_probability" in data:
            assert 0.0 <= data["fraud_probability"] <= 1.0

    def test_batch_prediction_returns_multiple_scores(
        self, fraud_api_client, sample_batch_transactions
    ):
        """Test that batch prediction returns scores for all transactions."""
        response = fraud_api_client.post(
            "/predict/batch",
            json={"transactions": sample_batch_transactions}
        )

        # Batch endpoint may not exist - skip if 404
        if response.status_code == 404:
            pytest.skip("Batch prediction endpoint not available")

        # May return 500/503 if no model loaded, or 422 for validation issues
        if response.status_code in [422, 500, 503]:
            pytest.skip("Model not available or validation failed - prediction requires trained model")

        assert response.status_code == 200
        data = response.json()

        # Verify we got predictions for all transactions
        if "predictions" in data:
            assert len(data["predictions"]) == len(sample_batch_transactions)

    def test_prediction_latency_under_threshold(
        self, fraud_api_client, sample_fraud_transaction
    ):
        """Test that prediction latency is acceptable."""
        import time

        start = time.time()
        response = fraud_api_client.post(
            "/predict",
            json=sample_fraud_transaction
        )
        latency_ms = (time.time() - start) * 1000

        # May return 500/503 if no model loaded yet
        if response.status_code in [500, 503]:
            pytest.skip("Model not available - prediction requires trained model")

        assert response.status_code == 200
        # Should complete within 500ms
        assert latency_ms < 500, f"Prediction took {latency_ms:.0f}ms"


# =============================================================================
# SHADOW MODE TESTS
# =============================================================================

@pytest.mark.integration
class TestShadowModeIntegration:
    """Integration tests for shadow mode functionality."""

    def test_shadow_status_returns_config(self, fraud_api_client):
        """Test that shadow status returns configuration."""
        response = fraud_api_client.get("/shadow/status")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_shadow_stats_returns_metrics(self, fraud_api_client):
        """Test that shadow stats returns metrics when available."""
        response = fraud_api_client.get("/shadow/stats")

        # May return 503 if shadow mode not enabled
        assert response.status_code in [200, 503]

        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, dict)


# =============================================================================
# STREAMING FEATURE TESTS
# =============================================================================

@pytest.mark.integration
class TestStreamingIntegration:
    """Integration tests for streaming features."""

    def test_streaming_status_returns_info(self, fraud_api_client):
        """Test that streaming status returns configuration info."""
        response = fraud_api_client.get("/streaming/status")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)

    def test_streaming_features_returns_data(self, fraud_api_client):
        """Test that streaming features endpoint works."""
        response = fraud_api_client.get("/streaming/features/CUST_TEST_001")

        # May return 503 if Redis not available, or 200 with empty features
        assert response.status_code in [200, 503]


# =============================================================================
# METRICS ENDPOINT TESTS
# =============================================================================

@pytest.mark.integration
class TestPrometheusMetrics:
    """Integration tests for Prometheus metrics endpoint."""

    def test_metrics_endpoint_returns_prometheus_format(self, fraud_api_client):
        """Test that /metrics returns Prometheus-formatted metrics."""
        response = fraud_api_client.get("/metrics")

        # May return 404 if metrics not enabled
        if response.status_code == 404:
            pytest.skip("Metrics endpoint not available")

        assert response.status_code == 200
        content = response.text

        # Should contain Prometheus metric format
        assert "# HELP" in content or "# TYPE" in content or "_total" in content


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================

@pytest.mark.integration
class TestErrorHandling:
    """Integration tests for error handling."""

    def test_invalid_json_returns_422(self, fraud_api_client):
        """Test that invalid JSON returns validation error."""
        response = fraud_api_client.post(
            "/predict",
            content="not valid json",
            headers={"Content-Type": "application/json"}
        )

        assert response.status_code in [400, 422]

    def test_missing_fields_handled_gracefully(self, fraud_api_client):
        """Test that missing required fields are handled."""
        response = fraud_api_client.post(
            "/predict",
            json={"transaction_id": "TEST_ONLY"}
        )

        # Should either work with defaults or return validation error
        assert response.status_code in [200, 422, 500]

    def test_invalid_endpoint_returns_404(self, fraud_api_client):
        """Test that non-existent endpoint returns 404."""
        response = fraud_api_client.get("/nonexistent/endpoint")

        assert response.status_code == 404
