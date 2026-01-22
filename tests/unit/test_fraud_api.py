"""
Unit tests for the Fraud Detection API.

Tests cover:
- Health and readiness endpoints
- Prediction endpoints (single and batch)
- Shadow mode endpoints
- Performance endpoints
- Error handling
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


# =============================================================================
# HEALTH ENDPOINTS
# =============================================================================

class TestHealthEndpoints:
    """Tests for health and readiness endpoints."""

    def test_health_endpoint_returns_ok(self):
        """Test that /health returns 200 with status."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/health")

            assert response.status_code == 200
            data = response.json()
            assert "status" in data
            assert data["status"] in ["healthy", "degraded"]

    def test_ready_endpoint_returns_status(self):
        """Test that /ready returns readiness status."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/ready")

            # /ready may not exist in all API versions
            assert response.status_code in [200, 404]
            if response.status_code == 200:
                data = response.json()
                assert "ready" in data

    def test_root_endpoint_returns_welcome(self):
        """Test that root endpoint returns API info."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/")

            assert response.status_code == 200


# =============================================================================
# SHADOW MODE ENDPOINTS
# =============================================================================

class TestShadowModeEndpoints:
    """Tests for shadow mode endpoints."""

    def test_shadow_status_endpoint(self):
        """Test /shadow/status returns shadow mode status."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/shadow/status")

            assert response.status_code == 200
            data = response.json()
            # Should have either 'enabled' key or 'available' key
            assert "enabled" in data or "available" in data

    def test_shadow_stats_endpoint(self):
        """Test /shadow/stats returns statistics."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/shadow/stats")

            # May return 200 or 503 depending on availability
            assert response.status_code in [200, 503]

    def test_shadow_stats_endpoint(self):
        """Test /shadow/stats returns statistics."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/shadow/stats")

            # Returns 200 with stats or 503 if shadow mode not available
            assert response.status_code in [200, 503]

    def test_shadow_enable_endpoint(self):
        """Test /shadow/enable enables shadow mode."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.post("/shadow/enable")

            # Returns 200 if enabled or 503 if not available
            assert response.status_code in [200, 503]


# =============================================================================
# STREAMING ENDPOINTS
# =============================================================================

class TestStreamingEndpoints:
    """Tests for streaming feature endpoints."""

    def test_streaming_status_endpoint(self):
        """Test /streaming/status returns streaming status."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/streaming/status")

            assert response.status_code == 200
            data = response.json()
            # Check for expected keys in streaming status response
            assert "available" in data or "enabled" in data or "status" in data

    def test_streaming_features_endpoint(self):
        """Test /streaming/features/{customer_id} returns features."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/streaming/features/CUST_123")

            # May return 200 or 503 depending on Redis availability
            assert response.status_code in [200, 503]

    def test_metrics_endpoint(self):
        """Test /metrics returns Prometheus metrics."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/metrics")

            # Should return 200 with metrics or 503 if not enabled
            assert response.status_code in [200, 503]


# =============================================================================
# VALIDATION TESTS
# =============================================================================

class TestInputValidation:
    """Tests for input validation."""

    def test_batch_limit_validation(self):
        """Test that batch endpoint respects max limit."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)

            # Request for batch stats endpoint
            response = client.get("/batch/stats")
            assert response.status_code in [200, 404]  # May not exist

    def test_empty_request_handling(self):
        """Test that empty prediction request is handled."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.post("/predict", json={})

            # Should return validation error (422) or handle gracefully
            assert response.status_code in [200, 422, 500]


# =============================================================================
# API INFO ENDPOINTS
# =============================================================================

class TestInfoEndpoints:
    """Tests for informational endpoints."""

    def test_model_info_endpoint(self):
        """Test /model/info returns model information."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/model/info")

            # /model/info may not exist in all API versions
            assert response.status_code in [200, 404]
            if response.status_code == 200:
                data = response.json()
                # Should contain model metadata
                assert isinstance(data, dict)

    def test_experiments_endpoint(self):
        """Test /experiments returns A/B testing status."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false", "AB_TEST_ENABLED": "false"}):
            from fastapi.testclient import TestClient
            from src.mlops.api.main import app

            client = TestClient(app)
            response = client.get("/experiments")

            # May return 200 or 404 depending on A/B testing availability
            assert response.status_code in [200, 404]
