"""
Unit tests for api.serving.features.feature_service module.

Tests the FeatureService class which unifies batch (Feast)
and streaming (Redis) feature retrieval.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from src.api.serving.features.feature_service import FeatureService, FeatureVector


class TestFeatureVector:
    """Tests for FeatureVector dataclass."""

    def test_default_values(self):
        """Test default values for FeatureVector."""
        fv = FeatureVector()

        assert fv.batch_features == {}
        assert fv.streaming_features == {}
        assert fv.batch_latency_ms == 0.0
        assert fv.streaming_latency_ms == 0.0
        assert fv.batch_available is False
        assert fv.streaming_available is False
        assert fv.warnings == []

    def test_total_latency_ms(self):
        """Test total_latency_ms property (max of parallel fetches)."""
        fv = FeatureVector(
            batch_latency_ms=10.5,
            streaming_latency_ms=5.2,
        )

        # Features are fetched in parallel, so total is max not sum
        assert fv.total_latency_ms == 10.5

    def test_merged_features(self):
        """Test merged property combines features correctly."""
        fv = FeatureVector(
            batch_features={"a": 1, "b": 2, "c": 3},
            streaming_features={"b": 20, "d": 4},  # 'b' overlaps
        )

        merged = fv.merged

        assert merged["a"] == 1
        assert merged["b"] == 20  # Streaming takes precedence
        assert merged["c"] == 3
        assert merged["d"] == 4

    def test_source_both_available(self):
        """Test source property when both sources available."""
        fv = FeatureVector(
            batch_available=True,
            streaming_available=True,
        )

        assert fv.source == "feast+streaming"

    def test_source_only_batch(self):
        """Test source property when only batch available."""
        fv = FeatureVector(
            batch_available=True,
            streaming_available=False,
        )

        assert fv.source == "feast"

    def test_source_only_streaming(self):
        """Test source property when only streaming available."""
        fv = FeatureVector(
            batch_available=False,
            streaming_available=True,
        )

        assert fv.source == "streaming"

    def test_source_none_available(self):
        """Test source property when no sources available."""
        fv = FeatureVector(
            batch_available=False,
            streaming_available=False,
        )

        assert fv.source == "none"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        fv = FeatureVector(
            batch_features={"a": 1},
            streaming_features={"b": 2},
            batch_latency_ms=10.0,
            streaming_latency_ms=5.0,
            batch_available=True,
            streaming_available=True,
            warnings=["warning1"],
        )

        d = fv.to_dict()

        assert d["batch_features"] == {"a": 1}
        assert d["streaming_features"] == {"b": 2}
        assert d["batch_latency_ms"] == 10.0
        assert d["streaming_latency_ms"] == 5.0
        assert d["batch_available"] is True
        assert d["streaming_available"] is True
        # Features are fetched in parallel, so total is max not sum
        assert d["total_latency_ms"] == 10.0
        assert d["source"] == "feast+streaming"
        assert d["warnings"] == ["warning1"]


class TestFeatureService:
    """Tests for FeatureService class."""

    @pytest.fixture
    def mock_feast_client(self):
        """Create a mock FeastClient."""
        mock = Mock()
        mock.get_all_features.return_value = (
            {"feature1": 1.0, "feature2": 2.0},
            5.0,  # latency
        )
        mock.is_available.return_value = True
        mock._init_error = None
        # Mock health_check method for new SOLID-compliant interface
        mock.health_check.return_value = {
            "source": "feast",
            "available": True,
            "error": None,
        }
        return mock

    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock StreamingRedisClient."""
        mock = Mock()
        mock.get_streaming_features.return_value = (
            {"stream_feature1": 10, "stream_feature2": 20},
            3.0,  # latency
            True,  # success
        )
        mock.is_available.return_value = True
        mock.get_stats.return_value = {"keys": 100}
        # Mock health_check method for new SOLID-compliant interface
        mock.health_check.return_value = {
            "source": "streaming_redis",
            "available": True,
            "stats": {"keys": 100},
        }
        return mock

    @pytest.fixture
    def service_with_mocks(self, mock_feast_client, mock_redis_client):
        """Create FeatureService with mocked clients."""
        with patch('src.api.serving.features.feature_service.FeastClient') as MockFeast, \
             patch('src.api.serving.features.feature_service.StreamingRedisClient') as MockRedis:
            MockFeast.return_value = mock_feast_client
            MockRedis.return_value = mock_redis_client
            service = FeatureService()
            service._feast_client = mock_feast_client
            service._redis_client = mock_redis_client
            return service

    def test_get_features_both_sources(self, service_with_mocks, mock_feast_client, mock_redis_client):
        """Test get_features retrieves from both sources."""
        result = service_with_mocks.get_features(
            customer_id="C123",
            transaction_id="T456",
            include_streaming=True,
        )

        assert result.batch_available is True
        assert result.streaming_available is True
        assert result.batch_features == {"feature1": 1.0, "feature2": 2.0}
        assert result.streaming_features == {"stream_feature1": 10, "stream_feature2": 20}
        assert result.batch_latency_ms == 5.0
        assert result.streaming_latency_ms == 3.0

        mock_feast_client.get_all_features.assert_called_once_with(
            customer_id="C123",
            transaction_id="T456",
        )
        mock_redis_client.get_streaming_features.assert_called_once_with(
            customer_id="C123",
        )

    def test_get_features_without_streaming(self, service_with_mocks, mock_feast_client, mock_redis_client):
        """Test get_features with streaming disabled."""
        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=False,
        )

        assert result.batch_available is True
        assert result.streaming_available is False
        assert result.streaming_features == {}

        mock_feast_client.get_all_features.assert_called_once()
        mock_redis_client.get_streaming_features.assert_not_called()

    def test_get_features_feast_error(self, service_with_mocks, mock_feast_client, mock_redis_client):
        """Test get_features handles Feast errors gracefully."""
        mock_feast_client.get_all_features.side_effect = Exception("Feast connection error")

        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=True,
        )

        assert result.batch_available is False
        assert result.batch_features == {}
        assert any("Feast error" in w for w in result.warnings)
        # Streaming should still work
        assert result.streaming_available is True

    def test_get_features_redis_error(self, service_with_mocks, mock_feast_client, mock_redis_client):
        """Test get_features handles Redis errors gracefully."""
        mock_redis_client.get_streaming_features.side_effect = Exception("Redis connection error")

        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=True,
        )

        # Batch should still work
        assert result.batch_available is True
        assert result.streaming_available is False
        assert any("Streaming error" in w for w in result.warnings)

    def test_get_features_no_batch_features(self, service_with_mocks, mock_feast_client):
        """Test warning when no batch features found."""
        mock_feast_client.get_all_features.return_value = ({}, 5.0)

        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=False,
        )

        assert result.batch_available is False
        assert any("No batch features found" in w for w in result.warnings)

    def test_get_features_no_streaming_features(self, service_with_mocks, mock_redis_client):
        """Test warning when no streaming features found."""
        mock_redis_client.get_streaming_features.return_value = ({}, 3.0, True)

        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=True,
        )

        assert result.streaming_available is False
        assert any("No streaming features found" in w for w in result.warnings)

    def test_get_features_streaming_unavailable(self, service_with_mocks, mock_redis_client):
        """Test when streaming service returns unavailable."""
        mock_redis_client.get_streaming_features.return_value = ({}, 0.0, False)

        result = service_with_mocks.get_features(
            customer_id="C123",
            include_streaming=True,
        )

        assert result.streaming_available is False
        assert any("unavailable" in w for w in result.warnings)

    def test_health_check(self, service_with_mocks, mock_feast_client, mock_redis_client):
        """Test health_check method."""
        health = service_with_mocks.health_check()

        assert "feast" in health
        assert "redis" in health
        # New SOLID-compliant interface - clients return full health dict
        assert health["feast"]["available"] is True
        assert health["feast"]["error"] is None
        assert health["redis"]["available"] is True
        assert health["redis"]["stats"] == {"keys": 100}

    def test_health_check_redis_unavailable(self, service_with_mocks, mock_redis_client):
        """Test health_check when Redis is unavailable."""
        # Update mock to return unavailable status
        mock_redis_client.health_check.return_value = {
            "source": "streaming_redis",
            "available": False,
        }

        health = service_with_mocks.health_check()

        assert health["redis"]["available"] is False

    def test_feast_client_property(self, service_with_mocks, mock_feast_client):
        """Test feast_client property accessor."""
        assert service_with_mocks.feast_client == mock_feast_client

    def test_redis_client_property(self, service_with_mocks, mock_redis_client):
        """Test redis_client property accessor."""
        assert service_with_mocks.redis_client == mock_redis_client


class TestFeatureServiceIntegration:
    """Integration-style tests for FeatureService."""

    def test_merged_features_streaming_precedence(self):
        """Test that streaming features take precedence in merge."""
        fv = FeatureVector(
            batch_features={
                "tx_count_30d": 10,
                "avg_amount_30d": 100.0,
                "velocity_score": 0.3,  # Old batch value
            },
            streaming_features={
                "velocity_score": 0.8,  # Fresh streaming value
                "tx_count_5min": 5,
            },
            batch_available=True,
            streaming_available=True,
        )

        merged = fv.merged

        # Streaming value should override batch
        assert merged["velocity_score"] == 0.8
        # Batch-only features preserved
        assert merged["tx_count_30d"] == 10
        assert merged["avg_amount_30d"] == 100.0
        # Streaming-only features included
        assert merged["tx_count_5min"] == 5

    def test_empty_merged_features(self):
        """Test merged with empty feature dicts."""
        fv = FeatureVector()

        assert fv.merged == {}
        assert fv.source == "none"
