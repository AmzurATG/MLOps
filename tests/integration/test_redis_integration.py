"""
Integration tests for Redis (Online Feature Store).

These tests verify Redis connectivity and feature store operations.
Run with: pytest tests/integration/test_redis_integration.py -v -m integration
"""

import pytest
from tests.integration.conftest import requires_redis


# =============================================================================
# CONNECTIVITY TESTS
# =============================================================================

@pytest.mark.integration
@requires_redis
class TestRedisConnectivity:
    """Integration tests for Redis connectivity."""

    def test_redis_ping_succeeds(self, redis_client):
        """Test that Redis responds to PING."""
        result = redis_client.ping()
        assert result is True

    def test_redis_info_returns_data(self, redis_client):
        """Test that Redis INFO command returns server information."""
        info = redis_client.info()

        assert "redis_version" in info
        assert "connected_clients" in info

    def test_redis_dbsize_returns_count(self, redis_client):
        """Test that Redis DBSIZE returns key count."""
        size = redis_client.dbsize()
        assert isinstance(size, int)
        assert size >= 0


# =============================================================================
# KEY-VALUE OPERATIONS
# =============================================================================

@pytest.mark.integration
@requires_redis
class TestRedisOperations:
    """Integration tests for Redis key-value operations."""

    def test_set_and_get_string(self, redis_client, redis_test_prefix):
        """Test basic SET and GET operations."""
        key = f"{redis_test_prefix}:test_string"
        value = "test_value"

        redis_client.set(key, value)
        result = redis_client.get(key)

        assert result == value

    def test_set_with_expiry(self, redis_client, redis_test_prefix):
        """Test SET with TTL expiration."""
        import time

        key = f"{redis_test_prefix}:expiring_key"
        redis_client.setex(key, 2, "expiring_value")

        # Should exist immediately
        assert redis_client.exists(key)

        # Wait for expiry
        time.sleep(3)
        assert not redis_client.exists(key)

    def test_hash_operations(self, redis_client, redis_test_prefix):
        """Test HASH operations for feature storage."""
        key = f"{redis_test_prefix}:customer:CUST_001"
        features = {
            "avg_transaction_amount": "150.50",
            "transaction_count_7d": "25",
            "account_age_days": "365",
        }

        # Store features as hash
        redis_client.hset(key, mapping=features)

        # Retrieve all features
        result = redis_client.hgetall(key)
        assert result == features

        # Retrieve single feature
        single = redis_client.hget(key, "avg_transaction_amount")
        assert single == "150.50"

    def test_list_operations(self, redis_client, redis_test_prefix):
        """Test LIST operations for recent transactions."""
        key = f"{redis_test_prefix}:recent_txns"
        transactions = ["TXN_001", "TXN_002", "TXN_003"]

        # Push transactions
        for txn in transactions:
            redis_client.rpush(key, txn)

        # Get all
        result = redis_client.lrange(key, 0, -1)
        assert result == transactions

        # Get length
        length = redis_client.llen(key)
        assert length == 3


# =============================================================================
# FEATURE STORE PATTERN TESTS
# =============================================================================

@pytest.mark.integration
@requires_redis
class TestFeatureStorePatterns:
    """Integration tests for feature store patterns."""

    def test_feast_online_store_key_format(self, redis_client, redis_test_prefix):
        """Test Feast-compatible key format storage."""
        # Feast uses format: feast_feature_view:entity_key
        key = f"{redis_test_prefix}:fraud_features:customer_123"
        features = {
            "amount_mean_7d": "250.00",
            "amount_std_7d": "50.00",
            "transaction_count_1h": "3",
            "is_high_risk": "0",
        }

        redis_client.hset(key, mapping=features)
        result = redis_client.hgetall(key)

        assert len(result) == 4
        assert result["amount_mean_7d"] == "250.00"

    def test_feature_retrieval_latency(self, redis_client, redis_test_prefix):
        """Test that feature retrieval is fast enough for real-time serving."""
        import time

        # Setup test data
        key = f"{redis_test_prefix}:perf_test"
        features = {f"feature_{i}": str(i * 1.5) for i in range(50)}
        redis_client.hset(key, mapping=features)

        # Measure retrieval time
        start = time.time()
        for _ in range(100):
            redis_client.hgetall(key)
        elapsed_ms = (time.time() - start) * 1000

        # 100 retrievals should complete in < 100ms (1ms each)
        assert elapsed_ms < 100, f"Feature retrieval too slow: {elapsed_ms:.1f}ms"

    def test_pipeline_operations(self, redis_client, redis_test_prefix):
        """Test pipelined operations for batch feature retrieval."""
        # Setup multiple customer features
        customers = ["CUST_001", "CUST_002", "CUST_003"]
        for cust in customers:
            key = f"{redis_test_prefix}:features:{cust}"
            redis_client.hset(key, mapping={
                "risk_score": "0.5",
                "transaction_count": "10",
            })

        # Batch retrieval with pipeline
        pipe = redis_client.pipeline()
        for cust in customers:
            key = f"{redis_test_prefix}:features:{cust}"
            pipe.hgetall(key)

        results = pipe.execute()

        assert len(results) == 3
        for result in results:
            assert "risk_score" in result


# =============================================================================
# MEMORY AND PERSISTENCE TESTS
# =============================================================================

@pytest.mark.integration
@requires_redis
class TestRedisMemory:
    """Integration tests for Redis memory management."""

    def test_memory_usage_tracking(self, redis_client, redis_test_prefix):
        """Test that memory usage can be tracked."""
        key = f"{redis_test_prefix}:memory_test"
        redis_client.set(key, "x" * 1000)  # 1KB value

        memory = redis_client.memory_usage(key)
        assert memory is not None
        assert memory > 1000  # At least 1KB

    def test_key_expiry_is_set(self, redis_client, redis_test_prefix):
        """Test that TTL can be set and retrieved."""
        key = f"{redis_test_prefix}:ttl_test"
        redis_client.setex(key, 3600, "value")

        ttl = redis_client.ttl(key)
        assert 3500 < ttl <= 3600
