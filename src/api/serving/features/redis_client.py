"""
Streaming Redis Client
======================

Retrieves real-time streaming features from Redis.

Features are computed by ksqlDB from Kafka streams and stored in Redis
with a configurable TTL for sub-millisecond latency lookups.

Key Pattern: {prefix}{customer_id}
Default: feast:streaming:{customer_id}

Implements FeatureSource protocol for SOLID compliance.
"""

import os
import time
import json
import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from src.api.serving.features.protocols import FeatureResult, BaseFeatureSource

logger = logging.getLogger(__name__)


@dataclass
class StreamingRedisConfig:
    """Redis streaming client configuration."""
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "exp-redis"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    prefix: str = field(default_factory=lambda: os.getenv("REDIS_STREAMING_PREFIX", "feast:streaming:"))
    default_ttl: int = 3600  # 1 hour
    # Connection pool settings for concurrent access
    pool_max_connections: int = field(default_factory=lambda: int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "20")))
    pool_timeout: int = field(default_factory=lambda: int(os.getenv("REDIS_POOL_TIMEOUT", "5")))


class StreamingRedisClient(BaseFeatureSource):
    """
    Client for retrieving real-time streaming features from Redis.

    Streaming features are computed by ksqlDB and written to Redis
    by the streaming consumer for sub-millisecond lookups.

    Implements FeatureSource protocol for SOLID compliance.
    """

    def __init__(self, config: Optional[StreamingRedisConfig] = None):
        self.config = config or StreamingRedisConfig()
        self._client = None
        self._pool = None
        self._available = None

    @property
    def source_name(self) -> str:
        """Unique name identifying this source type."""
        return "streaming_redis"

    @property
    def client(self):
        """Lazy initialize Redis client with connection pooling."""
        if self._client is not None:
            return self._client

        try:
            import redis

            # Create connection pool for concurrent access
            self._pool = redis.ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                max_connections=self.config.pool_max_connections,
                decode_responses=True,
                socket_timeout=self.config.pool_timeout,
                socket_connect_timeout=self.config.pool_timeout,
            )

            # Create client using the pool
            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            self._client.ping()
            self._available = True
            logger.info(
                f"Redis connected with pool: {self.config.host}:{self.config.port} "
                f"(max_connections={self.config.pool_max_connections})"
            )
            return self._client

        except Exception as e:
            self._available = False
            logger.warning(f"Redis connection failed: {e}")
            return None

    def is_available(self) -> bool:
        """Check if Redis is available."""
        if self._available is not None:
            return self._available
        try:
            _ = self.client
            return self._available or False
        except Exception:
            return False

    def _get_key(self, customer_id: str) -> str:
        """Build Redis key for customer."""
        return f"{self.config.prefix}{customer_id}"

    def get_streaming_features(
        self,
        customer_id: str,
    ) -> Tuple[Dict[str, Any], float, bool]:
        """
        Fetch streaming features from Redis.

        The streaming consumer writes features as a Redis HASH using HSET,
        so we use HGETALL to retrieve all fields.

        Args:
            customer_id: Customer identifier

        Returns:
            Tuple of (features_dict, latency_ms, success_bool)
        """
        start = time.time()

        if not self.is_available():
            latency_ms = (time.time() - start) * 1000
            return {}, latency_ms, False

        try:
            key = self._get_key(customer_id)
            # Use HGETALL since consumer writes as HASH with HSET
            features = self.client.hgetall(key)

            latency_ms = (time.time() - start) * 1000

            if not features:
                logger.debug(f"No streaming features for customer: {customer_id}")
                return {}, latency_ms, True  # Success but no data

            # Decode bytes if needed (depends on decode_responses setting)
            decoded_features = {}
            for k, v in features.items():
                key_str = k.decode('utf-8') if isinstance(k, bytes) else k
                val_str = v.decode('utf-8') if isinstance(v, bytes) else v
                decoded_features[key_str] = val_str

            logger.debug(f"Retrieved {len(decoded_features)} streaming features for {customer_id}")

            return decoded_features, latency_ms, True

        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            logger.warning(f"Streaming features fetch failed: {e}")
            return {}, latency_ms, False

    def get_ttl(self, customer_id: str) -> int:
        """Get TTL for customer's streaming features key."""
        if not self.is_available():
            return -1

        try:
            key = self._get_key(customer_id)
            return self.client.ttl(key)
        except Exception:
            return -1

    def get_stats(self) -> Dict[str, Any]:
        """Get Redis connection and memory stats."""
        if not self.is_available():
            return {"status": "unavailable"}

        try:
            info = self.client.info("memory")
            keyspace = self.client.info("keyspace")

            # Count streaming keys
            streaming_keys = self.client.keys(f"{self.config.prefix}*")

            # Get pool stats if available
            pool_stats = {}
            if self._pool is not None:
                pool_stats = {
                    "pool_max_connections": self.config.pool_max_connections,
                    "pool_current_connections": len(self._pool._in_use_connections) if hasattr(self._pool, '_in_use_connections') else "unknown",
                }

            return {
                "status": "connected",
                "host": self.config.host,
                "port": self.config.port,
                "used_memory_human": info.get("used_memory_human", "unknown"),
                "streaming_keys_count": len(streaming_keys),
                "prefix": self.config.prefix,
                "keyspace": keyspace,
                **pool_stats,
            }

        except Exception as e:
            return {"status": "error", "error": str(e)}

    def close(self):
        """Close the connection pool."""
        if self._pool is not None:
            self._pool.disconnect()
            logger.info("Redis connection pool closed")

    def get_features(
        self,
        entity_id: str,
        **kwargs: Any,
    ) -> FeatureResult:
        """
        Retrieve features for an entity (FeatureSource protocol implementation).

        Args:
            entity_id: Customer ID
            **kwargs: Not used for streaming features

        Returns:
            FeatureResult with features and metadata
        """
        features, latency_ms, success = self.get_streaming_features(entity_id)
        return self._create_result(
            features=features,
            latency_ms=latency_ms,
            success=success,
            error=None if success else "Streaming features unavailable",
        )

    def health_check(self) -> Dict[str, Any]:
        """Return detailed health status."""
        base_health = {
            "source": self.source_name,
            "available": self.is_available(),
        }
        if self.is_available():
            base_health["stats"] = self.get_stats()
        return base_health


class AsyncStreamingRedisClient:
    """
    Async Redis client for streaming features.

    OPTIMIZED: Uses redis.asyncio (built into redis>=4.2) for non-blocking
    I/O operations, enabling high concurrency in FastAPI async endpoints.

    Usage:
        client = AsyncStreamingRedisClient()
        features, latency, success = await client.get_streaming_features("cust_123")
    """

    def __init__(self, config: Optional[StreamingRedisConfig] = None):
        self.config = config or StreamingRedisConfig()
        self._client = None
        self._pool = None
        self._available = None

    @property
    def source_name(self) -> str:
        return "async_streaming_redis"

    def _get_key(self, customer_id: str) -> str:
        """Build Redis key for customer."""
        return f"{self.config.prefix}{customer_id}"

    async def _get_client(self):
        """Lazy initialize async Redis client with connection pooling."""
        if self._client is not None:
            return self._client

        try:
            import redis.asyncio as redis_async

            # Create async connection pool
            self._pool = redis_async.ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                max_connections=self.config.pool_max_connections,
                decode_responses=True,
                socket_timeout=self.config.pool_timeout,
                socket_connect_timeout=self.config.pool_timeout,
            )

            self._client = redis_async.Redis(connection_pool=self._pool)

            # Test connection
            await self._client.ping()
            self._available = True
            logger.info(
                f"Async Redis connected: {self.config.host}:{self.config.port} "
                f"(max_connections={self.config.pool_max_connections})"
            )
            return self._client

        except Exception as e:
            self._available = False
            logger.warning(f"Async Redis connection failed: {e}")
            return None

    async def is_available(self) -> bool:
        """Check if Redis is available (async)."""
        if self._available is not None:
            return self._available
        try:
            client = await self._get_client()
            return client is not None
        except Exception:
            return False

    async def get_streaming_features(
        self,
        customer_id: str,
    ) -> Tuple[Dict[str, Any], float, bool]:
        """
        Fetch streaming features from Redis (async).

        The streaming consumer writes features as a Redis HASH using HSET,
        so we use HGETALL to retrieve all fields.

        Args:
            customer_id: Customer identifier

        Returns:
            Tuple of (features_dict, latency_ms, success_bool)
        """
        start = time.time()

        client = await self._get_client()
        if client is None:
            latency_ms = (time.time() - start) * 1000
            return {}, latency_ms, False

        try:
            key = self._get_key(customer_id)
            # Use HGETALL since consumer writes as HASH with HSET
            features = await client.hgetall(key)

            latency_ms = (time.time() - start) * 1000

            if not features:
                logger.debug(f"No streaming features for customer: {customer_id}")
                return {}, latency_ms, True  # Success but no data

            # Decode bytes if needed (depends on decode_responses setting)
            decoded_features = {}
            for k, v in features.items():
                key_str = k.decode('utf-8') if isinstance(k, bytes) else k
                val_str = v.decode('utf-8') if isinstance(v, bytes) else v
                decoded_features[key_str] = val_str

            logger.debug(f"Retrieved {len(decoded_features)} streaming features for {customer_id}")

            return decoded_features, latency_ms, True

        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            logger.warning(f"Async streaming features fetch failed: {e}")
            return {}, latency_ms, False

    async def get_features(
        self,
        entity_id: str,
        **kwargs: Any,
    ) -> FeatureResult:
        """
        Retrieve features for an entity (async FeatureSource protocol).

        Args:
            entity_id: Customer ID
            **kwargs: Not used for streaming features

        Returns:
            FeatureResult with features and metadata
        """
        features, latency_ms, success = await self.get_streaming_features(entity_id)
        return FeatureResult(
            features=features,
            latency_ms=latency_ms,
            success=success,
            source="async_streaming_redis",
            error=None if success else "Streaming features unavailable",
        )

    async def close(self):
        """Close the async connection pool."""
        if self._client is not None:
            await self._client.close()
        if self._pool is not None:
            await self._pool.disconnect()
        logger.info("Async Redis connection pool closed")
