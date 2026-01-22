"""
Unified Feature Service
=======================

Provides a single interface for retrieving features from multiple sources:
- Feast online store (batch features)
- Redis (streaming features)
- Additional custom sources (via FeatureSource protocol)

SOLID Principles:
- Open/Closed: Add new sources without modifying this class
- Dependency Inversion: Depends on FeatureSource abstraction
- Single Responsibility: Orchestrates feature retrieval only

Handles merging, fallbacks, and latency tracking.
"""

import time
import logging
from typing import Dict, Any, Optional, List, Tuple, Sequence
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.api.serving.features.protocols import FeatureSource, FeatureResult
from src.api.serving.features.feast_client import FeastClient, FeastConfig
from src.api.serving.features.redis_client import StreamingRedisClient, StreamingRedisConfig

logger = logging.getLogger(__name__)


@dataclass
class FeatureVector:
    """
    Container for features from all sources.

    Provides unified access to batch and streaming features
    with metadata about latency and availability.
    """
    batch_features: Dict[str, Any] = field(default_factory=dict)
    streaming_features: Dict[str, Any] = field(default_factory=dict)

    batch_latency_ms: float = 0.0
    streaming_latency_ms: float = 0.0

    batch_available: bool = False
    streaming_available: bool = False

    warnings: List[str] = field(default_factory=list)

    @property
    def total_latency_ms(self) -> float:
        """Total feature fetch latency (parallel: max of batch and streaming)."""
        return max(self.batch_latency_ms, self.streaming_latency_ms)

    @property
    def merged(self) -> Dict[str, Any]:
        """Merge all features (streaming takes precedence for overlapping keys)."""
        merged = {}
        merged.update(self.batch_features)
        merged.update(self.streaming_features)
        return merged

    @property
    def source(self) -> str:
        """Describe feature sources."""
        sources = []
        if self.batch_available:
            sources.append("feast")
        if self.streaming_available:
            sources.append("streaming")
        return "+".join(sources) if sources else "none"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "batch_features": self.batch_features,
            "streaming_features": self.streaming_features,
            "batch_latency_ms": self.batch_latency_ms,
            "streaming_latency_ms": self.streaming_latency_ms,
            "batch_available": self.batch_available,
            "streaming_available": self.streaming_available,
            "total_latency_ms": self.total_latency_ms,
            "source": self.source,
            "warnings": self.warnings,
        }


class FeatureService:
    """
    Unified feature retrieval service.

    Combines batch features from Feast and streaming features from Redis
    into a single FeatureVector for model inference.

    SOLID Compliance:
    - Open/Closed: Add custom sources via additional_sources parameter
    - Dependency Inversion: Accepts FeatureSource implementations
    - Single Responsibility: Only orchestrates feature retrieval

    Usage:
        # Default (backward compatible)
        service = FeatureService()

        # With dependency injection (for testing)
        service = FeatureService(
            feast_client=mock_feast,
            redis_client=mock_redis,
        )

        # With additional sources (extensible)
        service = FeatureService(
            additional_sources=[postgres_source, dynamo_source],
        )
    """

    def __init__(
        self,
        feast_config: Optional[FeastConfig] = None,
        redis_config: Optional[StreamingRedisConfig] = None,
        feast_client: Optional[FeastClient] = None,
        redis_client: Optional[StreamingRedisClient] = None,
        additional_sources: Optional[Sequence[FeatureSource]] = None,
    ):
        """
        Initialize feature service with configurable sources.

        Args:
            feast_config: Configuration for Feast client (if not injecting client)
            redis_config: Configuration for Redis client (if not injecting client)
            feast_client: Pre-configured Feast client (dependency injection)
            redis_client: Pre-configured Redis client (dependency injection)
            additional_sources: Additional feature sources implementing FeatureSource
        """
        # Support dependency injection or create defaults
        self._feast_client = feast_client or FeastClient(feast_config)
        self._redis_client = redis_client or StreamingRedisClient(redis_config)
        self._additional_sources: List[FeatureSource] = list(additional_sources or [])

    @property
    def feast_client(self) -> FeastClient:
        """Feast client instance."""
        return self._feast_client

    @property
    def redis_client(self) -> StreamingRedisClient:
        """Streaming Redis client instance."""
        return self._redis_client

    @property
    def additional_sources(self) -> List[FeatureSource]:
        """Additional feature sources."""
        return self._additional_sources

    def add_source(self, source: FeatureSource) -> None:
        """
        Add a feature source at runtime (Open/Closed Principle).

        Args:
            source: Feature source implementing FeatureSource protocol
        """
        self._additional_sources.append(source)

    def get_features(
        self,
        customer_id: str,
        transaction_id: Optional[str] = None,
        include_streaming: bool = True,
    ) -> FeatureVector:
        """
        Retrieve all features for a customer/transaction in parallel.

        Args:
            customer_id: Customer identifier
            transaction_id: Optional transaction identifier
            include_streaming: Whether to fetch streaming features from Redis

        Returns:
            FeatureVector containing all features and metadata
        """
        feature_vector = FeatureVector()

        def fetch_batch() -> Tuple[Dict[str, Any], float, Optional[str]]:
            """Fetch batch features from Feast."""
            try:
                features, latency = self._feast_client.get_all_features(
                    customer_id=customer_id,
                    transaction_id=transaction_id,
                )
                warning = None if features else "No batch features found in Feast"
                return features, latency, warning
            except Exception as e:
                logger.warning(f"Feast feature fetch failed: {e}")
                return {}, 0.0, f"Feast error: {str(e)[:50]}"

        def fetch_streaming() -> Tuple[Dict[str, Any], float, bool, Optional[str]]:
            """Fetch streaming features from Redis."""
            try:
                features, latency, success = self._redis_client.get_streaming_features(
                    customer_id=customer_id,
                )
                warning = None
                if success and not features:
                    warning = "No streaming features found for customer"
                elif not success:
                    warning = "Streaming features unavailable"
                return features, latency, success, warning
            except Exception as e:
                logger.warning(f"Streaming feature fetch failed: {e}")
                return {}, 0.0, False, f"Streaming error: {str(e)[:50]}"

        # Fetch features in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {"batch": executor.submit(fetch_batch)}
            if include_streaming:
                futures["streaming"] = executor.submit(fetch_streaming)

            # Process batch result
            batch_result = futures["batch"].result()
            feature_vector.batch_features = batch_result[0]
            feature_vector.batch_latency_ms = batch_result[1]
            feature_vector.batch_available = bool(batch_result[0])
            if batch_result[2]:
                feature_vector.warnings.append(batch_result[2])

            # Process streaming result
            if include_streaming and "streaming" in futures:
                streaming_result = futures["streaming"].result()
                feature_vector.streaming_features = streaming_result[0]
                feature_vector.streaming_latency_ms = streaming_result[1]
                feature_vector.streaming_available = streaming_result[2] and bool(streaming_result[0])
                if streaming_result[3]:
                    feature_vector.warnings.append(streaming_result[3])

        return feature_vector

    def health_check(self) -> Dict[str, Any]:
        """Check health of all feature sources."""
        health = {
            "feast": self._feast_client.health_check(),
            "redis": self._redis_client.health_check(),
        }

        # Include additional sources
        for source in self._additional_sources:
            source_health = source.health_check()
            source_name = source_health.get("source", f"source_{len(health)}")
            health[source_name] = source_health

        return health
