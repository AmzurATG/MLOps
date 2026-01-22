"""
Feature Source Protocols
========================

Abstract interfaces for feature sources following the Dependency Inversion Principle.
Allows adding new feature sources without modifying FeatureService.

Usage:
    class MyCustomClient(FeatureSource):
        def get_features(self, entity_id: str, **kwargs) -> FeatureResult:
            ...

        def is_available(self) -> bool:
            ...

        def health_check(self) -> Dict[str, Any]:
            ...
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple, Protocol, runtime_checkable
from dataclasses import dataclass, field


@dataclass
class FeatureResult:
    """
    Standardized result from a feature source.

    All feature sources return this to enable uniform handling.
    """
    features: Dict[str, Any] = field(default_factory=dict)
    latency_ms: float = 0.0
    success: bool = True
    error: Optional[str] = None
    source_name: str = "unknown"

    @property
    def available(self) -> bool:
        """Whether features were successfully retrieved."""
        return self.success and bool(self.features)


@runtime_checkable
class FeatureSource(Protocol):
    """
    Protocol defining the interface for all feature sources.

    Implementations:
        - FeastClient: Batch features from Feast online store
        - StreamingRedisClient: Real-time streaming features from Redis
        - Future: PostgreSQL, DynamoDB, custom sources

    Following Interface Segregation Principle - minimal interface.
    """

    def get_features(
        self,
        entity_id: str,
        **kwargs: Any,
    ) -> FeatureResult:
        """
        Retrieve features for an entity.

        Args:
            entity_id: Primary entity identifier (e.g., customer_id)
            **kwargs: Source-specific parameters

        Returns:
            FeatureResult with features and metadata
        """
        ...

    def is_available(self) -> bool:
        """Check if the feature source is available and healthy."""
        ...

    def health_check(self) -> Dict[str, Any]:
        """
        Return detailed health status.

        Returns:
            Dict with 'available' bool and source-specific details
        """
        ...


class BaseFeatureSource(ABC):
    """
    Abstract base class for feature sources.

    Provides common functionality and enforces interface implementation.
    Use this when you need shared behavior across sources.
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique name identifying this source type."""
        pass

    @abstractmethod
    def get_features(
        self,
        entity_id: str,
        **kwargs: Any,
    ) -> FeatureResult:
        """Retrieve features for an entity."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the source is available."""
        pass

    def health_check(self) -> Dict[str, Any]:
        """Default health check implementation."""
        return {
            "source": self.source_name,
            "available": self.is_available(),
        }

    def _create_result(
        self,
        features: Dict[str, Any],
        latency_ms: float,
        success: bool = True,
        error: Optional[str] = None,
    ) -> FeatureResult:
        """Helper to create standardized FeatureResult."""
        return FeatureResult(
            features=features,
            latency_ms=latency_ms,
            success=success,
            error=error,
            source_name=self.source_name,
        )


class CustomerFeatureSource(Protocol):
    """
    Specialized protocol for customer-level features.

    Extends FeatureSource with customer-specific methods.
    """

    def get_customer_features(
        self,
        customer_id: str,
    ) -> FeatureResult:
        """Get features for a customer."""
        ...


class TransactionFeatureSource(Protocol):
    """
    Specialized protocol for transaction-level features.

    Extends FeatureSource with transaction-specific methods.
    """

    def get_transaction_features(
        self,
        transaction_id: str,
    ) -> FeatureResult:
        """Get features for a transaction."""
        ...
