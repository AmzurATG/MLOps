"""
Feast Feature Client
====================

Retrieves batch features from the Feast online store.

Features:
- Lazy initialization
- Configurable feature sets
- Error handling with fallbacks
- Implements FeatureSource protocol for SOLID compliance
"""

import os
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

from src.api.serving.features.protocols import FeatureResult, BaseFeatureSource

logger = logging.getLogger(__name__)


@dataclass
class FeastConfig:
    """Feast client configuration."""
    repo_path: str = field(default_factory=lambda: os.getenv("FEAST_REPO_PATH", "/app/feast_repo"))

    # Customer-based feature views
    customer_features: List[str] = field(default_factory=lambda: [
        # Behavioral features
        "customer_behavioral:transactions_before",
        "customer_behavioral:total_spend_before",
        "customer_behavioral:avg_amount_before",
        "customer_behavioral:customer_tenure_days",
        "customer_behavioral:days_since_last_tx",
        "customer_behavioral:is_new_customer",
        "customer_behavioral:is_very_new_account",
        # Spending features
        "customer_spending:total_spend_7d",
        "customer_spending:tx_count_7d",
        "customer_spending:total_spend_30d",
        "customer_spending:tx_count_30d",
        "customer_spending:avg_amount_30d",
        "customer_spending:max_amount_30d",
        "customer_spending:num_countries_90d",
        "customer_spending:num_payment_methods_30d",
    ])

    # Transaction-based feature views
    transaction_features: List[str] = field(default_factory=lambda: [
        # Risk indicators
        "risk_indicators:amount_vs_avg",
        "risk_indicators:is_high_amount_vs_hist",
        "risk_indicators:high_velocity",
        "risk_indicators:risky_payment",
        "risk_indicators:risky_category",
        # Calendar features
        "calendar:is_weekend",
        "calendar:is_night",
        "calendar:transaction_day",
        "calendar:transaction_month",
        # Address features
        "address:address_mismatch",
        "address:high_risk_shipping",
    ])


class FeastClient(BaseFeatureSource):
    """
    Client for retrieving batch features from Feast online store.

    Lazy-initializes the Feast store on first use.
    Implements FeatureSource protocol for SOLID compliance.
    """

    def __init__(self, config: Optional[FeastConfig] = None):
        self.config = config or FeastConfig()
        self._store = None
        self._init_error: Optional[str] = None

    @property
    def source_name(self) -> str:
        """Unique name identifying this source type."""
        return "feast"

    @property
    def store(self):
        """Lazy initialize Feast store."""
        if self._store is not None:
            return self._store

        try:
            from feast import FeatureStore

            if not os.path.exists(self.config.repo_path):
                raise FileNotFoundError(f"Feast repo not found: {self.config.repo_path}")

            self._store = FeatureStore(repo_path=self.config.repo_path)

            fvs = self._store.list_feature_views()
            logger.info(f"Feast initialized with {len(fvs)} feature views")

            self._init_error = None
            return self._store

        except Exception as e:
            self._init_error = str(e)
            logger.error(f"Feast init failed: {e}")
            raise

    def is_available(self) -> bool:
        """Check if Feast is available."""
        try:
            _ = self.store
            return True
        except Exception:
            return False

    def get_customer_features(
        self,
        customer_id: str,
    ) -> Tuple[Dict[str, Any], float]:
        """
        Fetch customer-based features.

        Args:
            customer_id: Customer identifier

        Returns:
            Tuple of (features_dict, latency_ms)
        """
        start = time.time()
        features = {}

        try:
            response = self.store.get_online_features(
                features=self.config.customer_features,
                entity_rows=[{"customer_id": customer_id}],
            ).to_dict()

            # Flatten (remove list wrapping)
            for k, v in response.items():
                if k != "customer_id":
                    features[k] = v[0] if v else None

            latency_ms = (time.time() - start) * 1000
            return features, latency_ms

        except Exception as e:
            logger.warning(f"Customer features fetch failed: {e}")
            latency_ms = (time.time() - start) * 1000
            return {}, latency_ms

    def get_transaction_features(
        self,
        transaction_id: str,
    ) -> Tuple[Dict[str, Any], float]:
        """
        Fetch transaction-based features.

        Args:
            transaction_id: Transaction identifier

        Returns:
            Tuple of (features_dict, latency_ms)
        """
        start = time.time()
        features = {}

        try:
            response = self.store.get_online_features(
                features=self.config.transaction_features,
                entity_rows=[{"transaction_id": transaction_id}],
            ).to_dict()

            # Flatten (remove list wrapping)
            for k, v in response.items():
                if k != "transaction_id":
                    features[k] = v[0] if v else None

            latency_ms = (time.time() - start) * 1000
            return features, latency_ms

        except Exception as e:
            logger.warning(f"Transaction features fetch failed: {e}")
            latency_ms = (time.time() - start) * 1000
            return {}, latency_ms

    def get_all_features(
        self,
        customer_id: str,
        transaction_id: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], float]:
        """
        Fetch all features for a customer and optionally transaction.

        OPTIMIZED: Uses single Feast call with all features instead of
        sequential calls, reducing latency by ~50%.

        Args:
            customer_id: Customer identifier
            transaction_id: Optional transaction identifier

        Returns:
            Tuple of (merged_features_dict, total_latency_ms)
        """
        start = time.time()
        features = {}

        try:
            # Combine all features into single request
            all_features = self.config.customer_features.copy()
            if transaction_id:
                all_features.extend(self.config.transaction_features)

            # Build entity row with all required keys
            entity_row = {"customer_id": customer_id}
            if transaction_id:
                entity_row["transaction_id"] = transaction_id

            # Single Feast call for all features
            response = self.store.get_online_features(
                features=all_features,
                entity_rows=[entity_row],
            ).to_dict()

            # Flatten (remove list wrapping)
            for k, v in response.items():
                if k not in ("customer_id", "transaction_id"):
                    features[k] = v[0] if v else None

            latency_ms = (time.time() - start) * 1000
            logger.debug(f"Feast batch fetch: {len(features)} features in {latency_ms:.1f}ms")
            return features, latency_ms

        except Exception as e:
            logger.warning(f"Feast all features fetch failed: {e}")
            # Fallback to sequential calls on error
            logger.info("Falling back to sequential Feast calls")
            features = {}
            total_latency = 0.0

            customer_features, customer_latency = self.get_customer_features(customer_id)
            features.update(customer_features)
            total_latency += customer_latency

            if transaction_id:
                tx_features, tx_latency = self.get_transaction_features(transaction_id)
                features.update(tx_features)
                total_latency += tx_latency

            return features, total_latency

    def get_features(
        self,
        entity_id: str,
        **kwargs: Any,
    ) -> FeatureResult:
        """
        Retrieve features for an entity (FeatureSource protocol implementation).

        Args:
            entity_id: Customer ID
            **kwargs: Optional 'transaction_id' for transaction features

        Returns:
            FeatureResult with features and metadata
        """
        transaction_id = kwargs.get("transaction_id")

        try:
            features, latency_ms = self.get_all_features(
                customer_id=entity_id,
                transaction_id=transaction_id,
            )
            return self._create_result(
                features=features,
                latency_ms=latency_ms,
                success=True,
            )
        except Exception as e:
            logger.warning(f"Feast get_features failed: {e}")
            return self._create_result(
                features={},
                latency_ms=0.0,
                success=False,
                error=str(e)[:100],
            )

    def health_check(self) -> Dict[str, Any]:
        """Return detailed health status."""
        return {
            "source": self.source_name,
            "available": self.is_available(),
            "error": self._init_error,
            "repo_path": self.config.repo_path,
        }
