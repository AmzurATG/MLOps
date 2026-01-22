"""
Scoring Pipeline
================

Orchestrates the complete inference flow:
1. Feature retrieval (Feast + Redis)
2. Model selection (Production, A/B, Shadow)
3. Model inference
4. Score adjustments (streaming rules)
5. Telemetry recording

This is the main entry point for the serving layer.
"""

import os
import time
import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import numpy as np

from src.api.serving.features.feature_service import FeatureService, FeatureVector
from src.api.serving.models.model_loader import ModelLoader, LoadedModel
from src.api.serving.models.model_selector import ModelSelector, SelectionContext
from src.api.serving.scoring.adjustments import ScoreAdjuster, AdjustmentResult

logger = logging.getLogger(__name__)


# Risk level thresholds
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))


@dataclass
class RequestContext:
    """Context for a scoring request."""
    # Model selection
    model_stage: Optional[str] = None
    run_id: Optional[str] = None
    experiment_name: Optional[str] = None
    enable_shadow: bool = False

    # Feature options
    include_streaming: bool = True
    apply_streaming_rules: bool = True

    # Response options
    include_features: bool = False
    include_shap: bool = False


@dataclass
class ScoringResult:
    """Result of scoring pipeline."""
    # Identifiers
    transaction_id: str
    customer_id: str

    # Scores
    fraud_score: float
    is_fraud: bool
    risk_level: str

    # Model info
    model_name: str
    model_version: str
    model_stage: str
    run_id: Optional[str] = None
    contract_version: Optional[str] = None
    schema_version: Optional[str] = None  # SCHEMA EVOLUTION: Contract schema version

    # Streaming adjustment
    ml_score: Optional[float] = None
    streaming_boost: Optional[float] = None
    triggered_rules: List[str] = field(default_factory=list)
    streaming_available: bool = False
    streaming_features: Optional[Dict[str, Any]] = None  # Real-time velocity features

    # Latency
    feature_latency_ms: float = 0.0
    inference_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    feature_source: str = "none"

    # Shadow mode (if enabled)
    shadow_score: Optional[float] = None
    shadow_model_stage: Optional[str] = None

    # Metadata
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    warnings: List[str] = field(default_factory=list)
    features: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            "transaction_id": self.transaction_id,
            "customer_id": self.customer_id,
            "fraud_score": round(self.fraud_score, 4),
            "is_fraud": self.is_fraud,
            "risk_level": self.risk_level,
            "model_name": self.model_name,
            "model_version": self.model_version,
            "model_stage": self.model_stage,
            "run_id": self.run_id,
            "contract_version": self.contract_version,
            "schema_version": self.schema_version,  # SCHEMA EVOLUTION
            "feature_source": self.feature_source,
            "feature_latency_ms": round(self.feature_latency_ms, 2),
            "inference_latency_ms": round(self.inference_latency_ms, 2),
            "total_latency_ms": round(self.total_latency_ms, 2),
            "timestamp": self.timestamp,
            "warnings": self.warnings,
        }

        # Add streaming info if available
        if self.streaming_available:
            result["ml_score"] = round(self.ml_score, 4) if self.ml_score is not None else None
            result["streaming_boost"] = round(self.streaming_boost, 4) if self.streaming_boost else 0.0
            result["triggered_rules"] = self.triggered_rules if self.triggered_rules else []
            result["streaming_available"] = self.streaming_available
            # Include key velocity features for visibility
            if self.streaming_features:
                result["streaming_features"] = {
                    "tx_count_5min": self.streaming_features.get("tx_count_5min"),
                    "tx_count_1h": self.streaming_features.get("tx_count_1h"),
                    "tx_count_24h": self.streaming_features.get("tx_count_24h"),
                    "velocity_score": self.streaming_features.get("velocity_score"),
                    "high_velocity_flag": self.streaming_features.get("high_velocity_flag"),
                    "address_mismatch": self.streaming_features.get("address_mismatch"),
                    "is_night": self.streaming_features.get("is_night"),
                    "risky_payment": self.streaming_features.get("risky_payment"),
                }

        # Add shadow if available
        if self.shadow_score is not None:
            result["shadow_score"] = round(self.shadow_score, 4)
            result["shadow_model_stage"] = self.shadow_model_stage

        # Add features if requested
        if self.features:
            result["features"] = self.features

        return result


def get_risk_level(score: float) -> str:
    """Map score to risk level."""
    if score >= 0.8:
        return "HIGH"
    elif score >= FRAUD_THRESHOLD:
        return "MEDIUM"
    elif score >= 0.3:
        return "LOW"
    else:
        return "MINIMAL"


class ScoringPipeline:
    """
    Main scoring pipeline.

    Orchestrates feature retrieval, model inference, and score adjustments.
    """

    def __init__(
        self,
        feature_service: Optional[FeatureService] = None,
        model_loader: Optional[ModelLoader] = None,
        score_adjuster: Optional[ScoreAdjuster] = None,
    ):
        self.feature_service = feature_service or FeatureService()
        self.model_loader = model_loader or ModelLoader()
        self.model_selector = ModelSelector(self.model_loader)
        self.score_adjuster = score_adjuster or ScoreAdjuster()

    def _run_inference(
        self,
        loaded_model: LoadedModel,
        request_features: Dict[str, Any],
        batch_features: Dict[str, Any],
    ) -> Tuple[float, List[str]]:
        """
        Run model inference.

        Args:
            loaded_model: The loaded model with contract/transformer
            request_features: Features from the request
            batch_features: Features from Feast

        Returns:
            Tuple of (fraud_probability, warnings)
        """
        warnings = []

        if loaded_model.transformer and loaded_model.contract:
            # SCHEMA EVOLUTION: Check compatibility against merged features (request + Feast + streaming)
            if loaded_model.compatibility:
                # Merge all feature sources for compatibility check
                all_features = {**batch_features, **request_features}
                compatible, compat_warnings = loaded_model.check_compatibility(all_features)
                if compat_warnings:
                    warnings.extend(compat_warnings)
                if not compatible:
                    warnings.append("WARN: Feature compatibility check failed")

            # Use feature contract for consistency (with schema validation)
            features, feat_warnings = loaded_model.transformer.transform_for_inference(
                request_features, batch_features,
                validate_schema=True,  # SCHEMA EVOLUTION: Validate schema
                strict_mode=False,     # Log warnings but don't fail
            )
            warnings.extend([f"{k}: {v}" for k, v in feat_warnings.items()])

            feature_vector = loaded_model.transformer.assemble_feature_vector(features)
            feature_df = pd.DataFrame(
                [feature_vector],
                columns=loaded_model.contract.feature_order
            )
        elif loaded_model.contract and hasattr(loaded_model.contract, 'category_mappings'):
            # SimpleFeatureContract mode - contract without full transformer
            # Use the contract's category_mappings for encoding
            warnings.append("Using SimpleFeatureContract (contract-based encoding)")
            feature_df = self._build_features_with_contract(
                request_features, batch_features, loaded_model.contract
            )
        else:
            # Legacy mode
            warnings.append("Using legacy feature assembly (no schema evolution)")
            feature_df = self._build_features_legacy(request_features, batch_features)

        # Predict
        proba = loaded_model.model.predict_proba(feature_df)[0][1]

        return float(proba), warnings

    def _build_features_with_contract(
        self,
        request_features: Dict[str, Any],
        batch_features: Dict[str, Any],
        contract: Any,
    ) -> pd.DataFrame:
        """
        Build features using SimpleFeatureContract category mappings.

        This is used when we have the contract JSON but the transformer module
        failed to load (e.g., syntax error in artifact).
        """
        from datetime import datetime

        def get_feat(name: str, default: Any = 0):
            return batch_features.get(name, default) or default

        def get_req(name: str, default: Any = None):
            return request_features.get(name, default)

        # Basic request features
        amount = get_req("amount", 0)
        account_age_days = get_req("account_age_days", 0)
        tx_hour = get_req("tx_hour", datetime.now().hour)
        tx_dayofweek = get_req("tx_dayofweek", datetime.now().weekday())

        # Country/address features
        country = get_req("country", "US")
        shipping_country = get_req("shipping_country", country)
        billing_country = get_req("billing_country", country)
        ip_address = get_req("ip_address", "0.0.0.0")

        # Computed features from Feast
        avg_amount_30d = get_feat("avg_amount_30d", 0.0)
        days_since_last_tx = get_feat("days_since_last_tx", 9999.0)
        customer_tenure_days = get_feat("customer_tenure_days", account_age_days)

        # Derived features
        amount_vs_avg = amount / avg_amount_30d if avg_amount_30d > 0 else 1.0
        address_mismatch = 1 if shipping_country != billing_country else 0

        # High-risk shipping countries
        HIGH_RISK_COUNTRIES = {"NG", "GH", "KE", "PH", "ID", "VN", "BD", "PK"}
        high_risk_shipping = 1 if shipping_country in HIGH_RISK_COUNTRIES else 0

        # IP prefix extraction
        ip_parts = ip_address.split(".")
        ip_prefix = ip_parts[0] if ip_parts else "0"

        # Build features dict with contract-based encoding for categorical features
        features = {
            # Numerical features
            "amount": amount,
            "quantity": get_req("quantity", 1),
            "account_age_days": account_age_days,
            "tx_hour": tx_hour,
            "tx_dayofweek": tx_dayofweek,

            # Calendar features
            "transaction_day": datetime.now().day,
            "transaction_month": datetime.now().month,
            "is_weekend": 1 if tx_dayofweek >= 5 else 0,
            "is_night": 1 if tx_hour >= 22 or tx_hour < 6 else 0,

            # Address features (derived)
            "address_mismatch": address_mismatch,
            "high_risk_shipping": high_risk_shipping,

            # Customer behavioral (from Feast)
            "transactions_before": get_feat("transactions_before", 0),
            "total_spend_before": get_feat("total_spend_before", 0.0),
            "avg_amount_before": get_feat("avg_amount_before", 0.0),
            "customer_tenure_days": customer_tenure_days,
            "days_since_last_tx": days_since_last_tx,
            "is_new_customer": get_feat("is_new_customer", 1 if customer_tenure_days < 30 else 0),
            "is_very_new_account": 1 if customer_tenure_days < 7 else 0,

            # Spending features (from Feast)
            "total_spend_7d": get_feat("total_spend_7d", 0.0),
            "total_spend_30d": get_feat("total_spend_30d", 0.0),
            "tx_count_7d": get_feat("tx_count_7d", 0),
            "tx_count_30d": get_feat("tx_count_30d", 0),
            "avg_amount_30d": avg_amount_30d,
            "max_amount_30d": get_feat("max_amount_30d", amount),
            "num_countries_90d": get_feat("num_countries_90d", 1),
            "num_payment_methods_30d": get_feat("num_payment_methods_30d", 1),

            # Risk indicators
            "amount_vs_avg": amount_vs_avg,
            "is_high_amount_vs_hist": 1 if amount_vs_avg > 3.0 else 0,
            "high_velocity": 1 if days_since_last_tx < 0.25 else 0,
            "risky_payment": 1 if get_req("payment_method") in ["crypto", "wire_transfer"] else 0,
            "risky_category": 1 if get_req("category") in ["Electronics", "Luxury", "Digital", "Gift_Card"] else 0,

            # Categorical features - use contract's category_mappings
            "country": contract.encode_category("country", country),
            "device_type": contract.encode_category("device_type", get_req("device_type", "desktop")),
            "payment_method": contract.encode_category("payment_method", get_req("payment_method", "credit_card")),
            "category": contract.encode_category("category", get_req("category", "electronics")),
            "shipping_country": contract.encode_category("shipping_country", shipping_country),
            "billing_country": contract.encode_category("billing_country", billing_country),
            "ip_prefix": contract.encode_category("ip_prefix", ip_prefix),
        }

        # Create DataFrame with features in correct order as per contract
        feature_vector = [features.get(f, 0) for f in contract.feature_order]
        return pd.DataFrame([feature_vector], columns=contract.feature_order)

    def _build_features_legacy(
        self,
        request_features: Dict[str, Any],
        batch_features: Dict[str, Any],
    ) -> pd.DataFrame:
        """Legacy feature building without contract - includes ALL 38 model features."""
        from datetime import datetime

        def get_feat(name: str, default: Any = 0):
            return batch_features.get(name, default) or default

        def get_req(name: str, default: Any = None):
            return request_features.get(name, default)

        # Basic request features
        amount = get_req("amount", 0)
        account_age_days = get_req("account_age_days", 0)
        tx_hour = get_req("tx_hour", datetime.now().hour)
        tx_dayofweek = get_req("tx_dayofweek", datetime.now().weekday())

        # Country/address features
        country = get_req("country", "US")
        shipping_country = get_req("shipping_country", country)
        billing_country = get_req("billing_country", country)
        ip_address = get_req("ip_address", "0.0.0.0")

        # Computed features from Feast
        avg_amount_30d = get_feat("avg_amount_30d", 0.0)
        days_since_last_tx = get_feat("days_since_last_tx", 9999.0)
        customer_tenure_days = get_feat("customer_tenure_days", account_age_days)

        # Derived features
        amount_vs_avg = amount / avg_amount_30d if avg_amount_30d > 0 else 1.0
        address_mismatch = 1 if shipping_country != billing_country else 0

        # High-risk shipping countries
        HIGH_RISK_COUNTRIES = {"NG", "GH", "KE", "PH", "ID", "VN", "BD", "PK"}
        high_risk_shipping = 1 if shipping_country in HIGH_RISK_COUNTRIES else 0

        # IP prefix extraction
        ip_parts = ip_address.split(".")
        ip_prefix = int(ip_parts[0]) if ip_parts else 0

        # Categorical encoding (simple hash-based)
        def encode_cat(val: str, max_val: int = 100) -> int:
            return hash(str(val)) % max_val

        features = {
            # Primitives
            "amount": amount,
            "quantity": get_req("quantity", 1),
            "country": encode_cat(country),
            "device_type": encode_cat(get_req("device_type", "desktop"), 10),
            "payment_method": encode_cat(get_req("payment_method", "credit_card"), 10),
            "category": encode_cat(get_req("category", "Electronics"), 20),
            "account_age_days": account_age_days,
            "tx_hour": tx_hour,
            "tx_dayofweek": tx_dayofweek,

            # Calendar features
            "transaction_day": datetime.now().day,
            "transaction_month": datetime.now().month,
            "is_weekend": 1 if tx_dayofweek >= 5 else 0,
            "is_night": 1 if tx_hour >= 22 or tx_hour < 6 else 0,

            # Address features
            "shipping_country": encode_cat(shipping_country),
            "billing_country": encode_cat(billing_country),
            "address_mismatch": address_mismatch,
            "high_risk_shipping": high_risk_shipping,

            # Customer behavioral (from Feast)
            "transactions_before": get_feat("transactions_before", 0),
            "total_spend_before": get_feat("total_spend_before", 0.0),
            "avg_amount_before": get_feat("avg_amount_before", 0.0),
            "customer_tenure_days": customer_tenure_days,
            "days_since_last_tx": days_since_last_tx,
            "is_new_customer": get_feat("is_new_customer", 1 if customer_tenure_days < 30 else 0),
            "is_very_new_account": 1 if customer_tenure_days < 7 else 0,

            # Spending features (from Feast)
            "total_spend_7d": get_feat("total_spend_7d", 0.0),
            "total_spend_30d": get_feat("total_spend_30d", 0.0),
            "tx_count_7d": get_feat("tx_count_7d", 0),
            "tx_count_30d": get_feat("tx_count_30d", 0),
            "avg_amount_30d": avg_amount_30d,
            "max_amount_30d": get_feat("max_amount_30d", amount),
            "num_countries_90d": get_feat("num_countries_90d", 1),
            "num_payment_methods_30d": get_feat("num_payment_methods_30d", 1),

            # Risk indicators
            "amount_vs_avg": amount_vs_avg,
            "is_high_amount_vs_hist": 1 if amount_vs_avg > 3.0 else 0,
            "high_velocity": 1 if days_since_last_tx < 0.25 else 0,
            "risky_payment": 1 if get_req("payment_method") in ["crypto", "wire_transfer"] else 0,
            "risky_category": 1 if get_req("category") in ["Electronics", "Luxury", "Digital", "Gift_Card"] else 0,
            "ip_prefix": ip_prefix,
        }

        return pd.DataFrame([features])

    def score(
        self,
        customer_id: str,
        transaction_id: str,
        request_features: Dict[str, Any],
        context: Optional[RequestContext] = None,
    ) -> ScoringResult:
        """
        Score a transaction.

        Args:
            customer_id: Customer identifier
            transaction_id: Transaction identifier
            request_features: Features from the request
            context: Optional request context for model selection and options

        Returns:
            ScoringResult with score and metadata
        """
        start_time = time.time()
        context = context or RequestContext()
        warnings = []

        # 1. Get features
        feature_vector = self.feature_service.get_features(
            customer_id=customer_id,
            transaction_id=transaction_id,
            include_streaming=context.include_streaming,
        )
        warnings.extend(feature_vector.warnings)

        # 2. Select model
        selection_context = SelectionContext(
            customer_id=customer_id,
            transaction_id=transaction_id,
            model_stage=context.model_stage,
            run_id=context.run_id,
            experiment_name=context.experiment_name,
            enable_shadow=context.enable_shadow,
        )
        selection = self.model_selector.select(selection_context)

        # 3. Run inference (use merged features: batch + streaming)
        inference_start = time.time()
        merged_features = feature_vector.merged  # Combines Feast + streaming features
        ml_score, inf_warnings = self._run_inference(
            selection.primary_model,
            request_features,
            merged_features,
        )
        inference_latency_ms = (time.time() - inference_start) * 1000
        warnings.extend(inf_warnings)

        # 4. Shadow inference (if enabled)
        shadow_score = None
        shadow_stage = None
        if selection.shadow_model:
            try:
                shadow_score, _ = self._run_inference(
                    selection.shadow_model,
                    request_features,
                    merged_features,
                )
                shadow_stage = selection.shadow_model.info.stage
            except Exception as e:
                warnings.append(f"Shadow inference failed: {str(e)[:50]}")

        # 5. Apply streaming adjustments
        final_score = ml_score
        adjustment = None

        if context.apply_streaming_rules and feature_vector.streaming_available:
            adjustment = self.score_adjuster.apply(
                ml_score,
                feature_vector.streaming_features,
            )
            final_score = adjustment.final_score

        # 6. Build result
        total_latency_ms = (time.time() - start_time) * 1000
        model_info = selection.primary_model.info

        # SCHEMA EVOLUTION: Get schema version from model
        schema_version = None
        if selection.primary_model.contract:
            schema_version = getattr(selection.primary_model.contract, "schema_version", None)

        result = ScoringResult(
            transaction_id=transaction_id,
            customer_id=customer_id,
            fraud_score=final_score,
            is_fraud=final_score >= FRAUD_THRESHOLD,
            risk_level=get_risk_level(final_score),
            model_name=model_info.model_name,
            model_version=str(model_info.version or "unknown"),
            model_stage=model_info.stage,
            run_id=model_info.run_id,
            contract_version=model_info.contract_version,
            schema_version=schema_version,  # SCHEMA EVOLUTION
            # Always show ml_score when streaming is available (even if no boost)
            ml_score=ml_score if feature_vector.streaming_available else None,
            streaming_boost=adjustment.total_boost if adjustment else 0.0,
            triggered_rules=adjustment.triggered_rules if adjustment else [],
            streaming_available=feature_vector.streaming_available,
            streaming_features=feature_vector.streaming_features if feature_vector.streaming_available else None,
            feature_latency_ms=feature_vector.total_latency_ms,
            inference_latency_ms=inference_latency_ms,
            total_latency_ms=total_latency_ms,
            feature_source=feature_vector.source,
            shadow_score=shadow_score,
            shadow_model_stage=shadow_stage,
            warnings=warnings,
            features=feature_vector.merged if context.include_features else None,
        )

        return result

    def health_check(self) -> Dict[str, Any]:
        """Check health of all components."""
        return {
            "features": self.feature_service.health_check(),
            "models": {
                "cached_count": self.model_loader.cached_count,
                "available_models": len(self.model_loader.list_models()),
            },
        }
