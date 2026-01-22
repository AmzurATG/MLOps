"""
Model Explainability Module - SHAP-based Explanations
═══════════════════════════════════════════════════════════════════════════════

Provides human-readable explanations for fraud predictions using SHAP values.

Features:
- Feature contribution analysis (why was this flagged?)
- Risk factors identification
- Protective factors identification
- Waterfall visualization data
- Global feature importance

Usage:
    explainer = FraudExplainer(model)
    explanation = explainer.explain_prediction(features_df)
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from functools import lru_cache
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Try to import SHAP - graceful fallback if not available
try:
    import shap
    SHAP_AVAILABLE = True
    logger.info("[Explainability] SHAP library available")
except ImportError:
    SHAP_AVAILABLE = False
    logger.warning("[Explainability] SHAP not available - using fallback explanations")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class FeatureContribution:
    """Single feature's contribution to prediction."""
    feature: str
    value: Any
    contribution: float  # Absolute SHAP value
    shap_value: float    # Signed SHAP value
    direction: str       # "increases_risk" or "decreases_risk"
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class PredictionExplanation:
    """Complete explanation for a prediction."""
    transaction_id: str
    prediction: float
    base_value: float
    risk_factors: List[str]
    protective_factors: List[str]
    top_features: List[FeatureContribution]
    explanation_text: str
    explanation_type: str = "shap"
    feature_importance: Dict[str, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "transaction_id": self.transaction_id,
            "prediction": self.prediction,
            "base_value": self.base_value,
            "risk_factors": self.risk_factors,
            "protective_factors": self.protective_factors,
            "top_features": [f.to_dict() for f in self.top_features],
            "explanation_text": self.explanation_text,
            "explanation_type": self.explanation_type,
            "feature_importance": self.feature_importance,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE NAME MAPPINGS (for human-readable explanations)
# ═══════════════════════════════════════════════════════════════════════════════

FEATURE_DISPLAY_NAMES = {
    # Transaction features
    "amount": "Transaction Amount",
    "quantity": "Item Quantity",
    "country": "Transaction Country",
    "device_type": "Device Type",
    "payment_method": "Payment Method",
    "category": "Product Category",
    "tx_hour": "Transaction Hour",
    "account_age_days": "Account Age",

    # Behavioral features
    "transactions_before": "Previous Transactions",
    "total_spend_before": "Historical Total Spend",
    "avg_amount_before": "Historical Average Amount",
    "customer_tenure_days": "Customer Tenure (Days)",
    "days_since_last_tx": "Days Since Last Transaction",
    "is_new_customer": "New Customer Flag",
    "is_very_new_account": "Very New Account Flag",

    # Spending features
    "total_spend_7d": "Spend (Last 7 Days)",
    "tx_count_7d": "Transactions (Last 7 Days)",
    "total_spend_30d": "Spend (Last 30 Days)",
    "tx_count_30d": "Transactions (Last 30 Days)",
    "avg_amount_30d": "Average Amount (30 Days)",
    "max_amount_30d": "Max Amount (30 Days)",
    "num_countries_90d": "Countries Used (90 Days)",
    "num_payment_methods_30d": "Payment Methods (30 Days)",

    # Risk indicators
    "amount_vs_avg": "Amount vs Customer Average",
    "is_high_amount_vs_hist": "Unusually High Amount",
    "high_velocity": "High Transaction Velocity",
    "risky_payment": "Risky Payment Method",
    "risky_category": "Risky Category",

    # ═══════════════════════════════════════════════════════════════════════════
    # STREAMING VELOCITY FEATURES (from Kafka → Redis consumer)
    # ═══════════════════════════════════════════════════════════════════════════

    # Transaction velocity counts
    "tx_count_5min": "Transactions (Last 5 Min)",
    "tx_count_1h": "Transactions (Last Hour)",
    "tx_count_24h": "Transactions (Last 24 Hours)",

    # Amount aggregations
    "amount_sum_24h": "Total Spend (Last 24 Hours)",
    "avg_amount_24h": "Avg Transaction Amount (24h)",
    "max_amount_24h": "Max Transaction Amount (24h)",

    # Velocity risk indicators
    "velocity_score": "Velocity Risk Score",
    "high_velocity_flag": "High Velocity Alert",
    "amount_anomaly_flag": "Amount Anomaly Alert",

    # Streaming enriched features (from ksqlDB)
    "address_mismatch": "Billing/Shipping Mismatch",
    "is_night": "Night Transaction",
    "is_weekend": "Weekend Transaction",
    "shipping_country": "Shipping Country",
    "billing_country": "Billing Country",
    "ip_prefix": "IP Address Region",
}

RISK_EXPLANATIONS = {
    # Standard risk factors
    "amount": "Transaction amount is unusually high",
    "is_new_customer": "Customer account is new (limited history)",
    "is_very_new_account": "Account was created very recently",
    "high_velocity": "High number of transactions in short period",
    "risky_payment": "Payment method has higher fraud risk",
    "risky_category": "Product category has higher fraud risk",
    "amount_vs_avg": "Amount significantly higher than customer's average",
    "is_high_amount_vs_hist": "Amount exceeds customer's historical pattern",
    "tx_hour": "Transaction at unusual hour",
    "num_countries_90d": "Multiple countries used recently",
    "days_since_last_tx": "Unusual gap since last transaction",

    # Streaming velocity risk factors
    "tx_count_5min": "Multiple transactions within 5 minutes (velocity spike)",
    "tx_count_1h": "High transaction count in the last hour",
    "tx_count_24h": "Unusually high transaction volume today",
    "velocity_score": "Elevated velocity risk score",
    "high_velocity_flag": "Transaction velocity exceeds normal patterns",
    "amount_anomaly_flag": "Amount significantly higher than recent average",
    "amount_sum_24h": "Total spending today exceeds typical patterns",

    # Streaming enriched risk factors
    "address_mismatch": "Shipping and billing addresses are in different countries",
    "is_night": "Transaction occurred during high-risk night hours (12AM-6AM)",
    "is_weekend": "Transaction on weekend (higher fraud rates)",
}

PROTECTIVE_EXPLANATIONS = {
    # Standard protective factors
    "customer_tenure_days": "Established customer with long history",
    "transactions_before": "Customer has many previous transactions",
    "total_spend_before": "Customer has significant spending history",
    "avg_amount_before": "Transaction consistent with customer's typical amount",
    "tx_count_30d": "Regular transaction pattern",

    # Streaming protective factors
    "avg_amount_24h": "Amount consistent with recent spending pattern",
    "max_amount_24h": "Amount within customer's recent maximum",
}


# ═══════════════════════════════════════════════════════════════════════════════
# FRAUD EXPLAINER CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class FraudExplainer:
    """
    Generate SHAP-based explanations for fraud predictions.
    
    Args:
        model: Trained sklearn model (supports tree-based and linear models)
        feature_names: List of feature names (optional, inferred from model if available)
    """
    
    def __init__(self, model: Any, feature_names: Optional[List[str]] = None):
        self.model = model
        self._explainer = None
        self._background_data = None
        
        # Get feature names
        if feature_names:
            self.feature_names = feature_names
        elif hasattr(model, 'feature_names_in_'):
            self.feature_names = list(model.feature_names_in_)
        else:
            self.feature_names = None
        
        logger.info(f"[Explainability] Initialized with {len(self.feature_names) if self.feature_names else 'unknown'} features")
    
    def _get_explainer(self) -> Optional[Any]:
        """Lazy-load SHAP explainer."""
        if not SHAP_AVAILABLE:
            return None
        
        if self._explainer is None:
            try:
                # Use TreeExplainer for tree-based models (faster)
                if hasattr(self.model, 'estimators_') or hasattr(self.model, 'tree_'):
                    self._explainer = shap.TreeExplainer(self.model)
                    logger.info("[Explainability] Using TreeExplainer")
                else:
                    # Fallback to KernelExplainer for other models
                    # Would need background data for this
                    logger.warning("[Explainability] Non-tree model - SHAP may be slow")
                    self._explainer = shap.TreeExplainer(self.model)
            except Exception as e:
                logger.error(f"[Explainability] Failed to create SHAP explainer: {e}")
                return None
        
        return self._explainer
    
    def explain_prediction(
        self,
        features: pd.DataFrame,
        transaction_id: str = "unknown",
        top_k: int = 5,
    ) -> PredictionExplanation:
        """
        Generate explanation for a single prediction.
        
        Args:
            features: DataFrame with one row of features
            transaction_id: Transaction ID for reference
            top_k: Number of top features to return
        
        Returns:
            PredictionExplanation object with risk factors, protective factors, etc.
        """
        # Get prediction probability
        try:
            prediction = float(self.model.predict_proba(features)[0][1])
        except Exception:
            prediction = float(self.model.predict(features)[0])
        
        explainer = self._get_explainer()
        
        if explainer is not None:
            return self._explain_with_shap(
                features, prediction, transaction_id, top_k, explainer
            )
        else:
            return self._explain_with_fallback(
                features, prediction, transaction_id, top_k
            )
    
    def _explain_with_shap(
        self,
        features: pd.DataFrame,
        prediction: float,
        transaction_id: str,
        top_k: int,
        explainer: Any,
    ) -> PredictionExplanation:
        """Generate explanation using SHAP values."""
        try:
            shap_values = explainer.shap_values(features)
            
            # Handle binary classification (get fraud class SHAP values)
            if isinstance(shap_values, list):
                shap_values = shap_values[1]  # Fraud class
            
            # Get base value
            if isinstance(explainer.expected_value, (list, np.ndarray)):
                base_value = float(explainer.expected_value[1])
            else:
                base_value = float(explainer.expected_value)
            
            # Build feature contributions
            feature_names = self.feature_names or features.columns.tolist()
            contributions = []
            
            for i, col in enumerate(feature_names):
                if i < len(shap_values[0]):
                    shap_val = float(shap_values[0][i])
                    feature_val = features[col].iloc[0] if col in features.columns else None
                    
                    contributions.append(FeatureContribution(
                        feature=col,
                        value=feature_val,
                        contribution=abs(shap_val),
                        shap_value=shap_val,
                        direction="increases_risk" if shap_val > 0 else "decreases_risk"
                    ))
            
            # Sort by absolute contribution
            contributions.sort(key=lambda x: x.contribution, reverse=True)
            
            # Extract risk and protective factors
            risk_factors = []
            protective_factors = []
            
            for c in contributions[:top_k * 2]:  # Consider more features
                display_name = FEATURE_DISPLAY_NAMES.get(c.feature, c.feature)
                
                if c.direction == "increases_risk" and len(risk_factors) < top_k:
                    risk_factors.append(display_name)
                elif c.direction == "decreases_risk" and len(protective_factors) < top_k:
                    protective_factors.append(display_name)
            
            # Generate human-readable explanation
            explanation_text = self._generate_explanation_text(
                prediction, risk_factors, protective_factors, contributions[:top_k]
            )
            
            return PredictionExplanation(
                transaction_id=transaction_id,
                prediction=round(prediction, 4),
                base_value=round(base_value, 4),
                risk_factors=risk_factors,
                protective_factors=protective_factors,
                top_features=contributions[:top_k],
                explanation_text=explanation_text,
                explanation_type="shap",
                feature_importance=self.get_feature_importance(),
            )
            
        except Exception as e:
            logger.error(f"[Explainability] SHAP failed: {e}, using fallback")
            return self._explain_with_fallback(features, prediction, transaction_id, top_k)
    
    def _explain_with_fallback(
        self,
        features: pd.DataFrame,
        prediction: float,
        transaction_id: str,
        top_k: int,
    ) -> PredictionExplanation:
        """
        Generate explanation using feature importance (fallback when SHAP unavailable).
        """
        # Use model's feature importance if available
        importance = self.get_feature_importance()
        
        contributions = []
        feature_names = self.feature_names or features.columns.tolist()
        
        for col in feature_names:
            if col in features.columns:
                feature_val = features[col].iloc[0]
                imp = importance.get(col, 0.0)
                
                # Heuristic: high values of risky features increase risk
                # Includes streaming velocity features
                is_risky = col in [
                    # Standard risk indicators
                    "is_new_customer", "high_velocity", "risky_payment",
                    "risky_category", "amount_vs_avg", "is_high_amount_vs_hist",
                    # Streaming velocity risk indicators
                    "tx_count_5min", "tx_count_1h", "velocity_score",
                    "high_velocity_flag", "amount_anomaly_flag",
                    # Streaming enriched risk indicators
                    "address_mismatch", "is_night",
                ]
                
                if is_risky and feature_val:
                    direction = "increases_risk"
                elif col in [
                    # Standard protective factors
                    "customer_tenure_days", "transactions_before", "total_spend_before",
                    # Streaming protective factors (when amount is within normal range)
                    "avg_amount_24h", "max_amount_24h",
                ]:
                    direction = "decreases_risk" if feature_val and feature_val > 0 else "increases_risk"
                else:
                    direction = "increases_risk" if prediction > 0.5 else "decreases_risk"
                
                contributions.append(FeatureContribution(
                    feature=col,
                    value=feature_val,
                    contribution=imp,
                    shap_value=imp if direction == "increases_risk" else -imp,
                    direction=direction
                ))
        
        contributions.sort(key=lambda x: x.contribution, reverse=True)
        
        risk_factors = [
            FEATURE_DISPLAY_NAMES.get(c.feature, c.feature)
            for c in contributions[:top_k] if c.direction == "increases_risk"
        ]
        protective_factors = [
            FEATURE_DISPLAY_NAMES.get(c.feature, c.feature)
            for c in contributions[:top_k] if c.direction == "decreases_risk"
        ]
        
        explanation_text = self._generate_explanation_text(
            prediction, risk_factors, protective_factors, contributions[:top_k]
        )
        
        return PredictionExplanation(
            transaction_id=transaction_id,
            prediction=round(prediction, 4),
            base_value=0.05,  # Approximate base fraud rate
            risk_factors=risk_factors[:top_k],
            protective_factors=protective_factors[:top_k],
            top_features=contributions[:top_k],
            explanation_text=explanation_text,
            explanation_type="feature_importance",
            feature_importance=importance,
        )
    
    def _generate_explanation_text(
        self,
        prediction: float,
        risk_factors: List[str],
        protective_factors: List[str],
        top_contributions: List[FeatureContribution],
    ) -> str:
        """Generate human-readable explanation text."""
        risk_level = "HIGH" if prediction >= 0.7 else "MEDIUM" if prediction >= 0.4 else "LOW"
        
        lines = [
            f"Fraud Risk: {prediction:.1%} ({risk_level})",
            "",
        ]
        
        if risk_factors:
            lines.append("Risk Factors:")
            for rf in risk_factors[:3]:
                lines.append(f"  • {rf}")
            lines.append("")
        
        if protective_factors:
            lines.append("Protective Factors:")
            for pf in protective_factors[:3]:
                lines.append(f"  • {pf}")
        
        if not risk_factors and not protective_factors:
            lines.append("No significant factors identified.")
        
        return "\n".join(lines)
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get global feature importance from model."""
        if hasattr(self.model, 'feature_importances_'):
            if self.feature_names:
                return dict(zip(self.feature_names, self.model.feature_importances_.tolist()))
            return {f"feature_{i}": v for i, v in enumerate(self.model.feature_importances_)}
        
        # For linear models
        if hasattr(self.model, 'coef_'):
            coefs = np.abs(self.model.coef_).flatten()
            if self.feature_names:
                return dict(zip(self.feature_names, coefs.tolist()))
            return {f"feature_{i}": v for i, v in enumerate(coefs)}
        
        return {}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def create_explainer_for_model(model: Any) -> FraudExplainer:
    """Factory function to create explainer for a model."""
    return FraudExplainer(model)


def explain_batch(
    explainer: FraudExplainer,
    features_df: pd.DataFrame,
    transaction_ids: List[str],
    top_k: int = 5,
) -> List[PredictionExplanation]:
    """Generate explanations for multiple transactions."""
    explanations = []
    
    for i, tx_id in enumerate(transaction_ids):
        row = features_df.iloc[[i]]
        explanation = explainer.explain_prediction(row, tx_id, top_k)
        explanations.append(explanation)
    
    return explanations


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE EXPORTS
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "FraudExplainer",
    "PredictionExplanation",
    "FeatureContribution",
    "create_explainer_for_model",
    "explain_batch",
    "SHAP_AVAILABLE",
    "FEATURE_DISPLAY_NAMES",
]