"""
Feature Transformer - Single Source of Truth for Feature Engineering

This module ensures IDENTICAL feature computation logic between:
- Training (SQL in mlops_training_features.py)
- Inference (Python in fraud_api.py)

SCHEMA EVOLUTION SUPPORT:
- Semantic versioning for feature contracts (MAJOR.MINOR.PATCH)
- Type definitions for all features
- Schema validation with fail-fast behavior
- Categorical evolution with __unknown__ handling
- Feature deprecation lifecycle

Usage:
    transformer = FeatureTransformer.from_contract(contract_path)
    features = transformer.transform(request_features, feast_features)
"""

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
import pickle


# =============================================================================
# SCHEMA EVOLUTION: Type Definitions
# =============================================================================

# Expected types for each feature type category
EXPECTED_TYPES = {
    "float64": (float, int),
    "int64": (int,),
    "category": (str,),
    "bool": (bool, int),
    "string": (str,),
}

# Default values by type
DEFAULT_BY_TYPE = {
    "float64": 0.0,
    "int64": 0,
    "category": "__unknown__",
    "bool": 0,
    "string": "",
}


@dataclass
class SchemaValidationResult:
    """Result of schema validation with detailed error information."""
    valid: bool
    missing_features: List[str]
    type_mismatches: Dict[str, Tuple[str, str]]  # feature -> (expected_type, actual_type)
    unknown_categories: Dict[str, str]  # feature -> unknown_value
    out_of_range: Dict[str, Tuple[float, float, float]]  # feature -> (value, min, max)
    warnings: List[str]

    def is_critical(self) -> bool:
        """Check if validation has critical errors that should block inference."""
        return len(self.missing_features) > 0 or len(self.type_mismatches) > 0


@dataclass
class DeprecationInfo:
    """Information about a deprecated feature."""
    deprecated_in: str  # Version when deprecated (e.g., "1.2.0")
    removed_in: Optional[str]  # Version when removed (None = soft deprecation)
    replacement: Optional[str]  # Suggested replacement feature
    reason: str  # Reason for deprecation

    def to_dict(self) -> dict:
        return {
            "deprecated_in": self.deprecated_in,
            "removed_in": self.removed_in,
            "replacement": self.replacement,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DeprecationInfo":
        return cls(
            deprecated_in=data["deprecated_in"],
            removed_in=data.get("removed_in"),
            replacement=data.get("replacement"),
            reason=data.get("reason", "No reason provided"),
        )


class SchemaValidator:
    """
    Validates features against a FeatureContract schema.

    Provides:
    - Missing feature detection
    - Type validation and coercion
    - Range validation against training statistics
    - Unknown category detection
    - Deprecation warnings
    """

    def __init__(self, contract: "FeatureContract", strict_mode: bool = False):
        """
        Initialize validator.

        Args:
            contract: The feature contract to validate against
            strict_mode: If True, treat warnings as errors
        """
        self.contract = contract
        self.strict_mode = strict_mode

    def validate(self, features: Dict[str, Any]) -> SchemaValidationResult:
        """
        Validate features against the contract schema.

        Returns:
            SchemaValidationResult with detailed validation information
        """
        missing_features = []
        type_mismatches = {}
        unknown_categories = {}
        out_of_range = {}
        warnings = []

        # Check for missing required features
        required_features = set(self.contract.feature_order)
        provided_features = set(features.keys())
        missing = required_features - provided_features

        for feat in missing:
            if feat not in self.contract.deprecated_features:
                missing_features.append(feat)

        # Type validation
        for feat, expected_type in self.contract.feature_types.items():
            if feat not in features:
                continue

            value = features[feat]
            if value is None:
                continue

            valid_types = EXPECTED_TYPES.get(expected_type, (str,))
            if not isinstance(value, valid_types):
                actual_type = type(value).__name__
                type_mismatches[feat] = (expected_type, actual_type)

        # Categorical validation
        for col, mappings in self.contract.category_mappings.items():
            if col not in features:
                continue
            value = str(features[col])
            if value not in mappings and "__unknown__" not in mappings:
                unknown_categories[col] = value

        # Range validation
        for col, stats in self.contract.feature_stats.items():
            if col not in features:
                continue
            try:
                val = float(features[col])
                min_val = stats.get("min", float("-inf"))
                max_val = stats.get("max", float("inf"))

                # Flag if outside 3x range (likely anomaly)
                if val < min_val * 0.33 or val > max_val * 3:
                    out_of_range[col] = (val, min_val, max_val)
                    warnings.append(f"{col}={val} outside training range [{min_val:.2f}, {max_val:.2f}]")
            except (ValueError, TypeError):
                pass

        # Deprecation warnings
        for feat, info in self.contract.deprecated_features.items():
            if feat in features:
                if info.removed_in and self._version_gte(
                    self.contract.schema_version, info.removed_in
                ):
                    warnings.append(f"REMOVED: {feat} was removed in {info.removed_in}")
                    missing_features.append(feat)  # Treat as missing
                else:
                    warnings.append(
                        f"DEPRECATED: {feat} deprecated in {info.deprecated_in}: {info.reason}"
                    )

        valid = len(missing_features) == 0 and len(type_mismatches) == 0
        if self.strict_mode:
            valid = valid and len(warnings) == 0

        return SchemaValidationResult(
            valid=valid,
            missing_features=missing_features,
            type_mismatches=type_mismatches,
            unknown_categories=unknown_categories,
            out_of_range=out_of_range,
            warnings=warnings,
        )

    def _version_gte(self, v1: str, v2: str) -> bool:
        """Check if version v1 >= v2 using semantic versioning."""
        def parse_version(v: str) -> Tuple[int, int, int]:
            match = re.match(r"(\d+)\.(\d+)\.(\d+)", v)
            if match:
                return int(match.group(1)), int(match.group(2)), int(match.group(3))
            return (0, 0, 0)

        return parse_version(v1) >= parse_version(v2)


@dataclass
class FeatureContract:
    """
    Feature contract that defines the exact schema for model training/inference.
    Saved with the model and loaded at inference time.

    SCHEMA EVOLUTION:
    - schema_version: Semantic version (MAJOR.MINOR.PATCH)
      - MAJOR: Breaking changes (feature removed, type changed)
      - MINOR: New features added (backward compatible)
      - PATCH: Bug fixes, threshold tweaks
    - min_compatible_version: Minimum version this contract works with
    - feature_types: Type definition for each feature
    - deprecated_features: Features marked for removal
    """
    # Feature ordering (must match model training)
    feature_order: List[str]

    # Categorical encodings (saved from training data)
    category_mappings: Dict[str, Dict[str, int]]

    # Feature statistics for validation
    feature_stats: Dict[str, Dict[str, float]]

    # Feature taxonomy
    primitive_features: List[str]
    aggregate_features: List[str]
    derived_features: List[str]

    # Risk indicator thresholds (must match training SQL)
    risk_thresholds: Dict[str, Any] = field(default_factory=lambda: {
        "high_amount_multiplier": 3.0,  # amount_vs_avg > 3.0 = high
        "high_velocity_days": 0.25,      # < 0.25 days between transactions
        "risky_payment_methods": ["credit_card", "wallet"],
        "risky_categories": ["Electronics", "Luxury", "Digital"],
        "high_risk_countries": ["NG", "PK", "RU", "BR"],
    })

    # Metadata
    created_at: str = ""
    training_samples: int = 0
    model_version: str = ""

    # ==========================================================================
    # SCHEMA EVOLUTION: New fields for robust schema management
    # ==========================================================================

    # Semantic version of this contract (MAJOR.MINOR.PATCH)
    schema_version: str = "1.0.0"

    # Minimum compatible version (for backward compatibility)
    min_compatible_version: str = "1.0.0"

    # Type definitions for all features
    # Format: {"feature_name": "float64|int64|category|bool|string"}
    feature_types: Dict[str, str] = field(default_factory=dict)

    # Deprecated features with lifecycle information
    deprecated_features: Dict[str, DeprecationInfo] = field(default_factory=dict)

    # Required vs optional features
    required_features: Set[str] = field(default_factory=set)
    optional_features: Set[str] = field(default_factory=set)

    def __post_init__(self):
        """Initialize derived fields after construction."""
        # Auto-populate feature_types if empty
        if not self.feature_types:
            self.feature_types = self._infer_feature_types()

        # Auto-populate required/optional if empty
        if not self.required_features:
            self.required_features = set(self.feature_order)

        # Ensure __unknown__ category exists for categorical mappings
        for col, mappings in self.category_mappings.items():
            if "__unknown__" not in mappings:
                # Add __unknown__ with next available index
                max_idx = max(mappings.values()) if mappings else -1
                mappings["__unknown__"] = max_idx + 1

    def _infer_feature_types(self) -> Dict[str, str]:
        """Infer feature types from taxonomy and known patterns."""
        types = {}

        # Categorical features
        for col in self.category_mappings.keys():
            types[col] = "category"

        # Known numeric features
        numeric_features = [
            "amount", "quantity", "account_age_days", "customer_age",
            "transactions_before", "total_spend_before", "avg_amount_before",
            "customer_tenure_days", "days_since_last_tx", "total_spend_7d",
            "tx_count_7d", "total_spend_30d", "tx_count_30d", "avg_amount_30d",
            "max_amount_30d", "num_countries_90d", "num_payment_methods_30d",
            "amount_vs_avg",
        ]
        for feat in numeric_features:
            if feat in self.feature_order:
                types[feat] = "float64"

        # Known integer features
        int_features = [
            "tx_hour", "tx_dayofweek", "transaction_day", "transaction_month",
            "is_weekend", "is_night", "address_mismatch", "high_risk_shipping",
            "is_new_customer", "is_very_new_account", "is_high_amount_vs_hist",
            "high_velocity", "risky_payment", "risky_category",
        ]
        for feat in int_features:
            if feat in self.feature_order:
                types[feat] = "int64"

        # String features
        string_features = ["ip_prefix", "ip_address"]
        for feat in string_features:
            if feat in self.feature_order:
                types[feat] = "string"

        return types

    def is_compatible_with(self, other_version: str) -> bool:
        """
        Check if this contract is compatible with another version.

        Compatible if other_version >= min_compatible_version.
        """
        def parse_version(v: str) -> Tuple[int, int, int]:
            match = re.match(r"(\d+)\.(\d+)\.(\d+)", v)
            if match:
                return int(match.group(1)), int(match.group(2)), int(match.group(3))
            return (0, 0, 0)

        return parse_version(other_version) >= parse_version(self.min_compatible_version)

    def bump_version(self, bump_type: str = "patch") -> str:
        """
        Bump the schema version.

        Args:
            bump_type: "major", "minor", or "patch"

        Returns:
            New version string
        """
        match = re.match(r"(\d+)\.(\d+)\.(\d+)", self.schema_version)
        if not match:
            self.schema_version = "1.0.0"
            return self.schema_version

        major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))

        if bump_type == "major":
            self.schema_version = f"{major + 1}.0.0"
            self.min_compatible_version = self.schema_version  # Breaking change
        elif bump_type == "minor":
            self.schema_version = f"{major}.{minor + 1}.0"
        else:  # patch
            self.schema_version = f"{major}.{minor}.{patch + 1}"

        return self.schema_version

    def deprecate_feature(
        self,
        feature: str,
        reason: str,
        removed_in: Optional[str] = None,
        replacement: Optional[str] = None,
    ):
        """Mark a feature as deprecated."""
        self.deprecated_features[feature] = DeprecationInfo(
            deprecated_in=self.schema_version,
            removed_in=removed_in,
            replacement=replacement,
            reason=reason,
        )

    def get_schema_diff(self, other: "FeatureContract") -> Dict[str, Any]:
        """
        Get the difference between this contract and another.

        Returns dict with added, removed, type_changed, categorical_changed keys.
        """
        diff = {
            "added_features": [],
            "removed_features": [],
            "type_changes": {},
            "categorical_changes": {},
            "threshold_changes": {},
        }

        # Feature additions/removals
        old_features = set(other.feature_order)
        new_features = set(self.feature_order)

        diff["added_features"] = list(new_features - old_features)
        diff["removed_features"] = list(old_features - new_features)

        # Type changes
        for feat in new_features & old_features:
            old_type = other.feature_types.get(feat)
            new_type = self.feature_types.get(feat)
            if old_type and new_type and old_type != new_type:
                diff["type_changes"][feat] = {"from": old_type, "to": new_type}

        # Categorical changes
        for col in set(self.category_mappings.keys()) | set(other.category_mappings.keys()):
            old_cats = set(other.category_mappings.get(col, {}).keys())
            new_cats = set(self.category_mappings.get(col, {}).keys())

            added = new_cats - old_cats - {"__unknown__"}
            removed = old_cats - new_cats - {"__unknown__"}

            if added or removed:
                diff["categorical_changes"][col] = {
                    "added": list(added),
                    "removed": list(removed),
                }

        # Threshold changes
        for key in set(self.risk_thresholds.keys()) | set(other.risk_thresholds.keys()):
            old_val = other.risk_thresholds.get(key)
            new_val = self.risk_thresholds.get(key)
            if old_val != new_val:
                diff["threshold_changes"][key] = {"from": old_val, "to": new_val}

        return diff

    def to_dict(self) -> dict:
        """Serialize contract to dictionary."""
        return {
            "feature_order": self.feature_order,
            "category_mappings": self.category_mappings,
            "feature_stats": self.feature_stats,
            "primitive_features": self.primitive_features,
            "aggregate_features": self.aggregate_features,
            "derived_features": self.derived_features,
            "risk_thresholds": self.risk_thresholds,
            "created_at": self.created_at,
            "training_samples": self.training_samples,
            "model_version": self.model_version,
            # Schema evolution fields
            "schema_version": self.schema_version,
            "min_compatible_version": self.min_compatible_version,
            "feature_types": self.feature_types,
            "deprecated_features": {
                k: v.to_dict() for k, v in self.deprecated_features.items()
            },
            "required_features": list(self.required_features),
            "optional_features": list(self.optional_features),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FeatureContract":
        """Deserialize contract from dictionary."""
        # Parse deprecated features
        deprecated = {}
        for k, v in data.get("deprecated_features", {}).items():
            deprecated[k] = DeprecationInfo.from_dict(v)

        return cls(
            feature_order=data["feature_order"],
            category_mappings=data["category_mappings"],
            feature_stats=data["feature_stats"],
            primitive_features=data["primitive_features"],
            aggregate_features=data["aggregate_features"],
            derived_features=data["derived_features"],
            risk_thresholds=data.get("risk_thresholds", {}),
            created_at=data.get("created_at", ""),
            training_samples=data.get("training_samples", 0),
            model_version=data.get("model_version", ""),
            # Schema evolution fields
            schema_version=data.get("schema_version", "1.0.0"),
            min_compatible_version=data.get("min_compatible_version", "1.0.0"),
            feature_types=data.get("feature_types", {}),
            deprecated_features=deprecated,
            required_features=set(data.get("required_features", [])),
            optional_features=set(data.get("optional_features", [])),
        )

    def save(self, path: str):
        """Save contract to JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str) -> "FeatureContract":
        """Load contract from JSON file."""
        with open(path, "r") as f:
            return cls.from_dict(json.load(f))


class FeatureTransformer:
    """
    Transforms raw features into model-ready format.
    
    Ensures training/serving consistency by:
    1. Using saved categorical encodings (not per-request)
    2. Computing derived features with identical logic to SQL
    3. Assembling features in exact training order
    4. Validating feature ranges
    """
    
    # Default feature taxonomy (matches mlops_training_features.py)
    DEFAULT_PRIMITIVE_FEATURES = [
        "amount", "quantity", "country", "device_type", "payment_method",
        "category", "account_age_days", "tx_hour", "tx_dayofweek",
        "customer_age", "ip_address",
    ]
    
    DEFAULT_CALENDAR_FEATURES = [
        "transaction_day", "transaction_month", "is_weekend", "is_night",
    ]
    
    DEFAULT_ADDRESS_FEATURES = [
        "shipping_country", "billing_country", "address_mismatch", "high_risk_shipping",
    ]
    
    DEFAULT_AGGREGATE_FEATURES = [
        # Behavioral
        "transactions_before", "total_spend_before", "avg_amount_before",
        "customer_tenure_days", "days_since_last_tx", "is_new_customer", "is_very_new_account",
        # Spending windows
        "total_spend_7d", "tx_count_7d", "total_spend_30d", "tx_count_30d",
        "avg_amount_30d", "max_amount_30d", "num_countries_90d", "num_payment_methods_30d",
    ]
    
    DEFAULT_DERIVED_FEATURES = [
        "amount_vs_avg", "is_high_amount_vs_hist", "high_velocity",
        "risky_payment", "risky_category", "ip_prefix",
    ]
    
    def __init__(self, contract: Optional[FeatureContract] = None):
        self.contract = contract
        
    @classmethod
    def from_contract_file(cls, path: str) -> "FeatureTransformer":
        """Load transformer from saved contract."""
        contract = FeatureContract.load(path)
        return cls(contract)
    
    @classmethod
    def from_mlflow(cls, model_uri: str) -> "FeatureTransformer":
        """Load transformer from MLflow model artifacts."""
        import mlflow
        
        # Download artifact
        local_path = mlflow.artifacts.download_artifacts(
            artifact_uri=f"{model_uri}/feature_contract.json"
        )
        return cls.from_contract_file(local_path)
    
    def build_contract_from_training_data(
        self,
        df,
        feature_columns: List[str],
        categorical_columns: List[str],
        model_version: str = "1.0.0",
    ) -> FeatureContract:
        """
        Build feature contract from training DataFrame.
        Call this during training to capture encodings and stats.
        """
        import pandas as pd
        
        # Build categorical mappings from training data
        category_mappings = {}
        for col in categorical_columns:
            if col in df.columns:
                unique_values = df[col].dropna().unique().tolist()
                # Sort for deterministic ordering
                unique_values = sorted([str(v) for v in unique_values])
                category_mappings[col] = {v: i for i, v in enumerate(unique_values)}
        
        # Compute feature statistics
        feature_stats = {}
        for col in feature_columns:
            if col in df.columns and df[col].dtype in ['int64', 'float64', 'Int64', 'Float64']:
                feature_stats[col] = {
                    "min": float(df[col].min()) if pd.notna(df[col].min()) else 0.0,
                    "max": float(df[col].max()) if pd.notna(df[col].max()) else 0.0,
                    "mean": float(df[col].mean()) if pd.notna(df[col].mean()) else 0.0,
                    "std": float(df[col].std()) if pd.notna(df[col].std()) else 0.0,
                }
        
        # Determine feature taxonomy
        primitive_features = [f for f in feature_columns if f in self.DEFAULT_PRIMITIVE_FEATURES]
        calendar_features = [f for f in feature_columns if f in self.DEFAULT_CALENDAR_FEATURES]
        address_features = [f for f in feature_columns if f in self.DEFAULT_ADDRESS_FEATURES]
        aggregate_features = [f for f in feature_columns if f in self.DEFAULT_AGGREGATE_FEATURES]
        derived_features = [f for f in feature_columns if f in self.DEFAULT_DERIVED_FEATURES]
        
        contract = FeatureContract(
            feature_order=feature_columns,
            category_mappings=category_mappings,
            feature_stats=feature_stats,
            primitive_features=primitive_features + calendar_features + address_features,
            aggregate_features=aggregate_features,
            derived_features=derived_features,
            created_at=datetime.now().isoformat(),
            training_samples=len(df),
            model_version=model_version,
        )
        
        self.contract = contract
        return contract
    
    def encode_categorical(self, value: Any, column: str, unknown_value: int = -1) -> int:
        """
        Encode categorical value using saved mapping.

        SCHEMA EVOLUTION: Now handles unknown categories gracefully using
        the __unknown__ mapping if available, otherwise falls back to unknown_value.
        """
        if self.contract is None:
            raise ValueError("No contract loaded. Call build_contract_from_training_data or load from file.")

        mapping = self.contract.category_mappings.get(column, {})
        str_value = str(value)

        # Known category - return mapping
        if str_value in mapping:
            return mapping[str_value]

        # Unknown category - use __unknown__ if available (schema evolution)
        if "__unknown__" in mapping:
            self._record_unknown_category(column, str_value)
            return mapping["__unknown__"]

        # Fallback to provided unknown_value
        return unknown_value

    def _record_unknown_category(self, column: str, value: str):
        """Record unknown category for monitoring/alerting."""
        # Track in instance for later reporting
        if not hasattr(self, "_unknown_categories"):
            self._unknown_categories = {}
        if column not in self._unknown_categories:
            self._unknown_categories[column] = set()
        self._unknown_categories[column].add(value)

    def coerce_type(self, value: Any, feature: str) -> Tuple[Any, Optional[str]]:
        """
        Coerce value to expected type with validation.

        SCHEMA EVOLUTION: Type coercion with detailed error tracking.

        Returns:
            Tuple of (coerced_value, warning_message or None)
        """
        if self.contract is None or feature not in self.contract.feature_types:
            return value, None

        expected_type = self.contract.feature_types[feature]
        valid_types = EXPECTED_TYPES.get(expected_type, (str,))

        # Already correct type
        if isinstance(value, valid_types):
            return value, None

        # Handle None
        if value is None:
            default = DEFAULT_BY_TYPE.get(expected_type, 0)
            return default, f"{feature}: null coerced to {default}"

        # Attempt coercion
        try:
            if expected_type == "float64":
                return float(value), None
            elif expected_type == "int64":
                return int(float(value)), None  # Handle "3.0" -> 3
            elif expected_type == "category" or expected_type == "string":
                return str(value), None
            elif expected_type == "bool":
                if isinstance(value, str):
                    return 1 if value.lower() in ("true", "1", "yes") else 0, None
                return int(bool(value)), None
        except (ValueError, TypeError) as e:
            default = DEFAULT_BY_TYPE.get(expected_type, 0)
            warning = f"{feature}: coercion failed ({type(value).__name__} -> {expected_type}), using {default}"
            return default, warning

        return value, None
    
    def compute_calendar_features(
        self,
        tx_hour: int,
        tx_dayofweek: int,
        transaction_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Compute calendar features - matches SQL logic in training.
        """
        # If we have full datetime, extract day/month
        if transaction_date:
            transaction_day = transaction_date.day
            transaction_month = transaction_date.month
        else:
            # Use current time as fallback
            now = datetime.now()
            transaction_day = now.day
            transaction_month = now.month
        
        return {
            "transaction_day": transaction_day,
            "transaction_month": transaction_month,
            # SQL: CASE WHEN EXTRACT(DOW FROM transaction_date) IN (5, 6) THEN 1 ELSE 0 END
            "is_weekend": 1 if tx_dayofweek in [5, 6] else 0,
            # SQL: CASE WHEN transaction_hour >= 22 OR transaction_hour <= 5 THEN 1 ELSE 0 END
            "is_night": 1 if (tx_hour >= 22 or tx_hour <= 5) else 0,
        }
    
    def compute_address_features(
        self,
        shipping_address: Optional[str] = None,
        billing_address: Optional[str] = None,
        shipping_country: Optional[str] = None,
        billing_country: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compute address features - matches SQL logic in training.
        """
        thresholds = self.contract.risk_thresholds if self.contract else {
            "high_risk_countries": ["NG", "PK", "RU", "BR"]
        }
        high_risk_countries = thresholds.get("high_risk_countries", ["NG", "PK", "RU", "BR"])
        
        # Extract country from address if not provided
        if shipping_country is None and shipping_address:
            shipping_country = self._extract_country_from_address(shipping_address)
        if billing_country is None and billing_address:
            billing_country = self._extract_country_from_address(billing_address)
        
        # Default to unknown
        shipping_country = shipping_country or "XX"
        billing_country = billing_country or "XX"
        
        return {
            "shipping_country": shipping_country,
            "billing_country": billing_country,
            # SQL: CASE WHEN shipping_country != billing_country THEN 1 ELSE 0 END
            "address_mismatch": 1 if shipping_country != billing_country else 0,
            # SQL: CASE WHEN shipping_country IN ('NG', 'PK', 'RU', 'BR') THEN 1 ELSE 0 END
            "high_risk_shipping": 1 if shipping_country in high_risk_countries else 0,
        }
    
    def _extract_country_from_address(self, address: str) -> str:
        """Extract country code from address string - matches SQL CASE logic."""
        country_patterns = [
            (", NG", "NG"), (", PK", "PK"), (", RU", "RU"), (", BR", "BR"),
            (", GB", "GB"), (", US", "US"), (", IN", "IN"),
        ]
        for pattern, code in country_patterns:
            if pattern in address:
                return code
        # Fallback: last 2 chars
        return address[-2:] if len(address) >= 2 else "XX"
    
    def compute_derived_features(
        self,
        amount: float,
        avg_amount_before: float,
        avg_amount_30d: float,
        max_amount_30d: float,
        days_since_last_tx: float,
        payment_method: str,
        category: str,
        ip_address: str,
    ) -> Dict[str, Any]:
        """
        Compute derived risk features - MUST match SQL logic exactly.
        
        These are the features most prone to training/serving skew.
        """
        thresholds = self.contract.risk_thresholds if self.contract else {}
        
        high_amount_mult = thresholds.get("high_amount_multiplier", 3.0)
        high_velocity_days = thresholds.get("high_velocity_days", 0.25)
        risky_payments = thresholds.get("risky_payment_methods", ["credit_card", "wallet"])
        risky_cats = thresholds.get("risky_categories", ["Electronics", "Luxury", "Digital"])
        
        # SQL: CASE WHEN avg_amount_30d > 0 THEN transaction_amount / avg_amount_30d ELSE 1.0 END
        if avg_amount_30d > 0:
            amount_vs_avg = amount / avg_amount_30d
        else:
            amount_vs_avg = 1.0
        
        # SQL: CASE WHEN avg_amount_30d > 0 AND (transaction_amount / avg_amount_30d) > 3.0 THEN 1 ELSE 0 END
        if avg_amount_30d > 0 and amount_vs_avg > high_amount_mult:
            is_high_amount_vs_hist = 1
        else:
            is_high_amount_vs_hist = 0
        
        # SQL: CASE WHEN prev_transaction_date IS NOT NULL 
        #           AND DATE_DIFF('second', prev_transaction_date, transaction_date) / 86400.0 < 0.25 
        #      THEN 1 ELSE 0 END
        # Note: days_since_last_tx = 9999.0 means first transaction (no previous)
        if days_since_last_tx < 9999.0 and days_since_last_tx < high_velocity_days:
            high_velocity = 1
        else:
            high_velocity = 0
        
        # SQL: CASE WHEN payment_method IN ('credit_card', 'wallet') THEN 1 ELSE 0 END
        risky_payment = 1 if payment_method in risky_payments else 0
        
        # SQL: CASE WHEN product_category IN ('Electronics', 'Luxury', 'Digital') THEN 1 ELSE 0 END
        risky_category = 1 if category in risky_cats else 0
        
        # SQL: CASE WHEN POSITION('.' IN ip_address) > 0 
        #      THEN SUBSTR(ip_address, 1, POSITION('.' IN ip_address) - 1)
        #      ELSE ip_address END
        if "." in ip_address:
            ip_prefix = ip_address.split(".")[0]
        else:
            ip_prefix = ip_address
        
        return {
            "amount_vs_avg": amount_vs_avg,
            "is_high_amount_vs_hist": is_high_amount_vs_hist,
            "high_velocity": high_velocity,
            "risky_payment": risky_payment,
            "risky_category": risky_category,
            "ip_prefix": ip_prefix,
        }
    
    def transform_for_inference(
        self,
        request_features: Dict[str, Any],
        feast_features: Dict[str, Any],
        validate_schema: bool = True,
        strict_mode: bool = False,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """
        Transform raw features into model-ready format.

        SCHEMA EVOLUTION:
        - Validates schema before transformation (optional)
        - Applies type coercion with warnings
        - Handles unknown categories using __unknown__ mapping
        - Reports deprecation warnings

        Args:
            request_features: Features from API request (primitives)
            feast_features: Features from Feast online store (aggregates)
            validate_schema: If True, run schema validation before transform
            strict_mode: If True, fail on any validation warning

        Returns:
            Tuple of (feature_dict, warnings)

        Raises:
            SchemaValidationError: If validation fails in strict_mode
        """
        if self.contract is None:
            raise ValueError("No contract loaded")

        warnings = {}
        features = {}

        # Reset unknown categories tracking
        self._unknown_categories = {}

        # 1. Primitive features from request
        for feat in self.contract.primitive_features:
            if feat in request_features:
                features[feat] = request_features[feat]
            elif feat in feast_features:
                features[feat] = feast_features[feat]
            else:
                features[feat] = self._get_default_value(feat)
                warnings[feat] = "missing_using_default"

        # 2. Aggregate features from Feast
        for feat in self.contract.aggregate_features:
            if feat in feast_features and feast_features[feat] is not None:
                features[feat] = feast_features[feat]
            else:
                features[feat] = self._get_default_value(feat)
                if feat not in ["is_new_customer", "is_very_new_account"]:
                    warnings[feat] = "missing_from_feast"

        # 3. Compute calendar features
        calendar = self.compute_calendar_features(
            tx_hour=request_features.get("tx_hour", 12),
            tx_dayofweek=request_features.get("tx_dayofweek", 1),
            transaction_date=request_features.get("transaction_date"),
        )
        for feat in ["transaction_day", "transaction_month", "is_weekend", "is_night"]:
            if feat in self.contract.feature_order:
                features[feat] = calendar.get(feat, 0)

        # 4. Compute address features
        address = self.compute_address_features(
            shipping_address=request_features.get("shipping_address"),
            billing_address=request_features.get("billing_address"),
            shipping_country=request_features.get("shipping_country"),
            billing_country=request_features.get("billing_country"),
        )
        for feat in ["shipping_country", "billing_country", "address_mismatch", "high_risk_shipping"]:
            if feat in self.contract.feature_order:
                features[feat] = address.get(feat, 0)

        # 5. Compute derived features (REAL-TIME using request amount + Feast history)
        derived = self.compute_derived_features(
            amount=request_features.get("amount", 0.0),
            avg_amount_before=features.get("avg_amount_before", 0.0),
            avg_amount_30d=features.get("avg_amount_30d", 0.0),
            max_amount_30d=features.get("max_amount_30d", 0.0),
            days_since_last_tx=features.get("days_since_last_tx", 9999.0),
            payment_method=request_features.get("payment_method", "unknown"),
            category=request_features.get("category", "unknown"),
            ip_address=request_features.get("ip_address", "0.0.0.0"),
        )
        features.update(derived)

        # 6. SCHEMA EVOLUTION: Apply type coercion
        for feat in list(features.keys()):
            coerced_value, coercion_warning = self.coerce_type(features[feat], feat)
            features[feat] = coerced_value
            if coercion_warning:
                warnings[f"type_coercion:{feat}"] = coercion_warning

        # 7. Encode categoricals using saved mappings (with __unknown__ support)
        categorical_cols = list(self.contract.category_mappings.keys())
        for col in categorical_cols:
            if col in features:
                original_value = features[col]
                encoded_value = self.encode_categorical(original_value, col)
                if encoded_value == -1:
                    warnings[col] = f"unknown_category:{original_value}"
                    encoded_value = 0  # Default to first category
                features[col] = encoded_value

        # 8. SCHEMA EVOLUTION: Run schema validation
        if validate_schema:
            validator = SchemaValidator(self.contract, strict_mode=strict_mode)
            validation_result = validator.validate(features)

            # Add validation warnings
            for w in validation_result.warnings:
                warnings[f"validation:{w[:20]}"] = w

            # Add unknown category warnings
            for col, value in validation_result.unknown_categories.items():
                warnings[f"unknown_category:{col}"] = value

            # Fail if critical errors in strict mode
            if strict_mode and validation_result.is_critical():
                error_msg = f"Schema validation failed: missing={validation_result.missing_features}, type_errors={validation_result.type_mismatches}"
                raise ValueError(error_msg)

        # 9. Track unknown categories for monitoring
        if self._unknown_categories:
            for col, values in self._unknown_categories.items():
                warnings[f"new_category:{col}"] = f"values={list(values)[:5]}"

        return features, warnings
    
    def assemble_feature_vector(self, features: Dict[str, Any]) -> List[Any]:
        """
        Assemble features in exact training order.
        """
        if self.contract is None:
            raise ValueError("No contract loaded")
        
        return [features.get(col, 0) for col in self.contract.feature_order]
    
    def validate_features(self, features: Dict[str, Any]) -> List[str]:
        """
        Validate feature values against training statistics.
        Returns list of warnings for anomalous values.
        """
        if self.contract is None:
            return []
        
        warnings = []
        for col, stats in self.contract.feature_stats.items():
            if col not in features:
                continue
            
            val = features[col]
            if val is None:
                warnings.append(f"{col}: null value")
                continue
            
            try:
                val = float(val)
                min_val = stats.get("min", float("-inf"))
                max_val = stats.get("max", float("inf"))
                
                # Warn if outside 2x range of training data
                if val < min_val * 0.5 or val > max_val * 2:
                    warnings.append(f"{col}: {val} outside training range [{min_val}, {max_val}]")
            except (ValueError, TypeError):
                pass
        
        return warnings
    
    def _get_default_value(self, feature: str) -> Any:
        """Get sensible default value for missing features."""
        # Numeric defaults
        numeric_defaults = {
            "amount": 0.0,
            "quantity": 1,
            "account_age_days": 0,
            "customer_age": 30,
            "tx_hour": 12,
            "tx_dayofweek": 1,
            "transactions_before": 0,
            "total_spend_before": 0.0,
            "avg_amount_before": 0.0,
            "customer_tenure_days": 0,
            "days_since_last_tx": 9999.0,
            "is_new_customer": 1,
            "is_very_new_account": 1,
            "total_spend_7d": 0.0,
            "tx_count_7d": 0,
            "total_spend_30d": 0.0,
            "tx_count_30d": 0,
            "avg_amount_30d": 0.0,
            "max_amount_30d": 0.0,
            "num_countries_90d": 1,
            "num_payment_methods_30d": 1,
            "amount_vs_avg": 1.0,
            "is_high_amount_vs_hist": 0,
            "high_velocity": 0,
            "risky_payment": 0,
            "risky_category": 0,
            "transaction_day": 15,
            "transaction_month": 6,
            "is_weekend": 0,
            "is_night": 0,
            "address_mismatch": 0,
            "high_risk_shipping": 0,
        }
        
        # String defaults
        string_defaults = {
            "country": "US",
            "device_type": "desktop",
            "payment_method": "credit_card",
            "category": "Electronics",
            "ip_address": "0.0.0.0",
            "ip_prefix": "0",
            "shipping_country": "US",
            "billing_country": "US",
        }
        
        if feature in numeric_defaults:
            return numeric_defaults[feature]
        if feature in string_defaults:
            return string_defaults[feature]
        return 0