"""
MLflow Model Loader
====================

Loads and caches models from MLflow registry.

Features:
- Lazy initialization
- Model caching with TTL and LRU eviction (OPTIMIZED)
- Feature contract loading (if available)

SCHEMA EVOLUTION:
- Contract version compatibility checking
- Feature compatibility matrix
- Schema version validation before inference
"""

import os
import time
import logging
import re
from typing import Dict, Any, Optional, Tuple, List, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMA EVOLUTION: Compatibility Errors
# =============================================================================

class SchemaCompatibilityError(Exception):
    """Raised when schema versions are incompatible."""
    pass


class MissingFeaturesError(SchemaCompatibilityError):
    """Raised when required features are missing."""
    def __init__(self, missing: List[str]):
        self.missing = missing
        super().__init__(f"Missing required features: {missing}")


class IncompatibleContractError(SchemaCompatibilityError):
    """Raised when contract version is incompatible."""
    def __init__(self, model_version: str, contract_version: str, min_version: str):
        self.model_version = model_version
        self.contract_version = contract_version
        self.min_version = min_version
        super().__init__(
            f"Contract version {contract_version} incompatible with model {model_version} "
            f"(requires >= {min_version})"
        )


class TTLCache:
    """
    Simple TTL cache with LRU eviction.

    OPTIMIZED: Prevents unbounded memory growth from model cache
    by evicting entries after TTL and when maxsize is exceeded.
    """

    def __init__(self, maxsize: int = 10, ttl: int = 3600):
        """
        Args:
            maxsize: Maximum number of items in cache
            ttl: Time-to-live in seconds (default 1 hour)
        """
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, Any] = {}
        self._timestamps: Dict[str, float] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache if exists and not expired."""
        if key in self._cache:
            if time.time() - self._timestamps[key] < self.ttl:
                return self._cache[key]
            else:
                # Expired - remove it
                del self._cache[key]
                del self._timestamps[key]
                logger.debug(f"Cache evicted (TTL expired): {key}")
        return None

    def set(self, key: str, value: Any):
        """Set item in cache, evicting oldest if at capacity."""
        # Evict oldest if at capacity
        if len(self._cache) >= self.maxsize and key not in self._cache:
            oldest = min(self._timestamps, key=self._timestamps.get)
            del self._cache[oldest]
            del self._timestamps[oldest]
            logger.debug(f"Cache evicted (LRU): {oldest}")

        self._cache[key] = value
        self._timestamps[key] = time.time()

    def __contains__(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        return self.get(key) is not None

    def __getitem__(self, key: str) -> Any:
        """Get item using dict-style access."""
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: Any):
        """Set item using dict-style access."""
        self.set(key, value)

    def pop(self, key: str, default: Any = None) -> Any:
        """Remove and return item, or default if not found."""
        if key in self._cache:
            value = self._cache.pop(key)
            self._timestamps.pop(key, None)
            return value
        return default

    def clear(self):
        """Clear all cached items."""
        self._cache.clear()
        self._timestamps.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        return {
            "size": len(self._cache),
            "maxsize": self.maxsize,
            "ttl_seconds": self.ttl,
            "keys": list(self._cache.keys()),
        }


@dataclass
class SimpleFeatureContract:
    """
    Simple feature contract loaded directly from JSON.

    Used as fallback when the full FeatureTransformer module cannot be loaded.
    Provides feature_order and category_mappings for inference.
    """
    feature_order: List[str]
    category_mappings: Dict[str, Dict[str, int]]
    model_version: str = "1.0.0"
    schema_version: str = "1.0.0"
    min_compatible_version: str = "1.0.0"
    required_features: List[str] = field(default_factory=list)
    optional_features: List[str] = field(default_factory=list)
    feature_types: Dict[str, str] = field(default_factory=dict)
    feature_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str) -> "SimpleFeatureContract":
        """Load contract from JSON file."""
        import json
        with open(path, "r") as f:
            data = json.load(f)
        return cls(
            feature_order=data.get("feature_order", []),
            category_mappings=data.get("category_mappings", {}),
            model_version=data.get("model_version", "1.0.0"),
            schema_version=data.get("schema_version", "1.0.0"),
            min_compatible_version=data.get("min_compatible_version", "1.0.0"),
            required_features=data.get("required_features", []),
            optional_features=data.get("optional_features", []),
            feature_types=data.get("feature_types", {}),
            feature_stats=data.get("feature_stats", {}),
        )

    def encode_category(self, feature_name: str, value: Any) -> int:
        """Encode a categorical value using the contract's mappings."""
        if feature_name not in self.category_mappings:
            # Not a categorical feature, return as-is
            return value

        mapping = self.category_mappings[feature_name]
        str_value = str(value)

        if str_value in mapping:
            return mapping[str_value]
        elif "__unknown__" in mapping:
            return mapping["__unknown__"]
        else:
            # Default to 0 if no unknown mapping
            return 0


@dataclass
class CompatibilityMatrix:
    """
    SCHEMA EVOLUTION: Feature compatibility matrix for model-contract validation.

    Defines which features a model requires and supports.
    """
    model_version: str
    min_contract_version: str  # Minimum compatible contract version
    max_contract_version: Optional[str] = None  # Maximum compatible (None = any)
    required_features: Set[str] = field(default_factory=set)
    optional_features: Set[str] = field(default_factory=set)
    supported_contract_versions: List[str] = field(default_factory=list)

    def is_contract_compatible(self, contract_version: str) -> bool:
        """Check if a contract version is compatible with this model."""
        def parse_version(v: str) -> Tuple[int, int, int]:
            match = re.match(r"(\d+)\.(\d+)\.(\d+)", v)
            if match:
                return int(match.group(1)), int(match.group(2)), int(match.group(3))
            return (0, 0, 0)

        contract_v = parse_version(contract_version)
        min_v = parse_version(self.min_contract_version)

        if contract_v < min_v:
            return False

        if self.max_contract_version:
            max_v = parse_version(self.max_contract_version)
            if contract_v > max_v:
                return False

        return True

    def check_features(self, available_features: Set[str]) -> Tuple[bool, List[str]]:
        """
        Check if all required features are available.

        Returns:
            Tuple of (all_present, missing_features)
        """
        missing = self.required_features - available_features
        return len(missing) == 0, list(missing)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_version": self.model_version,
            "min_contract_version": self.min_contract_version,
            "max_contract_version": self.max_contract_version,
            "required_features": list(self.required_features),
            "optional_features": list(self.optional_features),
            "supported_contract_versions": self.supported_contract_versions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompatibilityMatrix":
        return cls(
            model_version=data.get("model_version", "1.0.0"),
            min_contract_version=data.get("min_contract_version", "1.0.0"),
            max_contract_version=data.get("max_contract_version"),
            required_features=set(data.get("required_features", [])),
            optional_features=set(data.get("optional_features", [])),
            supported_contract_versions=data.get("supported_contract_versions", []),
        )

    @classmethod
    def from_contract(cls, contract: Any) -> "CompatibilityMatrix":
        """Build compatibility matrix from a FeatureContract."""
        return cls(
            model_version=getattr(contract, "model_version", "1.0.0"),
            min_contract_version=getattr(contract, "min_compatible_version", "1.0.0"),
            required_features=set(getattr(contract, "required_features", [])) or set(contract.feature_order),
            optional_features=set(getattr(contract, "optional_features", [])),
        )


@dataclass
class ModelInfo:
    """Model metadata with schema evolution support."""
    model_name: str
    model_uri: str
    stage: str
    version: Optional[str] = None
    run_id: Optional[str] = None
    contract_version: Optional[str] = None

    # SCHEMA EVOLUTION: Additional metadata
    schema_version: Optional[str] = None
    min_compatible_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "model_uri": self.model_uri,
            "stage": self.stage,
            "version": self.version or "unknown",
            "run_id": self.run_id,
            "contract_version": self.contract_version,
            "schema_version": self.schema_version,
            "min_compatible_version": self.min_compatible_version,
        }


@dataclass
class LoadedModel:
    """Container for loaded model and related artifacts with schema evolution."""
    model: Any
    info: ModelInfo
    contract: Optional[Any] = None
    transformer: Optional[Any] = None
    compatibility: Optional[CompatibilityMatrix] = None

    @property
    def has_contract(self) -> bool:
        return self.contract is not None

    @property
    def schema_version(self) -> str:
        """Get the schema version from contract."""
        if self.contract and hasattr(self.contract, "schema_version"):
            return self.contract.schema_version
        return "1.0.0"

    def check_compatibility(self, features: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Check if features are compatible with this model's requirements.

        Returns:
            Tuple of (compatible, warnings)
        """
        warnings = []

        if not self.compatibility:
            return True, warnings

        # Check required features
        available = set(features.keys())
        all_present, missing = self.compatibility.check_features(available)

        if not all_present:
            warnings.append(f"Missing required features: {missing}")

        return all_present, warnings


@dataclass
class ModelLoaderConfig:
    """Model loader configuration."""
    tracking_uri: str = field(
        default_factory=lambda: os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")
    )
    model_name: str = field(
        default_factory=lambda: os.getenv("MLFLOW_MODEL_NAME", "fraud_detection_model")
    )
    default_stage: str = "Production"


class ModelLoader:
    """
    Loads and caches ML models from MLflow registry.

    Supports loading by:
    - Model stage (Production, Staging, etc.)
    - Specific run_id

    Automatically loads feature contracts if available.
    """

    def __init__(self, config: Optional[ModelLoaderConfig] = None):
        self.config = config or ModelLoaderConfig()
        self._mlflow_client = None

        # OPTIMIZED: TTL cache for models (max 10 models, 1 hour TTL)
        # Prevents unbounded memory growth from stale cached models
        self._model_cache = TTLCache(maxsize=10, ttl=3600)

        # Feature transformer support
        self._feature_contract_class = None
        self._feature_transformer_class = None
        self._transformer_available = False
        self._load_transformer_classes()

    def _load_transformer_classes(self):
        """Load feature transformer classes if available from local paths."""
        # Try multiple paths for the transformer
        possible_paths = [
            "/app/pipelines/feature_transformer.py",
            "/app/src/mlops/pipelines/feature_transformer.py",
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "mlops", "pipelines", "feature_transformer.py"),
        ]

        for path in possible_paths:
            try:
                if not os.path.exists(path):
                    continue

                import importlib.util
                spec = importlib.util.spec_from_file_location(
                    "feature_transformer",
                    path
                )
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self._feature_contract_class = module.FeatureContract
                    self._feature_transformer_class = module.FeatureTransformer
                    self._transformer_available = True
                    logger.info(f"Feature transformer loaded from {path}")
                    return
            except Exception as e:
                logger.debug(f"Failed to load transformer from {path}: {e}")
                continue

        # If local loading failed, we'll try to load from MLflow artifacts per-model
        logger.info("Local transformer not found, will load from MLflow artifacts per-model")
        self._transformer_available = False

    def _load_transformer_from_artifact(self, run_id: str) -> Tuple[Optional[Any], Optional[Any]]:
        """
        Download and load feature transformer from MLflow artifacts.

        Returns:
            Tuple of (FeatureContract class, FeatureTransformer class) or (None, None)
        """
        import mlflow
        import tempfile

        try:
            # Download transformer artifact
            artifact_path = mlflow.artifacts.download_artifacts(
                run_id=run_id,
                artifact_path="fraud_detection_transformer.py",
            )

            # Load the module dynamically
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                f"feature_transformer_{run_id[:8]}",
                artifact_path
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                logger.info(f"Loaded transformer from MLflow artifact for run {run_id[:8]}")
                return module.FeatureContract, module.FeatureTransformer

        except Exception as e:
            logger.debug(f"Failed to load transformer artifact for run {run_id}: {e}")

        return None, None

    @property
    def mlflow_client(self):
        """Lazy initialize MLflow client."""
        if self._mlflow_client is None:
            import mlflow
            mlflow.set_tracking_uri(self.config.tracking_uri)
            self._mlflow_client = mlflow.tracking.MlflowClient()
            logger.info(f"MLflow client connected: {self.config.tracking_uri}")
        return self._mlflow_client

    def _build_cache_key(
        self,
        model_stage: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """Build cache key for model."""
        if run_id:
            return f"run_{run_id}"
        return f"stage_{model_stage or self.config.default_stage}"

    def load(
        self,
        model_stage: Optional[str] = None,
        run_id: Optional[str] = None,
        use_cache: bool = True,
    ) -> LoadedModel:
        """
        Load a model from MLflow.

        Args:
            model_stage: Model stage (Production, Staging, etc.)
            run_id: Specific MLflow run ID
            use_cache: Whether to use cached model

        Returns:
            LoadedModel with model and metadata
        """
        import mlflow

        cache_key = self._build_cache_key(model_stage, run_id)

        # Check cache
        if use_cache and cache_key in self._model_cache:
            return self._model_cache[cache_key]

        # Build model URI
        if run_id:
            model_uri = f"runs:/{run_id}/model"
            stage = "experiment"
        else:
            stage = model_stage or self.config.default_stage
            model_uri = f"models:/{self.config.model_name}/{stage}"

        logger.info(f"Loading model: {model_uri}")

        try:
            # Load model
            model = mlflow.sklearn.load_model(model_uri)

            # Build model info
            info = ModelInfo(
                model_name=self.config.model_name,
                model_uri=model_uri,
                stage=stage,
            )

            # Get version and run_id
            if run_id:
                info.run_id = run_id
            else:
                try:
                    versions = self.mlflow_client.get_latest_versions(
                        self.config.model_name,
                        stages=[stage]
                    )
                    if versions:
                        info.run_id = versions[0].run_id
                        info.version = versions[0].version
                except Exception:
                    pass

            # Load feature contract if available
            contract = None
            transformer = None
            compatibility = None
            actual_run_id = info.run_id or run_id

            if actual_run_id:
                # Get transformer classes - either from local or from MLflow artifact
                contract_class = self._feature_contract_class
                transformer_class = self._feature_transformer_class

                # If local transformer not available, try loading from MLflow artifact
                if not self._transformer_available:
                    contract_class, transformer_class = self._load_transformer_from_artifact(actual_run_id)

                if contract_class and transformer_class:
                    try:
                        contract_path = mlflow.artifacts.download_artifacts(
                            run_id=actual_run_id,
                            artifact_path="feature_contract.json",
                        )
                        contract = contract_class.load(contract_path)
                        transformer = transformer_class(contract)
                        info.contract_version = contract.model_version

                        # SCHEMA EVOLUTION: Extract schema version info
                        info.schema_version = getattr(contract, "schema_version", "1.0.0")
                        info.min_compatible_version = getattr(contract, "min_compatible_version", "1.0.0")

                        # Build compatibility matrix from contract
                        compatibility = CompatibilityMatrix.from_contract(contract)

                        logger.info(
                            f"Loaded contract for {cache_key}: "
                            f"schema_version={info.schema_version}, "
                            f"features={len(contract.feature_order)}"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to load contract for {cache_key}: {e}")
                else:
                    # Fallback: Load contract JSON directly using SimpleFeatureContract
                    # This allows inference even when the transformer module can't be loaded
                    logger.info(f"Using SimpleFeatureContract fallback for {cache_key}")
                    try:
                        contract_path = mlflow.artifacts.download_artifacts(
                            run_id=actual_run_id,
                            artifact_path="feature_contract.json",
                        )
                        contract = SimpleFeatureContract.load(contract_path)
                        # No transformer available, but we have contract with category_mappings
                        transformer = None
                        info.contract_version = contract.model_version
                        info.schema_version = contract.schema_version
                        info.min_compatible_version = contract.min_compatible_version

                        # Build compatibility matrix
                        compatibility = CompatibilityMatrix(
                            model_version=contract.model_version,
                            min_contract_version=contract.min_compatible_version,
                            required_features=set(contract.required_features) or set(contract.feature_order),
                            optional_features=set(contract.optional_features),
                        )

                        logger.info(
                            f"Loaded SimpleFeatureContract for {cache_key}: "
                            f"features={len(contract.feature_order)}, "
                            f"category_mappings={len(contract.category_mappings)}"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to load SimpleFeatureContract for {cache_key}: {e}")

            loaded = LoadedModel(
                model=model,
                info=info,
                contract=contract,
                transformer=transformer,
                compatibility=compatibility,
            )

            # Cache
            self._model_cache[cache_key] = loaded
            logger.info(f"Loaded model: {cache_key}")

            return loaded

        except Exception as e:
            logger.error(f"Failed to load {model_uri}: {e}")
            raise

    def list_models(self) -> list:
        """List available models in registry."""
        models = []
        for stage in ["Production", "Staging", "Archived", "None"]:
            try:
                versions = self.mlflow_client.get_latest_versions(
                    self.config.model_name,
                    stages=[stage]
                )
                for v in versions:
                    cache_key = f"stage_{stage}"
                    cached = self._model_cache.get(cache_key)
                    models.append({
                        "model_name": self.config.model_name,
                        "version": v.version,
                        "stage": stage,
                        "run_id": v.run_id,
                        "status": v.status,
                        "has_contract": cached.has_contract if cached else False,
                        "cached": cached is not None,
                    })
            except Exception:
                pass
        return models

    def clear_cache(self, cache_key: Optional[str] = None):
        """Clear model cache."""
        if cache_key:
            self._model_cache.pop(cache_key, None)
        else:
            self._model_cache.clear()

    @property
    def cached_count(self) -> int:
        """Number of cached models."""
        return len(self._model_cache)

    # =========================================================================
    # SCHEMA EVOLUTION: Compatibility Methods
    # =========================================================================

    def check_compatibility(
        self,
        loaded_model: LoadedModel,
        contract_version: Optional[str] = None,
        features: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Check if a model is compatible with given contract version and features.

        Args:
            loaded_model: The model to check
            contract_version: Contract version to check against (optional)
            features: Feature dict to validate (optional)

        Returns:
            Tuple of (compatible, warnings)

        Raises:
            IncompatibleContractError: If contract version is incompatible
            MissingFeaturesError: If required features are missing
        """
        warnings = []

        if not loaded_model.compatibility:
            return True, ["No compatibility matrix available"]

        # Check contract version compatibility
        if contract_version:
            if not loaded_model.compatibility.is_contract_compatible(contract_version):
                raise IncompatibleContractError(
                    model_version=loaded_model.info.version or "unknown",
                    contract_version=contract_version,
                    min_version=loaded_model.compatibility.min_contract_version,
                )

        # Check feature compatibility
        if features:
            compatible, missing = loaded_model.compatibility.check_features(set(features.keys()))
            if not compatible:
                warnings.append(f"Missing features: {missing}")
                # Optionally raise error in strict mode
                # raise MissingFeaturesError(missing)

        return True, warnings

    def validate_model_contract(
        self,
        model_stage: str = "Production",
        expected_features: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate a model's contract for deployment readiness.

        Returns dict with validation results.
        """
        result = {
            "valid": True,
            "model_stage": model_stage,
            "errors": [],
            "warnings": [],
            "schema_info": {},
        }

        try:
            loaded = self.load(model_stage=model_stage)

            if not loaded.has_contract:
                result["warnings"].append("No feature contract found")

            if loaded.contract:
                result["schema_info"] = {
                    "schema_version": getattr(loaded.contract, "schema_version", "unknown"),
                    "min_compatible_version": getattr(loaded.contract, "min_compatible_version", "unknown"),
                    "feature_count": len(loaded.contract.feature_order),
                    "categorical_columns": list(loaded.contract.category_mappings.keys()),
                    "has_unknown_category": all(
                        "__unknown__" in m for m in loaded.contract.category_mappings.values()
                    ),
                }

                # Check for deprecated features
                deprecated = getattr(loaded.contract, "deprecated_features", {})
                if deprecated:
                    result["warnings"].append(f"Deprecated features: {list(deprecated.keys())}")

            # Validate expected features if provided
            if expected_features and loaded.compatibility:
                _, missing = loaded.compatibility.check_features(set(expected_features))
                if missing:
                    result["errors"].append(f"Missing expected features: {missing}")
                    result["valid"] = False

        except Exception as e:
            result["valid"] = False
            result["errors"].append(str(e))

        return result
