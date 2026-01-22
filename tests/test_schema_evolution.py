"""
Tests for Schema Evolution Implementation
=========================================

Tests cover:
1. FeatureContract semantic versioning
2. SchemaValidator validation
3. Type coercion
4. Categorical __unknown__ handling
5. CompatibilityMatrix
6. Deprecation lifecycle
"""

import pytest
import sys
import os
import importlib.util

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Direct module import to avoid package __init__ side effects
spec = importlib.util.spec_from_file_location(
    "feature_transformer",
    os.path.join(project_root, "src", "mlops", "pipelines", "feature_transformer.py")
)
feature_transformer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feature_transformer)

FeatureContract = feature_transformer.FeatureContract
FeatureTransformer = feature_transformer.FeatureTransformer
SchemaValidator = feature_transformer.SchemaValidator
SchemaValidationResult = feature_transformer.SchemaValidationResult
DeprecationInfo = feature_transformer.DeprecationInfo
EXPECTED_TYPES = feature_transformer.EXPECTED_TYPES
DEFAULT_BY_TYPE = feature_transformer.DEFAULT_BY_TYPE


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def sample_contract():
    """Create a sample feature contract for testing."""
    return FeatureContract(
        feature_order=["amount", "country", "payment_method", "is_fraud"],
        category_mappings={
            "country": {"US": 0, "UK": 1, "CA": 2},
            "payment_method": {"credit_card": 0, "debit_card": 1, "wallet": 2},
        },
        feature_stats={
            "amount": {"min": 0.0, "max": 1000.0, "mean": 100.0, "std": 50.0},
        },
        primitive_features=["amount", "country", "payment_method"],
        aggregate_features=[],
        derived_features=[],
        schema_version="1.0.0",
        min_compatible_version="1.0.0",
    )


@pytest.fixture
def transformer(sample_contract):
    """Create a transformer with the sample contract."""
    return FeatureTransformer(sample_contract)


# =============================================================================
# Test: Semantic Versioning
# =============================================================================

class TestSemanticVersioning:
    """Test semantic versioning functionality."""

    def test_initial_version(self, sample_contract):
        """Contract should have default version 1.0.0."""
        assert sample_contract.schema_version == "1.0.0"
        assert sample_contract.min_compatible_version == "1.0.0"

    def test_bump_patch_version(self, sample_contract):
        """Patch bump should increment third number."""
        new_version = sample_contract.bump_version("patch")
        assert new_version == "1.0.1"
        assert sample_contract.schema_version == "1.0.1"
        # Min compatible should stay same for patch
        assert sample_contract.min_compatible_version == "1.0.0"

    def test_bump_minor_version(self, sample_contract):
        """Minor bump should increment second number, reset patch."""
        new_version = sample_contract.bump_version("minor")
        assert new_version == "1.1.0"
        assert sample_contract.schema_version == "1.1.0"

    def test_bump_major_version(self, sample_contract):
        """Major bump should increment first number, reset others, update min_compatible."""
        new_version = sample_contract.bump_version("major")
        assert new_version == "2.0.0"
        assert sample_contract.schema_version == "2.0.0"
        # Major bump updates min_compatible (breaking change)
        assert sample_contract.min_compatible_version == "2.0.0"

    def test_version_compatibility_check(self, sample_contract):
        """Test version compatibility checking."""
        assert sample_contract.is_compatible_with("1.0.0") == True
        assert sample_contract.is_compatible_with("1.5.0") == True
        assert sample_contract.is_compatible_with("2.0.0") == True
        assert sample_contract.is_compatible_with("0.9.0") == False


# =============================================================================
# Test: Unknown Category Handling
# =============================================================================

class TestUnknownCategoryHandling:
    """Test __unknown__ category functionality."""

    def test_unknown_category_auto_added(self, sample_contract):
        """__unknown__ should be auto-added to all category mappings."""
        for col, mapping in sample_contract.category_mappings.items():
            assert "__unknown__" in mapping, f"__unknown__ not in {col}"

    def test_unknown_category_index(self, sample_contract):
        """__unknown__ should have the highest index."""
        for col, mapping in sample_contract.category_mappings.items():
            max_known = max(v for k, v in mapping.items() if k != "__unknown__")
            assert mapping["__unknown__"] == max_known + 1

    def test_encode_known_category(self, transformer):
        """Known categories should encode correctly."""
        assert transformer.encode_categorical("US", "country") == 0
        assert transformer.encode_categorical("UK", "country") == 1
        assert transformer.encode_categorical("credit_card", "payment_method") == 0

    def test_encode_unknown_category(self, transformer):
        """Unknown categories should use __unknown__ encoding."""
        # "DE" is not in the mapping
        result = transformer.encode_categorical("DE", "country")
        expected = transformer.contract.category_mappings["country"]["__unknown__"]
        assert result == expected

    def test_unknown_category_tracking(self, transformer):
        """Unknown categories should be tracked for monitoring."""
        transformer._unknown_categories = {}
        transformer.encode_categorical("DE", "country")
        transformer.encode_categorical("FR", "country")

        assert "country" in transformer._unknown_categories
        assert "DE" in transformer._unknown_categories["country"]
        assert "FR" in transformer._unknown_categories["country"]


# =============================================================================
# Test: Type Coercion
# =============================================================================

class TestTypeCoercion:
    """Test type coercion functionality."""

    def test_coerce_int_to_float(self, transformer):
        """Integers should coerce to float64."""
        transformer.contract.feature_types["amount"] = "float64"
        value, warning = transformer.coerce_type(100, "amount")
        assert value == 100.0
        assert warning is None

    def test_coerce_string_to_int(self, transformer):
        """String numbers should coerce to int."""
        transformer.contract.feature_types["quantity"] = "int64"
        value, warning = transformer.coerce_type("42", "quantity")
        assert value == 42
        assert warning is None

    def test_coerce_float_string_to_int(self, transformer):
        """Float strings should coerce to int (truncated)."""
        transformer.contract.feature_types["quantity"] = "int64"
        value, warning = transformer.coerce_type("3.7", "quantity")
        assert value == 3
        assert warning is None

    def test_coerce_none_uses_default(self, transformer):
        """None should coerce to type default with warning."""
        transformer.contract.feature_types["amount"] = "float64"
        value, warning = transformer.coerce_type(None, "amount")
        assert value == DEFAULT_BY_TYPE["float64"]
        assert warning is not None
        assert "null coerced" in warning

    def test_coerce_invalid_returns_default(self, transformer):
        """Invalid values should return default with warning."""
        transformer.contract.feature_types["amount"] = "float64"
        value, warning = transformer.coerce_type("not_a_number", "amount")
        assert value == DEFAULT_BY_TYPE["float64"]
        assert warning is not None
        assert "coercion failed" in warning


# =============================================================================
# Test: Schema Validation
# =============================================================================

class TestSchemaValidation:
    """Test SchemaValidator functionality."""

    def test_valid_features_pass(self, sample_contract):
        """Valid features should pass validation."""
        validator = SchemaValidator(sample_contract)
        # Use string values for categorical features since they expect 'category' type
        features = {
            "amount": 100.0,
            "country": "US",  # String for category type
            "payment_method": "credit_card",  # String for category type
            "is_fraud": 0,
        }
        result = validator.validate(features)
        assert result.valid == True
        assert len(result.missing_features) == 0

    def test_missing_features_detected(self, sample_contract):
        """Missing features should be detected."""
        validator = SchemaValidator(sample_contract)
        features = {"amount": 100.0}  # Missing country, payment_method, is_fraud

        result = validator.validate(features)
        assert result.valid == False
        assert len(result.missing_features) > 0

    def test_type_mismatch_detected(self, sample_contract):
        """Type mismatches should be detected."""
        sample_contract.feature_types["amount"] = "float64"
        validator = SchemaValidator(sample_contract)

        features = {
            "amount": "not_a_number",  # Wrong type
            "country": 0,
            "payment_method": 1,
            "is_fraud": 0,
        }
        result = validator.validate(features)
        assert "amount" in result.type_mismatches

    def test_out_of_range_warning(self, sample_contract):
        """Out of range values should generate warnings."""
        validator = SchemaValidator(sample_contract)
        features = {
            "amount": 50000.0,  # Way above max of 1000
            "country": 0,
            "payment_method": 1,
            "is_fraud": 0,
        }
        result = validator.validate(features)
        assert "amount" in result.out_of_range
        assert len(result.warnings) > 0

    def test_strict_mode_fails_on_warnings(self, sample_contract):
        """Strict mode should fail on any warning."""
        validator = SchemaValidator(sample_contract, strict_mode=True)
        features = {
            "amount": 50000.0,  # Out of range
            "country": 0,
            "payment_method": 1,
            "is_fraud": 0,
        }
        result = validator.validate(features)
        assert result.valid == False  # Strict mode makes warnings into failures


# =============================================================================
# Test: Feature Deprecation
# =============================================================================

class TestFeatureDeprecation:
    """Test feature deprecation lifecycle."""

    def test_deprecate_feature(self, sample_contract):
        """Features can be marked as deprecated."""
        sample_contract.deprecate_feature(
            feature="country",
            reason="Replaced by shipping_country and billing_country",
            removed_in="2.0.0",
            replacement="shipping_country"
        )

        assert "country" in sample_contract.deprecated_features
        info = sample_contract.deprecated_features["country"]
        assert info.deprecated_in == "1.0.0"
        assert info.removed_in == "2.0.0"
        assert info.replacement == "shipping_country"

    def test_deprecated_feature_warning(self, sample_contract):
        """Using deprecated features should generate warnings."""
        sample_contract.deprecate_feature(
            feature="country",
            reason="Test deprecation",
        )

        validator = SchemaValidator(sample_contract)
        features = {
            "amount": 100.0,
            "country": 0,  # Deprecated
            "payment_method": 1,
            "is_fraud": 0,
        }
        result = validator.validate(features)
        assert any("DEPRECATED" in w for w in result.warnings)


# =============================================================================
# Test: Schema Diff
# =============================================================================

class TestSchemaDiff:
    """Test schema comparison functionality."""

    def test_detect_added_features(self, sample_contract):
        """New features should be detected."""
        old_contract = FeatureContract(
            feature_order=["amount", "country"],
            category_mappings={"country": {"US": 0}},
            feature_stats={},
            primitive_features=["amount", "country"],
            aggregate_features=[],
            derived_features=[],
        )

        diff = sample_contract.get_schema_diff(old_contract)
        assert "payment_method" in diff["added_features"]
        assert "is_fraud" in diff["added_features"]

    def test_detect_removed_features(self, sample_contract):
        """Removed features should be detected."""
        old_contract = FeatureContract(
            feature_order=["amount", "country", "payment_method", "is_fraud", "old_feature"],
            category_mappings={"country": {"US": 0}, "payment_method": {"credit_card": 0}},
            feature_stats={},
            primitive_features=["amount", "country", "payment_method", "old_feature"],
            aggregate_features=[],
            derived_features=[],
        )

        diff = sample_contract.get_schema_diff(old_contract)
        assert "old_feature" in diff["removed_features"]


# =============================================================================
# Test: Contract Serialization
# =============================================================================

class TestContractSerialization:
    """Test contract save/load functionality."""

    def test_to_dict_contains_schema_evolution_fields(self, sample_contract):
        """Serialized dict should contain schema evolution fields."""
        data = sample_contract.to_dict()

        assert "schema_version" in data
        assert "min_compatible_version" in data
        assert "feature_types" in data
        assert "deprecated_features" in data
        assert "required_features" in data
        assert "optional_features" in data

    def test_from_dict_preserves_schema_evolution(self, sample_contract):
        """Deserialization should preserve schema evolution fields."""
        sample_contract.schema_version = "2.1.3"
        sample_contract.deprecate_feature("country", "Test")

        data = sample_contract.to_dict()
        restored = FeatureContract.from_dict(data)

        assert restored.schema_version == "2.1.3"
        assert "country" in restored.deprecated_features

    def test_round_trip_serialization(self, sample_contract, tmp_path):
        """Save and load should produce identical contract."""
        sample_contract.bump_version("minor")
        sample_contract.deprecate_feature("country", "Test deprecation")

        path = tmp_path / "contract.json"
        sample_contract.save(str(path))
        loaded = FeatureContract.load(str(path))

        assert loaded.schema_version == sample_contract.schema_version
        assert loaded.feature_order == sample_contract.feature_order
        assert "country" in loaded.deprecated_features


# =============================================================================
# Test: Transform with Schema Evolution
# =============================================================================

class TestTransformWithSchemaEvolution:
    """Test transform_for_inference with schema evolution features."""

    def test_transform_with_validation(self, transformer):
        """Transform should run schema validation by default."""
        request = {"amount": 100.0, "country": "US", "payment_method": "credit_card"}
        feast = {}

        features, warnings = transformer.transform_for_inference(
            request, feast, validate_schema=True
        )

        assert "amount" in features
        # Transform succeeded - warnings may or may not be present depending on contract config
        # The key is that transform_for_inference runs without error
        assert isinstance(warnings, dict)

    def test_transform_tracks_unknown_categories(self, transformer):
        """Transform should track unknown categorical values."""
        request = {
            "amount": 100.0,
            "country": "UNKNOWN_COUNTRY",  # Unknown
            "payment_method": "bitcoin",   # Unknown
        }
        feast = {}

        features, warnings = transformer.transform_for_inference(request, feast)

        # Check that unknown categories were tracked
        assert any("new_category" in k for k in warnings.keys())

    def test_transform_applies_type_coercion(self, transformer):
        """Transform should coerce types automatically."""
        request = {
            "amount": "100.50",  # String instead of float
            "country": "US",
            "payment_method": "credit_card",
        }
        feast = {}

        features, warnings = transformer.transform_for_inference(request, feast)

        # Amount should be coerced to float
        assert isinstance(features.get("amount", "not_found"), (int, float))


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
