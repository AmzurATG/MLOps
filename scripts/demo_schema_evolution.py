#!/usr/bin/env python3
"""
Schema Evolution Demo
=====================

This script demonstrates real schema evolution capabilities across:
1. Feature Contract versioning and deprecation
2. Streaming consumer typed field handling
3. Categorical evolution with __unknown__ handling
4. Type coercion and validation
5. API integration with schema checking

Run: python scripts/demo_schema_evolution.py
"""

import sys
import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, Any

# Add project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Direct imports to avoid package init issues
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
feature_transformer = load_module(
    "feature_transformer",
    os.path.join(project_root, "pipelines", "feature_transformer.py")
)

FeatureContract = feature_transformer.FeatureContract
FeatureTransformer = feature_transformer.FeatureTransformer
SchemaValidator = feature_transformer.SchemaValidator
DeprecationInfo = feature_transformer.DeprecationInfo


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_section(title: str):
    print(f"\n--- {title} ---")


# =============================================================================
# DEMO 1: Feature Contract Versioning
# =============================================================================

def demo_contract_versioning():
    print_header("DEMO 1: Feature Contract Semantic Versioning")

    # Create a base contract
    contract = FeatureContract(
        feature_order=["amount", "country", "payment_method", "tx_hour"],
        category_mappings={
            "country": {"US": 0, "UK": 1, "CA": 2},
            "payment_method": {"credit_card": 0, "debit_card": 1},
        },
        feature_stats={
            "amount": {"min": 0.0, "max": 1000.0, "mean": 100.0, "std": 50.0},
        },
        primitive_features=["amount", "country", "payment_method"],
        aggregate_features=["tx_hour"],
        derived_features=[],
        schema_version="1.0.0",
        min_compatible_version="1.0.0",
    )

    print_section("Initial Contract")
    print(f"Schema Version: {contract.schema_version}")
    print(f"Min Compatible: {contract.min_compatible_version}")
    print(f"Features: {contract.feature_order}")

    # PATCH bump: Bug fix
    print_section("PATCH Bump (Bug Fix)")
    new_version = contract.bump_version("patch")
    print(f"Version after bug fix: {new_version}")
    print(f"Min Compatible (unchanged): {contract.min_compatible_version}")

    # MINOR bump: Add new feature
    print_section("MINOR Bump (New Feature Added)")
    contract.feature_order.append("device_fingerprint")
    contract.optional_features.add("device_fingerprint")
    new_version = contract.bump_version("minor")
    print(f"Version after new feature: {new_version}")
    print(f"Features: {contract.feature_order}")
    print(f"Optional features: {contract.optional_features}")

    # MAJOR bump: Breaking change
    print_section("MAJOR Bump (Breaking Change)")
    old_features = contract.feature_order.copy()
    contract.feature_order.remove("country")  # Remove feature
    new_version = contract.bump_version("major")
    print(f"Version after breaking change: {new_version}")
    print(f"Min Compatible (updated): {contract.min_compatible_version}")
    print(f"Removed feature: country")

    # Version compatibility check
    print_section("Compatibility Checks")
    test_versions = ["1.0.0", "1.5.0", "2.0.0", "2.5.0", "0.9.0"]
    for v in test_versions:
        compatible = contract.is_compatible_with(v)
        status = "COMPATIBLE" if compatible else "INCOMPATIBLE"
        print(f"  {v}: {status}")

    return contract


# =============================================================================
# DEMO 2: Unknown Category Handling
# =============================================================================

def demo_unknown_categories():
    print_header("DEMO 2: Unknown Category Handling (__unknown__)")

    contract = FeatureContract(
        feature_order=["amount", "country", "payment_method"],
        category_mappings={
            "country": {"US": 0, "UK": 1, "CA": 2},
            "payment_method": {"credit_card": 0, "debit_card": 1},
        },
        feature_stats={},
        primitive_features=["amount", "country", "payment_method"],
        aggregate_features=[],
        derived_features=[],
    )

    transformer = FeatureTransformer(contract)

    print_section("Category Mappings (with auto-added __unknown__)")
    for col, mapping in contract.category_mappings.items():
        print(f"  {col}: {mapping}")

    print_section("Encoding Known Categories")
    known_tests = [
        ("US", "country"),
        ("UK", "country"),
        ("credit_card", "payment_method"),
    ]
    for value, col in known_tests:
        encoded = transformer.encode_categorical(value, col)
        print(f"  {col}='{value}' -> {encoded}")

    print_section("Encoding Unknown Categories (Uses __unknown__)")
    unknown_tests = [
        ("DE", "country"),        # Germany - not in training
        ("FR", "country"),        # France - not in training
        ("bitcoin", "payment_method"),  # Crypto - not in training
        ("apple_pay", "payment_method"),
    ]
    for value, col in unknown_tests:
        encoded = transformer.encode_categorical(value, col)
        unknown_idx = contract.category_mappings[col]["__unknown__"]
        print(f"  {col}='{value}' -> {encoded} (mapped to __unknown__={unknown_idx})")

    print_section("Tracked Unknown Categories (for monitoring)")
    if hasattr(transformer, "_unknown_categories"):
        for col, values in transformer._unknown_categories.items():
            print(f"  {col}: {list(values)}")


# =============================================================================
# DEMO 3: Type Coercion
# =============================================================================

def demo_type_coercion():
    print_header("DEMO 3: Type Coercion & Validation")

    contract = FeatureContract(
        feature_order=["amount", "quantity", "is_fraud"],
        category_mappings={},
        feature_stats={},
        primitive_features=["amount", "quantity", "is_fraud"],
        aggregate_features=[],
        derived_features=[],
        feature_types={
            "amount": "float64",
            "quantity": "int64",
            "is_fraud": "bool",
        },
    )

    transformer = FeatureTransformer(contract)

    print_section("Feature Type Definitions")
    for feat, ftype in contract.feature_types.items():
        print(f"  {feat}: {ftype}")

    print_section("Type Coercion Examples")
    coercion_tests = [
        # (value, feature, description)
        (100, "amount", "int -> float64"),
        ("99.99", "amount", "string -> float64"),
        ("42", "quantity", "string -> int64"),
        ("3.7", "quantity", "float string -> int64 (truncated)"),
        (None, "amount", "None -> default (0.0)"),
        ("not_a_number", "amount", "invalid -> default with warning"),
        ("true", "is_fraud", "string -> bool"),
        (1, "is_fraud", "int -> bool"),
    ]

    for value, feat, desc in coercion_tests:
        coerced, warning = transformer.coerce_type(value, feat)
        warning_str = f" [WARNING: {warning}]" if warning else ""
        print(f"  {desc}: {repr(value)} -> {repr(coerced)}{warning_str}")


# =============================================================================
# DEMO 4: Feature Deprecation
# =============================================================================

def demo_deprecation():
    print_header("DEMO 4: Feature Deprecation Lifecycle")

    contract = FeatureContract(
        feature_order=["amount", "country", "shipping_country", "billing_country"],
        category_mappings={
            "country": {"US": 0, "UK": 1},
            "shipping_country": {"US": 0, "UK": 1},
            "billing_country": {"US": 0, "UK": 1},
        },
        feature_stats={},
        primitive_features=["amount", "country", "shipping_country", "billing_country"],
        aggregate_features=[],
        derived_features=[],
        schema_version="1.0.0",
    )

    print_section("Phase 1: Soft Deprecation")
    contract.deprecate_feature(
        feature="country",
        reason="Replaced by shipping_country and billing_country for better accuracy",
        removed_in="2.0.0",
        replacement="shipping_country"
    )
    contract.bump_version("minor")  # 1.1.0

    print(f"Schema Version: {contract.schema_version}")
    print(f"Deprecated Features:")
    for feat, info in contract.deprecated_features.items():
        print(f"  - {feat}:")
        print(f"      Deprecated in: {info.deprecated_in}")
        print(f"      Removed in: {info.removed_in}")
        print(f"      Replacement: {info.replacement}")
        print(f"      Reason: {info.reason}")

    print_section("Validation with Deprecated Feature")
    validator = SchemaValidator(contract)
    features = {
        "amount": 100.0,
        "country": "US",  # Deprecated!
        "shipping_country": "US",
        "billing_country": "US",
    }
    result = validator.validate(features)
    print(f"Valid: {result.valid}")
    print(f"Warnings:")
    for w in result.warnings:
        print(f"  - {w}")

    print_section("Phase 2: Hard Removal (v2.0.0)")
    contract.feature_order.remove("country")
    del contract.category_mappings["country"]
    contract.bump_version("major")  # 2.0.0
    print(f"Schema Version: {contract.schema_version}")
    print(f"Features: {contract.feature_order}")
    print("Feature 'country' has been removed from the contract")


# =============================================================================
# DEMO 5: Schema Diff
# =============================================================================

def demo_schema_diff():
    print_header("DEMO 5: Schema Diff Between Versions")

    # Old contract (v1.0.0)
    old_contract = FeatureContract(
        feature_order=["amount", "country", "payment_method"],
        category_mappings={
            "country": {"US": 0, "UK": 1},
            "payment_method": {"credit_card": 0, "debit_card": 1},
        },
        feature_stats={"amount": {"min": 0, "max": 1000}},
        primitive_features=["amount", "country", "payment_method"],
        aggregate_features=[],
        derived_features=[],
        risk_thresholds={"high_amount_multiplier": 3.0},
        schema_version="1.0.0",
    )

    # New contract (v2.0.0)
    new_contract = FeatureContract(
        feature_order=["amount", "shipping_country", "payment_method", "device_score"],
        category_mappings={
            "shipping_country": {"US": 0, "UK": 1, "DE": 2},  # New country
            "payment_method": {"credit_card": 0, "debit_card": 1, "apple_pay": 2},
        },
        feature_stats={"amount": {"min": 0, "max": 5000}},  # Changed
        primitive_features=["amount", "shipping_country", "payment_method", "device_score"],
        aggregate_features=[],
        derived_features=[],
        risk_thresholds={"high_amount_multiplier": 2.5},  # Changed
        schema_version="2.0.0",
    )

    print_section("Comparing Contracts")
    print(f"Old Version: {old_contract.schema_version}")
    print(f"New Version: {new_contract.schema_version}")

    diff = new_contract.get_schema_diff(old_contract)

    print_section("Schema Diff")
    print(f"Added Features: {diff['added_features']}")
    print(f"Removed Features: {diff['removed_features']}")

    if diff['categorical_changes']:
        print("Categorical Changes:")
        for col, changes in diff['categorical_changes'].items():
            print(f"  {col}: +{changes['added']}, -{changes['removed']}")

    if diff['threshold_changes']:
        print("Threshold Changes:")
        for key, change in diff['threshold_changes'].items():
            print(f"  {key}: {change['from']} -> {change['to']}")


# =============================================================================
# DEMO 6: Streaming Schema Validation
# =============================================================================

def demo_streaming_schema():
    print_header("DEMO 6: Streaming Consumer Schema Handling")

    try:
        streaming_module = load_module(
            "fraud_streaming_consumer",
            os.path.join(project_root, "src", "mlops", "streaming", "consumer.py")
        )
        FieldDefinition = streaming_module.FieldDefinition
        TYPED_FEATURE_MAPPING = streaming_module.TYPED_FEATURE_MAPPING
        STREAMING_SCHEMA_VERSION = streaming_module.STREAMING_SCHEMA_VERSION
        FraudStreamingConsumer = streaming_module.FraudStreamingConsumer
        Config = streaming_module.Config

        print_section("Streaming Schema Version")
        print(f"  Version: {STREAMING_SCHEMA_VERSION}")
        print(f"  Total typed fields: {len(TYPED_FEATURE_MAPPING) // 2}")  # Div 2 for lowercase variants

        print_section("Typed Field Mapping (Sample)")
        sample_fields = ["AMOUNT", "ADDRESS_MISMATCH", "IS_NIGHT", "CUSTOMER_ID"]
        for field_name in sample_fields:
            if field_name in TYPED_FEATURE_MAPPING:
                fd = TYPED_FEATURE_MAPPING[field_name]
                print(f"  {fd.ksqldb_name}:")
                print(f"    Redis name: {fd.redis_name}")
                print(f"    Type: {fd.field_type}")
                print(f"    Required: {fd.required}")
                print(f"    Default: {fd.default}")

        print_section("Type Coercion Examples")
        if hasattr(FieldDefinition, 'coerce'):
            fd = FieldDefinition("AMOUNT", "amount", "float", True, 0.0)
            test_values = [100, "99.99", None, "invalid"]
            for val in test_values:
                coerced, warning = fd.coerce(val)
                warning_str = f" [{warning}]" if warning else ""
                print(f"  AMOUNT: {repr(val)} -> {repr(coerced)}{warning_str}")

        print_section("Full Message Validation (validate_and_map_features)")
        # Create consumer instance without connecting
        config = Config()
        consumer = FraudStreamingConsumer(config)

        # Test message with known fields
        known_message = {
            "CUSTOMER_ID": "CUST123",
            "TRANSACTION_ID": "TX456",
            "AMOUNT": "250.50",
            "IS_NIGHT": 1,
            "ADDRESS_MISMATCH": 0,
            "SHIPPING_COUNTRY": "US",
            "PAYMENT_METHOD": "credit_card",
        }
        features, warnings = consumer.validate_and_map_features(known_message)
        print(f"  Known fields message:")
        print(f"    Input fields: {len(known_message)}")
        print(f"    Mapped fields: {len(features)}")
        print(f"    Warnings: {len(warnings)}")
        print(f"    Amount (coerced): {features.get('amount')}")

        # Test message with unknown fields (schema drift)
        drift_message = {
            "CUSTOMER_ID": "CUST789",
            "AMOUNT": 500.0,
            "NEW_FIELD_2025": "unknown_value",  # Schema drift!
            "FUTURE_SCORE": 0.99,  # Another unknown!
        }
        features2, warnings2 = consumer.validate_and_map_features(drift_message)
        print(f"\n  Message with unknown fields (drift):")
        print(f"    Input fields: {len(drift_message)}")
        print(f"    Mapped fields: {len(features2)}")
        print(f"    Schema warnings: {len(warnings2)}")

        # Check drift stats
        stats = consumer.get_schema_stats()
        print(f"\n  Schema drift stats:")
        print(f"    Unknown fields detected: {stats['unknown_fields_seen']}")
        print(f"    Total warnings: {stats['total_warnings']}")

    except Exception as e:
        import traceback
        print(f"Streaming module not available: {e}")
        traceback.print_exc()


# =============================================================================
# DEMO 7: Real API Integration
# =============================================================================

def demo_api_integration():
    print_header("DEMO 7: API Integration with Schema Evolution")

    api_url = os.getenv("FRAUD_API_URL", "http://localhost:8000")

    print_section("Testing API Health")
    try:
        resp = requests.get(f"{api_url}/health", timeout=5)
        print(f"API Status: {resp.status_code}")
        if resp.status_code == 200:
            health = resp.json()
            print(f"Health: {json.dumps(health, indent=2)[:200]}...")
    except Exception as e:
        print(f"API not available: {e}")
        print("Skipping live API tests")
        return

    print_section("Testing Prediction with Known Categories")
    payload = {
        "transaction_id": f"demo_{int(time.time())}",
        "customer_id": "CUST001",
        "amount": 150.0,
        "country": "US",
        "payment_method": "credit_card",
        "category": "Electronics",
        "shipping_address": "123 Main St",
        "billing_address": "123 Main St",
        "ip_address": "192.168.1.1",
    }

    try:
        resp = requests.post(f"{api_url}/predict", json=payload, timeout=10)
        print(f"Response: {resp.status_code}")
        if resp.status_code == 200:
            result = resp.json()
            print(f"Prediction: {result.get('is_fraud', 'N/A')}")
            print(f"Probability: {result.get('fraud_probability', 'N/A'):.4f}")
    except Exception as e:
        print(f"Prediction error: {e}")

    print_section("Testing with Unknown Categories (Schema Evolution)")
    payload_unknown = {
        "transaction_id": f"demo_unknown_{int(time.time())}",
        "customer_id": "CUST002",
        "amount": 500.0,
        "country": "ZZ",  # Unknown country code
        "payment_method": "cryptocurrency",  # Unknown payment method
        "category": "NFT",  # Unknown category
        "shipping_address": "456 Test Ave",
        "billing_address": "456 Test Ave",
        "ip_address": "10.0.0.1",
    }

    try:
        resp = requests.post(f"{api_url}/predict", json=payload_unknown, timeout=10)
        print(f"Response: {resp.status_code}")
        if resp.status_code == 200:
            result = resp.json()
            print(f"Prediction: {result.get('is_fraud', 'N/A')}")
            print(f"Probability: {result.get('fraud_probability', 'N/A'):.4f}")
            print("Unknown categories handled gracefully via __unknown__ mapping!")
        else:
            print(f"Error: {resp.text[:200]}")
    except Exception as e:
        print(f"Prediction error: {e}")


# =============================================================================
# DEMO 8: Iceberg Schema Evolution
# =============================================================================

def demo_iceberg_evolution():
    print_header("DEMO 8: Iceberg Schema Evolution (Trino)")

    print_section("SQL Script Location")
    sql_path = os.path.join(project_root, "scripts", "enable_iceberg_evolution.sql")
    print(f"Script: {sql_path}")

    if os.path.exists(sql_path):
        print("\nScript contents (summary):")
        with open(sql_path, 'r') as f:
            content = f.read()

        # Count tables being modified
        tables = content.count("ALTER TABLE iceberg")
        creates = content.count("CREATE TABLE")
        print(f"  - ALTER TABLE statements: {tables}")
        print(f"  - CREATE TABLE statements: {creates}")

        print("\nTables with schema evolution enabled:")
        for line in content.split('\n'):
            if 'ALTER TABLE iceberg' in line:
                table = line.split('ALTER TABLE ')[1].split('\n')[0].strip()
                print(f"  - {table}")

    print_section("Running Iceberg Evolution SQL")
    print("To enable schema evolution on Iceberg tables, run:")
    print(f"  docker exec exp-trino trino < {sql_path}")

    # Try to run if Trino is available
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "exp-trino", "trino", "--execute",
             "SHOW CATALOGS"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("\nTrino is accessible. Catalogs available:")
            for line in result.stdout.strip().split('\n')[:5]:
                print(f"  - {line}")
    except Exception as e:
        print(f"\nTrino check skipped: {e}")


# =============================================================================
# DEMO 9: End-to-End Contract Round-Trip
# =============================================================================

def demo_contract_persistence():
    print_header("DEMO 9: Contract Persistence & Round-Trip")

    import tempfile

    # Create complex contract
    original = FeatureContract(
        feature_order=["amount", "country", "payment_method", "tx_hour"],
        category_mappings={
            "country": {"US": 0, "UK": 1, "CA": 2},
            "payment_method": {"credit_card": 0, "debit_card": 1},
        },
        feature_stats={"amount": {"min": 0, "max": 1000, "mean": 100}},
        primitive_features=["amount", "country", "payment_method"],
        aggregate_features=["tx_hour"],
        derived_features=[],
        schema_version="1.2.3",
        min_compatible_version="1.0.0",
    )

    # Add deprecation
    original.deprecate_feature("country", "Testing", "2.0.0", "shipping_country")

    print_section("Original Contract")
    print(f"Schema Version: {original.schema_version}")
    print(f"Deprecated: {list(original.deprecated_features.keys())}")
    print(f"__unknown__ in mappings: {all('__unknown__' in m for m in original.category_mappings.values())}")

    # Save and load
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        temp_path = f.name

    original.save(temp_path)
    loaded = FeatureContract.load(temp_path)

    print_section("Loaded Contract")
    print(f"Schema Version: {loaded.schema_version}")
    print(f"Deprecated: {list(loaded.deprecated_features.keys())}")
    print(f"__unknown__ preserved: {all('__unknown__' in m for m in loaded.category_mappings.values())}")

    # Verify round-trip
    print_section("Round-Trip Verification")
    checks = [
        ("schema_version", original.schema_version == loaded.schema_version),
        ("feature_order", original.feature_order == loaded.feature_order),
        ("category_mappings", original.category_mappings == loaded.category_mappings),
        ("deprecated_features", list(original.deprecated_features.keys()) == list(loaded.deprecated_features.keys())),
    ]
    all_passed = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print(f"\nOverall: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")

    # Cleanup
    os.unlink(temp_path)


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print(" SCHEMA EVOLUTION DEMO - Fraud Detection MLOps Platform")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    demos = [
        ("Contract Versioning", demo_contract_versioning),
        ("Unknown Categories", demo_unknown_categories),
        ("Type Coercion", demo_type_coercion),
        ("Deprecation Lifecycle", demo_deprecation),
        ("Schema Diff", demo_schema_diff),
        ("Streaming Schema", demo_streaming_schema),
        ("API Integration", demo_api_integration),
        ("Iceberg Evolution", demo_iceberg_evolution),
        ("Contract Persistence", demo_contract_persistence),
    ]

    results = []
    for name, demo_func in demos:
        try:
            demo_func()
            results.append((name, "SUCCESS"))
        except Exception as e:
            print(f"\n[ERROR] Demo '{name}' failed: {e}")
            results.append((name, f"FAILED: {e}"))

    print_header("DEMO SUMMARY")
    for name, status in results:
        print(f"  {name}: {status}")

    print("\n" + "=" * 70)
    print(" Schema Evolution Demo Complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
