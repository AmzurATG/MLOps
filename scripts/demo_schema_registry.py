#!/usr/bin/env python3
"""
Schema Registry Demo
====================

This script demonstrates how Confluent Schema Registry:
1. Prevents incompatible schema changes
2. Validates message structure before sending
3. Tracks schema versions

Run: python scripts/demo_schema_registry.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from streaming.schema_registry_client import (
    SchemaRegistryClient,
    SchemaIncompatibleError,
    load_avro_schema,
)


def print_header(title: str):
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_section(title: str):
    print(f"\n--- {title} ---")


def demo_without_registry():
    """Show what happens WITHOUT schema registry."""
    print_header("SCENARIO 1: Without Schema Registry (Current Risk)")

    print_section("Day 1: Producer sends valid data")
    message_v1 = {
        "customer_id": "CUST123",
        "amount": 150.50,  # float
        "payment_method": "credit_card",
    }
    print(f"  Message: {json.dumps(message_v1)}")
    print(f"  Consumer processes: amount = {message_v1['amount']} (float) ✓")

    print_section("Day 30: Developer changes schema (no validation!)")
    message_v2_bad = {
        "customer_id": "CUST123",
        "amount": "150.50",  # Changed to STRING!
        "payment_method": "credit_card",
    }
    print(f"  Message: {json.dumps(message_v2_bad)}")
    print(f"  Type of amount: {type(message_v2_bad['amount']).__name__}")
    print("  ⚠️  No one catches this change!")
    print("  ⚠️  Messages flow to Kafka without validation")

    print_section("Day 31: Consumer starts failing")
    print("  Consumer code: model.predict(float(amount))")
    print("  Result: Works for '150.50' → 150.50")
    print()
    print("  But then producer sends:")
    message_v2_crash = {
        "customer_id": "CUST456",
        "amount": "N/A",  # Invalid!
        "payment_method": "credit_card",
    }
    print(f"  Message: {json.dumps(message_v2_crash)}")
    try:
        float(message_v2_crash["amount"])
    except ValueError as e:
        print(f"  ❌ CRASH: ValueError: {e}")
        print("  ❌ Consumer dies, fraud goes undetected!")


def demo_with_registry():
    """Show what happens WITH schema registry."""
    print_header("SCENARIO 2: With Schema Registry (Protected)")

    # Check if registry is available
    client = SchemaRegistryClient("http://localhost:8081")

    if not client.is_available():
        print("\n⚠️  Schema Registry not running. Showing simulated behavior.")
        print("   Start with: docker compose up -d exp-schema-registry")
        demo_with_registry_simulated()
        return

    demo_with_registry_live(client)


def demo_with_registry_simulated():
    """Simulated demo when registry isn't running."""

    print_section("Step 1: Register Original Schema (v1)")
    schema_v1 = {
        "type": "record",
        "name": "FraudTransaction",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "double"},  # DOUBLE
            {"name": "payment_method", "type": "string"},
        ]
    }
    print(f"  Schema v1 registered with amount: double")
    print(f"  Schema ID: 1")

    print_section("Step 2: Try to Register Breaking Change")
    schema_v2_bad = {
        "type": "record",
        "name": "FraudTransaction",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "string"},  # Changed to STRING!
            {"name": "payment_method", "type": "string"},
        ]
    }
    print(f"  Attempting to register schema with amount: string")
    print()
    print("  ❌ Schema Registry REJECTS this change!")
    print("  ❌ Error: 'amount' type changed from 'double' to 'string'")
    print("  ❌ Compatibility: BACKWARD requires new readers to read old data")
    print()
    print("  ✅ The bad schema NEVER reaches Kafka")
    print("  ✅ Producer is forced to fix the issue")
    print("  ✅ Consumer continues working normally")

    print_section("Step 3: Compatible Evolution (Adding Optional Field)")
    schema_v2_good = {
        "type": "record",
        "name": "FraudTransaction",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "payment_method", "type": "string"},
            {"name": "device_fingerprint", "type": ["null", "string"], "default": None},  # NEW, optional
        ]
    }
    print(f"  Adding optional field 'device_fingerprint' with default null")
    print(f"  ✅ Schema Registry ACCEPTS this change!")
    print(f"  ✅ Schema ID: 2, Version: 2")
    print()
    print("  Why is this compatible?")
    print("  - Old consumers can still read (they ignore new field)")
    print("  - New consumers can read old data (field defaults to null)")


def demo_with_registry_live(client: SchemaRegistryClient):
    """Live demo with running registry."""

    subject = "demo.fraud.transactions-value"

    print_section("Step 1: Register Original Schema (v1)")
    schema_v1 = {
        "type": "record",
        "name": "FraudTransaction",
        "namespace": "com.fraud.demo",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "payment_method", "type": "string"},
        ]
    }

    try:
        # Clean up from previous runs
        client.delete_subject(subject, permanent=False)
        client.delete_subject(subject, permanent=True)
    except:
        pass

    schema_id = client.register_schema(subject, schema_v1)
    print(f"  Registered schema with amount: double")
    print(f"  Schema ID: {schema_id}")

    print_section("Step 2: Try to Register Breaking Change")
    schema_v2_bad = {
        "type": "record",
        "name": "FraudTransaction",
        "namespace": "com.fraud.demo",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "string"},  # BREAKING CHANGE!
            {"name": "payment_method", "type": "string"},
        ]
    }

    print(f"  Attempting to change amount: double → string")

    # Check compatibility first
    is_compatible, error = client.check_compatibility(subject, schema_v2_bad)
    print()
    if not is_compatible:
        print("  ❌ Schema Registry REJECTS this change!")
        print(f"  ❌ Reason: {error or 'Type mismatch detected'}")
        print("  ✅ Bad schema BLOCKED before reaching Kafka!")

    try:
        client.register_schema(subject, schema_v2_bad)
        print("  ⚠️  Unexpectedly accepted (compatibility may be set to NONE)")
    except SchemaIncompatibleError as e:
        print(f"  ❌ Registration failed: Incompatible schema")

    print_section("Step 3: Compatible Evolution (Adding Optional Field)")
    schema_v2_good = {
        "type": "record",
        "name": "FraudTransaction",
        "namespace": "com.fraud.demo",
        "fields": [
            {"name": "customer_id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "payment_method", "type": "string"},
            {"name": "device_fingerprint", "type": ["null", "string"], "default": None},
        ]
    }

    is_compatible, _ = client.check_compatibility(subject, schema_v2_good)
    print(f"  Adding optional field 'device_fingerprint'")
    print(f"  Compatibility check: {'PASS ✅' if is_compatible else 'FAIL ❌'}")

    if is_compatible:
        schema_id_v2 = client.register_schema(subject, schema_v2_good)
        print(f"  ✅ Schema registered! ID: {schema_id_v2}")

    print_section("Current Schema Versions")
    versions = client.get_schema_versions(subject)
    print(f"  Subject: {subject}")
    print(f"  Versions: {versions}")

    # Cleanup
    client.delete_subject(subject)


def demo_comparison():
    """Side-by-side comparison."""
    print_header("COMPARISON: Before vs After Schema Registry")

    print("""
┌─────────────────────────────────────┬─────────────────────────────────────┐
│      WITHOUT Schema Registry        │       WITH Schema Registry          │
├─────────────────────────────────────┼─────────────────────────────────────┤
│ Producer changes field type         │ Producer changes field type         │
│           ↓                         │           ↓                         │
│ Message goes to Kafka               │ Schema Registry BLOCKS it ❌        │
│           ↓                         │           ↓                         │
│ Consumer tries to process           │ Producer gets error, must fix       │
│           ↓                         │           ↓                         │
│ Consumer CRASHES ❌                 │ Consumer never sees bad data ✅     │
│           ↓                         │                                     │
│ Fraud goes undetected 💸            │ System continues working 🛡️         │
└─────────────────────────────────────┴─────────────────────────────────────┘
    """)

    print("Key Benefits:")
    print("  1. VALIDATION AT PRODUCER - Bad data never enters Kafka")
    print("  2. COMPATIBILITY CHECK  - Breaking changes are blocked")
    print("  3. VERSION HISTORY      - Audit trail of all schema changes")
    print("  4. CENTRALIZED SCHEMA   - Single source of truth")


def main():
    print("\n" + "=" * 70)
    print(" SCHEMA REGISTRY DEMO - Why It Matters")
    print("=" * 70)

    demo_without_registry()
    demo_with_registry()
    demo_comparison()

    print_header("SUMMARY")
    print("""
Schema Registry protects your streaming pipeline by:

  1. PREVENTING type changes that break consumers
     amount: double → string = BLOCKED ❌

  2. ALLOWING safe additions (optional fields with defaults)
     + device_fingerprint: string (optional) = ALLOWED ✅

  3. REJECTING field removals (unless explicitly allowed)
     - payment_method = BLOCKED ❌

Your current setup:
  - Custom FieldDefinition: Catches errors at RUNTIME (consumer side)
  - Schema Registry: Catches errors at PRODUCER (before data enters Kafka)

Both work together for defense in depth!
""")


if __name__ == "__main__":
    main()
