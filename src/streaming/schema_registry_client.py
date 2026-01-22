"""
Schema Registry Client
======================

Utility module for interacting with Confluent Schema Registry.

Features:
- Register Avro/JSON schemas
- Check schema compatibility
- Get schema versions
- Validate messages against registered schemas

Usage:
    client = SchemaRegistryClient("http://localhost:8081")

    # Register a schema
    schema_id = client.register_schema("fraud.streaming.features-value", schema_dict)

    # Check compatibility
    is_compatible = client.check_compatibility("fraud.streaming.features-value", new_schema)
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


@dataclass
class SchemaInfo:
    """Information about a registered schema."""
    schema_id: int
    subject: str
    version: int
    schema: Dict[str, Any]
    schema_type: str  # AVRO, JSON, PROTOBUF


class SchemaRegistryError(Exception):
    """Base exception for schema registry errors."""
    pass


class SchemaNotFoundError(SchemaRegistryError):
    """Raised when schema is not found."""
    pass


class SchemaIncompatibleError(SchemaRegistryError):
    """Raised when schema is incompatible with existing version."""
    pass


class SchemaRegistryClient:
    """
    Client for Confluent Schema Registry.

    Provides methods to:
    - Register schemas
    - Check compatibility
    - Get schema versions
    - Delete schemas (for development)
    """

    def __init__(self, url: Optional[str] = None):
        """
        Initialize client.

        Args:
            url: Schema Registry URL. Defaults to SCHEMA_REGISTRY_URL env var
                 or http://localhost:8081
        """
        self.url = url or os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.url = self.url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        })

    def is_available(self) -> bool:
        """Check if Schema Registry is available."""
        try:
            resp = self._session.get(f"{self.url}/subjects", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    def get_subjects(self) -> List[str]:
        """Get all registered subjects."""
        resp = self._session.get(f"{self.url}/subjects")
        resp.raise_for_status()
        return resp.json()

    def get_schema_versions(self, subject: str) -> List[int]:
        """Get all versions for a subject."""
        resp = self._session.get(f"{self.url}/subjects/{subject}/versions")
        if resp.status_code == 404:
            raise SchemaNotFoundError(f"Subject not found: {subject}")
        resp.raise_for_status()
        return resp.json()

    def get_schema(self, subject: str, version: str = "latest") -> SchemaInfo:
        """
        Get schema for a subject.

        Args:
            subject: Subject name (e.g., "fraud.streaming.features-value")
            version: Version number or "latest"

        Returns:
            SchemaInfo with schema details
        """
        resp = self._session.get(f"{self.url}/subjects/{subject}/versions/{version}")
        if resp.status_code == 404:
            raise SchemaNotFoundError(f"Schema not found: {subject}:{version}")
        resp.raise_for_status()

        data = resp.json()
        return SchemaInfo(
            schema_id=data["id"],
            subject=data["subject"],
            version=data["version"],
            schema=json.loads(data["schema"]),
            schema_type=data.get("schemaType", "AVRO"),
        )

    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """Get schema by global ID."""
        resp = self._session.get(f"{self.url}/schemas/ids/{schema_id}")
        if resp.status_code == 404:
            raise SchemaNotFoundError(f"Schema ID not found: {schema_id}")
        resp.raise_for_status()
        return json.loads(resp.json()["schema"])

    def register_schema(
        self,
        subject: str,
        schema: Dict[str, Any],
        schema_type: str = "AVRO",
    ) -> int:
        """
        Register a new schema version.

        Args:
            subject: Subject name (e.g., "fraud.streaming.features-value")
            schema: Schema definition (Avro record, JSON schema, etc.)
            schema_type: AVRO, JSON, or PROTOBUF

        Returns:
            Schema ID

        Raises:
            SchemaIncompatibleError: If schema is incompatible with existing
        """
        payload = {
            "schema": json.dumps(schema),
            "schemaType": schema_type,
        }

        resp = self._session.post(
            f"{self.url}/subjects/{subject}/versions",
            json=payload,
        )

        if resp.status_code == 409:
            raise SchemaIncompatibleError(
                f"Schema incompatible with existing version: {resp.json()}"
            )

        resp.raise_for_status()
        return resp.json()["id"]

    def check_compatibility(
        self,
        subject: str,
        schema: Dict[str, Any],
        version: str = "latest",
        schema_type: str = "AVRO",
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if schema is compatible with existing version.

        Args:
            subject: Subject name
            schema: New schema to check
            version: Version to check against ("latest" or version number)
            schema_type: Schema type

        Returns:
            Tuple of (is_compatible, error_message)
        """
        payload = {
            "schema": json.dumps(schema),
            "schemaType": schema_type,
        }

        resp = self._session.post(
            f"{self.url}/compatibility/subjects/{subject}/versions/{version}",
            json=payload,
        )

        if resp.status_code == 404:
            # No existing schema, so any schema is compatible
            return True, None

        resp.raise_for_status()
        result = resp.json()

        is_compatible = result.get("is_compatible", False)
        messages = result.get("messages", [])

        return is_compatible, "; ".join(messages) if messages else None

    def set_compatibility(self, subject: str, level: str) -> str:
        """
        Set compatibility level for a subject.

        Args:
            subject: Subject name (or use "_global_" for global config)
            level: BACKWARD, FORWARD, FULL, NONE

        Returns:
            Current compatibility level
        """
        if subject == "_global_":
            url = f"{self.url}/config"
        else:
            url = f"{self.url}/config/{subject}"

        resp = self._session.put(url, json={"compatibility": level})
        resp.raise_for_status()
        return resp.json()["compatibility"]

    def get_compatibility(self, subject: str = "_global_") -> str:
        """Get compatibility level for a subject."""
        if subject == "_global_":
            url = f"{self.url}/config"
        else:
            url = f"{self.url}/config/{subject}"

        resp = self._session.get(url)
        if resp.status_code == 404:
            return "BACKWARD"  # Default
        resp.raise_for_status()
        return resp.json().get("compatibilityLevel", "BACKWARD")

    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """
        Delete a subject and all its versions.

        Args:
            subject: Subject name
            permanent: If True, permanently delete (requires soft delete first)

        Returns:
            List of deleted version numbers
        """
        url = f"{self.url}/subjects/{subject}"
        if permanent:
            url += "?permanent=true"

        resp = self._session.delete(url)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return resp.json()

    def delete_version(self, subject: str, version: int, permanent: bool = False) -> int:
        """Delete a specific version of a subject."""
        url = f"{self.url}/subjects/{subject}/versions/{version}"
        if permanent:
            url += "?permanent=true"

        resp = self._session.delete(url)
        resp.raise_for_status()
        return resp.json()


def load_avro_schema(path: str) -> Dict[str, Any]:
    """Load Avro schema from file."""
    with open(path, "r") as f:
        return json.load(f)


def register_fraud_transaction_schema(
    registry_url: Optional[str] = None,
    subject: str = "fraud.streaming.features-value",
) -> int:
    """
    Register the fraud transaction schema.

    Returns:
        Schema ID
    """
    client = SchemaRegistryClient(registry_url)

    # Load schema from file
    schema_path = os.path.join(
        os.path.dirname(__file__),
        "schemas",
        "fraud_transaction.avsc",
    )
    schema = load_avro_schema(schema_path)

    # Check compatibility first
    is_compatible, error = client.check_compatibility(subject, schema)
    if not is_compatible:
        logger.warning(f"Schema compatibility warning: {error}")

    # Register
    schema_id = client.register_schema(subject, schema)
    logger.info(f"Registered schema: {subject} (id={schema_id})")

    return schema_id


def main():
    """CLI for schema registry operations."""
    import argparse

    parser = argparse.ArgumentParser(description="Schema Registry CLI")
    parser.add_argument(
        "--url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Schema Registry URL",
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # List subjects
    subparsers.add_parser("list", help="List all subjects")

    # Get schema
    get_parser = subparsers.add_parser("get", help="Get schema for subject")
    get_parser.add_argument("subject", help="Subject name")
    get_parser.add_argument("--version", default="latest", help="Version")

    # Register schema
    register_parser = subparsers.add_parser("register", help="Register schema")
    register_parser.add_argument("subject", help="Subject name")
    register_parser.add_argument("schema_file", help="Path to schema file")
    register_parser.add_argument("--type", default="AVRO", help="Schema type")

    # Check compatibility
    compat_parser = subparsers.add_parser("compat", help="Check compatibility")
    compat_parser.add_argument("subject", help="Subject name")
    compat_parser.add_argument("schema_file", help="Path to schema file")

    # Register fraud schema
    subparsers.add_parser("register-fraud", help="Register fraud transaction schema")

    args = parser.parse_args()
    client = SchemaRegistryClient(args.url)

    if args.command == "list":
        subjects = client.get_subjects()
        print(f"Subjects ({len(subjects)}):")
        for s in subjects:
            print(f"  - {s}")

    elif args.command == "get":
        info = client.get_schema(args.subject, args.version)
        print(f"Subject: {info.subject}")
        print(f"Version: {info.version}")
        print(f"ID: {info.schema_id}")
        print(f"Type: {info.schema_type}")
        print(f"Schema:\n{json.dumps(info.schema, indent=2)}")

    elif args.command == "register":
        schema = load_avro_schema(args.schema_file)
        schema_id = client.register_schema(args.subject, schema, args.type)
        print(f"Registered schema: {args.subject} (id={schema_id})")

    elif args.command == "compat":
        schema = load_avro_schema(args.schema_file)
        is_compat, error = client.check_compatibility(args.subject, schema)
        if is_compat:
            print(f"Schema is compatible with {args.subject}")
        else:
            print(f"Schema is NOT compatible: {error}")

    elif args.command == "register-fraud":
        schema_id = register_fraud_transaction_schema(args.url)
        print(f"Registered fraud transaction schema (id={schema_id})")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
