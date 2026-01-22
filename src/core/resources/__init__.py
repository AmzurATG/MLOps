"""
Core Resources Module
=====================

Connection resources for external services used across all domains.

Available Resources:
- TrinoResource: SQL query engine
- LakeFSResource: Data versioning
- MinIOResource: Object storage
- FeastResource: Feature store
- LabelStudioResource: Annotation platform
- NessieResource: Iceberg catalog
- AirbyteResource: Data ingestion

Usage:
    from src.core.resources import TrinoResource, LakeFSResource

    # As Dagster resources
    trino = TrinoResource(host="exp-trino", port=8080)

    # Standalone helper functions
    from src.core.resources import get_trino_client, get_lakefs_client
    trino = get_trino_client()
"""

from src.core.resources.connections import (
    TrinoResource,
    LakeFSResource,
    MinIOResource,
    FeastResource,
    LabelStudioResource,
    NessieResource,
    AirbyteResource,
    get_trino_client,
    get_lakefs_client,
)

__all__ = [
    "TrinoResource",
    "LakeFSResource",
    "MinIOResource",
    "FeastResource",
    "LabelStudioResource",
    "NessieResource",
    "AirbyteResource",
    "get_trino_client",
    "get_lakefs_client",
]
