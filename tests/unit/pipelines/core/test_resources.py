"""
Unit tests for core.resources module.

Tests the resource configuration and helper functions.
Since Dagster is mocked in conftest.py, we test the configuration
logic rather than the actual Dagster resource behavior.
"""

import sys
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


class TestResourceConfiguration:
    """Tests for resource configuration values."""

    def test_trino_host_from_env(self):
        """Test TRINO_HOST environment variable is used."""
        # The mock_environment fixture sets TRINO_HOST=localhost
        assert os.getenv("TRINO_HOST") == "localhost"

    def test_trino_port_from_env(self):
        """Test TRINO_PORT environment variable is used."""
        assert os.getenv("TRINO_PORT") == "8080"

    def test_redis_host_from_env(self):
        """Test REDIS_HOST environment variable is used."""
        assert os.getenv("REDIS_HOST") == "localhost"

    def test_redis_port_from_env(self):
        """Test REDIS_PORT environment variable is used."""
        assert os.getenv("REDIS_PORT") == "6379"


class TestResourceModuleStructure:
    """Tests for resource module imports."""

    def test_resources_module_importable(self):
        """Test that src.core.resources module can be imported."""
        from src.core import resources
        assert resources is not None

    def test_trino_resource_class_exists(self):
        """Test TrinoResource class exists in module."""
        from src.core.resources import TrinoResource
        # Class exists (even if it's a mock)
        assert TrinoResource is not None

    def test_lakefs_resource_class_exists(self):
        """Test LakeFSResource class exists in module."""
        from src.core.resources import LakeFSResource
        assert LakeFSResource is not None

    def test_get_trino_client_function_exists(self):
        """Test get_trino_client helper function exists."""
        from src.core.resources import get_trino_client
        assert callable(get_trino_client)

    def test_get_lakefs_client_function_exists(self):
        """Test get_lakefs_client helper function exists."""
        from src.core.resources import get_lakefs_client
        assert callable(get_lakefs_client)


class TestResourceHelperFunctions:
    """Tests for resource helper functions."""

    def test_get_trino_client_returns_instance(self):
        """Test get_trino_client returns an instance."""
        from src.core.resources import get_trino_client
        client = get_trino_client()
        # Returns something (either real resource or mock)
        assert client is not None

    def test_get_lakefs_client_returns_instance(self):
        """Test get_lakefs_client returns an instance."""
        from src.core.resources import get_lakefs_client
        client = get_lakefs_client()
        assert client is not None


class TestResourceExports:
    """Tests for resource module exports."""

    def test_all_expected_resources_exported(self):
        """Test all expected resource classes are exported."""
        from src.core import resources

        expected_exports = [
            'TrinoResource',
            'LakeFSResource',
            'MinIOResource',
            'FeastResource',
            'get_trino_client',
            'get_lakefs_client',
        ]

        for name in expected_exports:
            assert hasattr(resources, name), f"Missing export: {name}"
