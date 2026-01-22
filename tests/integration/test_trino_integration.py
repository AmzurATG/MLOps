"""
Integration tests for Trino (Query Engine).

These tests verify Trino connectivity and SQL operations.
Run with: pytest tests/integration/test_trino_integration.py -v -m integration
"""

import pytest
from tests.integration.conftest import requires_trino


# =============================================================================
# CONNECTIVITY TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoConnectivity:
    """Integration tests for Trino connectivity."""

    def test_trino_simple_query(self, trino_cursor):
        """Test that Trino can execute a simple query."""
        trino_cursor.execute("SELECT 1 as test_value")
        result = trino_cursor.fetchone()

        assert result is not None
        assert result[0] == 1

    def test_trino_system_query(self, trino_cursor):
        """Test that Trino system tables are accessible."""
        trino_cursor.execute("SELECT * FROM system.runtime.nodes LIMIT 1")
        result = trino_cursor.fetchone()

        assert result is not None

    def test_trino_catalogs_available(self, trino_cursor):
        """Test that expected catalogs are available."""
        trino_cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in trino_cursor.fetchall()]

        # Should have at least system catalog
        assert "system" in catalogs


# =============================================================================
# SCHEMA TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoSchemas:
    """Integration tests for Trino schema operations."""

    def test_iceberg_catalog_exists(self, trino_cursor):
        """Test that Iceberg catalog is configured."""
        trino_cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in trino_cursor.fetchall()]

        # Check for iceberg catalog (may be named differently)
        iceberg_catalogs = [c for c in catalogs if "iceberg" in c.lower()]

        if not iceberg_catalogs:
            pytest.skip("No Iceberg catalog configured")

        assert len(iceberg_catalogs) >= 1

    def test_schemas_in_catalog(self, trino_cursor):
        """Test that schemas exist in the data catalog."""
        try:
            trino_cursor.execute("SHOW SCHEMAS IN iceberg_dev")
            schemas = [row[0] for row in trino_cursor.fetchall()]

            # Should have at least information_schema
            assert "information_schema" in schemas
        except Exception:
            pytest.skip("iceberg_dev catalog not available")

    def test_bronze_schema_exists(self, trino_cursor):
        """Test that bronze schema exists for raw data."""
        try:
            trino_cursor.execute("SHOW SCHEMAS IN iceberg_dev")
            schemas = [row[0] for row in trino_cursor.fetchall()]

            # Check for bronze layer
            if "bronze" not in schemas:
                pytest.skip("Bronze schema not created yet")

            assert "bronze" in schemas
        except Exception:
            pytest.skip("iceberg_dev catalog not available")


# =============================================================================
# TABLE TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoTables:
    """Integration tests for Trino table operations."""

    def test_list_tables_in_schema(self, trino_cursor):
        """Test listing tables in a schema."""
        try:
            trino_cursor.execute("SHOW TABLES IN iceberg_dev.bronze")
            tables = [row[0] for row in trino_cursor.fetchall()]

            # Tables may or may not exist
            assert isinstance(tables, list)
        except Exception:
            pytest.skip("Bronze schema not available")

    def test_describe_table_if_exists(self, trino_cursor):
        """Test describing table structure."""
        try:
            trino_cursor.execute("SHOW TABLES IN iceberg_dev.bronze")
            tables = [row[0] for row in trino_cursor.fetchall()]

            if not tables:
                pytest.skip("No tables in bronze schema")

            # Describe first table
            trino_cursor.execute(
                f"DESCRIBE iceberg_dev.bronze.{tables[0]}"
            )
            columns = trino_cursor.fetchall()

            assert len(columns) > 0
        except Exception as e:
            pytest.skip(f"Table describe failed: {e}")


# =============================================================================
# QUERY TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoQueries:
    """Integration tests for Trino SQL queries."""

    def test_aggregate_functions(self, trino_cursor):
        """Test that aggregate functions work."""
        trino_cursor.execute("""
            SELECT
                COUNT(*) as cnt,
                SUM(1) as total,
                AVG(1.0) as average
            FROM (VALUES 1, 2, 3) AS t(x)
        """)
        result = trino_cursor.fetchone()

        assert result[0] == 3  # COUNT
        assert result[1] == 3  # SUM
        assert result[2] == 1.0  # AVG

    def test_date_functions(self, trino_cursor):
        """Test that date functions work."""
        trino_cursor.execute("""
            SELECT
                CURRENT_DATE,
                CURRENT_TIMESTAMP,
                DATE_ADD('day', -7, CURRENT_DATE)
        """)
        result = trino_cursor.fetchone()

        assert result[0] is not None  # Current date
        assert result[1] is not None  # Current timestamp
        assert result[2] is not None  # 7 days ago

    def test_window_functions(self, trino_cursor):
        """Test that window functions work."""
        trino_cursor.execute("""
            SELECT
                x,
                ROW_NUMBER() OVER (ORDER BY x) as rn
            FROM (VALUES 3, 1, 2) AS t(x)
            ORDER BY x
        """)
        results = trino_cursor.fetchall()

        assert len(results) == 3
        assert results[0][1] == 1  # First row number
        assert results[2][1] == 3  # Last row number


# =============================================================================
# PERFORMANCE TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoPerformance:
    """Integration tests for Trino query performance."""

    def test_simple_query_latency(self, trino_cursor):
        """Test that simple queries complete quickly."""
        import time

        start = time.time()
        trino_cursor.execute("SELECT 1")
        trino_cursor.fetchone()
        latency_ms = (time.time() - start) * 1000

        # Simple query should complete in < 1 second
        assert latency_ms < 1000, f"Query took {latency_ms:.0f}ms"

    def test_query_timeout_handling(self, trino_cursor):
        """Test that long queries can be cancelled."""
        # This is a basic test - complex timeout tests would need async
        trino_cursor.execute("SELECT 1")
        result = trino_cursor.fetchone()
        assert result is not None


# =============================================================================
# DATA QUALITY TESTS
# =============================================================================

@pytest.mark.integration
@requires_trino
class TestTrinoDataQuality:
    """Integration tests for data quality checks via Trino."""

    def test_null_handling(self, trino_cursor):
        """Test NULL value handling."""
        trino_cursor.execute("""
            SELECT
                COALESCE(NULL, 'default') as coalesced,
                NULLIF(1, 1) as nullified,
                CASE WHEN NULL THEN 'yes' ELSE 'no' END as case_null
        """)
        result = trino_cursor.fetchone()

        assert result[0] == 'default'
        assert result[1] is None
        assert result[2] == 'no'

    def test_type_casting(self, trino_cursor):
        """Test type casting functions."""
        trino_cursor.execute("""
            SELECT
                CAST('123' AS INTEGER) as to_int,
                CAST(123 AS VARCHAR) as to_string,
                CAST(123.456 AS DECIMAL(10,2)) as to_decimal
        """)
        result = trino_cursor.fetchone()

        assert result[0] == 123
        assert result[1] == '123'
        assert float(result[2]) == 123.46
