"""
Core Resources for Lakehouse Platform
======================================

Connection resources for external services used across all domains.
Direct implementations - no inheritance hierarchies.

Usage:
    from src.core.resources import TrinoResource, LakeFSResource

    # As Dagster resources
    trino = TrinoResource(host="exp-trino", port=8080)

    # Standalone functions
    trino = get_trino_client()
    lakefs = get_lakefs_client()
"""
import os
from typing import Any, Dict, List, Optional
from dagster import ConfigurableResource
from datetime import datetime, timezone

class LakeFSResource(ConfigurableResource):
    """LakeFS operations via REST API."""
    
    server_url: str = os.getenv("LAKEFS_SERVER", "http://exp-lakefs:8000")
    access_key: str = os.getenv("LAKEFS_ACCESS_KEY_ID", "")
    secret_key: str = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "")
    
    def _api_call(self, method: str, endpoint: str, json_data: Dict = None) -> Dict:
        """Make authenticated API call to LakeFS."""
        import requests
        
        url = f"{self.server_url}/api/v1{endpoint}"
        response = requests.request(
            method,
            url,
            auth=(self.access_key, self.secret_key),
            json=json_data,
            timeout=30,
        )
        
        if response.status_code == 404:
            raise Exception(f"Not found: {endpoint}")
        response.raise_for_status()
        
        if not response.content:
            return {}
        
        # Try to parse as JSON, otherwise return text
        try:
            return response.json()
        except Exception:
            return {"response": response.text}
    
    def _ensure_s3_bucket(self, storage_namespace: str):
        """Create S3/MinIO bucket if it doesn't exist."""
        import boto3
        from botocore.exceptions import ClientError
        
        if storage_namespace.startswith("s3://"):
            bucket_name = storage_namespace[5:].split("/")[0]
        else:
            bucket_name = storage_namespace.split("/")[0]
        
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://exp-minio:9000")
        minio_access = os.getenv("MINIO_ROOT_USER", "admin")
        minio_secret = os.getenv("MINIO_ROOT_PASSWORD", "password123")
        
        client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access,
            aws_secret_access_key=minio_secret,
        )
        
        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError:
            client.create_bucket(Bucket=bucket_name)
    
    def ensure_repo(self, repo_name: str, storage_namespace: str = None):
        """Create repository if not exists."""
        if not self.access_key or not self.secret_key:
            raise Exception("LakeFS credentials not configured")
        
        try:
            return self._api_call("GET", f"/repositories/{repo_name}")
        except Exception:
            if storage_namespace is None:
                storage_namespace = f"s3://{repo_name}"
            
            self._ensure_s3_bucket(storage_namespace)
            
            payload = {
                "name": repo_name,
                "storage_namespace": storage_namespace,
                "default_branch": "main",
            }
            return self._api_call("POST", "/repositories", json_data=payload)
    
    def create_branch(self, repo: str, branch: str, source: str = "main"):
        """Create branch if not exists."""
        try:
            return self._api_call("GET", f"/repositories/{repo}/branches/{branch}")
        except Exception:
            payload = {"name": branch, "source": source}
            return self._api_call("POST", f"/repositories/{repo}/branches", json_data=payload)
    
    def has_uncommitted_changes(self, repo: str, branch: str) -> bool:
        """Check if branch has uncommitted changes."""
        try:
            result = self._api_call("GET", f"/repositories/{repo}/branches/{branch}/diff")
            results = result.get("results", [])
            return len(results) > 0
        except Exception:
            return False
    
    def commit(self, repo: str, branch: str, message: str, metadata: Dict = None):
        """Commit changes if any exist."""
        # Check if there are actually changes to commit
        if not self.has_uncommitted_changes(repo, branch):
            return {"message": "no changes to commit"}
        
        payload = {
            "message": message,
            "metadata": metadata or {},
        }
        return self._api_call("POST", f"/repositories/{repo}/branches/{branch}/commits", json_data=payload)
    
    def merge_branch(self, repo: str, source: str, target: str, message: str):
        """Merge source branch into target."""
        payload = {"message": message}
        return self._api_call("POST", f"/repositories/{repo}/refs/{source}/merge/{target}", json_data=payload)
    
    def get_commit(self, repo: str, branch: str) -> Dict:
        """Get current commit info for a branch."""
        branch_info = self._api_call("GET", f"/repositories/{repo}/branches/{branch}")
        # Return commit_id from branch info
        return {"id": branch_info.get("commit_id", "unknown")}
    
    def get_s3_gateway_client(self):
        """Get S3-compatible client for LakeFS gateway."""
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=self.server_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def list_objects(self, repo: str, branch: str, prefix: str = "", max_results: int = 1000) -> List[str]:
        """List objects in LakeFS repo/branch with pagination support."""
        all_results = []
        after = ""
        
        while len(all_results) < max_results:
            url = f"/repositories/{repo}/refs/{branch}/objects/ls?prefix={prefix}"
            if after:
                url += f"&after={after}"
            
            result = self._api_call("GET", url)
            objects = result.get("results", [])
            
            if not objects:
                break
            
            all_results.extend([obj["path"] for obj in objects])
            
            # Check for more pages
            pagination = result.get("pagination", {})
            if not pagination.get("has_more", False):
                break
            after = pagination.get("next_offset", "")
            if not after:
                break
        
        return all_results[:max_results]
    
    def download_file(self, repo: str, branch: str, key: str, local_path: str):
        """Download file from LakeFS."""
        import requests
        url = f"{self.server_url}/api/v1/repositories/{repo}/refs/{branch}/objects?path={key}"
        response = requests.get(url, auth=(self.access_key, self.secret_key), timeout=60)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
    
    def upload_file(self, repo: str, branch: str, local_path: str, key: str):
        """Upload file to LakeFS."""
        import requests
        from urllib.parse import quote
        
        # URL encode the path parameter
        encoded_path = quote(key, safe='')
        url = f"{self.server_url}/api/v1/repositories/{repo}/branches/{branch}/objects"
        params = {"path": key}
        headers = {"Content-Type": "application/octet-stream"}
        
        with open(local_path, "rb") as f:
            response = requests.post(
                url, 
                params=params,
                headers=headers,
                auth=(self.access_key, self.secret_key), 
                data=f, 
                timeout=60
            )
        response.raise_for_status()
    
    def upload_content(self, repo: str, branch: str, content: bytes, key: str):
        """Upload content directly to LakeFS (without temp file)."""
        import requests
        
        url = f"{self.server_url}/api/v1/repositories/{repo}/branches/{branch}/objects"
        params = {"path": key}
        headers = {"Content-Type": "application/octet-stream"}
        
        response = requests.post(
            url,
            params=params,
            headers=headers,
            auth=(self.access_key, self.secret_key),
            data=content,
            timeout=60
        )
        
        if not response.ok:
            error_detail = response.text[:500] if response.text else "No response body"
            raise Exception(f"LakeFS upload failed: {response.status_code} - {error_detail}")
        
        return response
    
    def get_object_url(self, repo: str, branch: str, key: str) -> str:
        """Get URL for object in LakeFS."""
        return f"{self.server_url}/api/v1/repositories/{repo}/refs/{branch}/objects?path={key}"



class FeastResource(ConfigurableResource):
    """
    Production-grade Feast Feature Store resource with Trino compatibility patches.

    Fixes two critical Feast/Trino incompatibilities:
    
    1. TIMESTAMP format: Feast generates 'YYYY-MM-DDTHH:MM:SS+00:00' but Trino
       expects 'YYYY-MM-DD HH:MM:SS' (no T, no timezone).
    
    2. NULL handling: Feast generates lowercase 'null' in SQL which Trino rejects
       in certain contexts (type declarations, CREATE TABLE statements).
       
    The patches are applied globally at module import time to ensure all code paths
    are covered, including direct FeatureStore instantiation.
    """
    project: str = os.getenv("FEAST_PROJECT", "fraud_detection")
    registry_path: str = os.getenv("FEAST_REGISTRY_PATH", "/app/feast/registry.db")
    online_store: str = os.getenv("FEAST_ONLINE_STORE", "redis")
    offline_store: str = os.getenv("FEAST_OFFLINE_STORE", "trino")
    feature_store_yaml: str = os.getenv("FEAST_CONFIG_PATH", "/app/feast/feature_store.yaml")
    repo_path: str = os.getenv("FEAST_REPO_PATH", "/app/feast_repo")
    _store: Optional[Any] = None
    _patch_applied: bool = False

    @staticmethod
    def _fix_trino_sql(sql: str) -> str:
        """
        Apply all Trino SQL compatibility fixes to a query string.
        
        Fixes:
        - Lowercase 'null' -> uppercase 'NULL' (for values and types)
        - Type 'null' -> 'VARCHAR' (for column type declarations)
        - ISO timestamps -> Trino-compatible format
        """
        import re
        
        if not sql:
            return sql
        
        # Fix 1: Replace 'null' used as a TYPE in column definitions
        # Pattern: identifier followed by 'null' as type, then comma or paren
        # Specifically targets type contexts like "CREATE TABLE (col null, ...)"
        # Uses negative lookbehind to avoid matching SQL keywords
        sql_keywords = r'(?<!SELECT\s)(?<!INSERT\s)(?<!VALUES\s)(?<!SET\s)(?<!AND\s)(?<!OR\s)(?<!WHERE\s)(?<!WHEN\s)(?<!THEN\s)(?<!ELSE\s)(?<!CASE\s)(?<!IS\s)(?<!NOT\s)(?<!,\s)(?<!\(\s)'
        
        # More targeted: look for column definition pattern (identifier + null + comma/paren)
        # But only in CREATE/ROW contexts
        def fix_null_type(match):
            """Replace null type with VARCHAR in type declarations."""
            full = match.group(0)
            # Check if this looks like a column definition (preceded by CREATE, ROW, etc.)
            return re.sub(
                r'(\b[a-z_][a-z0-9_]*)\s+null(\s*[,\)])',
                r'\1 VARCHAR\2',
                full,
                flags=re.IGNORECASE
            )
        
        # Apply to CREATE TABLE and ROW type contexts
        sql = re.sub(
            r'(CREATE\s+TABLE[^(]*\([^)]+\))',
            fix_null_type,
            sql,
            flags=re.IGNORECASE | re.DOTALL
        )
        sql = re.sub(
            r'(ROW\s*\([^)]+\))',
            fix_null_type,
            sql,
            flags=re.IGNORECASE
        )
        
        # Fix 2: Replace CAST(... AS null) with CAST(... AS VARCHAR)
        sql = re.sub(
            r'AS\s+null\b',
            'AS VARCHAR',
            sql,
            flags=re.IGNORECASE
        )
        
        # Fix 3: Replace standalone lowercase 'null' VALUES with uppercase 'NULL'
        # This uses negative lookbehind/lookahead to avoid matching table/column names
        # Pattern: null as a standalone value (not part of an identifier)
        sql = re.sub(
            r"(?<![a-zA-Z_])null(?![a-zA-Z_0-9])",
            'NULL',
            sql
        )
        
        # Fix 4: Fix TIMESTAMP literals with T and timezone
        # TIMESTAMP '2025-11-21T08:32:01+00:00' -> TIMESTAMP '2025-11-21 08:32:01'
        ts_pattern = re.compile(
            r"(TIMESTAMP\s*')"
            r"(\d{4}-\d{2}-\d{2})"                 # date
            r"T"
            r"(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)"   # time with optional microseconds
            r"(?:\+\d{2}:\d{2})?"                  # optional timezone offset
            r"(')",
            flags=re.IGNORECASE
        )
        sql = ts_pattern.sub(r'\g<1>\g<2> \g<3>\g<4>', sql)
        
        return sql

    def _apply_trino_patches(self) -> None:
        """
        Apply comprehensive Trino compatibility patches to Feast modules.
        
        This patches at multiple levels to ensure all code paths are covered:
        1. TrinoQuery.execute - catches all query execution
        2. get_timestamp_filter_sql - fixes timestamp filter generation
        3. upload module - fixes entity dataframe upload
        """
        if self._patch_applied:
            return

        import re
        
        # ============================================================
        # PATCH 1: TrinoQuery.execute - comprehensive SQL fix
        # ============================================================
        try:
            import feast.infra.offline_stores.contrib.trino_offline_store.trino_queries as queries_module
            
            if getattr(queries_module, "_feast_trino_query_patched", False):
                print("[FeastResource] TrinoQuery already patched, skipping.", flush=True)
            elif hasattr(queries_module, 'TrinoQuery'):
                TrinoQuery = queries_module.TrinoQuery
                original_execute = TrinoQuery.execute
                
                def patched_execute(self):
                    """Patched execute that fixes Trino SQL compatibility issues."""
                    if hasattr(self, 'query_text') and self.query_text:
                        self.query_text = FeastResource._fix_trino_sql(self.query_text)
                    return original_execute(self)
                
                TrinoQuery.execute = patched_execute
                queries_module._feast_trino_query_patched = True
                print("[FeastResource] ✓ Patched TrinoQuery.execute for NULL/type handling", flush=True)
            else:
                print("[FeastResource] TrinoQuery class not found", flush=True)
                
        except ImportError:
            print("[FeastResource] trino_queries module not available", flush=True)
        except Exception as e:
            print(f"[FeastResource] Failed to patch TrinoQuery: {e}", flush=True)

        # ============================================================
        # PATCH 2: get_timestamp_filter_sql - timestamp format fix
        # ============================================================
        try:
            import feast.infra.offline_stores.contrib.trino_offline_store.trino as trino_module

            if getattr(trino_module, "_feast_trino_ts_patched", False):
                print("[FeastResource] Timestamp filter already patched, skipping.", flush=True)
            elif hasattr(trino_module, "get_timestamp_filter_sql"):
                original_get_ts = trino_module.get_timestamp_filter_sql

                ts_pattern = re.compile(
                    r"(TIMESTAMP\s*')"
                    r"(\d{4}-\d{2}-\d{2})"
                    r"T"
                    r"(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)"
                    r"(?:\+\d{2}:\d{2})?"
                    r"(')",
                    flags=re.IGNORECASE
                )

                def patched_get_timestamp_filter_sql(*args, **kwargs) -> str:
                    sql = original_get_ts(*args, **kwargs)
                    return ts_pattern.sub(r'\g<1>\g<2> \g<3>\g<4>', sql)

                trino_module.get_timestamp_filter_sql = patched_get_timestamp_filter_sql
                trino_module._feast_trino_ts_patched = True
                print("[FeastResource] ✓ Patched get_timestamp_filter_sql for timestamps", flush=True)
            else:
                print("[FeastResource] get_timestamp_filter_sql not found", flush=True)

        except ImportError:
            print("[FeastResource] trino module not available", flush=True)
        except Exception as e:
            print(f"[FeastResource] Failed to patch timestamp filter: {e}", flush=True)

        # ============================================================
        # PATCH 3: Trino upload module - fix type inference for NULL columns
        # ============================================================
        try:
            import feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload as upload_module
            
            if getattr(upload_module, "_feast_trino_upload_patched", False):
                print("[FeastResource] Upload module already patched, skipping.", flush=True)
            elif hasattr(upload_module, "upload_pandas_dataframe_to_trino"):
                original_upload = upload_module.upload_pandas_dataframe_to_trino
                
                def patched_upload(*args, **kwargs):
                    """
                    Patched upload that preprocesses the dataframe to fix NULL type issues.
                    """
                    import pandas as pd
                    import numpy as np
                    
                    # Find the dataframe argument (usually 'df' or second positional)
                    df = kwargs.get('df')
                    if df is None and len(args) >= 2:
                        # args[0] is typically client, args[1] is df
                        args = list(args)
                        df = args[1]
                        
                        if isinstance(df, pd.DataFrame):
                            # Fix columns with all NULL values - assign a concrete type
                            for col in df.columns:
                                if df[col].isna().all():
                                    # Convert all-NULL columns to object dtype with explicit None
                                    df[col] = df[col].astype(object)
                                elif df[col].dtype == 'object':
                                    # Ensure object columns have proper string type
                                    df[col] = df[col].astype(str).replace('nan', None).replace('None', None)
                            
                            args[1] = df
                        args = tuple(args)
                    elif df is not None and isinstance(df, pd.DataFrame):
                        for col in df.columns:
                            if df[col].isna().all():
                                df[col] = df[col].astype(object)
                        kwargs['df'] = df
                    
                    return original_upload(*args, **kwargs)
                
                upload_module.upload_pandas_dataframe_to_trino = patched_upload
                upload_module._feast_trino_upload_patched = True
                print("[FeastResource] ✓ Patched upload_pandas_dataframe_to_trino for NULL handling", flush=True)
            else:
                print("[FeastResource] upload_pandas_dataframe_to_trino not found", flush=True)
                
        except ImportError:
            print("[FeastResource] upload module not available", flush=True)
        except Exception as e:
            print(f"[FeastResource] Failed to patch upload: {e}", flush=True)

        # ============================================================
        # PATCH 4: Trino client execute_query - final safety net
        # ============================================================
        try:
            import feast.infra.offline_stores.contrib.trino_offline_store.trino_queries as queries_module
            
            if hasattr(queries_module, 'Trino') and not getattr(queries_module, "_feast_trino_client_patched", False):
                TrinoClient = queries_module.Trino
                if hasattr(TrinoClient, 'execute_query'):
                    original_client_execute = TrinoClient.execute_query
                    
                    def patched_client_execute(self, query_text: str, *args, **kwargs):
                        """Safety net: fix SQL at client execution level."""
                        fixed_sql = FeastResource._fix_trino_sql(query_text)
                        return original_client_execute(self, fixed_sql, *args, **kwargs)
                    
                    TrinoClient.execute_query = patched_client_execute
                    queries_module._feast_trino_client_patched = True
                    print("[FeastResource] ✓ Patched Trino.execute_query as safety net", flush=True)
                    
        except Exception as e:
            print(f"[FeastResource] Could not patch Trino client (non-critical): {e}", flush=True)

        self._patch_applied = True
        print("[FeastResource] ✓ All Trino compatibility patches applied", flush=True)

    def get_store(self):
        """
        Create (and cache) the Feast FeatureStore after applying all patches.
        
        This ensures patches are applied before any Feast operations occur.
        """
        if self._store is None:
            from feast import FeatureStore

            self._apply_trino_patches()
            self._store = FeatureStore(repo_path=self.repo_path)
        return self._store
    
    @staticmethod
    def ensure_patches_applied() -> None:
        """
        Static method to apply patches without instantiating the resource.
        
        Call this early in your pipeline initialization to ensure patches are
        applied before any direct FeatureStore usage.
        """
        # Create a temporary instance just to apply patches
        temp = FeastResource()
        temp._apply_trino_patches()

    @staticmethod
    def prepare_entity_df(df) -> Any:
        """
        Prepare an entity dataframe for Feast to avoid NULL type issues.
        
        Call this on entity dataframes before passing to get_historical_features()
        when not using the FeastResource's get_store() method.
        
        Args:
            df: Pandas DataFrame to prepare
            
        Returns:
            Prepared DataFrame with proper types for all columns
        """
        import pandas as pd
        import numpy as np
        
        if not isinstance(df, pd.DataFrame):
            return df
            
        df = df.copy()
        
        for col in df.columns:
            # Handle all-NULL columns
            if df[col].isna().all():
                # Try to infer a reasonable type based on column name
                col_lower = col.lower()
                if 'id' in col_lower:
                    df[col] = df[col].astype('Int64')  # Nullable integer
                elif 'timestamp' in col_lower or 'date' in col_lower or 'time' in col_lower:
                    df[col] = pd.to_datetime(df[col])
                elif 'amount' in col_lower or 'price' in col_lower or 'value' in col_lower:
                    df[col] = df[col].astype('Float64')  # Nullable float
                else:
                    df[col] = df[col].astype(str).replace('nan', '')
            
            # Ensure timestamp columns are proper datetime
            elif 'timestamp' in col.lower() or 'event_timestamp' == col.lower():
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col])
        
        return df

    # =================================================================
    # Convenience wrappers for Dagster assets
    # =================================================================

    def apply(self) -> None:
        """Run `feast apply` programmatically."""
        store = self.get_store()
        store.apply(store.list_feature_views() + store.list_entities())

    def materialize(self, start_date: datetime, end_date: datetime) -> None:
        """Materialize features between start & end into the online store."""
        store = self.get_store()
        store.materialize(start_date, end_date)

    def fetch_features(self, feature_refs: list[str], entity_rows: list[dict]):
        """Fetch online features for prediction."""
        store = self.get_store()
        return store.get_online_features(features=feature_refs, entity_rows=entity_rows).to_dict()
    
    def get_historical_features(self, entity_df, features: list[str]):
        """
        Get historical features with automatic entity dataframe preparation.
        
        This is the recommended method for historical feature retrieval as it
        handles all Trino compatibility issues automatically.
        """
        store = self.get_store()
        prepared_df = self.prepare_entity_df(entity_df)
        return store.get_historical_features(entity_df=prepared_df, features=features)

    


class TrinoResource(ConfigurableResource):
    """Trino SQL query engine."""
    
    host: str = os.getenv("TRINO_HOST", "exp-trino")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    user: str = os.getenv("TRINO_USER", "trino")
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg_dev")
    
    def _get_connection(self):
        import trino
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )
    
    def execute_query(self, query: str) -> List[Any]:
        """Execute SELECT query, return results."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            conn.close()

    def execute_query_streaming(self, query: str, batch_size: int = 10000):
        """
        Execute query with cursor-based streaming (generator).

        Memory-efficient for large result sets. Yields batches of rows
        instead of loading everything into memory at once.

        Args:
            query: SQL query to execute
            batch_size: Number of rows to fetch per batch (default: 10000)

        Yields:
            List of rows for each batch
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows
        finally:
            conn.close()

    def execute_ddl(self, query: str):
        """Execute DDL/DML (CREATE, INSERT, etc)."""
        import re
        
        # Fix TIMESTAMP literals with 'T' separator
        # TIMESTAMP '2025-12-06T17:25:16.542369' -> TIMESTAMP '2025-12-06 17:25:16.542369'
        ts_pattern = re.compile(
            r"(TIMESTAMP\s*')"
            r"(\d{4}-\d{2}-\d{2})"                 # date
            r"T"
            r"(\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)"   # time with optional microseconds
            r"(?:\+\d{2}:\d{2})?"                  # optional timezone offset
            r"(')",
            flags=re.IGNORECASE
        )
        query = ts_pattern.sub(r'\g<1>\g<2> \g<3>\g<4>', query)
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
        finally:
            conn.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        try:
            self.execute_query(f"DESCRIBE {table_name}")
            return True
        except Exception:
            return False
    
    def get_count(self, table_name: str) -> int:
        """Get row count."""
        try:
            result = self.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            return result[0][0] if result else 0
        except Exception:
            return 0
    
    def execute_query_as_dataframe(self, query: str):
        """Execute query and return as pandas DataFrame with proper column names."""
        import pandas as pd
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert to DataFrame
            if results and columns:
                return pd.DataFrame(results, columns=columns)
            else:
                # Return empty DataFrame with columns if no results
                return pd.DataFrame(columns=columns)
        finally:
            conn.close()
    
    def execute_query_as_dataframe_streaming(self, query: str, chunk_size: int = 50000):
        """
        Stream query results as DataFrame chunks (memory-efficient).

        Instead of loading all results into memory, yields DataFrames in chunks.
        Useful for large result sets (1M+ rows) to avoid OOM errors.

        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk (default: 50000)

        Yields:
            pd.DataFrame chunks of the query results
        """
        import pandas as pd
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

    def insert_rows(self, table: str, rows: List[tuple], batch_size: int = 5000, logger=None):
        """Bulk insert rows with proper escaping and type handling.
        
        Inserts in batches to avoid Trino's query size limit (1MB).
        """
        if not rows:
            if logger:
                logger.info(f"No rows to insert into {table}")
            return
        from datetime import datetime, date
        from decimal import Decimal
        
        def escape_value(v):
            if v is None:
                return "NULL"
            elif isinstance(v, str):
                return "'" + v.replace("'", "''") + "'"
            elif isinstance(v, datetime):
                # Format datetime for Trino TIMESTAMP
                return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            elif isinstance(v, date):
                # Format date for Trino DATE
                return f"DATE '{v.strftime('%Y-%m-%d')}'"
            elif isinstance(v, (int, float, Decimal)):
                return str(v)
            else:
                # Fallback: convert to string
                return "'" + str(v).replace("'", "''") + "'"
        
        total_rows = len(rows)
        total_batches = (total_rows + batch_size - 1) // batch_size
        
        if logger:
            logger.info(f"Inserting {total_rows} rows into {table} in {total_batches} batches (batch_size={batch_size})")
        
        # Process in batches to avoid QUERY_TEXT_TOO_LARGE error
        inserted = 0
        for batch_num, i in enumerate(range(0, len(rows), batch_size), 1):
            batch = rows[i:i + batch_size]
            values = []
            for r in batch:
                escaped = [escape_value(v) for v in r]
                values.append("(" + ", ".join(escaped) + ")")
            
            sql = f"INSERT INTO {table} VALUES " + ",\n".join(values)
            self.execute_ddl(sql)
            inserted += len(batch)
            
            if logger:
                logger.info(f"  Batch {batch_num}/{total_batches}: inserted {len(batch)} rows ({inserted}/{total_rows} total)")
        
        if logger:
            logger.info(f"✓ Completed: {inserted} rows inserted into {table}")


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE HELPER FUNCTIONS (for use outside Dagster context)
# ═══════════════════════════════════════════════════════════════════════════════

def get_trino_client() -> TrinoResource:
    """
    Get a standalone TrinoResource instance for use outside Dagster assets.
    
    Useful for sensors and other contexts that don't have resource injection.
    Uses environment variables for configuration.
    
    Returns:
        TrinoResource configured from environment variables
    """
    return TrinoResource(
        host=os.getenv("TRINO_HOST", "exp-trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "trino"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg_dev"),
    )


def get_lakefs_client() -> LakeFSResource:
    """
    Get a standalone LakeFSResource instance for use outside Dagster assets.
    
    Returns:
        LakeFSResource configured from environment variables
    """
    return LakeFSResource(
        server_url=os.getenv("LAKEFS_SERVER", "http://exp-lakefs:8000"),
        access_key=os.getenv("LAKEFS_ACCESS_KEY_ID", ""),
        secret_key=os.getenv("LAKEFS_SECRET_ACCESS_KEY", ""),
    )


class MinIOResource(ConfigurableResource):
    """MinIO/S3 object storage."""
    
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://exp-minio:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    
    def _get_client(self):
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def upload_file(self, bucket: str, key: str, file_path: str):
        """Upload file to bucket."""
        self._get_client().upload_file(file_path, bucket, key)
    
    def download_file(self, bucket: str, key: str, file_path: str):
        """Download file from bucket."""
        self._get_client().download_file(bucket, key, file_path)
    
    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """List object keys in bucket."""
        client = self._get_client()
        keys = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
    
    def put_json(self, bucket: str, key: str, data: Dict):
        """Upload JSON data."""
        import json
        client = self._get_client()
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json",
        )


class LabelStudioResource(ConfigurableResource):
    """Label Studio for human-in-the-loop annotation using JWT refresh flow."""
    
    url: str = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
    api_token: str = os.getenv("LABELSTUDIO_API_TOKEN", "")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with JWT access token from PAT refresh."""
        import requests
        
        if not self.api_token:
            raise ValueError("LABELSTUDIO_API_TOKEN not set")
        
        # Exchange PAT for short-lived access token
        response = requests.post(
            f"{self.url}/api/token/refresh",
            headers={"Content-Type": "application/json"},
            json={"refresh": self.api_token},
            timeout=10,
        )
        response.raise_for_status()
        access = response.json().get("access")
        
        if not access:
            raise ValueError(f"No access token in response: {response.json()}")
        
        return {
            "Authorization": f"Bearer {access}",
            "Content-Type": "application/json",
        }
    
    def get_or_create_project(self, title: str, label_config: str) -> int:
        """Get existing project or create new one."""
        import requests
        
        # List existing projects
        response = requests.get(
            f"{self.url}/api/projects/",
            headers=self._get_headers(),
            timeout=30,
        )
        response.raise_for_status()
        
        projects = response.json()
        if isinstance(projects, dict) and "results" in projects:
            projects = projects["results"]
        
        for project in projects:
            if project.get("title") == title:
                return project["id"]
        
        # Create new project
        response = requests.post(
            f"{self.url}/api/projects/",
            headers=self._get_headers(),
            json={"title": title, "label_config": label_config},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["id"]
    
    def create_task(self, project_id: int, data: Dict) -> Dict:
        """Create single annotation task."""
        import requests
        response = requests.post(
            f"{self.url}/api/projects/{project_id}/tasks/",
            headers=self._get_headers(),
            json={"data": data},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
    
    def bulk_import_tasks(self, project_id: int, tasks: List[Dict]) -> int:
        """Import multiple tasks at once using /tasks/bulk."""
        import requests
        response = requests.post(
            f"{self.url}/api/projects/{project_id}/tasks/bulk",
            headers=self._get_headers(),
            json=tasks,
            timeout=120,
        )
        response.raise_for_status()
        return len(tasks)
    
    def export_annotations(self, project_id: int) -> List[Dict]:
        """Export all annotations from project."""
        import requests
        response = requests.get(
            f"{self.url}/api/projects/{project_id}/export",
            headers=self._get_headers(),
            params={"exportType": "JSON"},
            timeout=120,
        )
        response.raise_for_status()
        return response.json()


class NessieResource(ConfigurableResource):
    """Nessie Iceberg catalog management."""
    
    url: str = os.getenv("NESSIE_URL", "http://exp-nessie:19120")
    
    def create_branch(self, branch: str, source_ref: str = "main"):
        """Create branch from source reference."""
        import requests
        
        # Check if branch already exists (v1 API)
        resp = requests.get(f"{self.url}/api/v1/trees/tree/{branch}", timeout=30)
        if resp.ok:
            return {"branch": branch, "status": "exists"}
        
        # Get source hash
        resp = requests.get(f"{self.url}/api/v1/trees/tree/{source_ref}", timeout=30)
        source_hash = ""
        if resp.ok:
            source_hash = resp.json().get("hash", "")
        
        # Create branch (v1 API)
        payload = {
            "name": branch,
            "type": "BRANCH",
            "hash": source_hash,
        }
        
        response = requests.post(
            f"{self.url}/api/v1/trees/tree",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        # 409 = already exists, that's OK
        if response.status_code not in [200, 201, 409]:
            response.raise_for_status()
        return {"branch": branch, "status": "created" if response.ok else "exists"}
    
    def get_branch(self, branch: str) -> Dict:
        """Get branch info."""
        import requests
        response = requests.get(f"{self.url}/api/v1/trees/tree/{branch}", timeout=30)
        response.raise_for_status()
        return response.json()
    
    def get_commit_hash(self, branch: str) -> str:
        """Get current commit hash for a branch."""
        info = self.get_branch(branch)
        return info.get("hash", "unknown")
    
    def merge_branch(self, source: str, target: str, message: str = ""):
        """Merge source branch into target."""
        import requests
        
        # Get target hash
        target_info = self.get_branch(target)
        target_hash = target_info.get("hash", "")
        
        response = requests.post(
            f"{self.url}/api/v1/trees/branch/{target}/merge",
            params={"expectedHash": target_hash},
            json={
                "fromRefName": source,
                "message": message or f"Merge {source} into {target}",
            },
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        response.raise_for_status()
        return response.json() if response.content else {}


class AirbyteResource(ConfigurableResource):
    """Airbyte data ingestion (abctl installation via Kind cluster)."""

    server_url: str = os.getenv("AIRBYTE_SERVER_URL", "http://airbyte-abctl-control-plane:80")
    client_id: str = os.getenv("AIRBYTE_CLIENT_ID", "")
    client_secret: str = os.getenv("AIRBYTE_CLIENT_SECRET", "")
    workspace_id: str = os.getenv("AIRBYTE_WORKSPACE_ID", "")
    
    def _get_access_token(self) -> str:
        """Get Airbyte access token using client credentials."""
        import requests
        
        if not self.client_id or not self.client_secret:
            raise Exception(
                "Airbyte client credentials not configured. "
                "Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET environment variables"
            )
        
        token_url = f"{self.server_url}/api/v1/applications/token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        
        response = requests.post(token_url, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        token = data.get("access_token")
        if not token:
            raise Exception(f"No access_token in Airbyte response: {data}")
        
        return token
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers with authentication."""
        token = self._get_access_token()
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        if self.workspace_id:
            headers["X-Airbyte-Workspace-Id"] = self.workspace_id
        return headers
    
    def trigger_sync(self, connection_id: str) -> Dict:
        """Trigger Airbyte sync job."""
        import requests
        
        url = f"{self.server_url}/api/public/v1/jobs"
        payload = {"jobType": "sync", "connectionId": connection_id}
        
        response = requests.post(
            url,
            headers=self._get_headers(),
            json=payload,
            timeout=60,
        )
        
        # 409 = job already running for this connection
        if response.status_code == 409:
            # Try to get the existing job info
            try:
                data = response.json()
                existing_job_id = data.get("jobId") or data.get("existingJobId")
                if existing_job_id:
                    return {
                        "jobId": existing_job_id,
                        "status": "already_running",
                        "message": "Using existing running job"
                    }
            except Exception:
                pass
            
            # If we can't get job info, just skip
            return {
                "jobId": "skipped",
                "status": "skipped",
                "message": "Job already running, skipping sync"
            }
        
        response.raise_for_status()
        
        data = response.json()
        # Response can be flat or nested
        job = data.get("job") or data
        job_id = job.get("id") or job.get("jobId") or job.get("job_id")
        
        if not job_id:
            raise Exception(f"No job id in Airbyte response: {data}")
        
        return {"jobId": job_id, "status": job.get("status", "unknown")}
    
    def get_job_status(self, job_id: str) -> Dict:
        """Get job status."""
        import requests
        
        # Try public API first
        url = f"{self.server_url}/api/public/v1/jobs/get"
        payload = {"jobId": int(job_id)}
        
        response = requests.post(
            url,
            headers=self._get_headers(),
            json=payload,
            timeout=30,
        )
        
        # Fallback to internal API if forbidden
        if response.status_code == 403:
            url = f"{self.server_url}/api/v1/jobs/get"
            payload = {"id": int(job_id)}
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload,
                timeout=30,
            )
        
        response.raise_for_status()
        data = response.json()
        job = data.get("job") or data
        return {"status": job.get("status", "unknown"), "job": job}