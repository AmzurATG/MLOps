"""
Feature Lineage Tracking System

Provides complete traceability:
- Feature Registry YAML → Generated Code → Training Data → Model
- Source table snapshots (LakeFS/Nessie commits)
- Feature change audit trail
- MLflow integration for reproducibility

Usage:
    tracker = FeatureLineageTracker(
        project_name="fraud_detection",
        mlflow_tracking_uri="http://mlflow:5000"
    )
    
    # Register feature version
    version_id = tracker.register_feature_version(
        yaml_path="fraud_detection.yaml",
        source_commit="lakefs_abc123",
        nessie_commit="nessie_xyz789"
    )
    
    # Link to training run
    tracker.link_to_training(version_id, mlflow_run_id)
"""

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict
import yaml


@dataclass
class FeatureVersion:
    """Immutable snapshot of feature configuration."""
    version_id: str                    # Hash-based version ID
    project_name: str
    yaml_hash: str                     # SHA256 of YAML content
    yaml_content: str                  # Full YAML content for reproducibility
    generated_sql_hash: str            # SHA256 of generated SQL
    generated_python_hash: str         # SHA256 of generated Python
    
    # Source data lineage
    source_table: str                  # e.g., iceberg_dev.gold.fraud_transactions
    source_lakefs_commit: Optional[str] = None
    source_nessie_commit: Optional[str] = None
    
    # Output data lineage
    output_table: str = ""             # e.g., iceberg_dev.gold.fraud_training_data
    output_lakefs_commit: Optional[str] = None
    output_nessie_commit: Optional[str] = None
    output_row_count: int = 0
    
    # Feature metadata
    feature_count: int = 0
    feature_names: List[str] = field(default_factory=list)
    categorical_columns: List[str] = field(default_factory=list)
    
    # Thresholds and parameters (for audit)
    thresholds: Dict[str, Any] = field(default_factory=dict)
    
    # Timestamps
    created_at: str = ""
    
    # Linked artifacts
    mlflow_run_ids: List[str] = field(default_factory=list)
    model_versions: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "FeatureVersion":
        return cls(**data)


@dataclass
class FeatureChangeRecord:
    """Record of a single feature change."""
    timestamp: str
    project_name: str
    change_type: str                   # added, removed, modified, threshold_changed
    feature_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    old_version_id: Optional[str] = None
    new_version_id: Optional[str] = None
    changed_by: str = "system"
    description: str = ""


class FeatureLineageTracker:
    """
    Tracks complete lineage from feature definition to model.
    
    Storage:
    - PostgreSQL: Feature versions and change audit
    - MLflow: Linked artifacts and metrics
    - LakeFS/Nessie: Data snapshots
    """
    
    def __init__(
        self,
        project_name: str,
        postgres_uri: Optional[str] = None,
        mlflow_tracking_uri: Optional[str] = None,
    ):
        self.project_name = project_name
        self.postgres_uri = postgres_uri or os.getenv(
            "FEAST_REGISTRY_URI",
            "postgresql://dagster:dagster@exp-postgres-dagster:5432/feast_registry"
        )
        self.mlflow_tracking_uri = mlflow_tracking_uri or os.getenv(
            "MLFLOW_TRACKING_URI",
            "http://mlflow:5000"
        )
        
        # Initialize storage
        self._init_storage()
    
    def _init_storage(self):
        """Initialize PostgreSQL tables for lineage tracking."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            conn.autocommit = True
            
            with conn.cursor() as cur:
                # Feature versions table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_versions (
                        version_id VARCHAR(64) PRIMARY KEY,
                        project_name VARCHAR(100) NOT NULL,
                        yaml_hash VARCHAR(64) NOT NULL,
                        yaml_content TEXT NOT NULL,
                        generated_sql_hash VARCHAR(64),
                        generated_python_hash VARCHAR(64),
                        source_table VARCHAR(200),
                        source_lakefs_commit VARCHAR(64),
                        source_nessie_commit VARCHAR(64),
                        output_table VARCHAR(200),
                        output_lakefs_commit VARCHAR(64),
                        output_nessie_commit VARCHAR(64),
                        output_row_count INTEGER,
                        feature_count INTEGER,
                        feature_names JSONB,
                        categorical_columns JSONB,
                        thresholds JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        mlflow_run_ids JSONB DEFAULT '[]',
                        model_versions JSONB DEFAULT '[]'
                    )
                """)
                
                # Feature change audit table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS feature_change_audit (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        project_name VARCHAR(100) NOT NULL,
                        change_type VARCHAR(50) NOT NULL,
                        feature_name VARCHAR(100) NOT NULL,
                        old_value JSONB,
                        new_value JSONB,
                        old_version_id VARCHAR(64),
                        new_version_id VARCHAR(64),
                        changed_by VARCHAR(100),
                        description TEXT
                    )
                """)
                
                # Index for fast lookups
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_feature_versions_project 
                    ON feature_versions(project_name, created_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_feature_audit_project 
                    ON feature_change_audit(project_name, timestamp DESC)
                """)
            
            conn.close()
            
        except Exception as e:
            print(f"[Lineage] Warning: Could not initialize storage: {e}")
    
    def compute_version_id(self, yaml_content: str, source_commit: str = "") -> str:
        """Compute deterministic version ID from content."""
        content = f"{yaml_content}:{source_commit}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def compute_hash(self, content: str) -> str:
        """Compute SHA256 hash of content."""
        return hashlib.sha256(content.encode()).hexdigest()
    
    def register_feature_version(
        self,
        yaml_path: str,
        generated_sql: str,
        generated_python: str,
        source_lakefs_commit: Optional[str] = None,
        source_nessie_commit: Optional[str] = None,
    ) -> FeatureVersion:
        """
        Register a new feature version.
        
        Returns:
            FeatureVersion with unique version_id
        """
        # Load and parse YAML
        with open(yaml_path, 'r') as f:
            yaml_content = f.read()
        
        yaml_data = yaml.safe_load(yaml_content)
        
        # Extract metadata from YAML
        project = yaml_data.get('project', {})
        model = yaml_data.get('model', {})
        features = yaml_data.get('features', {})
        
        # Extract thresholds from all feature groups
        thresholds = {}
        for group_name, group_data in features.items():
            for col in group_data.get('columns', []):
                if col.get('params'):
                    for param_name, param_value in col['params'].items():
                        thresholds[f"{col['name']}_{param_name}"] = param_value
        
        # Compute hashes
        yaml_hash = self.compute_hash(yaml_content)
        sql_hash = self.compute_hash(generated_sql)
        python_hash = self.compute_hash(generated_python)
        
        # Generate version ID
        version_id = self.compute_version_id(
            yaml_content, 
            source_lakefs_commit or source_nessie_commit or ""
        )
        
        # Build source and output table names
        source = project.get('source', {})
        output = project.get('output', {})
        source_table = f"{source.get('catalog')}.{source.get('schema')}.{source.get('table')}"
        output_table = f"{output.get('catalog')}.{output.get('schema')}.{output.get('table')}"
        
        # Create version object
        version = FeatureVersion(
            version_id=version_id,
            project_name=self.project_name,
            yaml_hash=yaml_hash,
            yaml_content=yaml_content,
            generated_sql_hash=sql_hash,
            generated_python_hash=python_hash,
            source_table=source_table,
            source_lakefs_commit=source_lakefs_commit,
            source_nessie_commit=source_nessie_commit,
            output_table=output_table,
            feature_count=len(model.get('feature_order', [])),
            feature_names=model.get('feature_order', []),
            categorical_columns=model.get('categorical_columns', []),
            thresholds=thresholds,
            created_at=datetime.now().isoformat(),
        )
        
        # Check for changes from previous version
        self._detect_and_record_changes(version)
        
        # Store in PostgreSQL
        self._store_version(version)
        
        return version
    
    def _detect_and_record_changes(self, new_version: FeatureVersion):
        """Detect changes from previous version and record them."""
        previous = self.get_latest_version()
        
        if previous is None:
            # First version - record all features as "added"
            for feature in new_version.feature_names:
                self._record_change(FeatureChangeRecord(
                    timestamp=datetime.now().isoformat(),
                    project_name=self.project_name,
                    change_type="added",
                    feature_name=feature,
                    new_value=None,
                    new_version_id=new_version.version_id,
                    description="Initial feature registration"
                ))
            return
        
        # Skip if same version
        if previous.yaml_hash == new_version.yaml_hash:
            return
        
        # Detect added features
        old_features = set(previous.feature_names)
        new_features = set(new_version.feature_names)
        
        for feature in new_features - old_features:
            self._record_change(FeatureChangeRecord(
                timestamp=datetime.now().isoformat(),
                project_name=self.project_name,
                change_type="added",
                feature_name=feature,
                old_version_id=previous.version_id,
                new_version_id=new_version.version_id,
            ))
        
        # Detect removed features
        for feature in old_features - new_features:
            self._record_change(FeatureChangeRecord(
                timestamp=datetime.now().isoformat(),
                project_name=self.project_name,
                change_type="removed",
                feature_name=feature,
                old_version_id=previous.version_id,
                new_version_id=new_version.version_id,
            ))
        
        # Detect threshold changes
        for key, new_val in new_version.thresholds.items():
            old_val = previous.thresholds.get(key)
            if old_val != new_val:
                self._record_change(FeatureChangeRecord(
                    timestamp=datetime.now().isoformat(),
                    project_name=self.project_name,
                    change_type="threshold_changed",
                    feature_name=key.rsplit('_', 1)[0],  # Extract feature name
                    old_value=old_val,
                    new_value=new_val,
                    old_version_id=previous.version_id,
                    new_version_id=new_version.version_id,
                    description=f"Threshold changed: {key} from {old_val} to {new_val}"
                ))
    
    def _record_change(self, change: FeatureChangeRecord):
        """Record a feature change in audit table."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO feature_change_audit 
                    (project_name, change_type, feature_name, old_value, new_value,
                     old_version_id, new_version_id, changed_by, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    change.project_name,
                    change.change_type,
                    change.feature_name,
                    json.dumps(change.old_value) if change.old_value else None,
                    json.dumps(change.new_value) if change.new_value else None,
                    change.old_version_id,
                    change.new_version_id,
                    change.changed_by,
                    change.description,
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Lineage] Warning: Could not record change: {e}")
    
    def _store_version(self, version: FeatureVersion):
        """Store feature version in PostgreSQL."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO feature_versions 
                    (version_id, project_name, yaml_hash, yaml_content,
                     generated_sql_hash, generated_python_hash,
                     source_table, source_lakefs_commit, source_nessie_commit,
                     output_table, feature_count, feature_names, 
                     categorical_columns, thresholds, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (version_id) DO UPDATE SET
                        mlflow_run_ids = feature_versions.mlflow_run_ids,
                        model_versions = feature_versions.model_versions
                """, (
                    version.version_id,
                    version.project_name,
                    version.yaml_hash,
                    version.yaml_content,
                    version.generated_sql_hash,
                    version.generated_python_hash,
                    version.source_table,
                    version.source_lakefs_commit,
                    version.source_nessie_commit,
                    version.output_table,
                    version.feature_count,
                    json.dumps(version.feature_names),
                    json.dumps(version.categorical_columns),
                    json.dumps(version.thresholds),
                    version.created_at,
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Lineage] Warning: Could not store version: {e}")
            raise
    
    def get_latest_version(self) -> Optional[FeatureVersion]:
        """Get the most recent feature version."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT version_id, project_name, yaml_hash, yaml_content,
                           generated_sql_hash, generated_python_hash,
                           source_table, source_lakefs_commit, source_nessie_commit,
                           output_table, output_lakefs_commit, output_nessie_commit,
                           output_row_count, feature_count, feature_names,
                           categorical_columns, thresholds, created_at,
                           mlflow_run_ids, model_versions
                    FROM feature_versions
                    WHERE project_name = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (self.project_name,))
                
                row = cur.fetchone()
                conn.close()
                
                if row is None:
                    return None
                
                return FeatureVersion(
                    version_id=row[0],
                    project_name=row[1],
                    yaml_hash=row[2],
                    yaml_content=row[3],
                    generated_sql_hash=row[4],
                    generated_python_hash=row[5],
                    source_table=row[6],
                    source_lakefs_commit=row[7],
                    source_nessie_commit=row[8],
                    output_table=row[9],
                    output_lakefs_commit=row[10],
                    output_nessie_commit=row[11],
                    output_row_count=row[12] or 0,
                    feature_count=row[13] or 0,
                    feature_names=row[14] or [],
                    categorical_columns=row[15] or [],
                    thresholds=row[16] or {},
                    created_at=row[17].isoformat() if row[17] else "",
                    mlflow_run_ids=row[18] or [],
                    model_versions=row[19] or [],
                )
                
        except Exception as e:
            print(f"[Lineage] Warning: Could not get latest version: {e}")
            return None
    
    def get_version(self, version_id: str) -> Optional[FeatureVersion]:
        """Get a specific feature version by ID."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT version_id, project_name, yaml_hash, yaml_content,
                           generated_sql_hash, generated_python_hash,
                           source_table, source_lakefs_commit, source_nessie_commit,
                           output_table, output_lakefs_commit, output_nessie_commit,
                           output_row_count, feature_count, feature_names,
                           categorical_columns, thresholds, created_at,
                           mlflow_run_ids, model_versions
                    FROM feature_versions
                    WHERE version_id = %s
                """, (version_id,))
                
                row = cur.fetchone()
                conn.close()
                
                if row is None:
                    return None
                
                return FeatureVersion(
                    version_id=row[0],
                    project_name=row[1],
                    yaml_hash=row[2],
                    yaml_content=row[3],
                    generated_sql_hash=row[4],
                    generated_python_hash=row[5],
                    source_table=row[6],
                    source_lakefs_commit=row[7],
                    source_nessie_commit=row[8],
                    output_table=row[9],
                    output_lakefs_commit=row[10],
                    output_nessie_commit=row[11],
                    output_row_count=row[12] or 0,
                    feature_count=row[13] or 0,
                    feature_names=row[14] or [],
                    categorical_columns=row[15] or [],
                    thresholds=row[16] or {},
                    created_at=row[17].isoformat() if row[17] else "",
                    mlflow_run_ids=row[18] or [],
                    model_versions=row[19] or [],
                )
                
        except Exception as e:
            print(f"[Lineage] Warning: Could not get version: {e}")
            return None
    
    def update_output_lineage(
        self,
        version_id: str,
        output_lakefs_commit: Optional[str] = None,
        output_nessie_commit: Optional[str] = None,
        output_row_count: int = 0,
    ):
        """Update version with output data lineage after training data creation."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE feature_versions
                    SET output_lakefs_commit = %s,
                        output_nessie_commit = %s,
                        output_row_count = %s
                    WHERE version_id = %s
                """, (output_lakefs_commit, output_nessie_commit, output_row_count, version_id))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Lineage] Warning: Could not update output lineage: {e}")
    
    def update_mlflow_run(
        self,
        version_id: str,
        mlflow_run_id: str,
        mlflow_experiment: Optional[str] = None,
    ):
        """Update version with MLflow run information."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                # Update mlflow_run_ids array
                cur.execute("""
                    UPDATE feature_versions
                    SET mlflow_run_ids = COALESCE(mlflow_run_ids, '[]'::jsonb) || %s::jsonb
                    WHERE version_id = %s
                """, (json.dumps([mlflow_run_id]), version_id))
            conn.commit()
            conn.close()
            
            # Also log to MLflow as tag
            try:
                import mlflow
                mlflow.set_tag("feature_version_id", version_id)
                if mlflow_experiment:
                    mlflow.set_tag("feature_experiment", mlflow_experiment)
            except:
                pass
                
        except Exception as e:
            print(f"[Lineage] Warning: Could not update MLflow run: {e}")
    
    def link_to_mlflow_run(self, version_id: str, mlflow_run_id: str):
        """Link feature version to MLflow run."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                # Append to mlflow_run_ids array
                cur.execute("""
                    UPDATE feature_versions
                    SET mlflow_run_ids = mlflow_run_ids || %s::jsonb
                    WHERE version_id = %s
                """, (json.dumps([mlflow_run_id]), version_id))
            conn.commit()
            conn.close()
            
            # Also log to MLflow
            self._log_to_mlflow(version_id, mlflow_run_id)
            
        except Exception as e:
            print(f"[Lineage] Warning: Could not link to MLflow: {e}")
    
    def link_to_model_version(self, version_id: str, model_version: str):
        """Link feature version to registered model version."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE feature_versions
                    SET model_versions = model_versions || %s::jsonb
                    WHERE version_id = %s
                """, (json.dumps([model_version]), version_id))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[Lineage] Warning: Could not link to model version: {e}")
    
    def _log_to_mlflow(self, version_id: str, mlflow_run_id: str):
        """Log feature version details to MLflow run."""
        import mlflow
        
        version = self.get_version(version_id)
        if version is None:
            return
        
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        
        with mlflow.start_run(run_id=mlflow_run_id):
            # Log feature version as parameter
            mlflow.log_param("feature_version_id", version_id)
            mlflow.log_param("feature_yaml_hash", version.yaml_hash)
            mlflow.log_param("feature_count", version.feature_count)
            
            # Log source lineage
            if version.source_lakefs_commit:
                mlflow.log_param("source_lakefs_commit", version.source_lakefs_commit)
            if version.source_nessie_commit:
                mlflow.log_param("source_nessie_commit", version.source_nessie_commit)
            
            # Log thresholds as parameters
            for key, value in version.thresholds.items():
                if isinstance(value, (int, float, str)):
                    mlflow.log_param(f"threshold_{key}", value)
            
            # Log YAML as artifact
            yaml_path = f"/tmp/feature_registry_{version_id}.yaml"
            with open(yaml_path, 'w') as f:
                f.write(version.yaml_content)
            mlflow.log_artifact(yaml_path, "feature_registry")
            
            # Log feature list
            features_path = f"/tmp/feature_list_{version_id}.json"
            with open(features_path, 'w') as f:
                json.dump({
                    "version_id": version_id,
                    "feature_order": version.feature_names,
                    "categorical_columns": version.categorical_columns,
                    "thresholds": version.thresholds,
                }, f, indent=2)
            mlflow.log_artifact(features_path, "feature_registry")
    
    def get_change_history(self, limit: int = 50) -> List[FeatureChangeRecord]:
        """Get feature change audit history."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT timestamp, project_name, change_type, feature_name,
                           old_value, new_value, old_version_id, new_version_id,
                           changed_by, description
                    FROM feature_change_audit
                    WHERE project_name = %s
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, (self.project_name, limit))
                
                rows = cur.fetchall()
                conn.close()
                
                return [
                    FeatureChangeRecord(
                        timestamp=row[0].isoformat() if row[0] else "",
                        project_name=row[1],
                        change_type=row[2],
                        feature_name=row[3],
                        old_value=json.loads(row[4]) if row[4] else None,
                        new_value=json.loads(row[5]) if row[5] else None,
                        old_version_id=row[6],
                        new_version_id=row[7],
                        changed_by=row[8] or "system",
                        description=row[9] or "",
                    )
                    for row in rows
                ]
                
        except Exception as e:
            print(f"[Lineage] Warning: Could not get change history: {e}")
            return []
    
    def get_model_features(self, model_version: str) -> Optional[FeatureVersion]:
        """Get the feature version used to train a specific model."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(self.postgres_uri)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT version_id
                    FROM feature_versions
                    WHERE model_versions @> %s::jsonb
                    LIMIT 1
                """, (json.dumps([model_version]),))
                
                row = cur.fetchone()
                conn.close()
                
                if row:
                    return self.get_version(row[0])
                return None
                
        except Exception as e:
            print(f"[Lineage] Warning: Could not get model features: {e}")
            return None
    
    def reproduce_features(self, version_id: str, output_dir: str = "."):
        """
        Reproduce exact feature code from a version.
        
        Use this to get the exact SQL/Python that was used for a model.
        """
        version = self.get_version(version_id)
        if version is None:
            raise ValueError(f"Version {version_id} not found")
        
        # Write YAML
        yaml_path = os.path.join(output_dir, f"{self.project_name}_v{version_id}.yaml")
        with open(yaml_path, 'w') as f:
            f.write(version.yaml_content)
        
        # Regenerate code from YAML
        from feature_registry.generator import FeatureGenerator
        
        generator = FeatureGenerator.from_yaml(yaml_path)
        
        sql_path = os.path.join(output_dir, f"{self.project_name}_v{version_id}.sql")
        with open(sql_path, 'w') as f:
            f.write(generator.generate_sql())
        
        python_path = os.path.join(output_dir, f"{self.project_name}_v{version_id}_transformer.py")
        with open(python_path, 'w') as f:
            f.write(generator.generate_python_transformer())
        
        print(f"Reproduced feature version {version_id}:")
        print(f"  YAML: {yaml_path}")
        print(f"  SQL: {sql_path}")
        print(f"  Python: {python_path}")
        
        return {
            "yaml": yaml_path,
            "sql": sql_path,
            "python": python_path,
        }