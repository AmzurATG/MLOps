"""
Feature Code Generator - Single Source of Truth

Reads feature registry YAML and generates:
1. SQL for batch feature engineering (Trino)
2. Python for real-time inference
3. Feast feature view definitions
4. Feature documentation

Usage:
    generator = FeatureGenerator.from_yaml("fraud_detection.yaml")

    # Generate SQL for training table
    sql = generator.generate_sql()

    # Generate Python transformer class
    python_code = generator.generate_python_transformer()

    # Generate Feast definitions
    feast_code = generator.generate_feast_definitions()

NOTE: Catalog (e.g., iceberg_dev) comes from TRINO_CATALOG environment variable,
not from the YAML file. This enables branch-based experimentation.
"""

import yaml
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path

# Get catalog from environment - this is the single source of truth for catalog name
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg_dev")


@dataclass
class FeatureColumn:
    """Single feature column definition."""
    name: str
    type: str
    logic: Optional[str] = None
    source_field: Optional[str] = None
    source: Optional[str] = None  # For primitives
    inputs: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None
    partition_by: Optional[str] = None
    order_by: Optional[str] = None
    window_days: Optional[int] = None
    default: Optional[Any] = None
    categorical: bool = False
    description: str = ""


@dataclass
class FeatureGroup:
    """Group of related features."""
    name: str
    entity: List[str]
    ttl_days: int
    online: bool
    columns: List[FeatureColumn]
    realtime_compute: bool = False


@dataclass
class StreamingAggregation:
    """Single aggregation in a window."""
    name: str
    function: str
    field: Optional[str] = None
    description: str = ""


@dataclass
class StreamingWindow:
    """Window configuration for streaming aggregations."""
    name: str
    duration_seconds: int
    type: str  # tumbling, hopping, session
    partition_by: str
    aggregations: List[StreamingAggregation] = field(default_factory=list)


@dataclass
class StreamingEnrichment:
    """Enrichment feature computed from raw fields."""
    name: str
    logic: str
    source_field: Optional[str] = None
    inputs: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None
    description: str = ""


@dataclass
class StreamingComputedFlag:
    """Computed flag derived from aggregations."""
    name: str
    logic: str
    source_field: Optional[str] = None
    inputs: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None
    cases: Optional[List[Dict[str, Any]]] = None
    conditions: Optional[List[Dict[str, Any]]] = None
    description: str = ""


@dataclass
class StreamingConfig:
    """Streaming feature configuration."""
    source_topic: str
    output_topic: str
    value_format: str = "JSON"
    source_schema: List[Dict[str, Any]] = field(default_factory=list)
    enrichment: List[StreamingEnrichment] = field(default_factory=list)
    windows: List[StreamingWindow] = field(default_factory=list)
    computed_flags: List[StreamingComputedFlag] = field(default_factory=list)
    redis_prefix: str = "feast:streaming:"
    redis_ttl_seconds: int = 3600
    redis_key_field: str = "customer_id"
    redis_output_fields: List[str] = field(default_factory=list)


@dataclass
class FeatureRegistry:
    """Complete feature registry for a project."""
    project_name: str
    version: str
    description: str
    source_table: str
    output_table: str
    timestamp_field: str
    entities: Dict[str, Dict[str, str]]
    feature_groups: Dict[str, FeatureGroup]
    feature_order: List[str]
    label: str
    categorical_columns: List[str]
    streaming: Optional[StreamingConfig] = None


class FeatureGenerator:
    """
    Generates code from feature registry YAML.
    """
    
    def __init__(self, registry: FeatureRegistry):
        self.registry = registry
        
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "FeatureGenerator":
        """Load feature registry from YAML file."""
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Parse entities
        entities = data.get('entities', {})
        
        # Parse feature groups
        feature_groups = {}
        for group_name, group_data in data.get('features', {}).items():
            columns = []
            for col_data in group_data.get('columns', []):
                columns.append(FeatureColumn(
                    name=col_data['name'],
                    type=col_data['type'],
                    logic=col_data.get('logic'),
                    source_field=col_data.get('source_field'),
                    source=col_data.get('source'),
                    inputs=col_data.get('inputs'),
                    params=col_data.get('params', {}),
                    partition_by=col_data.get('partition_by'),
                    order_by=col_data.get('order_by'),
                    window_days=col_data.get('window_days'),
                    default=col_data.get('default'),
                    categorical=col_data.get('categorical', False),
                    description=col_data.get('description', ''),
                ))
            
            feature_groups[group_name] = FeatureGroup(
                name=group_name,
                entity=group_data.get('entity', []),
                ttl_days=group_data.get('ttl_days', 90),
                online=group_data.get('online', True),
                columns=columns,
                realtime_compute=group_data.get('realtime_compute', False),
            )
        
        # Build registry
        project = data.get('project', {})
        model = data.get('model', {})
        source = project.get('source', {})
        output = project.get('output', {})

        # Use catalog from environment variable (TRINO_CATALOG), not from YAML
        # This enables branch-based experimentation without changing YAML
        catalog = TRINO_CATALOG

        # Parse streaming configuration
        streaming_config = None
        streaming_data = data.get('streaming')
        if streaming_data:
            # Parse enrichment features
            enrichment = []
            for e in streaming_data.get('enrichment', []):
                enrichment.append(StreamingEnrichment(
                    name=e['name'],
                    logic=e['logic'],
                    source_field=e.get('source_field'),
                    inputs=e.get('inputs'),
                    params=e.get('params'),
                    description=e.get('description', ''),
                ))

            # Parse windows
            windows = []
            for w in streaming_data.get('windows', []):
                aggregations = []
                for a in w.get('aggregations', []):
                    aggregations.append(StreamingAggregation(
                        name=a['name'],
                        function=a['function'],
                        field=a.get('field'),
                        description=a.get('description', ''),
                    ))
                windows.append(StreamingWindow(
                    name=w['name'],
                    duration_seconds=w['duration_seconds'],
                    type=w.get('type', 'tumbling'),
                    partition_by=w.get('partition_by', 'customer_id'),
                    aggregations=aggregations,
                ))

            # Parse computed flags
            computed_flags = []
            for cf in streaming_data.get('computed_flags', []):
                computed_flags.append(StreamingComputedFlag(
                    name=cf['name'],
                    logic=cf['logic'],
                    source_field=cf.get('source_field'),
                    inputs=cf.get('inputs'),
                    params=cf.get('params'),
                    cases=cf.get('cases'),
                    conditions=cf.get('conditions'),
                    description=cf.get('description', ''),
                ))

            # Parse redis config
            redis_config = streaming_data.get('redis', {})

            streaming_config = StreamingConfig(
                source_topic=streaming_data.get('source_topic', ''),
                output_topic=streaming_data.get('output_topic', ''),
                value_format=streaming_data.get('value_format', 'JSON'),
                source_schema=streaming_data.get('source_schema', []),
                enrichment=enrichment,
                windows=windows,
                computed_flags=computed_flags,
                redis_prefix=redis_config.get('prefix', 'feast:streaming:'),
                redis_ttl_seconds=redis_config.get('ttl_seconds', 3600),
                redis_key_field=redis_config.get('key_field', 'customer_id'),
                redis_output_fields=redis_config.get('output_fields', []),
            )

        registry = FeatureRegistry(
            project_name=project.get('name', 'unknown'),
            version=project.get('version', '1.0.0'),
            description=project.get('description', ''),
            source_table=f"{catalog}.{source.get('schema')}.{source.get('table')}",
            output_table=f"{catalog}.{output.get('schema')}.{output.get('table')}",
            timestamp_field=source.get('timestamp_field', 'event_timestamp'),
            entities=entities,
            feature_groups=feature_groups,
            feature_order=model.get('feature_order', []),
            label=model.get('label', 'label'),
            categorical_columns=model.get('categorical_columns', []),
            streaming=streaming_config,
        )

        return cls(registry)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SQL GENERATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def generate_sql(self) -> str:
        """Generate complete SQL for training table creation."""
        
        sql_parts = []
        
        # Header
        sql_parts.append(f"""-- ═══════════════════════════════════════════════════════════════════════════════
-- AUTO-GENERATED FROM: {self.registry.project_name} v{self.registry.version}
-- DO NOT EDIT DIRECTLY - Modify the feature registry YAML instead
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE {self.registry.output_table} AS
""")
        
        # CTE 1: Base features - primitives and simple transformations
        base_columns = self._generate_base_columns()
        sql_parts.append(f"""WITH base_features AS (
    SELECT 
        transaction_id,
        customer_id,
        {self.registry.timestamp_field},
        {self.registry.label},
{base_columns}
    FROM {self.registry.source_table}
    WHERE {self.registry.timestamp_field} IS NOT NULL
      AND customer_id IS NOT NULL
),
""")
        
        # CTE 2: Window features - window aggregations only
        window_columns = self._generate_window_columns()
        sql_parts.append(f"""window_features AS (
    SELECT 
        *,
{window_columns}
    FROM base_features
),
""")
        
        # CTE 3: Derived features - columns that depend on window results
        derived_columns = self._generate_derived_columns()
        sql_parts.append(f"""derived_features AS (
    SELECT 
        *,
{derived_columns}
    FROM window_features
)
""")
        
        # Final SELECT
        final_columns = self._generate_final_columns()
        sql_parts.append(f"""SELECT
    -- IDs
    transaction_id,
    customer_id,
    
{final_columns},
    
    -- Label
    {self.registry.label},
    
    -- Feast timestamp
    {self.registry.timestamp_field} as event_timestamp

FROM derived_features
ORDER BY customer_id, {self.registry.timestamp_field}
""")
        
        return '\n'.join(sql_parts)
    
    def _generate_base_columns(self) -> str:
        """Generate SQL for base/primitive columns."""
        lines = []
        
        # Primitives
        if 'primitives' in self.registry.feature_groups:
            for col in self.registry.feature_groups['primitives'].columns:
                if col.source:
                    lines.append(f"        {col.source} as {col.name}")
        
        # Calendar extractions
        if 'calendar' in self.registry.feature_groups:
            for col in self.registry.feature_groups['calendar'].columns:
                sql = self._logic_to_sql(col, 'base')
                if sql:
                    lines.append(f"        {sql} as {col.name}")
        
        # Address extractions
        if 'address' in self.registry.feature_groups:
            for col in self.registry.feature_groups['address'].columns:
                if col.logic == 'extract_country':
                    sql = self._generate_country_extraction_sql(col.source_field)
                    lines.append(f"        {sql} as {col.name}")
        
        return ',\n'.join(lines)
    
    def _generate_window_columns(self) -> str:
        """Generate SQL for window function columns (pure window functions only)."""
        lines = []
        
        # Window function logic types
        window_logic_types = {
            'row_number_minus_one', 'window_sum', 'running_avg', 
            'days_since_first', 'days_since_previous',
            'window_count', 'window_max', 'window_count_distinct',
            'cumsum_exclusive', 'cumavg_exclusive', 'window_avg',  # Added window_avg
        }
        
        # Behavioral window functions
        for group_name in ['customer_behavioral', 'customer_spending']:
            if group_name not in self.registry.feature_groups:
                continue
                
            group = self.registry.feature_groups[group_name]
            for col in group.columns:
                # Only include pure window functions
                if col.logic in window_logic_types:
                    sql = self._logic_to_sql(col, 'window')
                    if sql:
                        lines.append(f"        {sql} as {col.name}")
        
        # Address computed columns (mismatch, high_risk) - these use base columns
        if 'address' in self.registry.feature_groups:
            for col in self.registry.feature_groups['address'].columns:
                if col.logic in ['not_equal', 'in_list']:
                    sql = self._logic_to_sql(col, 'window')
                    if sql:
                        lines.append(f"        {sql} as {col.name}")
        
        return ',\n'.join(lines)
    
    def _generate_derived_columns(self) -> str:
        """Generate SQL for columns that depend on window results or need derivation."""
        lines = []
        
        # Logic types for derived stage (depend on window columns or simple transformations)
        derived_logic_types = {'equals_zero', 'less_than', 'in_list'}
        
        # Process customer_behavioral and customer_spending for derived columns
        for group_name in ['customer_behavioral', 'customer_spending']:
            if group_name not in self.registry.feature_groups:
                continue
                
            group = self.registry.feature_groups[group_name]
            for col in group.columns:
                if col.logic in derived_logic_types:
                    sql = self._logic_to_sql(col, 'derived')
                    if sql:
                        lines.append(f"        {sql} as {col.name}")
        
        # Process risk_indicators for columns that need derivation
        # (high_velocity, risky_payment, risky_category - NOT amount_vs_avg, is_high_amount, ip_prefix)
        if 'risk_indicators' in self.registry.feature_groups:
            group = self.registry.feature_groups['risk_indicators']
            for col in group.columns:
                if col.logic in derived_logic_types:
                    sql = self._logic_to_sql(col, 'derived')
                    if sql:
                        lines.append(f"        {sql} as {col.name}")
        
        return ',\n'.join(lines)
    
    def _generate_final_columns(self) -> str:
        """Generate SQL for final SELECT columns including derived features."""
        lines = []
        
        # All features in order
        for feature_name in self.registry.feature_order:
            col = self._find_column(feature_name)
            if col:
                # Check if it needs derivation in final SELECT
                if col.logic in ['safe_divide', 'ratio_above_threshold', 'split_first']:
                    sql = self._logic_to_sql(col, 'final')
                    lines.append(f"    {sql} as {feature_name}")
                else:
                    lines.append(f"    {feature_name}")
        
        return ',\n'.join(lines)
    
    def _logic_to_sql(self, col: FeatureColumn, stage: str) -> Optional[str]:
        """Convert feature logic to SQL."""
        
        logic = col.logic
        params = col.params or {}
        
        # Calendar features
        if logic == 'extract_day':
            return f"DAY({col.source_field})"
        elif logic == 'extract_month':
            return f"MONTH({col.source_field})"
        elif logic == 'is_weekend':
            return f"CASE WHEN EXTRACT(DOW FROM {col.source_field}) IN (5, 6) THEN 1 ELSE 0 END"
        elif logic == 'is_night':
            start = params.get('night_start', 22)
            end = params.get('night_end', 5)
            return f"CASE WHEN {col.source_field} >= {start} OR {col.source_field} <= {end} THEN 1 ELSE 0 END"
        
        # Address features
        elif logic == 'not_equal':
            inputs = col.inputs or []
            if len(inputs) >= 2:
                return f"CASE WHEN {inputs[0]} != {inputs[1]} THEN 1 ELSE 0 END"
        elif logic == 'in_list' and stage in ['window', 'base', 'derived']:
            values = params.get('values', [])
            values_str = ', '.join(f"'{v}'" for v in values)
            return f"CASE WHEN {col.source_field} IN ({values_str}) THEN 1 ELSE 0 END"
        
        # Behavioral window functions
        elif logic == 'row_number_minus_one':
            return f"ROW_NUMBER() OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by}) - 1"
        elif logic == 'cumsum_exclusive':
            return f"""SUM({col.source_field}) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) - {col.source_field}"""
        elif logic == 'cumavg_exclusive':
            return f"""CASE 
            WHEN ROW_NUMBER() OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by}) = 1 THEN 0.0
            ELSE (SUM({col.source_field}) OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - {col.source_field}) 
                 / NULLIF(ROW_NUMBER() OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by}) - 1, 0)
        END"""
        elif logic == 'days_since_first':
            return f"""DATE_DIFF('day', 
            MIN({col.order_by}) OVER (PARTITION BY {col.partition_by}), 
            {col.order_by})"""
        elif logic == 'days_since_previous':
            default = col.default or 9999.0
            return f"""CASE 
            WHEN LAG({col.order_by}) OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by}) IS NULL 
            THEN {default}
            ELSE DATE_DIFF('second', LAG({col.order_by}) OVER (PARTITION BY {col.partition_by} ORDER BY {col.order_by}), {col.order_by}) / 86400.0
        END"""
        elif logic == 'equals_zero' and stage in ['window', 'derived']:
            return f"CASE WHEN {col.source_field} = 0 THEN 1 ELSE 0 END"
        elif logic == 'less_than' and stage in ['window', 'derived']:
            threshold = params.get('threshold', 0)
            exclude_default = params.get('exclude_default', False)
            default = col.default or 9999.0
            if exclude_default:
                return f"CASE WHEN {col.source_field} < {default} AND {col.source_field} < {threshold} THEN 1 ELSE 0 END"
            return f"CASE WHEN {col.source_field} < {threshold} THEN 1 ELSE 0 END"
        
        # Spending window functions
        elif logic == 'window_sum':
            return f"""SUM({col.source_field}) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            RANGE BETWEEN INTERVAL '{col.window_days}' DAY PRECEDING AND CURRENT ROW)"""
        elif logic == 'window_count':
            return f"""COUNT(*) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            RANGE BETWEEN INTERVAL '{col.window_days}' DAY PRECEDING AND CURRENT ROW)"""
        elif logic == 'window_avg':
            return f"""AVG({col.source_field}) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            RANGE BETWEEN INTERVAL '{col.window_days}' DAY PRECEDING AND CURRENT ROW)"""
        elif logic == 'window_max':
            return f"""MAX({col.source_field}) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            RANGE BETWEEN INTERVAL '{col.window_days}' DAY PRECEDING AND CURRENT ROW)"""
        elif logic == 'window_count_distinct':
            return f"""COUNT(DISTINCT {col.source_field}) OVER (
            PARTITION BY {col.partition_by} 
            ORDER BY {col.order_by} 
            RANGE BETWEEN INTERVAL '{col.window_days}' DAY PRECEDING AND CURRENT ROW)"""
        
        # Risk indicators (final stage)
        elif logic == 'safe_divide' and stage == 'final':
            inputs = col.inputs or []
            default = col.default or 1.0
            if len(inputs) >= 2:
                return f"CASE WHEN {inputs[1]} > 0 THEN {inputs[0]} / {inputs[1]} ELSE {default} END"
        elif logic == 'ratio_above_threshold' and stage == 'final':
            inputs = col.inputs or []
            threshold = params.get('threshold', 3.0)
            if len(inputs) >= 2:
                return f"CASE WHEN {inputs[1]} > 0 AND ({inputs[0]} / {inputs[1]}) > {threshold} THEN 1 ELSE 0 END"
        elif logic == 'less_than' and stage == 'final':
            threshold = params.get('threshold', 0)
            exclude_default = params.get('exclude_default', False)
            default = col.default or 9999.0
            if exclude_default:
                return f"CASE WHEN {col.source_field} < {default} AND {col.source_field} < {threshold} THEN 1 ELSE 0 END"
            return f"CASE WHEN {col.source_field} < {threshold} THEN 1 ELSE 0 END"
        elif logic == 'in_list' and stage == 'final':
            values = params.get('values', [])
            values_str = ', '.join(f"'{v}'" for v in values)
            return f"CASE WHEN {col.source_field} IN ({values_str}) THEN 1 ELSE 0 END"
        elif logic == 'split_first' and stage == 'final':
            delimiter = params.get('delimiter', '.')
            return f"""CASE 
            WHEN POSITION('{delimiter}' IN {col.source_field}) > 0 
            THEN SUBSTR({col.source_field}, 1, POSITION('{delimiter}' IN {col.source_field}) - 1)
            ELSE {col.source_field}
        END"""
        
        return None
    
    def _generate_country_extraction_sql(self, field: str) -> str:
        """Generate SQL for country extraction from address."""
        return f"""CASE 
            WHEN {field} LIKE '%, NG%' THEN 'NG'
            WHEN {field} LIKE '%, PK%' THEN 'PK'
            WHEN {field} LIKE '%, RU%' THEN 'RU'
            WHEN {field} LIKE '%, BR%' THEN 'BR'
            WHEN {field} LIKE '%, GB%' THEN 'GB'
            WHEN {field} LIKE '%, US%' THEN 'US'
            WHEN {field} LIKE '%, IN%' THEN 'IN'
            ELSE SUBSTR({field}, LENGTH({field}) - 1)
        END"""
    
    def _find_column(self, name: str) -> Optional[FeatureColumn]:
        """Find a column by name across all feature groups."""
        for group in self.registry.feature_groups.values():
            for col in group.columns:
                if col.name == name:
                    return col
        return None
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PYTHON GENERATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def generate_python_transformer(self) -> str:
        """Generate Python transformer class."""
        
        code = f'''"""
Feature Transformer for {self.registry.project_name}
AUTO-GENERATED FROM YAML - DO NOT EDIT DIRECTLY

Version: {self.registry.version}
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime


class {self._to_class_name(self.registry.project_name)}Transformer:
    """
    Real-time feature transformer for {self.registry.project_name}.
    
    Computes derived features using IDENTICAL logic to SQL training pipeline.
    """
    
    # Feature order (must match training)
    FEATURE_ORDER = {self.registry.feature_order}
    
    # Categorical columns
    CATEGORICAL_COLUMNS = {self.registry.categorical_columns}
    
    # Risk indicator thresholds
    THRESHOLDS = {{
{self._generate_thresholds_dict()}
    }}
    
    def __init__(self, category_mappings: Optional[Dict[str, Dict[str, int]]] = None):
        """
        Initialize transformer.
        
        Args:
            category_mappings: Saved categorical encodings from training
        """
        self.category_mappings = category_mappings or {{}}
    
{self._generate_python_methods()}
    
    def transform(
        self,
        request_features: Dict[str, Any],
        feast_features: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Transform raw features into model-ready format.
        
        Args:
            request_features: Features from API request (primitives)
            feast_features: Features from Feast online store (aggregates)
        
        Returns:
            Tuple of (feature_dict, warnings)
        """
        warnings = []
        features = {{}}
        
        # Primitives from request
        for feat in ['amount', 'quantity', 'country', 'device_type', 'payment_method',
                     'category', 'account_age_days', 'tx_hour', 'tx_dayofweek']:
            features[feat] = request_features.get(feat, self._get_default(feat))
        
        # Aggregates from Feast
        aggregate_features = [
            'transactions_before', 'total_spend_before', 'avg_amount_before',
            'customer_tenure_days', 'days_since_last_tx', 'is_new_customer',
            'is_very_new_account', 'total_spend_7d', 'total_spend_30d',
            'tx_count_7d', 'tx_count_30d', 'avg_amount_30d', 'max_amount_30d',
            'num_countries_90d', 'num_payment_methods_30d'
        ]
        for feat in aggregate_features:
            val = feast_features.get(feat)
            if val is not None:
                features[feat] = val
            else:
                features[feat] = self._get_default(feat)
                warnings.append(f"{{feat}}: using default")
        
        # Compute calendar features
        calendar = self.compute_calendar_features(
            features['tx_hour'], 
            features['tx_dayofweek']
        )
        features.update(calendar)
        
        # Compute address features
        address = self.compute_address_features(
            request_features.get('shipping_country', features['country']),
            request_features.get('billing_country', features['country']),
        )
        features.update(address)
        
        # Compute risk indicators (real-time)
        risk = self.compute_risk_indicators(
            amount=features['amount'],
            avg_amount_30d=features['avg_amount_30d'],
            days_since_last_tx=features['days_since_last_tx'],
            payment_method=request_features.get('payment_method', 'unknown'),
            category=request_features.get('category', 'unknown'),
            ip_address=request_features.get('ip_address', '0.0.0.0'),
        )
        features.update(risk)
        
        # Encode categoricals
        for col in self.CATEGORICAL_COLUMNS:
            if col in features and col in self.category_mappings:
                original = features[col]
                encoded = self.category_mappings[col].get(str(original), -1)
                if encoded == -1:
                    warnings.append(f"{{col}}: unknown value '{{original}}'")
                    encoded = 0
                features[col] = encoded
        
        return features, warnings
    
    def to_feature_vector(self, features: Dict[str, Any]) -> List[Any]:
        """Assemble features in exact training order."""
        return [features.get(col, 0) for col in self.FEATURE_ORDER]
    
    def _get_default(self, feature: str) -> Any:
        """Get default value for missing features."""
        defaults = {{
            'amount': 0.0, 'quantity': 1, 'country': 'US', 'device_type': 'desktop',
            'payment_method': 'credit_card', 'category': 'Electronics',
            'account_age_days': 0, 'tx_hour': 12, 'tx_dayofweek': 1,
            'transactions_before': 0, 'total_spend_before': 0.0,
            'avg_amount_before': 0.0, 'customer_tenure_days': 0,
            'days_since_last_tx': 9999.0, 'is_new_customer': 1,
            'is_very_new_account': 1, 'total_spend_7d': 0.0, 'total_spend_30d': 0.0,
            'tx_count_7d': 0, 'tx_count_30d': 0, 'avg_amount_30d': 0.0,
            'max_amount_30d': 0.0, 'num_countries_90d': 1, 'num_payment_methods_30d': 1,
            'transaction_day': 15, 'transaction_month': 6, 'is_weekend': 0, 'is_night': 0,
            'shipping_country': 'US', 'billing_country': 'US', 'address_mismatch': 0,
            'high_risk_shipping': 0, 'amount_vs_avg': 1.0, 'is_high_amount_vs_hist': 0,
            'high_velocity': 0, 'risky_payment': 0, 'risky_category': 0, 'ip_prefix': '0',
        }}
        return defaults.get(feature, 0)
'''
        return code
    
    def _generate_python_methods(self) -> str:
        """Generate Python methods for feature computation."""
        
        # Get parameters from YAML
        calendar_group = self.registry.feature_groups.get('calendar')
        night_params = {}
        if calendar_group:
            for col in calendar_group.columns:
                if col.name == 'is_night':
                    night_params = col.params or {}
        
        risk_group = self.registry.feature_groups.get('risk_indicators')
        high_amount_threshold = 3.0
        velocity_threshold = 0.25
        risky_payments = ['credit_card', 'wallet']
        risky_categories = ['Electronics', 'Luxury', 'Digital']
        high_risk_countries = ['NG', 'PK', 'RU', 'BR']
        
        if risk_group:
            for col in risk_group.columns:
                params = col.params or {}
                if col.name == 'is_high_amount_vs_hist':
                    high_amount_threshold = params.get('threshold', 3.0)
                elif col.name == 'high_velocity':
                    velocity_threshold = params.get('threshold', 0.25)
                elif col.name == 'risky_payment':
                    risky_payments = params.get('values', risky_payments)
                elif col.name == 'risky_category':
                    risky_categories = params.get('values', risky_categories)
        
        address_group = self.registry.feature_groups.get('address')
        if address_group:
            for col in address_group.columns:
                if col.name == 'high_risk_shipping':
                    high_risk_countries = (col.params or {}).get('values', high_risk_countries)
        
        night_start = night_params.get('night_start', 22)
        night_end = night_params.get('night_end', 5)
        
        return f'''
    def compute_calendar_features(self, tx_hour: int, tx_dayofweek: int) -> Dict[str, Any]:
        """
        Compute calendar features.
        
        SQL equivalents:
        - is_weekend: CASE WHEN EXTRACT(DOW FROM date) IN (5, 6) THEN 1 ELSE 0 END
        - is_night: CASE WHEN hour >= {night_start} OR hour <= {night_end} THEN 1 ELSE 0 END
        """
        return {{
            'transaction_day': datetime.now().day,
            'transaction_month': datetime.now().month,
            'is_weekend': 1 if tx_dayofweek in [5, 6] else 0,
            'is_night': 1 if (tx_hour >= {night_start} or tx_hour <= {night_end}) else 0,
        }}
    
    def compute_address_features(
        self,
        shipping_country: str,
        billing_country: str,
    ) -> Dict[str, Any]:
        """
        Compute address features.
        
        SQL equivalents:
        - address_mismatch: CASE WHEN ship != bill THEN 1 ELSE 0 END
        - high_risk_shipping: CASE WHEN ship IN {high_risk_countries} THEN 1 ELSE 0 END
        """
        return {{
            'shipping_country': shipping_country,
            'billing_country': billing_country,
            'address_mismatch': 1 if shipping_country != billing_country else 0,
            'high_risk_shipping': 1 if shipping_country in {high_risk_countries} else 0,
        }}
    
    def compute_risk_indicators(
        self,
        amount: float,
        avg_amount_30d: float,
        days_since_last_tx: float,
        payment_method: str,
        category: str,
        ip_address: str,
    ) -> Dict[str, Any]:
        """
        Compute risk indicators - MUST match SQL logic exactly.
        
        SQL equivalents documented inline.
        """
        # SQL: CASE WHEN avg > 0 THEN amount / avg ELSE 1.0 END
        amount_vs_avg = amount / avg_amount_30d if avg_amount_30d > 0 else 1.0
        
        # SQL: CASE WHEN avg > 0 AND (amount / avg) > {high_amount_threshold} THEN 1 ELSE 0 END
        is_high_amount = 1 if (avg_amount_30d > 0 and amount_vs_avg > {high_amount_threshold}) else 0
        
        # SQL: CASE WHEN days < 9999 AND days < {velocity_threshold} THEN 1 ELSE 0 END
        high_velocity = 1 if (days_since_last_tx < 9999 and days_since_last_tx < {velocity_threshold}) else 0
        
        # SQL: CASE WHEN payment IN {risky_payments} THEN 1 ELSE 0 END
        risky_payment = 1 if payment_method in {risky_payments} else 0
        
        # SQL: CASE WHEN category IN {risky_categories} THEN 1 ELSE 0 END
        risky_category = 1 if category in {risky_categories} else 0
        
        # SQL: SUBSTR(ip, 1, POSITION('.' IN ip) - 1)
        ip_prefix = ip_address.split('.')[0] if '.' in ip_address else ip_address
        
        return {{
            'amount_vs_avg': amount_vs_avg,
            'is_high_amount_vs_hist': is_high_amount,
            'high_velocity': high_velocity,
            'risky_payment': risky_payment,
            'risky_category': risky_category,
            'ip_prefix': ip_prefix,
        }}'''
    
    def _generate_thresholds_dict(self) -> str:
        """Generate thresholds dictionary from YAML."""
        lines = []
        
        risk_group = self.registry.feature_groups.get('risk_indicators')
        if risk_group:
            for col in risk_group.columns:
                params = col.params or {}
                if params:
                    for key, value in params.items():
                        if isinstance(value, list):
                            lines.append(f"        '{col.name}_{key}': {value},")
                        else:
                            lines.append(f"        '{col.name}_{key}': {value},")
        
        return '\n'.join(lines) if lines else "        # No thresholds defined"
    
    def _to_class_name(self, name: str) -> str:
        """Convert project name to class name."""
        return ''.join(word.capitalize() for word in name.split('_'))
    
    # ═══════════════════════════════════════════════════════════════════════════
    # FEAST GENERATION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def generate_feast_definitions(self) -> str:
        """Generate Feast feature view definitions."""
        
        code = f'''"""
Feast Feature Definitions for {self.registry.project_name}
AUTO-GENERATED FROM YAML - DO NOT EDIT DIRECTLY

Version: {self.registry.version}
"""

from datetime import timedelta
from feast import Entity, FeatureView, Field, FeatureService, ValueType
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import TrinoSource
from feast.types import Float64, Int64, String

# ═══════════════════════════════════════════════════════════════════════════════
# ENTITIES
# ═══════════════════════════════════════════════════════════════════════════════

'''
        
        # Map YAML types to Feast ValueType
        type_mapping = {
            'string': 'ValueType.STRING',
            'int': 'ValueType.INT64',
            'int64': 'ValueType.INT64',
            'float': 'ValueType.FLOAT',
            'double': 'ValueType.DOUBLE',
            'bool': 'ValueType.BOOL',
        }

        # Generate entities
        for entity_name, entity_def in self.registry.entities.items():
            entity_type = entity_def.get('type', 'string').lower()
            value_type = type_mapping.get(entity_type, 'ValueType.STRING')
            code += f'''{entity_name} = Entity(
    name="{entity_def.get('join_key', entity_name)}",
    join_keys=["{entity_def.get('join_key', entity_name)}"],
    value_type={value_type},
    description="{entity_def.get('description', '')}"
)

'''
        
        # Data source - points to OUTPUT table which has pre-computed features
        # The output table is created by SQL generation and contains all window/derived features
        code += f'''# ═══════════════════════════════════════════════════════════════════════════════
# DATA SOURCE
# ═══════════════════════════════════════════════════════════════════════════════

feature_source = TrinoSource(
    name="{self.registry.project_name}_source",
    table="{self.registry.output_table}",
    timestamp_field="event_timestamp",
)

# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE VIEWS
# ═══════════════════════════════════════════════════════════════════════════════

'''
        
        # Generate feature views
        for group_name, group in self.registry.feature_groups.items():
            entity_refs = ', '.join(group.entity)
            
            # Generate schema fields
            fields = []
            for col in group.columns:
                feast_type = self._to_feast_type(col.type)
                fields.append(f'        Field(name="{col.name}", dtype={feast_type}),')
            
            fields_str = '\n'.join(fields)
            
            code += f'''{group_name}_fv = FeatureView(
    name="{group_name}",
    entities=[{entity_refs}],
    ttl=timedelta(days={group.ttl_days}),
    schema=[
{fields_str}
    ],
    source=feature_source,
    online={group.online},
)

'''
        
        # Feature service
        fv_names = ', '.join(f'{name}_fv' for name in self.registry.feature_groups.keys())
        code += f'''# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE SERVICE
# ═══════════════════════════════════════════════════════════════════════════════

{self.registry.project_name}_service = FeatureService(
    name="{self.registry.project_name}_v1",
    features=[{fv_names}],
    description="{self.registry.description}"
)
'''
        
        return code
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # KSQLDB GENERATION
    # ═══════════════════════════════════════════════════════════════════════════════

    def generate_ksqldb_streams(self) -> str:
        """Generate ksqlDB source and enriched stream definitions."""
        if not self.registry.streaming:
            return "-- No streaming configuration found\n"

        streaming = self.registry.streaming
        lines = []

        # Header
        lines.append(f"""-- =============================================================================
-- KSQLDB STREAM DEFINITIONS
-- AUTO-GENERATED FROM: {self.registry.project_name} v{self.registry.version}
-- DO NOT EDIT DIRECTLY - Modify the feature registry YAML instead
-- =============================================================================
""")

        # Source stream
        lines.append("-- Source stream from Debezium CDC")
        lines.append("CREATE STREAM IF NOT EXISTS transactions_raw (")

        schema_lines = []
        for col in streaming.source_schema:
            key_suffix = " KEY" if col.get('key') else ""
            schema_lines.append(f"    {col['name']} {col['type']}{key_suffix}")
        lines.append(",\n".join(schema_lines))

        lines.append(f""") WITH (
    KAFKA_TOPIC = '{streaming.source_topic}',
    VALUE_FORMAT = '{streaming.value_format}',
    TIMESTAMP = 'transaction_date',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss'
);
""")

        # Enriched stream
        lines.append("-- Enriched stream with computed fields")
        lines.append("CREATE STREAM IF NOT EXISTS transactions_enriched AS")
        lines.append("SELECT")

        # Pass through source columns
        source_cols = [col['name'] for col in streaming.source_schema]
        for col in source_cols:
            lines.append(f"    {col},")

        # Enrichment columns
        lines.append("")
        lines.append("    -- Enrichment features")
        for enrichment in streaming.enrichment:
            sql = self._enrichment_to_ksql(enrichment)
            lines.append(f"    {sql} AS {enrichment.name},")

        # Remove trailing comma from last line
        lines[-1] = lines[-1].rstrip(",")

        lines.append("""
FROM transactions_raw
EMIT CHANGES;
""")

        return "\n".join(lines)

    def generate_ksqldb_aggregations(self) -> str:
        """Generate ksqlDB windowed aggregation tables."""
        if not self.registry.streaming:
            return "-- No streaming configuration found\n"

        streaming = self.registry.streaming
        lines = []

        # Header
        lines.append(f"""-- =============================================================================
-- KSQLDB WINDOWED AGGREGATION TABLES
-- AUTO-GENERATED FROM: {self.registry.project_name} v{self.registry.version}
-- DO NOT EDIT DIRECTLY - Modify the feature registry YAML instead
-- =============================================================================
""")

        # Generate tables for each window
        for window in streaming.windows:
            window_size = self._seconds_to_ksql_duration(window.duration_seconds)

            lines.append(f"-- {window.name.upper()} ({window_size} window)")
            lines.append(f"CREATE TABLE IF NOT EXISTS {window.name} AS")
            lines.append("SELECT")
            lines.append(f"    {window.partition_by},")
            lines.append("")

            # Aggregations
            for agg in window.aggregations:
                sql = self._aggregation_to_ksql(agg)
                lines.append(f"    {sql} AS {agg.name},")

            # Window boundaries
            lines.append("")
            lines.append("    WINDOWSTART AS window_start,")
            lines.append("    WINDOWEND AS window_end")

            lines.append(f"""
FROM transactions_enriched
WINDOW TUMBLING (SIZE {window_size})
GROUP BY {window.partition_by}
EMIT CHANGES;
""")

        return "\n".join(lines)

    def generate_ksqldb_output(self) -> str:
        """Generate ksqlDB feature output stream."""
        if not self.registry.streaming:
            return "-- No streaming configuration found\n"

        streaming = self.registry.streaming
        lines = []

        # Header
        lines.append(f"""-- =============================================================================
-- KSQLDB FEATURE OUTPUT STREAM
-- AUTO-GENERATED FROM: {self.registry.project_name} v{self.registry.version}
-- DO NOT EDIT DIRECTLY - Modify the feature registry YAML instead
-- =============================================================================
""")

        # Build join aliases
        window_aliases = []
        for i, w in enumerate(streaming.windows):
            alias = f"v{i+1}"
            window_aliases.append((w.name, alias))

        lines.append("-- Streaming features output")
        lines.append("CREATE STREAM IF NOT EXISTS streaming_features AS")
        lines.append("SELECT")
        lines.append("    -- Transaction identifiers")
        lines.append("    t.transaction_id AS transaction_id,")
        lines.append("    t.customer_id AS customer_id,")
        lines.append("    t.transaction_amount AS amount,")
        lines.append("")

        # Add aggregation columns from each window
        for window_name, alias in window_aliases:
            window = next(w for w in streaming.windows if w.name == window_name)
            lines.append(f"    -- {window_name} features")
            for agg in window.aggregations:
                default = "0" if agg.function == "count" else "0.0"
                lines.append(f"    COALESCE({alias}.{agg.name}, {default}) AS {agg.name},")
            lines.append("")

        # Computed flags
        lines.append("    -- Computed flags")
        for flag in streaming.computed_flags:
            sql = self._computed_flag_to_ksql(flag, window_aliases)
            lines.append(f"    {sql} AS {flag.name},")
        lines.append("")

        # Enriched fields
        lines.append("    -- Enriched stream fields")
        for enrichment in streaming.enrichment:
            lines.append(f"    t.{enrichment.name} AS {enrichment.name},")

        lines.append("    t.ROWTIME AS event_timestamp")
        lines.append("")

        # FROM clause with JOINs
        lines.append("FROM transactions_enriched t")
        for window_name, alias in window_aliases:
            window = next(w for w in streaming.windows if w.name == window_name)
            lines.append(f"LEFT JOIN {window_name} {alias}")
            lines.append(f"    ON t.{window.partition_by} = {alias}.{window.partition_by}")

        lines.append("EMIT CHANGES;")
        lines.append("")

        # Output to Kafka topic
        lines.append(f"""-- Output to Kafka topic for Redis consumer
CREATE STREAM IF NOT EXISTS {self.registry.project_name}_streaming_features
WITH (
    KAFKA_TOPIC = '{streaming.output_topic}',
    VALUE_FORMAT = '{streaming.value_format}',
    PARTITIONS = 3,
    REPLICAS = 1
) AS
SELECT *
FROM streaming_features
EMIT CHANGES;
""")

        return "\n".join(lines)

    def generate_ksqldb_all(self) -> str:
        """Generate all ksqlDB SQL files combined."""
        parts = [
            self.generate_ksqldb_streams(),
            self.generate_ksqldb_aggregations(),
            self.generate_ksqldb_output(),
        ]
        return "\n\n".join(parts)

    def generate_streaming_consumer_config(self) -> Dict[str, Any]:
        """Generate configuration for the streaming consumer."""
        if not self.registry.streaming:
            return {}

        streaming = self.registry.streaming

        # Build field mapping
        fields = {}
        for window in streaming.windows:
            for agg in window.aggregations:
                default = 0 if agg.function == "count" else 0.0
                fields[agg.name] = {"type": agg.function, "default": default}

        for flag in streaming.computed_flags:
            fields[flag.name] = {"type": "computed", "default": 0}

        for enrichment in streaming.enrichment:
            fields[enrichment.name] = {"type": "enrichment", "default": ""}

        return {
            "source_topic": streaming.source_topic,
            "output_topic": streaming.output_topic,
            "redis": {
                "prefix": streaming.redis_prefix,
                "ttl_seconds": streaming.redis_ttl_seconds,
                "key_field": streaming.redis_key_field,
            },
            "fields": fields,
            "output_fields": streaming.redis_output_fields,
        }

    def _enrichment_to_ksql(self, enrichment: StreamingEnrichment) -> str:
        """Convert enrichment feature to ksqlDB SQL."""
        logic = enrichment.logic
        params = enrichment.params or {}

        if logic == "extract_last_part":
            field = enrichment.source_field
            delimiter = params.get("delimiter", ",")
            return f"""CASE
        WHEN {field} IS NOT NULL AND LEN({field}) > 0
        THEN TRIM(SPLIT({field}, '{delimiter}')[ARRAY_LENGTH(SPLIT({field}, '{delimiter}')) - 1])
        ELSE 'Unknown'
    END"""

        elif logic == "not_equal":
            inputs = enrichment.inputs or []
            if len(inputs) >= 2:
                return f"""CASE
        WHEN {inputs[0]} IS NOT NULL
         AND {inputs[1]} IS NOT NULL
         AND {inputs[0]} != {inputs[1]}
        THEN 1
        ELSE 0
    END"""

        elif logic == "hour_in_range":
            field = enrichment.source_field
            start = params.get("start", 22)
            end = params.get("end", 5)
            return f"""CASE
        WHEN {field} >= {start} OR {field} < {end}
        THEN 1
        ELSE 0
    END"""

        elif logic == "dayofweek_in":
            days = params.get("days", [1, 7])
            days_str = ", ".join(str(d) for d in days)
            return f"""CASE
        WHEN DAYOFWEEK(ROWTIME) IN ({days_str})
        THEN 1
        ELSE 0
    END"""

        elif logic == "in_list":
            field = enrichment.source_field
            values = params.get("values", [])
            values_str = ", ".join(f"'{v}'" for v in values)
            return f"""CASE
        WHEN {field} IN ({values_str})
        THEN 1
        ELSE 0
    END"""

        elif logic == "split_first":
            field = enrichment.source_field
            delimiter = params.get("delimiter", ".")
            return f"""CASE
        WHEN {field} IS NOT NULL AND LEN({field}) > 0
        THEN SPLIT({field}, '{delimiter}')[1]
        ELSE '0'
    END"""

        return f"-- Unknown logic: {logic}"

    def _aggregation_to_ksql(self, agg: StreamingAggregation) -> str:
        """Convert aggregation to ksqlDB SQL."""
        func = agg.function.upper()
        field = agg.field

        if func == "COUNT":
            return "COUNT(*)"
        elif func == "COUNT_DISTINCT":
            return f"COUNT_DISTINCT({field})"
        elif func in ["SUM", "AVG", "MAX", "MIN"]:
            return f"{func}({field})"

        return f"-- Unknown function: {func}"

    def _computed_flag_to_ksql(
        self, flag: StreamingComputedFlag, window_aliases: List[tuple]
    ) -> str:
        """Convert computed flag to ksqlDB SQL."""
        logic = flag.logic
        params = flag.params or {}

        # Helper to get window alias for a field
        def get_field_ref(field_name: str) -> str:
            for window_name, alias in window_aliases:
                window = next(
                    (w for w in self.registry.streaming.windows if w.name == window_name),
                    None
                )
                if window:
                    for agg in window.aggregations:
                        if agg.name == field_name:
                            return f"COALESCE({alias}.{field_name}, 0)"
            return field_name

        if logic == "case_threshold" and flag.cases:
            case_lines = ["CASE"]
            for case in flag.cases:
                if "condition" in case:
                    # Replace field references with COALESCE versions
                    condition = case["condition"]
                    for window in self.registry.streaming.windows:
                        for agg in window.aggregations:
                            if agg.name in condition:
                                condition = condition.replace(
                                    agg.name, get_field_ref(agg.name)
                                )
                    case_lines.append(f"        WHEN {condition} THEN {case['value']}")
                elif "default" in case:
                    default_expr = case["default"]
                    for window in self.registry.streaming.windows:
                        for agg in window.aggregations:
                            if agg.name in default_expr:
                                default_expr = default_expr.replace(
                                    agg.name, get_field_ref(agg.name)
                                )
                    case_lines.append(f"        ELSE {default_expr}")
            case_lines.append("    END")
            return "\n".join(case_lines)

        elif logic == "greater_than":
            field = flag.source_field
            threshold = params.get("threshold", 0)
            return f"""CASE
        WHEN {get_field_ref(field)} > {threshold} THEN 1
        ELSE 0
    END"""

        elif logic == "or_conditions" and flag.conditions:
            conditions = []
            for cond in flag.conditions:
                field = get_field_ref(cond["field"])
                op = cond["operator"]
                val = cond["value"]
                conditions.append(f"{field} {op} {val}")
            cond_str = "\n          OR ".join(conditions)
            return f"""CASE
        WHEN {cond_str}
        THEN 1
        ELSE 0
    END"""

        elif logic == "ratio_above_threshold":
            inputs = flag.inputs or []
            threshold = params.get("threshold", 3.0)
            if len(inputs) >= 2:
                amt = "t.transaction_amount"
                avg_field = get_field_ref(inputs[1])
                return f"""CASE
        WHEN {avg_field} > 0
         AND {amt} > {avg_field} * {threshold}
        THEN 1
        ELSE 0
    END"""

        return f"-- Unknown logic: {logic}"

    def _seconds_to_ksql_duration(self, seconds: int) -> str:
        """Convert seconds to ksqlDB duration string."""
        if seconds >= 86400 and seconds % 86400 == 0:
            return f"{seconds // 86400} HOURS" if seconds == 86400 else f"{seconds // 86400 * 24} HOURS"
        elif seconds >= 3600 and seconds % 3600 == 0:
            return f"{seconds // 3600} HOUR{'S' if seconds >= 7200 else ''}"
        elif seconds >= 60 and seconds % 60 == 0:
            return f"{seconds // 60} MINUTES"
        else:
            return f"{seconds} SECONDS"

    # ═══════════════════════════════════════════════════════════════════════════════
    # ALIAS METHODS (for cleaner API)
    # ═══════════════════════════════════════════════════════════════════════════════

    def generate_python(self) -> str:
        """Alias for generate_python_transformer()."""
        return self.generate_python_transformer()

    def generate_feast(self) -> str:
        """Alias for generate_feast_definitions()."""
        return self.generate_feast_definitions()

    def generate_ksqldb(self) -> str:
        """Alias for generate_ksqldb_all()."""
        return self.generate_ksqldb_all()

    def _to_feast_type(self, type_str: str) -> str:
        """Convert YAML type to Feast type."""
        type_map = {
            'float64': 'Float64',
            'int64': 'Int64',
            'string': 'String',
        }
        return type_map.get(type_str, 'String')


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    """CLI for code generation."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='Generate feature code from YAML')
    parser.add_argument('yaml_file', help='Path to feature registry YAML')
    parser.add_argument('--sql', action='store_true', help='Generate SQL')
    parser.add_argument('--python', action='store_true', help='Generate Python transformer')
    parser.add_argument('--feast', action='store_true', help='Generate Feast definitions')
    parser.add_argument('--ksqldb', action='store_true', help='Generate ksqlDB SQL files')
    parser.add_argument('--streaming-config', action='store_true', help='Generate streaming consumer config JSON')
    parser.add_argument('--all', action='store_true', help='Generate all (SQL, Python, Feast)')
    parser.add_argument('--all-streaming', action='store_true', help='Generate all streaming artifacts')
    parser.add_argument('--output-dir', '-o', default='.', help='Output directory')

    args = parser.parse_args()

    generator = FeatureGenerator.from_yaml(args.yaml_file)

    os.makedirs(args.output_dir, exist_ok=True)

    if args.sql or args.all:
        sql = generator.generate_sql()
        path = os.path.join(args.output_dir, f'{generator.registry.project_name}_features.sql')
        with open(path, 'w') as f:
            f.write(sql)
        print(f'Generated: {path}')

    if args.python or args.all:
        python_code = generator.generate_python_transformer()
        path = os.path.join(args.output_dir, f'{generator.registry.project_name}_transformer.py')
        with open(path, 'w') as f:
            f.write(python_code)
        print(f'Generated: {path}')

    if args.feast or args.all:
        feast_code = generator.generate_feast_definitions()
        path = os.path.join(args.output_dir, f'{generator.registry.project_name}_feast.py')
        with open(path, 'w') as f:
            f.write(feast_code)
        print(f'Generated: {path}')

    # ksqlDB generation
    if args.ksqldb or args.all_streaming:
        if generator.registry.streaming is None:
            print('Warning: No streaming section in YAML, skipping ksqlDB generation')
        else:
            # Create ksqldb output directory
            ksqldb_dir = os.path.join(args.output_dir, 'ksqldb')
            os.makedirs(ksqldb_dir, exist_ok=True)

            # Generate individual files
            streams_sql = generator.generate_ksqldb_streams()
            path = os.path.join(ksqldb_dir, '01_streams.sql')
            with open(path, 'w') as f:
                f.write(streams_sql)
            print(f'Generated: {path}')

            agg_sql = generator.generate_ksqldb_aggregations()
            path = os.path.join(ksqldb_dir, '02_aggregations.sql')
            with open(path, 'w') as f:
                f.write(agg_sql)
            print(f'Generated: {path}')

            output_sql = generator.generate_ksqldb_output()
            path = os.path.join(ksqldb_dir, '03_feature_output.sql')
            with open(path, 'w') as f:
                f.write(output_sql)
            print(f'Generated: {path}')

            # Generate combined file
            all_sql = generator.generate_ksqldb_all()
            path = os.path.join(ksqldb_dir, 'all_ksqldb.sql')
            with open(path, 'w') as f:
                f.write(all_sql)
            print(f'Generated: {path}')

    # Streaming consumer config
    if args.streaming_config or args.all_streaming:
        if generator.registry.streaming is None:
            print('Warning: No streaming section in YAML, skipping streaming config generation')
        else:
            config = generator.generate_streaming_consumer_config()
            path = os.path.join(args.output_dir, f'{generator.registry.project_name}_streaming_config.json')
            with open(path, 'w') as f:
                json.dump(config, f, indent=2)
            print(f'Generated: {path}')


if __name__ == '__main__':
    main()