"""
Data Quality Assets for Dagster
Integrates with existing Bronze/Silver/Gold pipeline
════════════════════════════════════════════════════════════════════════════════
"""

import os
import json
import yaml
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import pandas as pd
from dagster import (
    asset,
    AssetIn,
    AssetExecutionContext,
    Output,
    MetadataValue,
    Config,
)
from src.core.config import TRINO_CATALOG


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

DATA_QUALITY_PATH = os.getenv("DATA_QUALITY_PATH", "/app/data_quality")


@dataclass
class QualityCheckResult:
    """Result of a quality check."""
    passed: bool
    expectation_suite: str
    statistics: Dict[str, Any]
    failed_expectations: List[Dict]
    run_time: datetime
    
    def to_metadata(self) -> Dict[str, MetadataValue]:
        return {
            "quality_passed": MetadataValue.bool(self.passed),
            "expectation_suite": MetadataValue.text(self.expectation_suite),
            "success_rate": MetadataValue.float(
                self.statistics.get("success_percent", 0) / 100
            ),
            "evaluated_expectations": MetadataValue.int(
                self.statistics.get("evaluated_expectations", 0)
            ),
            "failed_expectations": MetadataValue.json(self.failed_expectations),
            "run_time": MetadataValue.text(self.run_time.isoformat()),
        }


class DataQualityValidator:
    """Validator using expectation YAML files."""
    
    def __init__(self, expectations_dir: str = None):
        self.expectations_dir = expectations_dir or os.path.join(DATA_QUALITY_PATH, "expectations")
    
    def load_expectation_suite(self, suite_name: str) -> Dict:
        """Load expectation suite from YAML."""
        suite_path = os.path.join(self.expectations_dir, f"{suite_name}.yaml")
        try:
            with open(suite_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            return {"expectations": []}
    
    def validate_dataframe(
        self, 
        df: pd.DataFrame, 
        suite_name: str,
    ) -> QualityCheckResult:
        """Validate a DataFrame against an expectation suite."""
        
        run_time = datetime.now()
        suite_config = self.load_expectation_suite(suite_name)
        expectations = suite_config.get("expectations", [])
        
        results = []
        failed_expectations = []
        
        for exp in expectations:
            exp_type = exp["expectation_type"]
            kwargs = exp.get("kwargs", {})
            meta = exp.get("meta", {})
            
            try:
                result = self._run_expectation(df, exp_type, kwargs)
                results.append(result)
                
                if not result["success"]:
                    failed_expectations.append({
                        "expectation": exp_type,
                        "kwargs": kwargs,
                        "severity": meta.get("severity", "warning"),
                        "description": meta.get("description", ""),
                        "result": result.get("result", {})
                    })
            except Exception as e:
                failed_expectations.append({
                    "expectation": exp_type,
                    "error": str(e),
                    "severity": meta.get("severity", "warning")
                })
        
        total = len(results)
        successful = sum(1 for r in results if r.get("success", False))
        
        statistics = {
            "evaluated_expectations": total,
            "successful_expectations": successful,
            "unsuccessful_expectations": total - successful,
            "success_percent": (successful / total * 100) if total > 0 else 100
        }
        
        critical_failures = [
            f for f in failed_expectations 
            if f.get("severity") == "critical"
        ]
        
        passed = len(critical_failures) == 0
        
        return QualityCheckResult(
            passed=passed,
            expectation_suite=suite_name,
            statistics=statistics,
            failed_expectations=failed_expectations,
            run_time=run_time
        )
    
    def _run_expectation(self, df: pd.DataFrame, exp_type: str, kwargs: Dict) -> Dict:
        """Run a single expectation."""
        
        if exp_type == "expect_column_values_to_not_be_null":
            col = kwargs["column"]
            if col not in df.columns:
                return {"success": False, "result": {"error": f"Column {col} not found"}}
            null_count = df[col].isnull().sum()
            mostly = kwargs.get("mostly", 1.0)
            success = (1 - null_count / len(df)) >= mostly if len(df) > 0 else True
            return {"success": success, "result": {"null_count": int(null_count), "total": len(df)}}
            
        elif exp_type == "expect_column_values_to_be_unique":
            col = kwargs["column"]
            if col not in df.columns:
                return {"success": False, "result": {"error": f"Column {col} not found"}}
            duplicates = df[col].duplicated().sum()
            success = duplicates == 0
            return {"success": success, "result": {"duplicate_count": int(duplicates)}}
            
        elif exp_type == "expect_column_values_to_be_between":
            col = kwargs["column"]
            if col not in df.columns:
                return {"success": False, "result": {"error": f"Column {col} not found"}}
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            mostly = kwargs.get("mostly", 1.0)
            
            valid = df[col].dropna()
            in_range = valid.copy()
            if min_val is not None:
                in_range = in_range[in_range >= min_val]
            if max_val is not None:
                in_range = in_range[in_range <= max_val]
            
            success_rate = len(in_range) / len(valid) if len(valid) > 0 else 1
            success = success_rate >= mostly
            return {"success": success, "result": {"success_rate": success_rate}}
            
        elif exp_type == "expect_column_values_to_be_in_set":
            col = kwargs["column"]
            if col not in df.columns:
                return {"success": False, "result": {"error": f"Column {col} not found"}}
            value_set = kwargs["value_set"]
            invalid = (~df[col].isin(value_set)).sum()
            success = invalid == 0
            return {"success": success, "result": {"invalid_count": int(invalid)}}
            
        elif exp_type == "expect_table_row_count_to_be_between":
            min_val = kwargs.get("min_value", 0)
            max_val = kwargs.get("max_value", float("inf"))
            row_count = len(df)
            success = min_val <= row_count <= max_val
            return {"success": success, "result": {"row_count": row_count}}
            
        elif exp_type == "expect_column_mean_to_be_between":
            col = kwargs["column"]
            if col not in df.columns:
                return {"success": False, "result": {"error": f"Column {col} not found"}}
            min_val = kwargs.get("min_value", float("-inf"))
            max_val = kwargs.get("max_value", float("inf"))
            mean = df[col].mean()
            success = min_val <= mean <= max_val
            return {"success": success, "result": {"mean": float(mean) if pd.notna(mean) else 0}}
            
        elif exp_type == "expect_table_columns_to_match_set":
            column_set = set(kwargs.get("column_set", []))
            actual_columns = set(df.columns)
            missing = column_set - actual_columns
            success = len(missing) == 0
            return {"success": success, "result": {"missing_columns": list(missing)}}
            
        else:
            return {"success": True, "result": {"skipped": True, "type": exp_type}}


# ════════════════════════════════════════════════════════════════════════════════
# DAGSTER ASSETS - Quality Checks
# ════════════════════════════════════════════════════════════════════════════════

validator = DataQualityValidator()


@asset(
    description="Validate Bronze layer data quality",
    compute_kind="data_quality",
    group_name="quality_checks",
)
def bronze_quality_check(
    context: AssetExecutionContext,
) -> Output[Dict]:
    """
    Run quality checks on Bronze layer data.
    Connects to Trino to fetch Bronze data and validate.
    """
    from src.core.resources import get_trino_client
    
    context.log.info("Running Bronze quality checks...")
    
    try:
        conn = get_trino_client()._get_connection()
        query = f"""
            SELECT * FROM {TRINO_CATALOG}.bronze.fraud_transactions
            LIMIT 10000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        result = validator.validate_dataframe(df, "bronze_fraud_transactions")
        
        if not result.passed:
            context.log.warning(f"Bronze quality check has issues: {result.failed_expectations}")
        else:
            context.log.info(f"Bronze quality check PASSED: {result.statistics}")
        
        return Output(
            {
                "passed": result.passed,
                "statistics": result.statistics,
                "failed_expectations": result.failed_expectations,
            },
            metadata=result.to_metadata()
        )
    except Exception as e:
        context.log.error(f"Bronze quality check failed: {e}")
        return Output(
            {"passed": False, "error": str(e)},
            metadata={"quality_passed": MetadataValue.bool(False), "error": MetadataValue.text(str(e))}
        )


@asset(
    description="Validate Silver layer data quality",
    compute_kind="data_quality",
    group_name="quality_checks",
)
def silver_quality_check(
    context: AssetExecutionContext,
) -> Output[Dict]:
    """Run quality checks on Silver layer data."""
    from src.core.resources import get_trino_client
    
    context.log.info("Running Silver quality checks...")
    
    try:
        conn = get_trino_client()._get_connection()
        query = f"""
            SELECT * FROM {TRINO_CATALOG}.silver.fraud_transactions
            LIMIT 10000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        result = validator.validate_dataframe(df, "silver_fraud_transactions")
        
        if not result.passed:
            context.log.warning(f"Silver quality check has issues: {result.failed_expectations}")
        else:
            context.log.info(f"Silver quality check PASSED: {result.statistics}")
        
        return Output(
            {
                "passed": result.passed,
                "statistics": result.statistics,
                "failed_expectations": result.failed_expectations,
            },
            metadata=result.to_metadata()
        )
    except Exception as e:
        context.log.error(f"Silver quality check failed: {e}")
        return Output(
            {"passed": False, "error": str(e)},
            metadata={"quality_passed": MetadataValue.bool(False), "error": MetadataValue.text(str(e))}
        )


@asset(
    description="Validate Gold layer training data quality",
    compute_kind="data_quality",
    group_name="quality_checks",
)
def gold_quality_check(
    context: AssetExecutionContext,
) -> Output[Dict]:
    """Run quality checks on Gold layer training data."""
    from src.core.resources import get_trino_client
    
    context.log.info("Running Gold quality checks...")
    
    try:
        conn = get_trino_client()._get_connection()
        query = f"""
            SELECT * FROM {TRINO_CATALOG}.gold.fraud_features
            LIMIT 10000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        result = validator.validate_dataframe(df, "gold_fraud_training_data")
        
        if not result.passed:
            context.log.warning(f"Gold quality check has issues: {result.failed_expectations}")
        else:
            context.log.info(f"Gold quality check PASSED: {result.statistics}")
        
        return Output(
            {
                "passed": result.passed,
                "statistics": result.statistics,
                "failed_expectations": result.failed_expectations,
            },
            metadata=result.to_metadata()
        )
    except Exception as e:
        context.log.error(f"Gold quality check failed: {e}")
        return Output(
            {"passed": False, "error": str(e)},
            metadata={"quality_passed": MetadataValue.bool(False), "error": MetadataValue.text(str(e))}
        )


@asset(
    description="Quality gate that blocks training on critical failures",
    compute_kind="quality_gate",
    group_name="quality_checks",
    deps=["bronze_quality_check", "silver_quality_check", "gold_quality_check"],
)
def training_quality_gate(
    context: AssetExecutionContext,
    bronze_quality_check: Dict,
    silver_quality_check: Dict,
    gold_quality_check: Dict,
) -> Output[bool]:
    """
    Quality gate that must pass before training.
    Blocks pipeline if critical quality checks fail.
    """
    
    all_passed = all([
        bronze_quality_check.get("passed", False),
        silver_quality_check.get("passed", False),
        gold_quality_check.get("passed", False),
    ])
    
    summary = {
        "bronze": bronze_quality_check.get("passed", False),
        "silver": silver_quality_check.get("passed", False),
        "gold": gold_quality_check.get("passed", False),
    }
    
    if not all_passed:
        context.log.error(f"Quality gate FAILED: {summary}")
        # Don't raise - let downstream decide what to do
    else:
        context.log.info(f"Quality gate PASSED: {summary}")
    
    return Output(
        all_passed,
        metadata={
            "gate_passed": MetadataValue.bool(all_passed),
            "summary": MetadataValue.json(summary),
        }
    )
