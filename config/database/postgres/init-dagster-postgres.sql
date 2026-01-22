-- =============================================================================
-- DAGSTER POSTGRESQL INITIALIZATION SCRIPT
-- =============================================================================
-- This script runs when exp-postgres-dagster container starts
-- Creates required databases and grants permissions
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create Dagster database (if not exists)
-- -----------------------------------------------------------------------------
-- Note: The 'dagster' database is typically created by POSTGRES_DB env var
-- This is a fallback

SELECT 'CREATE DATABASE dagster'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dagster')\gexec

-- -----------------------------------------------------------------------------
-- Create Feast Registry database
-- -----------------------------------------------------------------------------
-- Required for Feast feature store metadata
-- Used by: FEAST_REGISTRY_URI=postgresql://dagster:dagster@exp-postgres-dagster:5432/feast_registry

SELECT 'CREATE DATABASE feast_registry'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'feast_registry')\gexec

-- Grant privileges to dagster user
GRANT ALL PRIVILEGES ON DATABASE feast_registry TO dagster;

-- -----------------------------------------------------------------------------
-- Create Pipeline Lineage database
-- -----------------------------------------------------------------------------
-- Used for storing pipeline run lineage information

SELECT 'CREATE DATABASE pipeline_lineage'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pipeline_lineage')\gexec

GRANT ALL PRIVILEGES ON DATABASE pipeline_lineage TO dagster;

-- -----------------------------------------------------------------------------
-- Create extensions in dagster database
-- -----------------------------------------------------------------------------
\c dagster

-- Enable UUID extension for generating unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable pg_trgm for text similarity searches
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- -----------------------------------------------------------------------------
-- Create Feast Registry tables (in feast_registry database)
-- -----------------------------------------------------------------------------
\c feast_registry

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Feast will create its own tables, but we ensure the database exists

-- -----------------------------------------------------------------------------
-- Create Pipeline Lineage tables
-- -----------------------------------------------------------------------------
\c pipeline_lineage

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Pipeline runs lineage table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dagster_run_id VARCHAR(255) NOT NULL UNIQUE,
    pipeline_name VARCHAR(255) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'running',
    lineage_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dagster_run_id ON pipeline_runs(dagster_run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_name ON pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at ON pipeline_runs(started_at);

-- Feature lineage table
CREATE TABLE IF NOT EXISTS feature_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_run_id UUID REFERENCES pipeline_runs(id),
    feature_name VARCHAR(255) NOT NULL,
    feature_version VARCHAR(50),
    source_table VARCHAR(255),
    transformation_sql TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feature_lineage_feature_name ON feature_lineage(feature_name);

-- Model lineage table
CREATE TABLE IF NOT EXISTS model_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_run_id UUID REFERENCES pipeline_runs(id),
    model_name VARCHAR(255) NOT NULL,
    model_version VARCHAR(50),
    mlflow_run_id VARCHAR(255),
    training_data_snapshot VARCHAR(255),
    feature_list JSONB,
    metrics JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_model_lineage_model_name ON model_lineage(model_name);
CREATE INDEX IF NOT EXISTS idx_model_lineage_mlflow_run_id ON model_lineage(mlflow_run_id);

-- Grant all privileges to dagster user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dagster;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dagster;

-- =============================================================================
-- END OF INITIALIZATION
-- =============================================================================