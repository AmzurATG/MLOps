# Configuration Directory

Centralized configuration for all services in the Fraud Detection MLOps Platform.

## Directory Structure

```
config/
├── monitoring/              # Observability stack
│   ├── prometheus/          # Metrics collection
│   │   ├── prometheus.yml   # Scrape targets, retention
│   │   └── alerts.yml       # Alerting rules
│   ├── grafana/             # Dashboards & visualization
│   │   ├── dashboards/      # JSON dashboard definitions
│   │   └── provisioning/    # Datasource & dashboard provisioning
│   └── alertmanager/        # Alert routing
│       └── alertmanager.yml # Notification channels
│
├── streaming/               # Event streaming
│   ├── debezium/            # CDC connectors
│   │   ├── mysql-connector.json
│   │   └── evaluation-connector.json
│   ├── ksqldb/              # Stream processing
│   │   ├── 01_streams.sql   # Stream definitions
│   │   ├── 02_aggregations.sql
│   │   └── 03_feature_output.sql
│   └── schemas/             # Schema Registry
│       └── fraud_transaction.avsc  # Avro schema
│
├── database/                # Database initialization
│   ├── mysql/               # Transaction database
│   │   ├── init-mysql.sql
│   │   └── init-evaluation-mysql.sql
│   ├── postgres/            # Metadata stores
│   │   └── init-dagster-postgres.sql
│   └── iceberg/             # Data lake tables
│       ├── init_iceberg_monitoring.sql
│       └── enable_iceberg_evolution.sql
│
├── services/                # Service-specific configs
│   ├── trino/               # Query engine
│   │   ├── config.properties
│   │   ├── node.properties
│   │   ├── jvm.config
│   │   └── catalog/         # Iceberg catalogs
│   ├── traefik/             # Reverse proxy
│   │   ├── traefik.yml
│   │   └── dynamic/         # Dynamic routing
│   ├── dagster/             # Orchestration
│   │   ├── dagster.yaml
│   │   ├── workspace.yaml
│   │   └── dagster_webhooks.yaml
│   ├── feast/               # Feature store
│   │   └── feature_store.yaml
│   └── litellm/             # LLM gateway
│       └── litellm_config.yaml
│
├── data-quality/            # Data validation
│   └── expectations/        # Great Expectations
│       ├── bronze_fraud_transactions.yaml
│       ├── silver_fraud_transactions.yaml
│       └── gold_fraud_training_data.yaml
│
├── webhooks/                # Event webhooks
│   ├── lakefs-webhook-config-multitable.yaml
│   └── lakefs-cvops-webhook-config.yaml
│
└── bronze_sources.yaml      # Data ingestion sources
```

## Quick Reference

### Monitoring

| File | Description | Reload Command |
|------|-------------|----------------|
| `monitoring/prometheus/prometheus.yml` | Scrape targets | `curl -X POST http://localhost:9090/-/reload` |
| `monitoring/prometheus/alerts.yml` | Alert rules | Auto-reloaded by Prometheus |
| `monitoring/alertmanager/alertmanager.yml` | Notification routing | `curl -X POST http://localhost:9093/-/reload` |

### Streaming

| File | Description | Apply Command |
|------|-------------|---------------|
| `streaming/debezium/mysql-connector.json` | CDC connector | `curl -X POST http://localhost:8085/connectors -d @config/streaming/debezium/mysql-connector.json` |
| `streaming/ksqldb/*.sql` | Stream definitions | Run via ksqlDB CLI |
| `streaming/schemas/*.avsc` | Avro schemas | Register via Schema Registry |

### Database Init

| File | Description | Runs At |
|------|-------------|---------|
| `database/mysql/init-mysql.sql` | Create tables | Container first start |
| `database/postgres/init-dagster-postgres.sql` | Dagster metadata | Container first start |
| `database/iceberg/enable_iceberg_evolution.sql` | Schema evolution | Manual: `trino < config/database/iceberg/enable_iceberg_evolution.sql` |

### Services

| Service | Config Location | Documentation |
|---------|----------------|---------------|
| Trino | `services/trino/` | [Trino Docs](https://trino.io/docs/current/) |
| Traefik | `services/traefik/` | [Traefik Docs](https://doc.traefik.io/traefik/) |
| Dagster | `services/dagster/` | [Dagster Docs](https://docs.dagster.io/) |
| Feast | `services/feast/` | [Feast Docs](https://docs.feast.dev/) |
| LiteLLM | `services/litellm/` | [LiteLLM Docs](https://docs.litellm.ai/) |

## Environment Variables

Core environment variables are defined in `.env` at the repository root. Service-specific overrides can be set in docker-compose files.

## Adding New Configuration

1. Place config file in appropriate subdirectory
2. Update docker-compose volume mount if needed
3. Document in this README

## Validation

```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config/monitoring/prometheus/prometheus.yml'))"

# Validate JSON syntax
python -c "import json; json.load(open('config/streaming/debezium/mysql-connector.json'))"

# Validate Avro schema
python -c "import json; s=json.load(open('config/streaming/schemas/fraud_transaction.avsc')); print(f'Schema: {s[\"name\"]} with {len(s[\"fields\"])} fields')"
```

## File Count Summary

```
Total: 39 configuration files
├── Monitoring: 8 files
├── Streaming: 7 files
├── Database: 5 files
├── Services: 14 files
├── Data Quality: 3 files
└── Webhooks: 2 files
```
