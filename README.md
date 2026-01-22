# Unified MLOps/CVOps/LLMOps Platform


A production-ready **MLOps platform** for fraud detection, computer vision, and LLM observability, built on modern lakehouse architecture with **Dagster orchestration**, **Feast feature store**, and **MLflow model registry**.

## Key Features

| Domain | Capabilities |
|--------|-------------|
| **MLOps** | Fraud detection pipeline with Bronze/Silver/Gold layers, streaming velocity features, automated retraining |
| **CVOps** | YOLO-based object detection with LakeFS versioning, Label Studio integration |
| **LLMOps** | MLflow GenAI tracing, prompt versioning, model evaluation with A/B testing |
| **Serving** | Sub-100ms inference, shadow mode deployment, A/B testing, SHAP explainability |
| **Streaming** | ksqlDB velocity features (5min/1h/24h windows), real-time scoring rules |
| **Observability** | Prometheus metrics, Grafana dashboards, drift monitoring with Evidently |

## Architecture Overview

## See in Architecture folder

## Component Stack

| Layer | Components | Purpose |
|-------|-----------|---------|
| **Data Sources** | MySQL, Debezium, Kafka | CDC-based real-time ingestion |
| **Data Lake** | LakeFS, Apache Iceberg, Nessie, MinIO | Version-controlled lakehouse |
| **Query Engine** | Trino | SQL analytics on Iceberg tables |
| **Feature Store** | Feast (Redis + Trino) | Online/offline feature serving |
| **Streaming** | ksqlDB, Kafka Streams | Real-time velocity features |
| **ML Platform** | MLflow, Dagster, XGBoost | Training, tracking, orchestration |
| **LLM Platform** | LiteLLM, MLflow GenAI | LLM proxy, tracing, evaluation |
| **APIs** | FastAPI (Fraud, CV) | Model serving with streaming rules |
| **Observability** | Prometheus, Grafana, Evidently | Metrics, dashboards, drift |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose v2.0+
- Python 3.9+ (for local development)
- 16GB RAM (32GB for full stack)

### 1. Clone and Configure

```bash
git clone https://github.com/AmzurATG/MLOps.git
cd MLOps

# Copy environment template and configure
cp .env.example .env
# Edit .env with your LakeFS keys, API keys, etc.
```

### 2. Start Services (Layered Deployment)

```bash
cd deploy/docker/compose

# Option A: Core only (MLflow, MinIO, Trino, LakeFS, Redis)
docker compose -f docker-compose.core.yml up -d

# Option B: Core + Fraud Detection
docker compose -f docker-compose.core.yml -f docker-compose.fraud.yml up -d

# Option C: Core + LiteLLM (for GenAI demos)
docker compose -f docker-compose.core.yml -f docker-compose.litellm.yml up -d

# Option D: Full stack (all services)
docker compose up -d
```

### 3. Verify Services

```bash
# Check service health
docker compose ps

# Test APIs
curl http://localhost:15000/health    # MLflow
curl http://localhost:18000/api/v1/healthcheck  # LakeFS
curl http://localhost:18002/health    # Fraud API (if running)
```

### 4. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dagster** | http://localhost:13000 | - |
| **MLflow** | http://localhost:15000 | - |
| **Grafana** | http://localhost:3002 | admin/admin |
| **LakeFS** | http://localhost:18000 | See .env |
| **Jupyter** | http://localhost:18888 | token: jupyter |
| **LiteLLM** | http://localhost:4000 | sk-local-dev-2025 |
| **Label Studio** | http://localhost:18081 | admin@example.com |
| **Kafka UI** | http://localhost:8086 | - |

---

## Project Structure

```
MLOps/
├── src/                           # Main application code
│   ├── pipelines/                # Dagster definitions entry point
│   │   └── __init__.py           # Main Definitions builder
│   ├── mlops/                    # Fraud detection domain
│   │   ├── pipelines/            # Dagster assets & jobs
│   │   │   ├── mlops.py          # Init, promote assets
│   │   │   ├── mlops_bronze.py   # Bronze layer ingestion
│   │   │   ├── mlops_gold.py     # Gold layer creation
│   │   │   ├── mlops_labelstudio.py  # Human annotation
│   │   │   └── feature_pipeline.py   # Feast features + training
│   │   └── api/main.py           # FastAPI app
│   ├── cvops/                    # Computer vision domain
│   │   ├── pipelines/assets.py   # CV pipeline assets
│   │   └── api/main.py           # CV API
│   ├── llmops/                   # LLM domain
│   │   └── pipelines/            # LLM pipeline stubs
│   ├── api/                      # Shared API components
│   │   └── serving/              # Feature & model serving
│   └── core/                     # Shared infrastructure
│       ├── resources/            # Resource implementations
│       │   └── connections.py    # LakeFS, Trino, Feast, etc.
│       └── config/settings.py    # Pydantic settings
├── config/                       # External configuration
│   ├── services/dagster/         # Dagster config
│   ├── services/litellm/         # LiteLLM config
│   ├── streaming/ksqldb/         # ksqlDB queries
│   └── monitoring/               # Prometheus, Grafana
├── feature_registry/             # YAML feature definitions
│   ├── fraud_detection.yaml      # Feature specs
│   └── generator.py              # YAML → Feast code
├── notebooks/                    # Jupyter notebooks
│   ├── 00_codebase_exploration.ipynb
│   ├── 01_bronze_to_silver_FIXED.ipynb
│   ├── 02_gold_feature_exploration.ipynb
│   └── 03_mlflow_genai_demo.ipynb  # MLflow GenAI demo
├── deploy/                       # Deployment configs
│   ├── docker/compose/           # Docker Compose files
│   │   ├── docker-compose.core.yml      # Core infrastructure
│   │   ├── docker-compose.fraud.yml     # Fraud detection
│   │   ├── docker-compose.cv.yml        # Computer vision
│   │   ├── docker-compose.litellm.yml   # LiteLLM proxy
│   │   ├── docker-compose.monitoring.yml # Prometheus/Grafana
│   │   └── docker-compose.yml           # Full stack
│   └── helm/                     # Kubernetes Helm charts
├── scripts/                      # Utility scripts
│   ├── generate_fraud_data.py    # Generate sample data
│   └── branch_manager.sh         # LakeFS branch management
└── tests/                        # Test suite
```

---

## Dagster Pipeline Architecture

### How Dagster Initializes Services

**Entry Point**: `src/pipelines/__init__.py`

```python
# Resources are initialized once and shared across all assets
SHARED_RESOURCES = {
    "lakefs": LakeFSResource(),      # Data versioning
    "trino": TrinoResource(),        # SQL queries on Iceberg
    "feast": FeastResource(),        # Feature store
    "minio": MinIOResource(),        # Object storage
    "label_studio": LabelStudioResource(),  # Human annotation
}

# Pipeline mode selection via environment variable
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "combined")
# Options: "mlops", "cvops", "llmops", "combined"

defs = Definitions(
    assets=[*mlops_assets, *cvops_assets],
    jobs=[*mlops_jobs, *cvops_jobs],
    sensors=[*mlops_sensors, *cvops_sensors],
    resources=SHARED_RESOURCES,
)
```

### MLOps Asset Graph

```
mlops_init_lakefs          # Create LakeFS repos + Nessie branches
        ↓
mlops_airbyte_sync         # Ingest from data sources
        ↓
mlops_bronze_ingestion     # → iceberg_dev.bronze.fraud_transactions
        ↓
[silver_table_sensor]      # Monitor Silver table changes
        ↓
mlops_notify_jupyter       # Trigger Jupyter notebook
        ↓
(Silver created by Jupyter) # → iceberg_dev.silver.fraud_transactions
        ↓
mlops_export_to_labelstudio # Export for human annotation
        ↓
[labelstudio_annotations_sensor]  # Wait for annotations
        ↓
mlops_merge_annotations    # Merge labels back to Silver
        ↓
mlops_gold_table           # → iceberg_dev.gold.fraud_training_data
        ↓
generate_feature_code      # YAML → Feast definitions
        ↓
apply_feast_definitions    # Register with Feast
        ↓
create_training_data_feast # Historical features for training
        ↓
train_with_lineage         # Train model → MLflow
        ↓
materialize_online_features # Push to Redis
        ↓
mlops_promote_to_production # Merge dev → main in LakeFS
```

---

## API Usage

### Fraud Detection

```bash
# Single prediction
curl -X POST "http://localhost:18002/predict" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "TXN_001",
    "customer_id": "CUST_123",
    "amount": 1500.00,
    "category": "Electronics",
    "device_type": "mobile",
    "payment_method": "credit_card"
  }'

# Response
{
  "transaction_id": "TXN_001",
  "fraud_score": 0.78,
  "is_fraud": true,
  "risk_level": "MEDIUM",
  "ml_score": 0.45,
  "streaming_boost": 0.33,
  "triggered_rules": ["HIGH_VELOCITY_5MIN"]
}
```

### MLflow GenAI (LLMOps)

See the notebook `notebooks/03_mlflow_genai_demo.ipynb` for comprehensive examples:

```python
import mlflow
from openai import OpenAI

# Setup
mlflow.set_tracking_uri("http://localhost:15000")
mlflow.set_experiment("genai-demo")

# Enable auto-logging
mlflow.openai.autolog()

# Direct Groq SDK (free tier)
client = OpenAI(
    api_key=os.environ["GROQ_API_KEY"],
    base_url="https://api.groq.com/openai/v1"
)

# All calls automatically traced to MLflow
response = client.chat.completions.create(
    model="llama-3.1-8b-instant",
    messages=[{"role": "user", "content": "Explain fraud detection"}]
)

# View traces: http://localhost:15000 → Traces tab
```

---

## Streaming Features (ksqlDB)

### Velocity Windows

| Window | Features | Update |
|--------|----------|--------|
| 5 min | tx_count_5min, unique_devices_5min | Real-time |
| 1 hour | tx_count_1h, amount_sum_1h | Real-time |
| 24 hours | tx_count_24h, unique_payment_methods_24h | Real-time |

### Streaming Rules Engine

| Rule | Trigger | Adjustment |
|------|---------|------------|
| HIGH_VELOCITY_5MIN | >5 txns in 5 min | Score → 0.95 |
| MULTI_DEVICE_5MIN | Multiple devices | +0.25 |
| AMOUNT_ANOMALY | >3x avg | +0.15 |

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PIPELINE_MODE` | mlops, cvops, llmops, combined | combined |
| `TRINO_CATALOG` | Iceberg catalog branch | iceberg_dev |
| `LAKEFS_ACCESS_KEY_ID` | LakeFS access key | (required) |
| `GROQ_API_KEY` | Groq API key for LLMOps | (optional) |
| `MLOPS_SHADOW_ENABLED` | Enable shadow deployments | false |
| `MLOPS_RETRAIN_ENABLED` | Enable auto-retraining | true |

### Branch-Based Development

```bash
# Switch all downstream tables to a different Nessie branch
export TRINO_CATALOG=iceberg_experiment

# Create feature branches
./scripts/branch_manager.sh create experiment-v1
```

---

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `00_codebase_exploration.ipynb` | Explore all platform components |
| `01_bronze_to_silver_FIXED.ipynb` | Data transformation pipeline |
| `02_gold_feature_exploration.ipynb` | Feature engineering |
| `03_mlflow_genai_demo.ipynb` | **MLflow GenAI demo** (tracing, eval, A/B) |

---

## Testing

```bash
# Unit tests
pytest tests/unit -v

# Integration tests (requires Docker)
pytest tests/integration -v

# Coverage
pytest tests/ --cov=src --cov-report=html
```

---

## Deployment

### Docker Compose (Development)

```bash
cd deploy/docker/compose
docker compose up -d
```

### Kubernetes (Production)

```bash
helm install fraud-detection ./deploy/helm/fraud-detection \
  --namespace mlops \
  --values deploy/helm/fraud-detection/values-prod.yaml
```

---

## Key Files Reference

| Purpose | File |
|---------|------|
| Dagster Entry | `src/pipelines/__init__.py` |
| Resources | `src/core/resources/connections.py` |
| MLOps Assets | `src/mlops/pipelines/mlops*.py` |
| Feature Pipeline | `src/mlops/pipelines/feature_pipeline.py` |
| Fraud API | `src/mlops/api/main.py` |
| Feature Serving | `src/api/serving/features/feature_service.py` |
| Settings | `src/core/config/settings.py` |
| Docker Compose | `deploy/docker/compose/docker-compose*.yml` |
| LiteLLM Config | `config/services/litellm/litellm_config.yaml` |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`pytest tests/`)
4. Commit changes (`git commit -m 'Add amazing feature'`)
5. Push to branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request


## Support

