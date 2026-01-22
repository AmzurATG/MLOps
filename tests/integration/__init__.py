"""Integration tests for the MLOps platform.

These tests require running Docker services and should be run with:
    pytest tests/integration -v -m integration

Prerequisites:
    docker-compose up -d exp-redis exp-trino exp-mlflow exp-lakefs
"""
