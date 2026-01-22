"""
Telemetry Module
================

Provides unified observability:
- Prometheus metrics
- Performance tracking
- Request logging

Usage:
    from src.api.serving.telemetry import TelemetryObserver

    observer = TelemetryObserver()
    observer.record_prediction(result)
"""

from src.api.serving.telemetry.observer import TelemetryObserver
from src.api.serving.telemetry.metrics import MetricsRecorder
from src.api.serving.telemetry.tracker import PerformanceTracker

__all__ = [
    "TelemetryObserver",
    "MetricsRecorder",
    "PerformanceTracker",
]
