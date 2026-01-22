"""
Unit tests for core.monitoring module.

Tests the unified Prometheus monitoring utilities including
metric creation helpers, decorators, and the MonitoringMixin.

Note: These tests import directly from src.core.monitoring to
avoid loading the full pipelines package which requires dagster.
"""

import sys
import os
import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from prometheus_client import REGISTRY, Counter, Histogram, Gauge, Summary

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the monitoring module directly using importlib
import importlib.util
monitoring_path = os.path.join(project_root, 'src', 'core', 'monitoring', 'metrics.py')
spec = importlib.util.spec_from_file_location("core_monitoring_metrics", monitoring_path)
monitoring = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitoring)

# Get symbols from the directly loaded module
get_or_create_counter = monitoring.get_or_create_counter
get_or_create_histogram = monitoring.get_or_create_histogram
get_or_create_gauge = monitoring.get_or_create_gauge
get_or_create_summary = monitoring.get_or_create_summary
track_execution = monitoring.track_execution
track_async_execution = monitoring.track_async_execution
MonitoringMixin = monitoring.MonitoringMixin
OperationTracker = monitoring.OperationTracker
safe_observe = monitoring.safe_observe
safe_increment = monitoring.safe_increment
safe_set_gauge = monitoring.safe_set_gauge
LATENCY_BUCKETS_FAST = monitoring.LATENCY_BUCKETS_FAST
LATENCY_BUCKETS_MEDIUM = monitoring.LATENCY_BUCKETS_MEDIUM
LATENCY_BUCKETS_SLOW = monitoring.LATENCY_BUCKETS_SLOW
PROBABILITY_BUCKETS = monitoring.PROBABILITY_BUCKETS
error_counter = monitoring.error_counter

# Check if pytest-asyncio is available
try:
    import pytest_asyncio
    HAS_ASYNCIO = True
except ImportError:
    HAS_ASYNCIO = False


class TestBucketConstants:
    """Tests for bucket constant definitions."""

    def test_latency_buckets_fast(self):
        """Test fast latency bucket values."""
        assert LATENCY_BUCKETS_FAST[0] == 0.001
        assert LATENCY_BUCKETS_FAST[-1] == 1.0
        # Should be monotonically increasing
        for i in range(1, len(LATENCY_BUCKETS_FAST)):
            assert LATENCY_BUCKETS_FAST[i] > LATENCY_BUCKETS_FAST[i - 1]

    def test_latency_buckets_medium(self):
        """Test medium latency bucket values."""
        assert LATENCY_BUCKETS_MEDIUM[0] == 0.01
        assert LATENCY_BUCKETS_MEDIUM[-1] == 5.0

    def test_latency_buckets_slow(self):
        """Test slow latency bucket values."""
        assert LATENCY_BUCKETS_SLOW[0] == 0.1
        assert LATENCY_BUCKETS_SLOW[-1] == 120.0

    def test_probability_buckets(self):
        """Test probability bucket values."""
        assert PROBABILITY_BUCKETS[0] == 0.0
        assert PROBABILITY_BUCKETS[-1] == 1.0
        assert len(PROBABILITY_BUCKETS) == 11


class TestMetricCreationHelpers:
    """Tests for get_or_create_* functions."""

    def test_get_or_create_counter_new(self):
        """Test creating a new counter."""
        counter = get_or_create_counter(
            "test_counter_unique_1",
            "Test counter",
            ["label1", "label2"],
        )

        assert counter is not None
        # Should be able to use it
        counter.labels(label1="a", label2="b").inc()

    def test_get_or_create_counter_existing(self):
        """Test getting an existing counter."""
        # Create first
        counter1 = get_or_create_counter(
            "test_counter_existing",
            "Test counter",
            ["label"],
        )

        # Get same name again
        counter2 = get_or_create_counter(
            "test_counter_existing",
            "Test counter",
            ["label"],
        )

        # Should not raise error, should return existing or new
        assert counter2 is not None

    def test_get_or_create_histogram_with_buckets(self):
        """Test creating histogram with custom buckets."""
        histogram = get_or_create_histogram(
            "test_histogram_buckets",
            "Test histogram",
            ["label"],
            buckets=[0.1, 0.5, 1.0, 5.0],
        )

        assert histogram is not None
        histogram.labels(label="test").observe(0.3)

    def test_get_or_create_histogram_default_buckets(self):
        """Test creating histogram with default buckets."""
        histogram = get_or_create_histogram(
            "test_histogram_default",
            "Test histogram",
            ["label"],
        )

        assert histogram is not None

    def test_get_or_create_gauge(self):
        """Test creating a gauge."""
        gauge = get_or_create_gauge(
            "test_gauge_unique",
            "Test gauge",
            ["label"],
        )

        assert gauge is not None
        gauge.labels(label="test").set(42.0)

    def test_get_or_create_summary(self):
        """Test creating a summary."""
        summary = get_or_create_summary(
            "test_summary_unique",
            "Test summary",
            ["label"],
        )

        assert summary is not None
        summary.labels(label="test").observe(1.5)


class TestTrackExecutionDecorator:
    """Tests for track_execution decorator."""

    def test_track_execution_success(self):
        """Test tracking successful function execution."""
        mock_counter = Mock()
        mock_histogram = Mock()

        @track_execution(
            counter=mock_counter,
            histogram=mock_histogram,
            labels={"operation": "test"},
        )
        def my_function():
            return "result"

        result = my_function()

        assert result == "result"
        mock_counter.labels.assert_called_once()
        mock_histogram.labels.assert_called_once()

    def test_track_execution_error(self):
        """Test tracking function that raises error."""
        mock_counter = Mock()
        mock_histogram = Mock()

        @track_execution(
            counter=mock_counter,
            histogram=mock_histogram,
            labels={"operation": "test"},
        )
        def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            failing_function()

        # Counter should be called with status="error"
        mock_counter.labels.assert_called_once()
        call_kwargs = mock_counter.labels.call_args[1]
        assert call_kwargs["status"] == "error"

    def test_track_execution_no_metrics(self):
        """Test decorator with no metrics provided."""
        @track_execution()
        def simple_function():
            return 42

        result = simple_function()
        assert result == 42

    def test_track_execution_counter_only(self):
        """Test decorator with only counter."""
        mock_counter = Mock()

        @track_execution(counter=mock_counter)
        def my_function():
            return "done"

        my_function()

        mock_counter.labels.assert_called_once()

    def test_track_execution_histogram_only(self):
        """Test decorator with only histogram."""
        mock_histogram = Mock()

        @track_execution(histogram=mock_histogram, labels={"op": "test"})
        def my_function():
            time.sleep(0.01)
            return "done"

        my_function()

        mock_histogram.labels.assert_called_once()
        # Check that duration was observed
        mock_histogram.labels.return_value.observe.assert_called_once()

    def test_track_execution_handles_metric_errors(self):
        """Test decorator gracefully handles metric errors."""
        mock_counter = Mock()
        mock_counter.labels.side_effect = Exception("Metric error")

        @track_execution(counter=mock_counter, labels={"op": "test"})
        def my_function():
            return "done"

        # Should not raise, should complete normally
        result = my_function()
        assert result == "done"


@pytest.mark.skipif(not HAS_ASYNCIO, reason="pytest-asyncio not installed")
class TestTrackAsyncExecutionDecorator:
    """Tests for track_async_execution decorator."""

    @pytest.mark.asyncio
    async def test_track_async_execution_success(self):
        """Test tracking successful async function."""
        mock_counter = Mock()
        mock_histogram = Mock()

        @track_async_execution(
            counter=mock_counter,
            histogram=mock_histogram,
            labels={"operation": "async_test"},
        )
        async def async_function():
            return "async_result"

        result = await async_function()

        assert result == "async_result"
        mock_counter.labels.assert_called_once()

    @pytest.mark.asyncio
    async def test_track_async_execution_error(self):
        """Test tracking async function that raises error."""
        mock_counter = Mock()

        @track_async_execution(
            counter=mock_counter,
            labels={"operation": "async_test"},
        )
        async def failing_async():
            raise ValueError("Async error")

        with pytest.raises(ValueError):
            await failing_async()

        call_kwargs = mock_counter.labels.call_args[1]
        assert call_kwargs["status"] == "error"


class TestOperationTracker:
    """Tests for OperationTracker context manager."""

    def test_operation_tracker_success(self):
        """Test OperationTracker on successful operation."""
        mock_counter = Mock()
        mock_histogram = Mock()

        tracker = OperationTracker(
            counter=mock_counter,
            histogram=mock_histogram,
            operation="test_op",
        )

        with tracker:
            time.sleep(0.01)

        assert tracker.status == "success"
        mock_counter.labels.assert_called_once()
        mock_histogram.labels.assert_called_once()

    def test_operation_tracker_error(self):
        """Test OperationTracker on error."""
        mock_counter = Mock()
        mock_histogram = Mock()

        tracker = OperationTracker(
            counter=mock_counter,
            histogram=mock_histogram,
            operation="test_op",
        )

        with pytest.raises(RuntimeError):
            with tracker:
                raise RuntimeError("Test error")

        assert tracker.status == "error"
        call_kwargs = mock_counter.labels.call_args[1]
        assert call_kwargs["status"] == "error"

    def test_operation_tracker_no_counter(self):
        """Test OperationTracker with no counter."""
        mock_histogram = Mock()

        tracker = OperationTracker(
            counter=None,
            histogram=mock_histogram,
            operation="test_op",
        )

        with tracker:
            pass

        mock_histogram.labels.assert_called_once()

    def test_operation_tracker_no_histogram(self):
        """Test OperationTracker with no histogram."""
        mock_counter = Mock()

        tracker = OperationTracker(
            counter=mock_counter,
            histogram=None,
            operation="test_op",
        )

        with tracker:
            pass

        mock_counter.labels.assert_called_once()

    def test_operation_tracker_handles_errors(self):
        """Test OperationTracker handles metric errors gracefully."""
        mock_counter = Mock()
        mock_counter.labels.side_effect = Exception("Counter error")

        tracker = OperationTracker(
            counter=mock_counter,
            histogram=None,
            operation="test_op",
        )

        # Should not raise
        with tracker:
            pass


class TestMonitoringMixin:
    """Tests for MonitoringMixin class."""

    def test_init_metrics(self):
        """Test _init_metrics method."""
        class TestClass(MonitoringMixin):
            def __init__(self):
                super().__init__()
                self._init_metrics("test_component")

        obj = TestClass()

        assert obj._metrics_initialized is True
        assert obj._component_name == "test_component"
        assert obj._operation_counter is not None
        assert obj._operation_duration is not None

    def test_track_operation(self):
        """Test track_operation context manager."""
        class TestClass(MonitoringMixin):
            def __init__(self):
                super().__init__()
                self._init_metrics("test_mixin")
                # Replace with mocks
                self._operation_counter = Mock()
                self._operation_duration = Mock()

            def do_work(self):
                with self.track_operation("work"):
                    return "done"

        obj = TestClass()
        result = obj.do_work()

        assert result == "done"
        obj._operation_counter.labels.assert_called_once()
        obj._operation_duration.labels.assert_called_once()

    def test_record_error(self):
        """Test record_error method."""
        class TestClass(MonitoringMixin):
            def __init__(self):
                super().__init__()
                self._init_metrics("error_test")

        obj = TestClass()

        # Create mock for error_counter using the directly loaded module
        mock_error = Mock()
        original_error_counter = monitoring.error_counter
        try:
            monitoring.error_counter = mock_error
            obj.record_error("connection_error", Exception("Test"))
            mock_error.labels.assert_called_once()
        finally:
            monitoring.error_counter = original_error_counter


class TestSafeUtilityFunctions:
    """Tests for safe_* utility functions."""

    def test_safe_observe_success(self):
        """Test safe_observe with valid histogram."""
        mock_histogram = Mock()

        safe_observe(mock_histogram, 0.5, label="test")

        mock_histogram.labels.assert_called_once_with(label="test")
        mock_histogram.labels.return_value.observe.assert_called_once_with(0.5)

    def test_safe_observe_none(self):
        """Test safe_observe with None histogram."""
        # Should not raise
        safe_observe(None, 0.5, label="test")

    def test_safe_observe_error(self):
        """Test safe_observe handles errors."""
        mock_histogram = Mock()
        mock_histogram.labels.side_effect = Exception("Error")

        # Should not raise
        safe_observe(mock_histogram, 0.5, label="test")

    def test_safe_increment_success(self):
        """Test safe_increment with valid counter."""
        mock_counter = Mock()

        safe_increment(mock_counter, 5, label="test")

        mock_counter.labels.assert_called_once_with(label="test")
        mock_counter.labels.return_value.inc.assert_called_once_with(5)

    def test_safe_increment_default_amount(self):
        """Test safe_increment with default amount."""
        mock_counter = Mock()

        safe_increment(mock_counter, label="test")

        mock_counter.labels.return_value.inc.assert_called_once_with(1)

    def test_safe_increment_none(self):
        """Test safe_increment with None counter."""
        # Should not raise
        safe_increment(None, 1, label="test")

    def test_safe_set_gauge_success(self):
        """Test safe_set_gauge with valid gauge."""
        mock_gauge = Mock()

        safe_set_gauge(mock_gauge, 42.0, label="test")

        mock_gauge.labels.assert_called_once_with(label="test")
        mock_gauge.labels.return_value.set.assert_called_once_with(42.0)

    def test_safe_set_gauge_none(self):
        """Test safe_set_gauge with None gauge."""
        # Should not raise
        safe_set_gauge(None, 42.0, label="test")

    def test_safe_set_gauge_error(self):
        """Test safe_set_gauge handles errors."""
        mock_gauge = Mock()
        mock_gauge.labels.side_effect = Exception("Error")

        # Should not raise
        safe_set_gauge(mock_gauge, 42.0, label="test")
