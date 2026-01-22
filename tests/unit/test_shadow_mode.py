"""
Unit tests for Shadow Mode deployment.

Tests cover:
- ShadowModePredictor initialization
- Agreement calculation
- Statistics tracking
- Enable/disable functionality
- Disagreement detection
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


class TestShadowModePredictor:
    """Tests for ShadowModePredictor class."""

    def test_predictor_initialization_disabled(self):
        """Test predictor initializes with disabled state by default."""
        with patch.dict("os.environ", {"SHADOW_MODE_ENABLED": "false", "PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            assert predictor.enabled is False

    def test_predictor_initialization_enabled(self):
        """Test predictor initializes with enabled state when specified."""
        with patch.dict("os.environ", {"SHADOW_MODE_ENABLED": "true", "PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=True)
            assert predictor.enabled is True

    def test_enable_shadow_mode(self):
        """Test enabling shadow mode."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            predictor.enable()
            assert predictor.enabled is True

    def test_disable_shadow_mode(self):
        """Test disabling shadow mode."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=True)
            predictor.disable()
            assert predictor.enabled is False


class TestShadowModeStats:
    """Tests for shadow mode statistics tracking."""

    def test_initial_stats_are_zero(self):
        """Test that initial stats are all zero."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            stats = predictor.get_stats()

            assert stats.total_predictions == 0
            assert stats.agreements == 0
            assert stats.disagreements == 0
            assert stats.agreement_rate == 0.0

    def test_reset_stats(self):
        """Test that reset_stats clears all statistics."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            predictor.reset_stats()
            stats = predictor.get_stats()

            assert stats.total_predictions == 0

    def test_stats_to_dict(self):
        """Test that stats can be serialized to dict."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            stats = predictor.get_stats()
            stats_dict = stats.to_dict()

            assert isinstance(stats_dict, dict)
            assert "total_predictions" in stats_dict
            assert "agreement_rate" in stats_dict


class TestShadowModePredictions:
    """Tests for shadow mode prediction tracking."""

    def test_get_recent_predictions_empty(self):
        """Test getting recent predictions when none exist."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            predictions = predictor.get_recent_predictions(limit=10)

            assert isinstance(predictions, list)
            assert len(predictions) == 0

    def test_get_disagreements_empty(self):
        """Test getting disagreements when none exist."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            disagreements = predictor.get_disagreements(limit=10)

            assert isinstance(disagreements, list)
            assert len(disagreements) == 0


class TestShadowModeStatus:
    """Tests for shadow mode status reporting."""

    def test_get_status_returns_dict(self):
        """Test that get_status returns a dictionary."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=False)
            status = predictor.get_status()

            assert isinstance(status, dict)
            assert "enabled" in status
            assert "shadow_stage" in status
            assert "stats" in status

    def test_status_reflects_enabled_state(self):
        """Test that status reflects the enabled state."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import ShadowModePredictor

            predictor = ShadowModePredictor(enabled=True)
            status = predictor.get_status()

            assert status["enabled"] is True


class TestShadowModeGlobalFunctions:
    """Tests for global shadow mode functions."""

    def test_get_shadow_predictor_singleton(self):
        """Test that get_shadow_predictor returns singleton."""
        with patch.dict("os.environ", {"PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import get_shadow_predictor

            predictor1 = get_shadow_predictor()
            predictor2 = get_shadow_predictor()

            assert predictor1 is predictor2

    def test_is_shadow_mode_enabled_function(self):
        """Test the is_shadow_mode_enabled helper function."""
        with patch.dict("os.environ", {"SHADOW_MODE_ENABLED": "false", "PROMETHEUS_METRICS": "false"}):
            from src.api.shadow_mode import is_shadow_mode_enabled, get_shadow_predictor

            # Reset the singleton for clean test
            import src.api.shadow_mode as sm
            sm._shadow_predictor = None

            result = is_shadow_mode_enabled()
            assert isinstance(result, bool)
