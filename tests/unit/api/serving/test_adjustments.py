"""
Unit tests for api.serving.scoring.adjustments module.

Tests the ScoreAdjuster class which applies streaming-based
business rules to adjust ML fraud scores.
"""

import pytest
from unittest.mock import patch
from datetime import datetime

from src.api.serving.scoring.adjustments import (
    ScoreAdjuster,
    AdjustmentResult,
    VelocityThresholds,
)


class TestVelocityThresholds:
    """Tests for VelocityThresholds dataclass."""

    def test_default_thresholds(self):
        """Test default threshold values."""
        thresholds = VelocityThresholds()

        assert thresholds.high_5min == 5
        assert thresholds.medium_5min == 3
        assert thresholds.high_1h == 20
        assert thresholds.medium_1h == 10
        assert thresholds.high_24h == 50
        assert thresholds.medium_24h == 30

    def test_custom_thresholds(self):
        """Test custom threshold values."""
        thresholds = VelocityThresholds(
            high_5min=10,
            medium_5min=5,
            high_1h=40,
            medium_1h=20,
        )

        assert thresholds.high_5min == 10
        assert thresholds.medium_5min == 5
        assert thresholds.high_1h == 40
        assert thresholds.medium_1h == 20

    def test_from_env(self):
        """Test loading thresholds from environment."""
        with patch.dict('os.environ', {
            'VELOCITY_HIGH_5MIN': '15',
            'VELOCITY_MEDIUM_5MIN': '8',
        }):
            thresholds = VelocityThresholds.from_env()

            assert thresholds.high_5min == 15
            assert thresholds.medium_5min == 8


class TestAdjustmentResult:
    """Tests for AdjustmentResult dataclass."""

    def test_default_values(self):
        """Test default values for AdjustmentResult."""
        result = AdjustmentResult(
            original_score=0.5,
            final_score=0.7,
        )

        assert result.original_score == 0.5
        assert result.final_score == 0.7
        assert result.total_boost == 0.0
        assert result.triggered_rules == []
        assert result.streaming_features_used == {}
        assert result.streaming_available is False
        assert result.timestamp is not None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = AdjustmentResult(
            original_score=0.5,
            final_score=0.75,
            total_boost=0.25,
            triggered_rules=["rule1", "rule2"],
            streaming_available=True,
        )

        d = result.to_dict()

        assert d["original_score"] == 0.5
        assert d["final_score"] == 0.75
        assert d["total_boost"] == 0.25
        assert d["triggered_rules"] == ["rule1", "rule2"]
        assert d["streaming_available"] is True


class TestScoreAdjuster:
    """Tests for ScoreAdjuster class."""

    @pytest.fixture
    def adjuster(self):
        """Create a ScoreAdjuster with default thresholds."""
        return ScoreAdjuster()

    @pytest.fixture
    def custom_adjuster(self):
        """Create a ScoreAdjuster with custom thresholds."""
        thresholds = VelocityThresholds(
            high_5min=5,
            medium_5min=3,
            high_1h=20,
            medium_1h=10,
        )
        return ScoreAdjuster(thresholds=thresholds, max_boost=0.4)

    def test_no_streaming_features(self, adjuster):
        """Test when no streaming features are provided."""
        result = adjuster.apply(ml_score=0.5, streaming_features={})

        assert result.original_score == 0.5
        assert result.final_score == 0.5
        assert result.total_boost == 0.0
        assert result.streaming_available is False
        assert result.triggered_rules == []

    def test_no_streaming_features_none(self, adjuster):
        """Test when streaming_features is None-like."""
        result = adjuster.apply(ml_score=0.5, streaming_features=None)

        assert result.final_score == 0.5
        assert result.streaming_available is False

    def test_high_velocity_5min(self, adjuster):
        """Test high velocity rule for 5-minute window."""
        streaming_features = {
            "tx_count_5min": 6,  # Above high threshold (5)
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.original_score == 0.3
        assert result.total_boost == 0.3
        assert result.final_score == 0.6
        assert "high_velocity_5min" in result.triggered_rules[0]
        assert result.streaming_available is True

    def test_medium_velocity_5min(self, adjuster):
        """Test medium velocity rule for 5-minute window."""
        streaming_features = {
            "tx_count_5min": 4,  # Between medium (3) and high (5)
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.15
        assert abs(result.final_score - 0.45) < 1e-10  # Account for float precision
        assert "medium_velocity_5min" in result.triggered_rules[0]

    def test_high_velocity_1h(self, adjuster):
        """Test high velocity rule for 1-hour window."""
        streaming_features = {
            "tx_count_1h": 25,  # Above high threshold (20)
        }

        result = adjuster.apply(ml_score=0.4, streaming_features=streaming_features)

        assert result.total_boost == 0.2
        assert "high_velocity_1h" in result.triggered_rules[0]

    def test_multi_country_flag(self, adjuster):
        """Test multi-country (impossible travel) rule."""
        streaming_features = {
            "multi_country_flag": "1",  # String representation
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.25
        assert "multi_country" in result.triggered_rules[0]

    def test_multi_country_flag_bool(self, adjuster):
        """Test multi-country flag with boolean True."""
        streaming_features = {
            "multi_country_flag": True,
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.25

    def test_multi_device_flag(self, adjuster):
        """Test multi-device rule."""
        streaming_features = {
            "multi_device_flag": 1,  # Integer representation
        }

        result = adjuster.apply(ml_score=0.4, streaming_features=streaming_features)

        assert result.total_boost == 0.15
        assert "multi_device" in result.triggered_rules[0]

    def test_high_velocity_score_ksqldb(self, adjuster):
        """Test ksqlDB high velocity score rule."""
        streaming_features = {
            "velocity_score": 0.85,  # Above 0.8
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.25
        assert "ksqldb_high_risk" in result.triggered_rules[0]

    def test_medium_velocity_score_ksqldb(self, adjuster):
        """Test ksqlDB medium velocity score rule."""
        streaming_features = {
            "velocity_score": 0.6,  # Between 0.5 and 0.8
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.1
        assert "ksqldb_medium_risk" in result.triggered_rules[0]

    def test_amount_anomaly(self, adjuster):
        """Test amount anomaly rule."""
        streaming_features = {
            "amount_anomaly_flag": "true",  # String 'true'
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.total_boost == 0.2
        assert "amount_anomaly" in result.triggered_rules[0]

    def test_multiple_rules_triggered(self, adjuster):
        """Test when multiple rules are triggered."""
        streaming_features = {
            "tx_count_5min": 6,  # high_velocity_5min: 0.3
            "multi_country_flag": True,  # multi_country: 0.25
            "velocity_score": 0.9,  # ksqldb_high_risk: 0.25
        }

        result = adjuster.apply(ml_score=0.2, streaming_features=streaming_features)

        # Max boost should be 0.3 (highest individual boost)
        assert result.total_boost == 0.3
        assert len(result.triggered_rules) == 3

    def test_max_boost_cap(self, custom_adjuster):
        """Test that boost is capped at max_boost."""
        streaming_features = {
            "tx_count_5min": 10,  # Would give 0.3 boost
            "multi_country_flag": True,  # Would give 0.25 boost
            "velocity_score": 0.9,  # Would give 0.25 boost
        }

        result = custom_adjuster.apply(ml_score=0.5, streaming_features=streaming_features)

        # Custom max_boost is 0.4, but highest boost is 0.3
        assert result.total_boost == 0.3

    def test_score_capped_at_one(self, adjuster):
        """Test that final score is capped at 1.0."""
        streaming_features = {
            "tx_count_5min": 10,  # 0.3 boost
        }

        result = adjuster.apply(ml_score=0.9, streaming_features=streaming_features)

        assert result.final_score == 1.0

    def test_streaming_features_extraction(self, adjuster):
        """Test that streaming features are correctly extracted."""
        streaming_features = {
            "tx_count_5min": 3,
            "tx_count_1h": 15,
            "tx_count_24h": 40,
            "velocity_score": 0.6,
            "multi_country_flag": True,
            "multi_device_flag": False,
            "high_velocity_flag": False,
            "amount_anomaly_flag": False,
        }

        result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)

        assert result.streaming_features_used["tx_count_5min"] == 3
        assert result.streaming_features_used["tx_count_1h"] == 15
        assert result.streaming_features_used["tx_count_24h"] == 40
        assert result.streaming_features_used["velocity_score"] == 0.6
        assert result.streaming_features_used["multi_country_flag"] is True

    def test_safe_int_conversion(self, adjuster):
        """Test safe integer conversion for invalid values."""
        streaming_features = {
            "tx_count_5min": "invalid",
            "tx_count_1h": None,
        }

        result = adjuster.apply(ml_score=0.5, streaming_features=streaming_features)

        # Should not crash, values default to 0
        assert result.streaming_features_used["tx_count_5min"] == 0
        assert result.streaming_features_used["tx_count_1h"] == 0

    def test_safe_float_conversion(self, adjuster):
        """Test safe float conversion for invalid values."""
        streaming_features = {
            "velocity_score": "not_a_number",
        }

        result = adjuster.apply(ml_score=0.5, streaming_features=streaming_features)

        assert result.streaming_features_used["velocity_score"] == 0.0

    def test_is_truthy_various_values(self, adjuster):
        """Test truthy detection for various value types."""
        # Test values that should be truthy
        truthy_values = ['1', 1, True, 'true', 'True']

        for val in truthy_values:
            streaming_features = {"multi_country_flag": val}
            result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)
            assert result.streaming_features_used["multi_country_flag"] is True

        # Test values that should be falsy
        falsy_values = ['0', 0, False, 'false', '', None]

        for val in falsy_values:
            streaming_features = {"multi_country_flag": val}
            result = adjuster.apply(ml_score=0.3, streaming_features=streaming_features)
            assert result.streaming_features_used["multi_country_flag"] is False

    def test_below_all_thresholds(self, adjuster):
        """Test when all values are below thresholds."""
        streaming_features = {
            "tx_count_5min": 1,
            "tx_count_1h": 5,
            "tx_count_24h": 20,
            "velocity_score": 0.3,
            "multi_country_flag": False,
            "multi_device_flag": False,
            "amount_anomaly_flag": False,
        }

        result = adjuster.apply(ml_score=0.4, streaming_features=streaming_features)

        assert result.total_boost == 0.0
        assert result.final_score == 0.4
        assert result.triggered_rules == []
        assert result.streaming_available is True


class TestScoreAdjusterEdgeCases:
    """Edge case tests for ScoreAdjuster."""

    def test_zero_ml_score(self):
        """Test with zero ML score."""
        adjuster = ScoreAdjuster()
        streaming_features = {"tx_count_5min": 6}

        result = adjuster.apply(ml_score=0.0, streaming_features=streaming_features)

        assert result.original_score == 0.0
        assert result.final_score == 0.3

    def test_one_ml_score(self):
        """Test with ML score of 1.0."""
        adjuster = ScoreAdjuster()
        streaming_features = {"tx_count_5min": 6}

        result = adjuster.apply(ml_score=1.0, streaming_features=streaming_features)

        assert result.original_score == 1.0
        assert result.final_score == 1.0  # Capped at 1.0

    def test_negative_ml_score(self):
        """Test with negative ML score (edge case)."""
        adjuster = ScoreAdjuster()
        streaming_features = {}

        result = adjuster.apply(ml_score=-0.1, streaming_features=streaming_features)

        assert result.final_score == -0.1

    def test_zero_max_boost(self):
        """Test with max_boost of 0."""
        adjuster = ScoreAdjuster(max_boost=0.0)
        streaming_features = {"tx_count_5min": 10}

        result = adjuster.apply(ml_score=0.5, streaming_features=streaming_features)

        assert result.total_boost == 0.0
        assert result.final_score == 0.5
        # Rules are still triggered
        assert len(result.triggered_rules) > 0
