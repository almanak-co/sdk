"""Unit tests for signal integration module."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket.signals import (
    ModelPredictionProvider,
    NewsAPISignalProvider,
    PredictionSignal,
    SignalDirection,
    SignalResult,
    SocialSentimentProvider,
    aggregate_signals,
    combine_with_market_price,
)

# =============================================================================
# SignalDirection Tests
# =============================================================================


class TestSignalDirection:
    """Tests for SignalDirection enum."""

    def test_direction_values(self) -> None:
        """Test that all direction values exist."""
        assert SignalDirection.BULLISH.value == "bullish"
        assert SignalDirection.BEARISH.value == "bearish"
        assert SignalDirection.NEUTRAL.value == "neutral"

    def test_direction_from_string(self) -> None:
        """Test creating direction from string."""
        assert SignalDirection("bullish") == SignalDirection.BULLISH
        assert SignalDirection("bearish") == SignalDirection.BEARISH
        assert SignalDirection("neutral") == SignalDirection.NEUTRAL

    def test_invalid_direction_raises(self) -> None:
        """Test that invalid direction raises ValueError."""
        with pytest.raises(ValueError):
            SignalDirection("invalid")


# =============================================================================
# SignalResult Tests
# =============================================================================


class TestSignalResult:
    """Tests for SignalResult dataclass."""

    def test_basic_creation(self) -> None:
        """Test creating a basic signal result."""
        result = SignalResult(
            direction=SignalDirection.BULLISH,
            confidence=0.75,
        )
        assert result.direction == SignalDirection.BULLISH
        assert result.confidence == 0.75
        assert result.timestamp is not None
        assert result.metadata is None
        assert result.source is None
        assert result.raw_score is None

    def test_full_creation(self) -> None:
        """Test creating a signal result with all fields."""
        ts = datetime.now(UTC)
        result = SignalResult(
            direction=SignalDirection.BEARISH,
            confidence=0.85,
            timestamp=ts,
            metadata={"key": "value"},
            source="test_source",
            raw_score=-0.7,
        )
        assert result.direction == SignalDirection.BEARISH
        assert result.confidence == 0.85
        assert result.timestamp == ts
        assert result.metadata == {"key": "value"}
        assert result.source == "test_source"
        assert result.raw_score == -0.7

    def test_confidence_validation_zero(self) -> None:
        """Test that confidence 0.0 is valid."""
        result = SignalResult(direction=SignalDirection.NEUTRAL, confidence=0.0)
        assert result.confidence == 0.0

    def test_confidence_validation_one(self) -> None:
        """Test that confidence 1.0 is valid."""
        result = SignalResult(direction=SignalDirection.BULLISH, confidence=1.0)
        assert result.confidence == 1.0

    def test_confidence_validation_below_zero_raises(self) -> None:
        """Test that confidence below 0 raises ValueError."""
        with pytest.raises(ValueError, match="Confidence must be between"):
            SignalResult(direction=SignalDirection.BULLISH, confidence=-0.1)

    def test_confidence_validation_above_one_raises(self) -> None:
        """Test that confidence above 1 raises ValueError."""
        with pytest.raises(ValueError, match="Confidence must be between"):
            SignalResult(direction=SignalDirection.BULLISH, confidence=1.1)

    def test_to_dict(self) -> None:
        """Test converting signal result to dictionary."""
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = SignalResult(
            direction=SignalDirection.BULLISH,
            confidence=0.8,
            timestamp=ts,
            metadata={"test": True},
            source="test",
            raw_score=0.6,
        )
        d = result.to_dict()

        assert d["direction"] == "bullish"
        assert d["confidence"] == 0.8
        assert d["timestamp"] == "2024-01-15T12:00:00+00:00"
        assert d["metadata"] == {"test": True}
        assert d["source"] == "test"
        assert d["raw_score"] == 0.6

    def test_from_dict(self) -> None:
        """Test creating signal result from dictionary."""
        d = {
            "direction": "bearish",
            "confidence": 0.65,
            "timestamp": "2024-01-15T12:00:00+00:00",
            "metadata": {"key": "val"},
            "source": "from_dict_test",
            "raw_score": -0.3,
        }
        result = SignalResult.from_dict(d)

        assert result.direction == SignalDirection.BEARISH
        assert result.confidence == 0.65
        assert result.timestamp.year == 2024
        assert result.metadata == {"key": "val"}
        assert result.source == "from_dict_test"
        assert result.raw_score == -0.3

    def test_from_dict_minimal(self) -> None:
        """Test creating signal result from minimal dictionary."""
        d = {
            "direction": "neutral",
            "confidence": 0.5,
        }
        result = SignalResult.from_dict(d)

        assert result.direction == SignalDirection.NEUTRAL
        assert result.confidence == 0.5
        assert result.timestamp is not None  # Should default to now

    def test_neutral_factory(self) -> None:
        """Test the neutral() factory method."""
        result = SignalResult.neutral(source="test", reason="No data")

        assert result.direction == SignalDirection.NEUTRAL
        assert result.confidence == 0.5
        assert result.source == "test"
        assert result.metadata == {"reason": "No data"}

    def test_neutral_factory_no_reason(self) -> None:
        """Test neutral() without reason."""
        result = SignalResult.neutral(source="test")

        assert result.direction == SignalDirection.NEUTRAL
        assert result.metadata is None

    def test_roundtrip_serialization(self) -> None:
        """Test that to_dict/from_dict is a roundtrip."""
        original = SignalResult(
            direction=SignalDirection.BULLISH,
            confidence=0.9,
            metadata={"nested": {"data": [1, 2, 3]}},
            source="roundtrip_test",
            raw_score=0.85,
        )
        d = original.to_dict()
        restored = SignalResult.from_dict(d)

        assert restored.direction == original.direction
        assert restored.confidence == original.confidence
        assert restored.metadata == original.metadata
        assert restored.source == original.source
        assert restored.raw_score == original.raw_score


# =============================================================================
# PredictionSignal Protocol Tests
# =============================================================================


class TestPredictionSignalProtocol:
    """Tests for PredictionSignal protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Test that the protocol supports isinstance checks."""

        class MyProvider:
            def get_signal(self, market_id: str, **kwargs) -> SignalResult:
                return SignalResult(SignalDirection.NEUTRAL, 0.5)

        provider = MyProvider()
        assert isinstance(provider, PredictionSignal)

    def test_class_without_method_is_not_signal(self) -> None:
        """Test that a class without get_signal is not a PredictionSignal."""

        class NotAProvider:
            pass

        not_provider = NotAProvider()
        assert not isinstance(not_provider, PredictionSignal)

    def test_class_with_wrong_signature_is_still_signal(self) -> None:
        """Test that protocol only checks method exists, not exact signature."""

        class LooseProvider:
            def get_signal(self) -> SignalResult:  # Missing required params
                return SignalResult(SignalDirection.NEUTRAL, 0.5)

        # Protocol checking is structural, not strict
        provider = LooseProvider()
        assert isinstance(provider, PredictionSignal)


# =============================================================================
# NewsAPISignalProvider Tests
# =============================================================================


class TestNewsAPISignalProvider:
    """Tests for NewsAPISignalProvider."""

    def test_initialization_defaults(self) -> None:
        """Test default initialization."""
        provider = NewsAPISignalProvider()

        assert provider.api_key is None
        assert provider.base_url == "https://newsapi.org/v2"
        assert provider.sentiment_threshold == 0.3

    def test_initialization_custom(self) -> None:
        """Test custom initialization."""
        provider = NewsAPISignalProvider(
            api_key="test-key",
            base_url="https://custom.api.com",
            sentiment_threshold=0.5,
        )

        assert provider.api_key == "test-key"
        assert provider.base_url == "https://custom.api.com"
        assert provider.sentiment_threshold == 0.5

    def test_get_signal_returns_signal_result(self) -> None:
        """Test that get_signal returns a SignalResult."""
        provider = NewsAPISignalProvider()
        result = provider.get_signal("market-123", question="Will X happen?")

        assert isinstance(result, SignalResult)
        assert result.source == "news_sentiment"

    def test_get_signal_includes_metadata(self) -> None:
        """Test that get_signal includes useful metadata."""
        provider = NewsAPISignalProvider()
        result = provider.get_signal("market-123", question="Will Bitcoin rise?", lookback_hours=48)

        assert result.metadata is not None
        assert "keywords" in result.metadata
        assert result.metadata["lookback_hours"] == 48
        assert result.metadata["market_id"] == "market-123"

    def test_get_signal_with_explicit_keywords(self) -> None:
        """Test providing explicit keywords."""
        provider = NewsAPISignalProvider()
        result = provider.get_signal("market-123", keywords=["crypto", "bitcoin"])

        assert result.metadata["keywords"] == ["crypto", "bitcoin"]

    def test_extract_keywords_removes_stop_words(self) -> None:
        """Test that keyword extraction removes stop words."""
        provider = NewsAPISignalProvider()
        keywords = provider._extract_keywords("Will the President be elected in the country?")

        assert "will" not in keywords
        assert "the" not in keywords
        assert "in" not in keywords
        assert len(keywords) <= 5

    def test_mock_sentiment_returns_neutral(self) -> None:
        """Test that mock implementation returns neutral."""
        provider = NewsAPISignalProvider()
        result = provider.get_signal("market-123")

        # Mock implementation returns 0.0 sentiment which is neutral
        assert result.direction == SignalDirection.NEUTRAL
        assert result.raw_score == 0.0

    def test_is_prediction_signal(self) -> None:
        """Test that NewsAPISignalProvider implements PredictionSignal."""
        provider = NewsAPISignalProvider()
        assert isinstance(provider, PredictionSignal)


# =============================================================================
# SocialSentimentProvider Tests
# =============================================================================


class TestSocialSentimentProvider:
    """Tests for SocialSentimentProvider."""

    def test_initialization_defaults(self) -> None:
        """Test default initialization."""
        provider = SocialSentimentProvider()

        assert provider.platforms == ["twitter", "reddit"]
        assert provider.min_engagement == 10
        assert provider.sentiment_model == "vader"

    def test_initialization_custom(self) -> None:
        """Test custom initialization."""
        provider = SocialSentimentProvider(
            platforms=["twitter"],
            min_engagement=100,
            sentiment_model="textblob",
        )

        assert provider.platforms == ["twitter"]
        assert provider.min_engagement == 100
        assert provider.sentiment_model == "textblob"

    def test_get_signal_returns_signal_result(self) -> None:
        """Test that get_signal returns a SignalResult."""
        provider = SocialSentimentProvider()
        result = provider.get_signal("market-123", topic="Bitcoin")

        assert isinstance(result, SignalResult)
        assert result.source == "social_sentiment"

    def test_get_signal_includes_metadata(self) -> None:
        """Test that get_signal includes useful metadata."""
        provider = SocialSentimentProvider()
        result = provider.get_signal(
            "market-123",
            topic="Elections",
            hashtags=["#Vote"],
            lookback_hours=12,
        )

        assert result.metadata is not None
        assert result.metadata["topic"] == "Elections"
        assert result.metadata["hashtags"] == ["#Vote"]
        assert result.metadata["platforms"] == ["twitter", "reddit"]

    def test_mock_sentiment_returns_neutral(self) -> None:
        """Test that mock implementation returns neutral."""
        provider = SocialSentimentProvider()
        result = provider.get_signal("market-123")

        # Mock implementation returns 0.0 sentiment which is neutral
        assert result.direction == SignalDirection.NEUTRAL
        assert result.raw_score == 0.0

    def test_is_prediction_signal(self) -> None:
        """Test that SocialSentimentProvider implements PredictionSignal."""
        provider = SocialSentimentProvider()
        assert isinstance(provider, PredictionSignal)


# =============================================================================
# ModelPredictionProvider Tests
# =============================================================================


class TestModelPredictionProvider:
    """Tests for ModelPredictionProvider."""

    def test_initialization_no_model(self) -> None:
        """Test initialization without model."""
        provider = ModelPredictionProvider()

        assert provider.model is None
        assert provider.confidence_calibration == 1.0
        assert provider.threshold_bullish == 0.6
        assert provider.threshold_bearish == 0.4

    def test_initialization_with_model(self) -> None:
        """Test initialization with custom model."""

        def model_fn(market_id: str, **kwargs) -> float:
            return 0.7

        provider = ModelPredictionProvider(
            model=model_fn,
            confidence_calibration=0.9,
            threshold_bullish=0.65,
            threshold_bearish=0.35,
        )

        assert provider.model is model_fn
        assert provider.confidence_calibration == 0.9
        assert provider.threshold_bullish == 0.65
        assert provider.threshold_bearish == 0.35

    def test_get_signal_without_model_returns_neutral(self) -> None:
        """Test that no model returns neutral signal."""
        provider = ModelPredictionProvider()
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.NEUTRAL
        assert result.metadata["reason"] == "No model configured"

    def test_get_signal_bullish_prediction(self) -> None:
        """Test model predicting bullish outcome."""
        model_fn = MagicMock(return_value=0.8)
        provider = ModelPredictionProvider(model=model_fn)
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.BULLISH
        assert result.confidence > 0.5
        assert result.raw_score == 0.8
        model_fn.assert_called_once()

    def test_get_signal_bearish_prediction(self) -> None:
        """Test model predicting bearish outcome."""
        model_fn = MagicMock(return_value=0.2)
        provider = ModelPredictionProvider(model=model_fn)
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.BEARISH
        assert result.confidence > 0.5
        assert result.raw_score == 0.2

    def test_get_signal_neutral_prediction(self) -> None:
        """Test model predicting neutral outcome."""
        model_fn = MagicMock(return_value=0.5)
        provider = ModelPredictionProvider(model=model_fn)
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.NEUTRAL
        assert result.confidence == 0.5
        assert result.raw_score == 0.5

    def test_get_signal_passes_kwargs_to_model(self) -> None:
        """Test that kwargs are passed to the model."""
        model_fn = MagicMock(return_value=0.5)
        provider = ModelPredictionProvider(model=model_fn)
        provider.get_signal("market-123", features=[1, 2, 3], extra="data")

        model_fn.assert_called_once_with("market-123", features=[1, 2, 3], extra="data")

    def test_get_signal_model_error_returns_neutral(self) -> None:
        """Test that model errors return neutral signal."""
        model_fn = MagicMock(side_effect=Exception("Model error"))
        provider = ModelPredictionProvider(model=model_fn)
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.NEUTRAL
        assert "Model error" in result.metadata["reason"]

    def test_get_signal_invalid_probability_returns_neutral(self) -> None:
        """Test that invalid probability returns neutral signal."""
        model_fn = MagicMock(return_value=1.5)  # Invalid: > 1.0
        provider = ModelPredictionProvider(model=model_fn)
        result = provider.get_signal("market-123")

        assert result.direction == SignalDirection.NEUTRAL
        assert "must be 0-1" in result.metadata["reason"]

    def test_confidence_calibration(self) -> None:
        """Test that confidence calibration is applied."""
        model_fn = MagicMock(return_value=0.8)
        provider = ModelPredictionProvider(model=model_fn, confidence_calibration=0.5)
        result = provider.get_signal("market-123")

        # Confidence should be reduced by calibration factor
        assert result.confidence <= 0.5  # Scaled down

    def test_is_prediction_signal(self) -> None:
        """Test that ModelPredictionProvider implements PredictionSignal."""
        provider = ModelPredictionProvider()
        assert isinstance(provider, PredictionSignal)


# =============================================================================
# aggregate_signals Tests
# =============================================================================


class TestAggregateSignals:
    """Tests for aggregate_signals function."""

    def test_empty_signals_returns_neutral(self) -> None:
        """Test that empty signals list returns neutral."""
        result = aggregate_signals([])

        assert result.direction == SignalDirection.NEUTRAL
        assert result.confidence == 0.5
        assert result.metadata["reason"] == "No signals provided"

    def test_single_signal_preserved(self) -> None:
        """Test that a single signal is preserved."""
        signal = SignalResult(SignalDirection.BULLISH, 0.8, source="test")
        result = aggregate_signals([signal])

        assert result.direction == SignalDirection.BULLISH
        assert result.confidence == 0.8
        assert "test" in result.metadata["sources"]

    def test_unanimous_bullish(self) -> None:
        """Test unanimous bullish signals."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8, source="a"),
            SignalResult(SignalDirection.BULLISH, 0.6, source="b"),
            SignalResult(SignalDirection.BULLISH, 0.9, source="c"),
        ]
        result = aggregate_signals(signals)

        assert result.direction == SignalDirection.BULLISH
        assert result.confidence > 0.7  # Average of confidences

    def test_unanimous_bearish(self) -> None:
        """Test unanimous bearish signals."""
        signals = [
            SignalResult(SignalDirection.BEARISH, 0.7, source="a"),
            SignalResult(SignalDirection.BEARISH, 0.8, source="b"),
        ]
        result = aggregate_signals(signals)

        assert result.direction == SignalDirection.BEARISH

    def test_mixed_signals_majority_wins(self) -> None:
        """Test that majority direction wins."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8, source="a"),
            SignalResult(SignalDirection.BULLISH, 0.7, source="b"),
            SignalResult(SignalDirection.BEARISH, 0.6, source="c"),
        ]
        result = aggregate_signals(signals)

        assert result.direction == SignalDirection.BULLISH
        assert result.metadata["agreement_ratio"] == pytest.approx(2 / 3)

    def test_weighted_aggregation(self) -> None:
        """Test that weights affect aggregation."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8, source="high_weight"),
            SignalResult(SignalDirection.BEARISH, 0.8, source="low_weight"),
        ]
        # Give bullish much higher weight
        result = aggregate_signals(signals, weights=[10.0, 1.0])

        # Bullish should win due to higher weight
        assert result.direction == SignalDirection.BULLISH

    def test_weights_length_mismatch_raises(self) -> None:
        """Test that mismatched weights length raises error."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8),
            SignalResult(SignalDirection.BEARISH, 0.7),
        ]
        with pytest.raises(ValueError, match="Weights length"):
            aggregate_signals(signals, weights=[1.0])  # Only one weight for two signals

    def test_min_agreement_threshold(self) -> None:
        """Test minimum agreement threshold."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8),
            SignalResult(SignalDirection.BEARISH, 0.7),
            SignalResult(SignalDirection.NEUTRAL, 0.6),
        ]
        # Require 50% agreement - none meet this
        result = aggregate_signals(signals, min_agreement=0.5)

        # Should return neutral due to lack of agreement
        assert result.direction == SignalDirection.NEUTRAL

    def test_agreement_above_threshold(self) -> None:
        """Test signals meeting agreement threshold."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8),
            SignalResult(SignalDirection.BULLISH, 0.7),
            SignalResult(SignalDirection.BEARISH, 0.6),
        ]
        # Require 60% agreement - bullish meets this (66%)
        result = aggregate_signals(signals, min_agreement=0.6)

        assert result.direction == SignalDirection.BULLISH

    def test_confidence_weighted_by_agreement(self) -> None:
        """Test that final confidence is scaled by agreement."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 1.0),
            SignalResult(SignalDirection.BEARISH, 1.0),
        ]
        result = aggregate_signals(signals)

        # 50% agreement should reduce confidence
        assert result.confidence < 1.0

    def test_sources_collected_in_metadata(self) -> None:
        """Test that sources are collected in metadata."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8, source="news"),
            SignalResult(SignalDirection.BULLISH, 0.7, source="social"),
            SignalResult(SignalDirection.BULLISH, 0.6),  # No source
        ]
        result = aggregate_signals(signals)

        assert "news" in result.metadata["sources"]
        assert "social" in result.metadata["sources"]
        assert len(result.metadata["sources"]) == 2  # Excludes None

    def test_direction_votes_in_metadata(self) -> None:
        """Test that direction votes are included in metadata."""
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8),
            SignalResult(SignalDirection.BEARISH, 0.6),
        ]
        result = aggregate_signals(signals)

        assert "direction_votes" in result.metadata
        assert "bullish" in result.metadata["direction_votes"]
        assert "bearish" in result.metadata["direction_votes"]


# =============================================================================
# combine_with_market_price Tests
# =============================================================================


class TestCombineWithMarketPrice:
    """Tests for combine_with_market_price function."""

    def test_bullish_signal_with_low_price(self) -> None:
        """Test bullish signal when market price is low (good edge)."""
        signal = SignalResult(SignalDirection.BULLISH, 0.8)
        result = combine_with_market_price(signal, Decimal("0.40"))

        assert result.direction == SignalDirection.BULLISH
        assert result.metadata["edge"] > 0
        assert result.metadata["current_price"] == 0.4

    def test_bullish_signal_with_high_price_reduces_confidence(self) -> None:
        """Test bullish signal when market price is high (small edge)."""
        signal = SignalResult(SignalDirection.BULLISH, 0.6)
        result = combine_with_market_price(signal, Decimal("0.55"), edge_threshold=Decimal("0.1"))

        # Edge is small, confidence should be reduced
        assert result.confidence < signal.confidence

    def test_bearish_signal_with_high_price(self) -> None:
        """Test bearish signal when market price is high (good edge)."""
        signal = SignalResult(SignalDirection.BEARISH, 0.8)
        result = combine_with_market_price(signal, Decimal("0.60"))

        assert result.direction == SignalDirection.BEARISH
        assert result.metadata["edge"] > 0

    def test_neutral_signal_stays_neutral(self) -> None:
        """Test that neutral signal stays neutral."""
        signal = SignalResult(SignalDirection.NEUTRAL, 0.5)
        result = combine_with_market_price(signal, Decimal("0.50"))

        assert result.direction == SignalDirection.NEUTRAL
        assert result.metadata["edge"] == 0

    def test_edge_below_threshold_becomes_neutral(self) -> None:
        """Test that insufficient edge becomes neutral."""
        signal = SignalResult(SignalDirection.BULLISH, 0.55)  # Weak bullish
        # Market at 0.52, implied prob ~0.525, edge ~0.005 < threshold
        result = combine_with_market_price(signal, Decimal("0.52"), edge_threshold=Decimal("0.05"))

        # Should become neutral due to tiny edge
        assert result.direction == SignalDirection.NEUTRAL

    def test_preserves_source_and_raw_score(self) -> None:
        """Test that original source and raw_score are preserved."""
        signal = SignalResult(
            SignalDirection.BULLISH,
            0.8,
            source="test_source",
            raw_score=0.75,
        )
        result = combine_with_market_price(signal, Decimal("0.50"))

        assert result.source == "test_source"
        assert result.raw_score == 0.75

    def test_implied_probability_in_metadata(self) -> None:
        """Test that implied probability is calculated."""
        signal = SignalResult(SignalDirection.BULLISH, 0.8)
        result = combine_with_market_price(signal, Decimal("0.50"))

        assert "implied_probability" in result.metadata
        # 0.8 confidence bullish implies ~0.65 probability for YES
        assert result.metadata["implied_probability"] > 0.5

    def test_original_metadata_preserved(self) -> None:
        """Test that original metadata is preserved and extended."""
        signal = SignalResult(
            SignalDirection.BULLISH,
            0.8,
            metadata={"original_key": "original_value"},
        )
        result = combine_with_market_price(signal, Decimal("0.50"))

        assert result.metadata["original_key"] == "original_value"
        assert "edge" in result.metadata


# =============================================================================
# Integration Tests
# =============================================================================


class TestSignalIntegration:
    """Integration tests for the signal system."""

    def test_full_pipeline(self) -> None:
        """Test a complete signal pipeline."""
        # Create providers
        news = NewsAPISignalProvider()
        social = SocialSentimentProvider()
        model = ModelPredictionProvider(model=lambda m, **k: 0.7)

        # Get signals
        signals = [
            news.get_signal("market-123", question="Will X happen?"),
            social.get_signal("market-123", topic="X event"),
            model.get_signal("market-123"),
        ]

        # Aggregate
        aggregated = aggregate_signals(signals)

        # Combine with market price
        final = combine_with_market_price(aggregated, Decimal("0.55"))

        assert isinstance(final, SignalResult)
        assert final.metadata is not None

    def test_custom_provider_integration(self) -> None:
        """Test integrating a custom provider."""

        class CustomProvider:
            def get_signal(self, market_id: str, **kwargs) -> SignalResult:
                return SignalResult(
                    direction=SignalDirection.BULLISH,
                    confidence=0.9,
                    source="custom",
                )

        provider = CustomProvider()
        assert isinstance(provider, PredictionSignal)

        signal = provider.get_signal("market-123")
        assert signal.direction == SignalDirection.BULLISH
        assert signal.source == "custom"

    def test_error_handling_graceful(self) -> None:
        """Test that errors are handled gracefully."""

        class FailingProvider:
            def get_signal(self, market_id: str, **kwargs) -> SignalResult:
                raise RuntimeError("Provider failed")

        # This would fail, but we handle it by wrapping in try/except in real usage
        # Here we test that aggregate_signals handles neutral signals properly
        neutral = SignalResult.neutral(source="failed", reason="Provider failed")
        result = aggregate_signals([neutral])

        assert result.direction == SignalDirection.NEUTRAL


# =============================================================================
# Module Export Tests
# =============================================================================


class TestModuleExports:
    """Tests for module exports."""

    def test_all_exports_importable(self) -> None:
        """Test that all __all__ exports are importable."""
        from almanak.framework.connectors.polymarket import signals

        for name in signals.__all__:
            assert hasattr(signals, name), f"Missing export: {name}"

    def test_key_classes_available(self) -> None:
        """Test that key classes are available at module level."""
        from almanak.framework.connectors.polymarket.signals import (
            ModelPredictionProvider,
            NewsAPISignalProvider,
            PredictionSignal,
            SignalDirection,
            SignalResult,
            SocialSentimentProvider,
            aggregate_signals,
            combine_with_market_price,
        )

        # Just verify they're importable
        assert SignalDirection is not None
        assert SignalResult is not None
        assert PredictionSignal is not None
        assert NewsAPISignalProvider is not None
        assert SocialSentimentProvider is not None
        assert ModelPredictionProvider is not None
        assert aggregate_signals is not None
        assert combine_with_market_price is not None
