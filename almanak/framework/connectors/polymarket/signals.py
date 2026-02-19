"""External signal integration for Polymarket prediction strategies.

This module provides a framework for integrating external signals into prediction
market strategies. Signal providers can be used to make trading decisions based on
news sentiment, social media analysis, model predictions, or any other external data.

Architecture:
    The signal system follows a provider pattern:
    1. Define signal providers that implement the PredictionSignal protocol
    2. Each provider's get_signal() method returns a SignalResult
    3. Multiple signals can be aggregated using aggregate_signals()
    4. Strategies use the aggregated signal to inform trading decisions

Example:
    from almanak.framework.connectors.polymarket.signals import (
        SignalResult,
        SignalDirection,
        aggregate_signals,
    )

    class MyCustomSignalProvider:
        def get_signal(self, market_id: str, **kwargs) -> SignalResult:
            # Your signal logic here
            return SignalResult(
                direction=SignalDirection.BULLISH,
                confidence=0.75,
            )

    # Use in strategy
    provider = MyCustomSignalProvider()
    signal = provider.get_signal("market-123")
    if signal.direction == SignalDirection.BULLISH and signal.confidence > 0.6:
        # Execute buy

Implementing Custom Signal Providers:
    To create a custom signal provider, implement the PredictionSignal protocol:

    1. Create a class with a get_signal(market_id, **kwargs) -> SignalResult method
    2. The method should return a SignalResult with:
       - direction: SignalDirection (BULLISH, BEARISH, or NEUTRAL)
       - confidence: float from 0.0 to 1.0 (higher = more confident)
       - Optional: timestamp, metadata, source, raw_score

    Example implementation:
        from datetime import datetime, UTC
        from almanak.framework.connectors.polymarket.signals import (
            SignalResult, SignalDirection
        )

        class WeatherSignalProvider:
            '''Provides signals based on weather data for weather-related markets.'''

            def __init__(self, api_key: str):
                self.api_key = api_key

            def get_signal(self, market_id: str, **kwargs) -> SignalResult:
                location = kwargs.get("location", "New York")

                # Fetch weather data
                weather = self._fetch_weather(location)

                # Convert to signal
                if weather["precipitation_prob"] > 0.7:
                    direction = SignalDirection.BULLISH
                    confidence = weather["precipitation_prob"]
                elif weather["precipitation_prob"] < 0.3:
                    direction = SignalDirection.BEARISH
                    confidence = 1.0 - weather["precipitation_prob"]
                else:
                    direction = SignalDirection.NEUTRAL
                    confidence = 0.5

                return SignalResult(
                    direction=direction,
                    confidence=confidence,
                    source="weather_api",
                    metadata={"location": location, "raw_data": weather}
                )

            def _fetch_weather(self, location: str) -> dict:
                # Your API call here
                ...

Best Practices:
    1. Always return a SignalResult, even on errors (use NEUTRAL with low confidence)
    2. Include meaningful metadata for debugging and auditing
    3. Use the source field to identify which provider generated the signal
    4. Consider caching to avoid excessive API calls
    5. Handle rate limits and API errors gracefully
    6. Normalize confidence scores consistently (0.0 = no confidence, 1.0 = certain)
    7. Test with historical data before live trading
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import structlog

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


# =============================================================================
# Enums
# =============================================================================


class SignalDirection(StrEnum):
    """Direction of a prediction signal.

    BULLISH: Expect the YES outcome to increase in probability
    BEARISH: Expect the YES outcome to decrease in probability (NO increases)
    NEUTRAL: No clear directional signal
    """

    BULLISH = "bullish"  # Positive for YES outcome
    BEARISH = "bearish"  # Positive for NO outcome
    NEUTRAL = "neutral"  # No clear direction


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SignalResult:
    """Result from a signal provider.

    Represents the output of a signal analysis with direction, confidence,
    and optional metadata for debugging and auditing.

    Attributes:
        direction: The predicted direction (BULLISH, BEARISH, NEUTRAL)
        confidence: Confidence level from 0.0 (no confidence) to 1.0 (certain)
        timestamp: When the signal was generated (defaults to now)
        metadata: Additional data from the signal source (for debugging)
        source: Identifier for the signal provider
        raw_score: Raw numerical score before directional interpretation

    Example:
        result = SignalResult(
            direction=SignalDirection.BULLISH,
            confidence=0.85,
            source="news_sentiment",
            metadata={"headline": "Positive developments..."}
        )
    """

    direction: SignalDirection
    confidence: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict | None = None
    source: str | None = None
    raw_score: float | None = None

    def __post_init__(self) -> None:
        """Validate confidence is in valid range."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "direction": self.direction.value,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "source": self.source,
            "raw_score": self.raw_score,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SignalResult":
        """Create from dictionary."""
        return cls(
            direction=SignalDirection(data["direction"]),
            confidence=data["confidence"],
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(UTC),
            metadata=data.get("metadata"),
            source=data.get("source"),
            raw_score=data.get("raw_score"),
        )

    @classmethod
    def neutral(cls, source: str | None = None, reason: str | None = None) -> "SignalResult":
        """Create a neutral signal with low confidence.

        Useful for error cases or when no signal is available.
        """
        return cls(
            direction=SignalDirection.NEUTRAL,
            confidence=0.5,
            source=source,
            metadata={"reason": reason} if reason else None,
        )


# =============================================================================
# Protocol (Interface)
# =============================================================================


@runtime_checkable
class PredictionSignal(Protocol):
    """Protocol for prediction market signal providers.

    Any class implementing this protocol can be used as a signal provider.
    The protocol is runtime-checkable, so isinstance() can be used.

    Example:
        class MyProvider:
            def get_signal(self, market_id: str, **kwargs) -> SignalResult:
                return SignalResult(SignalDirection.BULLISH, 0.8)

        provider = MyProvider()
        assert isinstance(provider, PredictionSignal)  # True
    """

    def get_signal(self, market_id: str, **kwargs) -> SignalResult:
        """Get signal for a specific market.

        Args:
            market_id: The Polymarket market ID to analyze
            **kwargs: Additional parameters (market question, end date, etc.)

        Returns:
            SignalResult with direction and confidence
        """
        ...


# =============================================================================
# Example Signal Providers
# =============================================================================


class NewsAPISignalProvider:
    """Example signal provider using news sentiment analysis.

    This is a reference implementation showing how to build a news-based
    signal provider. In production, you would integrate with a real news
    API (e.g., NewsAPI, Aylien, or a custom NLP pipeline).

    The provider analyzes news headlines and articles related to the market
    question, extracts sentiment, and converts it to a trading signal.

    Attributes:
        api_key: API key for the news service
        base_url: Base URL for the news API
        sentiment_threshold: Minimum sentiment score to generate a signal

    Example:
        provider = NewsAPISignalProvider(api_key="your-api-key")
        signal = provider.get_signal(
            "market-123",
            question="Will Bitcoin reach $100k by end of 2024?"
        )
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://newsapi.org/v2",
        sentiment_threshold: float = 0.3,
    ):
        """Initialize the news signal provider.

        Args:
            api_key: API key for NewsAPI (or similar service)
            base_url: Base URL for the news API
            sentiment_threshold: Minimum absolute sentiment to generate non-neutral signal
        """
        self.api_key = api_key
        self.base_url = base_url
        self.sentiment_threshold = sentiment_threshold

    def get_signal(self, market_id: str, **kwargs) -> SignalResult:
        """Get signal based on news sentiment.

        This example implementation demonstrates the pattern. In production,
        replace _analyze_sentiment with actual API calls and NLP analysis.

        Args:
            market_id: The Polymarket market ID
            **kwargs: Optional parameters:
                - question: Market question text for keyword extraction
                - keywords: Explicit keywords to search for
                - lookback_hours: How far back to search (default 24)

        Returns:
            SignalResult with sentiment-based direction and confidence
        """
        question = kwargs.get("question", "")
        keywords = kwargs.get("keywords", [])
        lookback_hours = kwargs.get("lookback_hours", 24)

        try:
            # Extract keywords from question if not provided
            if not keywords and question:
                keywords = self._extract_keywords(question)

            # Analyze sentiment (mock implementation)
            sentiment_score = self._analyze_sentiment(keywords, lookback_hours)

            # Convert sentiment to signal
            if sentiment_score > self.sentiment_threshold:
                direction = SignalDirection.BULLISH
                confidence = min(0.5 + sentiment_score / 2, 1.0)
            elif sentiment_score < -self.sentiment_threshold:
                direction = SignalDirection.BEARISH
                confidence = min(0.5 + abs(sentiment_score) / 2, 1.0)
            else:
                direction = SignalDirection.NEUTRAL
                confidence = 0.5

            return SignalResult(
                direction=direction,
                confidence=confidence,
                source="news_sentiment",
                raw_score=sentiment_score,
                metadata={
                    "keywords": keywords,
                    "lookback_hours": lookback_hours,
                    "market_id": market_id,
                },
            )

        except Exception as e:
            logger.warning("news_signal_error", error=str(e), market_id=market_id)
            return SignalResult.neutral(source="news_sentiment", reason=str(e))

    def _extract_keywords(self, question: str) -> list[str]:
        """Extract keywords from market question.

        This is a simple implementation. In production, use NLP libraries
        like spaCy or NLTK for better keyword extraction.
        """
        # Remove common words and extract key terms
        stop_words = {"will", "the", "a", "an", "is", "are", "be", "to", "of", "in", "on", "by", "for", "at", "with"}
        words = question.lower().replace("?", "").replace("'", "").split()
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        return keywords[:5]  # Limit to 5 keywords

    def _analyze_sentiment(self, keywords: list[str], lookback_hours: int) -> float:
        """Analyze sentiment from news articles.

        This is a mock implementation. In production:
        1. Call news API to fetch recent articles matching keywords
        2. Use sentiment analysis (VADER, TextBlob, or ML model) on articles
        3. Aggregate sentiment scores

        Returns:
            Sentiment score from -1.0 (very negative) to 1.0 (very positive)
        """
        # Mock implementation - returns neutral sentiment
        # In production, this would call the actual news API and analyze
        logger.debug(
            "news_sentiment_analysis",
            keywords=keywords,
            lookback_hours=lookback_hours,
            note="Mock implementation - replace with real API",
        )
        return 0.0  # Neutral


class SocialSentimentProvider:
    """Example signal provider using social media sentiment.

    This is a reference implementation showing how to build a social media
    signal provider. In production, you would integrate with social APIs
    (e.g., Twitter/X API, Reddit API) or social listening platforms.

    The provider analyzes social media posts and discussions related to
    the market topic, extracts sentiment and engagement metrics, and
    converts them to a trading signal.

    Attributes:
        platforms: List of platforms to analyze (twitter, reddit, etc.)
        min_engagement: Minimum engagement score to consider
        sentiment_model: Sentiment analysis approach to use

    Example:
        provider = SocialSentimentProvider(platforms=["twitter", "reddit"])
        signal = provider.get_signal(
            "market-123",
            topic="Bitcoin ETF approval",
            hashtags=["#Bitcoin", "#ETF"]
        )
    """

    def __init__(
        self,
        platforms: list[str] | None = None,
        min_engagement: int = 10,
        sentiment_model: str = "vader",
    ):
        """Initialize the social sentiment provider.

        Args:
            platforms: Platforms to analyze (default: ["twitter", "reddit"])
            min_engagement: Minimum engagement (likes, upvotes) to consider
            sentiment_model: Model for sentiment analysis ("vader", "textblob", "ml")
        """
        self.platforms = platforms or ["twitter", "reddit"]
        self.min_engagement = min_engagement
        self.sentiment_model = sentiment_model

    def get_signal(self, market_id: str, **kwargs) -> SignalResult:
        """Get signal based on social media sentiment.

        This example implementation demonstrates the pattern. In production,
        replace with actual social API integration.

        Args:
            market_id: The Polymarket market ID
            **kwargs: Optional parameters:
                - topic: Topic to search for
                - hashtags: Hashtags to track
                - lookback_hours: How far back to search (default 6)

        Returns:
            SignalResult with social-sentiment-based direction and confidence
        """
        topic = kwargs.get("topic", "")
        hashtags = kwargs.get("hashtags", [])
        lookback_hours = kwargs.get("lookback_hours", 6)

        try:
            # Analyze social sentiment (mock implementation)
            sentiment_data = self._analyze_social_sentiment(topic, hashtags, lookback_hours)

            sentiment_score = sentiment_data["score"]
            volume = sentiment_data["volume"]

            # Volume-weighted confidence
            volume_factor = min(volume / 1000, 1.0)  # Normalize volume

            if abs(sentiment_score) < 0.1:
                direction = SignalDirection.NEUTRAL
                confidence = 0.5
            else:
                direction = SignalDirection.BULLISH if sentiment_score > 0 else SignalDirection.BEARISH
                # Confidence based on both sentiment strength and volume
                confidence = min(0.5 + abs(sentiment_score) * 0.4 + volume_factor * 0.1, 1.0)

            return SignalResult(
                direction=direction,
                confidence=confidence,
                source="social_sentiment",
                raw_score=sentiment_score,
                metadata={
                    "topic": topic,
                    "hashtags": hashtags,
                    "volume": volume,
                    "platforms": self.platforms,
                    "market_id": market_id,
                },
            )

        except Exception as e:
            logger.warning("social_signal_error", error=str(e), market_id=market_id)
            return SignalResult.neutral(source="social_sentiment", reason=str(e))

    def _analyze_social_sentiment(self, topic: str, hashtags: list[str], lookback_hours: int) -> dict:
        """Analyze sentiment from social media.

        This is a mock implementation. In production:
        1. Call social APIs (Twitter, Reddit) to fetch posts
        2. Filter by engagement threshold
        3. Apply sentiment analysis to each post
        4. Aggregate weighted by engagement

        Returns:
            Dictionary with sentiment score and volume
        """
        logger.debug(
            "social_sentiment_analysis",
            topic=topic,
            hashtags=hashtags,
            lookback_hours=lookback_hours,
            note="Mock implementation - replace with real API",
        )
        return {"score": 0.0, "volume": 0}  # Neutral mock


class ModelPredictionProvider:
    """Signal provider that wraps a machine learning model.

    This provider allows integration of custom ML models that predict
    market outcomes. The model should output a probability for the YES
    outcome, which is then converted to a SignalResult.

    Attributes:
        model: The prediction model (any callable returning float 0-1)
        confidence_calibration: Calibration factor for model confidence

    Example:
        from sklearn.linear_model import LogisticRegression

        # Train your model
        model = LogisticRegression()
        model.fit(X_train, y_train)

        # Wrap in provider
        def predict_fn(market_id, features):
            return model.predict_proba([features])[0][1]

        provider = ModelPredictionProvider(model=predict_fn)
        signal = provider.get_signal("market-123", features=[0.5, 0.3, 0.8])
    """

    def __init__(
        self,
        model: Callable | None = None,
        confidence_calibration: float = 1.0,
        threshold_bullish: float = 0.6,
        threshold_bearish: float = 0.4,
    ):
        """Initialize the model prediction provider.

        Args:
            model: Callable that takes (market_id, **kwargs) and returns probability 0-1
            confidence_calibration: Factor to scale model confidence
            threshold_bullish: Probability above which signal is BULLISH
            threshold_bearish: Probability below which signal is BEARISH
        """
        self.model = model
        self.confidence_calibration = confidence_calibration
        self.threshold_bullish = threshold_bullish
        self.threshold_bearish = threshold_bearish

    def get_signal(self, market_id: str, **kwargs) -> SignalResult:
        """Get signal from model prediction.

        Args:
            market_id: The Polymarket market ID
            **kwargs: Parameters to pass to the model (e.g., features)

        Returns:
            SignalResult based on model prediction
        """
        if self.model is None:
            return SignalResult.neutral(source="model_prediction", reason="No model configured")

        try:
            # Get model prediction (probability for YES outcome)
            probability = float(self.model(market_id, **kwargs))

            if not 0.0 <= probability <= 1.0:
                raise ValueError(f"Model probability must be 0-1, got {probability}")

            # Convert probability to direction
            if probability > self.threshold_bullish:
                direction = SignalDirection.BULLISH
                # Confidence increases with distance from threshold
                confidence = 0.5 + (probability - self.threshold_bullish) / (1 - self.threshold_bullish) * 0.5
            elif probability < self.threshold_bearish:
                direction = SignalDirection.BEARISH
                confidence = 0.5 + (self.threshold_bearish - probability) / self.threshold_bearish * 0.5
            else:
                direction = SignalDirection.NEUTRAL
                confidence = 0.5

            # Apply calibration
            confidence = min(confidence * self.confidence_calibration, 1.0)

            return SignalResult(
                direction=direction,
                confidence=confidence,
                source="model_prediction",
                raw_score=probability,
                metadata={
                    "market_id": market_id,
                    "threshold_bullish": self.threshold_bullish,
                    "threshold_bearish": self.threshold_bearish,
                },
            )

        except Exception as e:
            logger.warning("model_signal_error", error=str(e), market_id=market_id)
            return SignalResult.neutral(source="model_prediction", reason=str(e))


# =============================================================================
# Aggregation Utilities
# =============================================================================


def aggregate_signals(
    signals: list[SignalResult],
    weights: list[float] | None = None,
    min_agreement: float = 0.0,
) -> SignalResult:
    """Aggregate multiple signals into a single signal.

    Combines signals from multiple providers using weighted averaging.
    Each signal's contribution is weighted by both its explicit weight
    and its confidence level.

    Args:
        signals: List of SignalResult objects to aggregate
        weights: Optional weights for each signal (default: equal weights)
        min_agreement: Minimum fraction of signals that must agree on direction
                      for non-neutral result (0.0 to 1.0)

    Returns:
        Aggregated SignalResult

    Example:
        signals = [
            SignalResult(SignalDirection.BULLISH, 0.8, source="news"),
            SignalResult(SignalDirection.BULLISH, 0.6, source="social"),
            SignalResult(SignalDirection.NEUTRAL, 0.5, source="model"),
        ]
        result = aggregate_signals(signals)
        # result.direction = BULLISH (2/3 agreement)
        # result.confidence = weighted average of confidences

    Note:
        - Empty signals list returns neutral with 0.5 confidence
        - Signals with higher confidence have more influence
        - Direction is determined by vote weighted by confidence
        - Final confidence is the average confidence of the winning direction
    """
    if not signals:
        return SignalResult.neutral(source="aggregated", reason="No signals provided")

    # Default to equal weights
    if weights is None:
        weights = [1.0] * len(signals)

    if len(weights) != len(signals):
        raise ValueError(f"Weights length ({len(weights)}) must match signals length ({len(signals)})")

    # Calculate weighted votes for each direction
    direction_votes: dict[SignalDirection, float] = {
        SignalDirection.BULLISH: 0.0,
        SignalDirection.BEARISH: 0.0,
        SignalDirection.NEUTRAL: 0.0,
    }
    direction_confidences: dict[SignalDirection, list[float]] = {
        SignalDirection.BULLISH: [],
        SignalDirection.BEARISH: [],
        SignalDirection.NEUTRAL: [],
    }

    total_weight = 0.0
    for signal, weight in zip(signals, weights, strict=False):
        # Weight by both explicit weight and signal confidence
        effective_weight = weight * signal.confidence
        direction_votes[signal.direction] += effective_weight
        direction_confidences[signal.direction].append(signal.confidence)
        total_weight += effective_weight

    # Normalize votes
    if total_weight > 0:
        for direction in direction_votes:
            direction_votes[direction] /= total_weight

    # Check agreement threshold
    winning_direction = max(direction_votes.keys(), key=lambda d: direction_votes[d])

    # Count actual signals (not weighted) for agreement
    signal_counts = {
        SignalDirection.BULLISH: sum(1 for s in signals if s.direction == SignalDirection.BULLISH),
        SignalDirection.BEARISH: sum(1 for s in signals if s.direction == SignalDirection.BEARISH),
        SignalDirection.NEUTRAL: sum(1 for s in signals if s.direction == SignalDirection.NEUTRAL),
    }
    max_count = max(signal_counts.values())
    agreement_ratio = max_count / len(signals)

    if agreement_ratio < min_agreement:
        # Not enough agreement - return neutral
        winning_direction = SignalDirection.NEUTRAL

    # Calculate final confidence
    if winning_direction == SignalDirection.NEUTRAL:
        final_confidence = 0.5
    else:
        # Average confidence of signals agreeing with winning direction
        agreeing_confidences = direction_confidences[winning_direction]
        if agreeing_confidences:
            final_confidence = sum(agreeing_confidences) / len(agreeing_confidences)
            # Scale by agreement ratio
            final_confidence *= agreement_ratio
        else:
            final_confidence = 0.5

    # Collect sources
    sources = [s.source for s in signals if s.source]

    return SignalResult(
        direction=winning_direction,
        confidence=min(final_confidence, 1.0),
        source="aggregated",
        metadata={
            "sources": sources,
            "num_signals": len(signals),
            "agreement_ratio": agreement_ratio,
            "direction_votes": {d.value: v for d, v in direction_votes.items()},
        },
    )


def combine_with_market_price(
    signal: SignalResult,
    current_price: Decimal,
    edge_threshold: Decimal = Decimal("0.05"),
) -> SignalResult:
    """Combine signal with market price to calculate expected edge.

    Adjusts signal confidence based on the potential edge between
    the signal's implied probability and the current market price.

    Args:
        signal: The signal to evaluate
        current_price: Current YES token price (0-1)
        edge_threshold: Minimum edge required for confident signal

    Returns:
        Adjusted SignalResult with edge metadata

    Example:
        signal = SignalResult(SignalDirection.BULLISH, 0.8)
        current_price = Decimal("0.50")  # Market thinks 50/50
        adjusted = combine_with_market_price(signal, current_price)
        # If signal says bullish with 80% confidence, implied prob ~65-70%
        # Edge = 0.65 - 0.50 = 0.15 (15% edge)
    """
    # Convert signal confidence to implied probability
    if signal.direction == SignalDirection.BULLISH:
        # High confidence bullish = high probability for YES
        implied_probability = Decimal("0.5") + Decimal(str(signal.confidence - 0.5))
    elif signal.direction == SignalDirection.BEARISH:
        # High confidence bearish = low probability for YES
        implied_probability = Decimal("0.5") - Decimal(str(signal.confidence - 0.5))
    else:
        implied_probability = Decimal("0.5")

    # Clamp to valid range
    implied_probability = max(min(implied_probability, Decimal("0.99")), Decimal("0.01"))

    # Calculate edge
    # Edge represents profit opportunity: what we think it's worth minus what we pay
    if signal.direction == SignalDirection.BULLISH:
        # Buying YES: we pay current_price, we think it's worth implied_probability
        edge = implied_probability - current_price
    elif signal.direction == SignalDirection.BEARISH:
        # Buying NO: we pay (1 - current_price), we think NO is worth (1 - implied_probability)
        # Edge = (1 - implied_probability) - (1 - current_price) = current_price - implied_probability
        edge = current_price - implied_probability
    else:
        edge = Decimal("0")

    # Adjust confidence based on edge
    if abs(edge) < edge_threshold:
        # Edge too small - reduce confidence
        adjusted_confidence = signal.confidence * 0.5
        if adjusted_confidence < 0.55:
            adjusted_direction = SignalDirection.NEUTRAL
            adjusted_confidence = 0.5
        else:
            adjusted_direction = signal.direction
    else:
        adjusted_direction = signal.direction
        adjusted_confidence = signal.confidence

    return SignalResult(
        direction=adjusted_direction,
        confidence=adjusted_confidence,
        source=signal.source,
        raw_score=signal.raw_score,
        metadata={
            **(signal.metadata or {}),
            "implied_probability": float(implied_probability),
            "current_price": float(current_price),
            "edge": float(edge),
            "edge_threshold": float(edge_threshold),
        },
    )


__all__ = [
    # Enums
    "SignalDirection",
    # Data Classes
    "SignalResult",
    # Protocol
    "PredictionSignal",
    # Example Providers
    "NewsAPISignalProvider",
    "SocialSentimentProvider",
    "ModelPredictionProvider",
    # Utilities
    "aggregate_signals",
    "combine_with_market_price",
]
