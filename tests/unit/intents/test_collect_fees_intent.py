"""Tests for CollectFeesIntent vocabulary and factory method."""

import pytest

from almanak.framework.intents.vocabulary import (
    CollectFeesIntent,
    Intent,
    IntentType,
)


class TestCollectFeesIntent:
    """Tests for CollectFeesIntent creation and validation."""

    def test_create_basic(self):
        intent = CollectFeesIntent(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
        )
        assert intent.pool == "WAVAX/USDC/20"
        assert intent.protocol == "traderjoe_v2"
        assert intent.intent_type == IntentType.LP_COLLECT_FEES
        assert intent.chain is None
        assert intent.intent_id is not None
        assert intent.created_at is not None

    def test_create_with_chain(self):
        intent = CollectFeesIntent(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
            chain="avalanche",
        )
        assert intent.chain == "avalanche"

    def test_default_protocol_is_traderjoe_v2(self):
        intent = CollectFeesIntent(pool="WAVAX/USDC/20")
        assert intent.protocol == "traderjoe_v2"

    def test_custom_protocol(self):
        intent = CollectFeesIntent(
            pool="ETH/USDC/0.3%",
            protocol="uniswap_v3",
        )
        assert intent.protocol == "uniswap_v3"

    def test_empty_pool_raises(self):
        with pytest.raises(ValueError, match="pool is required"):
            CollectFeesIntent(pool="")

    def test_intent_type_property(self):
        intent = CollectFeesIntent(pool="WAVAX/USDC/20")
        assert intent.intent_type == IntentType.LP_COLLECT_FEES

    def test_immutable(self):
        """CollectFeesIntent should be immutable (AlmanakImmutableModel)."""
        intent = CollectFeesIntent(pool="WAVAX/USDC/20")
        with pytest.raises(Exception):
            intent.pool = "ETH/USDC/20"

    def test_unique_intent_ids(self):
        intent1 = CollectFeesIntent(pool="WAVAX/USDC/20")
        intent2 = CollectFeesIntent(pool="WAVAX/USDC/20")
        assert intent1.intent_id != intent2.intent_id


class TestCollectFeesFactory:
    """Tests for Intent.collect_fees() factory method."""

    def test_factory_basic(self):
        intent = Intent.collect_fees(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
        )
        assert isinstance(intent, CollectFeesIntent)
        assert intent.pool == "WAVAX/USDC/20"
        assert intent.protocol == "traderjoe_v2"

    def test_factory_with_chain(self):
        intent = Intent.collect_fees(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
            chain="avalanche",
        )
        assert intent.chain == "avalanche"

    def test_factory_default_protocol(self):
        intent = Intent.collect_fees(pool="WAVAX/USDC/20")
        assert intent.protocol == "traderjoe_v2"


class TestCollectFeesIntentType:
    """Tests for LP_COLLECT_FEES in IntentType enum."""

    def test_enum_value(self):
        assert IntentType.LP_COLLECT_FEES.value == "LP_COLLECT_FEES"

    def test_enum_name(self):
        assert IntentType.LP_COLLECT_FEES.name == "LP_COLLECT_FEES"

    def test_from_string(self):
        assert IntentType("LP_COLLECT_FEES") == IntentType.LP_COLLECT_FEES
