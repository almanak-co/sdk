"""Production-derived resource identity contracts stay exact and ticket-free."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from almanak.connectors._connector_descriptor import ImportRef
from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind, PoolReaderSpec
from almanak.framework.data.qa.production_identity import (
    DirectChainlinkFeedObservation,
    DirectChainlinkFeedRequirement,
    IdentityVerdict,
    ObservationProvenance,
    TokenObservation,
    TokenRequirement,
    V3PoolObservation,
    V3PoolRequirement,
    derive_chainlink_requirements,
    derive_production_requirements,
    derive_token_requirements,
    derive_v3_pool_requirements,
    evaluate_identity,
    evaluate_observation_set,
    requirements_digest,
)
from almanak.framework.data.tokens.models import ChainTokenConfig, Token
from almanak.integrations.chainlink.models import FeedKind, FeedSpec

TOKEN_A = "0x0000000000000000000000000000000000000001"
TOKEN_B = "0x0000000000000000000000000000000000000002"
POOL = "0x0000000000000000000000000000000000000003"
FEED = "0x0000000000000000000000000000000000000004"


def _provenance() -> ObservationProvenance:
    return ObservationProvenance(
        collector="gateway_rpc",
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
        block_number=123,
        block_hash="0x" + "ab" * 32,
        artifact_sha256="cd" * 32,
    )


def test_token_requirements_derive_chain_overrides_and_merge_display_aliases() -> None:
    tokens = [
        Token(symbol="AAA", name="A", decimals=18, addresses={"ethereum": TOKEN_A}),
        Token(symbol="A.A", name="Alias", decimals=18, addresses={"ethereum": TOKEN_A}),
        Token(
            symbol="BBB",
            name="B",
            decimals=18,
            addresses={"ethereum": TOKEN_B},
            chain_overrides={"base": ChainTokenConfig(address=TOKEN_B, decimals=6)},
        ),
    ]

    requirements = derive_token_requirements(tokens)

    ethereum_a = next(requirement for requirement in requirements if requirement.address == TOKEN_A)
    base_b = next(requirement for requirement in requirements if requirement.chain == "base")
    assert ethereum_a.symbols == ("A.A", "AAA")
    assert base_b.decimals == 6
    assert all("alm-" not in requirement.requirement_id for requirement in requirements)


class _Catalog:
    chains = ("ethereum",)

    def feeds(self, chain: str) -> dict[str, FeedSpec]:
        assert chain == "ethereum"
        return {
            "ETH/USD": FeedSpec(
                chain="ethereum",
                chain_id=1,
                pair="ETH/USD",
                address=FEED,
                kind=FeedKind.USD,
                decimals=8,
            )
        }


def test_chainlink_requirements_derive_from_public_catalog_enumeration() -> None:
    assert derive_chainlink_requirements(_Catalog()) == (
        DirectChainlinkFeedRequirement(
            chain="ethereum",
            address=FEED,
            pair="ETH/USD",
            decimals=8,
            feed_kind="usd",
        ),
    )


def test_direct_feed_inventory_has_one_logical_identity_per_contract() -> None:
    requirements = derive_chainlink_requirements()
    addresses = [(requirement.chain, requirement.address) for requirement in requirements]

    assert len(addresses) == len(set(addresses))


def _reader_spec(*, discriminator: PoolDiscriminatorKind = PoolDiscriminatorKind.FEE_TIER) -> PoolReaderSpec:
    return PoolReaderSpec(
        protocol="example_v3",
        factory_addresses={"ethereum": "0x0000000000000000000000000000000000000005"},
        known_pools={"ethereum": {(TOKEN_A, TOKEN_B, 500): POOL}},
        reader=ImportRef(module="almanak.framework.data.pools.reader", attribute="UniswapV3PoolPriceReader"),
        discriminator_kind=discriminator,
    )


def test_v3_pool_requirements_derive_only_fee_tier_known_pools() -> None:
    requirements = derive_v3_pool_requirements(
        [_reader_spec(), _reader_spec(discriminator=PoolDiscriminatorKind.TICK_SPACING)]
    )

    assert requirements == (
        V3PoolRequirement(
            protocol="example_v3",
            chain="ethereum",
            address=POOL,
            token_pair=(TOKEN_A, TOKEN_B),
            fee_tier=500,
        ),
    )


def test_production_projection_uses_current_public_registries_without_ticket_ids() -> None:
    requirements = derive_production_requirements()

    assert requirements
    assert {requirement.kind.value for requirement in requirements} >= {
        "token",
        "direct_chainlink_feed",
        "v3_pool",
    }
    assert all(
        "alm-" not in requirement.requirement_id and "vib-" not in requirement.requirement_id
        for requirement in requirements
    )


def test_requirements_digest_is_order_independent_and_load_bearing() -> None:
    token = TokenRequirement(chain="ethereum", address=TOKEN_A, decimals=18, symbols=("AAA",))
    feed = DirectChainlinkFeedRequirement(chain="ethereum", address=FEED, pair="ETH/USD", decimals=8, feed_kind="usd")

    digest = requirements_digest([token, feed])

    assert digest == requirements_digest([feed, token])
    assert digest != requirements_digest([replace(token, decimals=6), feed])
    assert len(digest) == 64


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("chain", "base", "token_chain_mismatch"),
        ("address", TOKEN_B, "token_address_mismatch"),
        ("decimals", 6, "token_decimals_mismatch"),
    ],
)
def test_token_evaluator_discriminates_every_exact_field(field: str, value: object, reason: str) -> None:
    requirement = TokenRequirement(chain="ethereum", address=TOKEN_A, decimals=18)
    observation = TokenObservation(
        requirement_id=requirement.requirement_id,
        chain="ethereum",
        address=TOKEN_A,
        decimals=18,
        provenance=_provenance(),
    )

    result = evaluate_identity(requirement, replace(observation, **{field: value}))

    assert result.verdict is IdentityVerdict.FAIL
    assert result.reason_codes == (reason,)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("provider", "other", "feed_provider_mismatch"),
        ("chain", "base", "feed_chain_mismatch"),
        ("address", TOKEN_B, "feed_address_mismatch"),
        ("pair", "BTC/USD", "feed_pair_mismatch"),
        ("decimals", 18, "feed_decimals_mismatch"),
        ("feed_kind", "eth", "feed_kind_mismatch"),
    ],
)
def test_feed_evaluator_discriminates_every_exact_field(field: str, value: object, reason: str) -> None:
    requirement = DirectChainlinkFeedRequirement(
        chain="ethereum", address=FEED, pair="ETH/USD", decimals=8, feed_kind="usd"
    )
    observation = DirectChainlinkFeedObservation(
        requirement_id=requirement.requirement_id,
        chain="ethereum",
        address=FEED,
        pair="ETH/USD",
        decimals=8,
        feed_kind="usd",
        provenance=_provenance(),
    )

    result = evaluate_identity(requirement, replace(observation, **{field: value}))

    assert result.verdict is IdentityVerdict.FAIL
    assert result.reason_codes == (reason,)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("protocol", "other_v3", "pool_protocol_mismatch"),
        ("chain", "base", "pool_chain_mismatch"),
        ("address", FEED, "pool_address_mismatch"),
        ("token_pair", (TOKEN_B, TOKEN_A), "pool_token_pair_mismatch"),
        ("fee_tier", 3000, "pool_fee_tier_mismatch"),
    ],
)
def test_pool_evaluator_discriminates_every_exact_field(field: str, value: object, reason: str) -> None:
    requirement = V3PoolRequirement(
        protocol="example_v3", chain="ethereum", address=POOL, token_pair=(TOKEN_A, TOKEN_B), fee_tier=500
    )
    observation = V3PoolObservation(
        requirement_id=requirement.requirement_id,
        protocol="example_v3",
        chain="ethereum",
        address=POOL,
        token_pair=(TOKEN_A, TOKEN_B),
        fee_tier=500,
        provenance=_provenance(),
    )

    result = evaluate_identity(requirement, replace(observation, **{field: value}))

    assert result.verdict is IdentityVerdict.FAIL
    assert result.reason_codes == (reason,)


def test_observation_set_keeps_missing_extra_and_duplicate_evidence_explicit() -> None:
    requirement = TokenRequirement(chain="ethereum", address=TOKEN_A, decimals=18)
    missing = evaluate_observation_set([requirement], [])
    extra = TokenObservation(
        requirement_id="token:ethereum:unexpected",
        chain="ethereum",
        address=TOKEN_B,
        decimals=18,
        provenance=_provenance(),
    )

    assert missing.results[0].verdict is IdentityVerdict.UNMEASURED
    assert missing.passed is False
    assert evaluate_observation_set([requirement], [extra]).unexpected_observation_ids == (extra.requirement_id,)
    with pytest.raises(ValueError, match="Duplicate observation"):
        evaluate_observation_set([requirement], [extra, extra])


def test_provenance_requires_an_independent_capture_anchor() -> None:
    valid = {
        "collector": "gateway_rpc",
        "captured_at": datetime.now(UTC),
        "block_number": 1,
        "block_hash": "0x" + "ab" * 32,
        "artifact_sha256": "cd" * 32,
    }
    with pytest.raises(ValueError, match="must be gateway_rpc"):
        ObservationProvenance(**{**valid, "collector": "self_attested"})
    with pytest.raises(ValueError, match="timezone-aware"):
        ObservationProvenance(**{**valid, "captured_at": datetime(2026, 8, 15)})
    with pytest.raises(ValueError, match="block_hash"):
        ObservationProvenance(**{**valid, "block_hash": "0x1234"})
    with pytest.raises(ValueError, match="artifact_sha256"):
        ObservationProvenance(**{**valid, "artifact_sha256": "not-a-digest"})
