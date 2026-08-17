"""GMX collateral validation derives from verified market metadata (ALM-3199)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.base.compiler import PerpCompilerContext
from almanak.connectors.gmx_v2.compiler import GMXV2Compiler
from almanak.connectors.gmx_v2.market_identity import canonicalise_market
from almanak.connectors.gmx_v2.permission_seed import permission_markets
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import PerpOpenIntent
from tests.unit.connectors.gmx_v2.market_fixtures import market_record, prime_catalog


def _ctx(*, chain: str = "arbitrum", permission_discovery: bool = False) -> PerpCompilerContext:
    return PerpCompilerContext(
        chain=chain,
        wallet_address="0x" + "ab" * 20,
        rpc_url=None,
        rpc_timeout=10.0,
        permission_discovery=permission_discovery,
        allow_placeholder_prices=True,
        token_resolver=None,
        gateway_client=None,
        price_oracle=None,
        cache={},
        services=MagicMock(),
        default_protocol="gmx_v2",
        protocol="gmx_v2",
    )


@pytest.mark.parametrize("value", ["ETH/USD", "ETH-USD", "ETH_USD", "ETH:USD", "eth-usd"])
def test_market_label_spellings_share_one_identity(value: str) -> None:
    assert canonicalise_market(value) == "ETH/USD"


def test_raw_market_address_preserves_checksum_case() -> None:
    address = market_record("arbitrum", "ETH/USD").market_token
    assert canonicalise_market(address) == address


def test_long_collateral_missing_from_old_mirror_resolves_from_verified_tuple() -> None:
    """VIB-6401: SOL must not depend on a connector token mirror entry."""
    compiler = GMXV2Compiler()
    record = market_record("arbitrum", "SOL/USD")
    prime_catalog(record, chain="arbitrum")

    resolved = compiler._resolve_collateral(
        _ctx(),
        "SOL",
        "intent-1",
        market_address=record.market_token,
    )

    assert resolved == record.long_token


def test_long_collateral_decimals_come_from_same_verified_tuple() -> None:
    compiler = GMXV2Compiler()
    record = market_record("arbitrum", "SOL/USD")
    prime_catalog(record, chain="arbitrum")

    decimals = compiler._market_collateral_decimals(
        _ctx(),
        record.market_token,
        record.long_token,
        "intent-1",
    )

    assert decimals == 9


def test_verified_market_symbol_precedes_generic_token_resolution() -> None:
    compiler = GMXV2Compiler()
    record = market_record("arbitrum", "ETH/USD")
    prime_catalog(record, chain="arbitrum")
    ctx = SimpleNamespace(
        chain="arbitrum",
        permission_discovery=False,
        token_resolver=object(),
        services=SimpleNamespace(resolve_token=lambda _token: SimpleNamespace(address="0x" + "1" * 40)),
    )

    resolved = compiler._resolve_collateral(
        ctx,
        "USDC",
        "intent-1",
        market_address=record.market_token,
    )

    assert resolved == record.short_token


def test_wrong_collateral_address_is_a_safety_refusal() -> None:
    compiler = GMXV2Compiler()
    record = market_record("arbitrum", "SOL/USD")
    prime_catalog(record, chain="arbitrum")
    intent = PerpOpenIntent(
        market=record.market_token,
        collateral_token="WETH",
        collateral_amount="1",
        size_usd="100",
        is_long=True,
        leverage="2",
        protocol="gmx_v2",
    )

    result = compiler._validate_market_collateral(
        _ctx(),
        record.market_token,
        market_record("arbitrum", "ETH/USD").long_token,
        intent,
    )

    assert result is not None
    assert result.status is CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert "SOL" in (result.error or "")
    assert "USDC" in (result.error or "")


@pytest.mark.parametrize("prefix", ["0x", "0X"])
def test_raw_collateral_address_forms_pass_through(prefix: str) -> None:
    address = prefix + "82aF49447D8a07e3bd95BD0d56f35241523fBab1"
    assert GMXV2Compiler()._resolve_collateral(_ctx(), address, "intent-1") == address


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_permission_collateral_comes_from_bounded_seed(chain: str) -> None:
    compiler = GMXV2Compiler()
    seed = permission_markets()[chain]

    resolved = compiler._resolve_collateral(
        _ctx(chain=chain, permission_discovery=True),
        seed.short_token_symbol,
        "permission-intent",
        market_address=seed.market_token,
    )

    assert resolved == seed.short_token


def test_unverified_runtime_market_has_no_static_collateral_grace() -> None:
    compiler = GMXV2Compiler()
    intent = SimpleNamespace(collateral_token="WETH", intent_id="intent-1")

    assert compiler._market_metadata("arbitrum", "0x" + "1" * 40) is None
    # No metadata means no connector-owned rule is invented. Runtime market
    # resolution fails before this point; this helper remains neutral for old
    # close records while the address verifier owns the fail-closed boundary.
    assert (
        compiler._validate_market_collateral(
            _ctx(),
            "0x" + "1" * 40,
            "0x" + "2" * 40,
            intent,
        )
        is None
    )
