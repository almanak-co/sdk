"""VIB-5463 / TD-05 — live per-KNOWN-position chain re-derivation for teardown.

Pins the two capabilities that let teardown honour blueprint 14:811 on a
wiped / ``--fresh`` / corrupt-WARM restart:

* ``redrive_lending_position`` re-derives a config-known lending market's live
  collateral / debt / HF from chain (generalises the ``morpho_looping`` pattern),
  and fails CLOSED to ``None`` (caller fall-back) when the read is unavailable —
  never a fabricated zero (Empty ≠ Zero).
* ``chain_verify_lp_open`` verifies a single KNOWN LP NFT's open-ness on-chain
  (per-position, never a wallet scan), returning True / False / None.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.teardown.live_position_reads import (
    LiveLendingPosition,
    chain_verify_lp_open,
    redrive_lending_position,
)
from almanak.framework.teardown.models import PositionInfo, PositionType


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd, health_factor):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = health_factor


class _FakeMarket:
    """MarketSnapshot double exposing only ``position_health`` + ``price``."""

    def __init__(self, *, health=None, raise_health=False, prices=None):
        self._health = health
        self._raise_health = raise_health
        self._prices = prices or {}

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        if self._raise_health:
            raise RuntimeError("gateway down")
        return self._health

    def price(self, token):
        if token not in self._prices:
            raise KeyError(token)
        return self._prices[token]


# ---------------------------------------------------------------------------
# redrive_lending_position
# ---------------------------------------------------------------------------


def test_redrive_returns_live_values_and_token_amounts() -> None:
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("1700"), Decimal("1.72")),
        prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.collateral_value_usd == Decimal("3400")
    assert live.debt_value_usd == Decimal("1700")
    assert live.health_factor == Decimal("1.72")
    assert live.collateral_amount == Decimal("1")  # 3400 / 3400
    assert live.debt_amount == Decimal("1700")  # 1700 / 1
    assert live.has_exposure() is True


def test_redrive_unavailable_read_returns_none_not_zero() -> None:
    # position_health raising ⇒ UNMEASURED ⇒ None (caller fall-backs to cache).
    market = _FakeMarket(raise_health=True, prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")})
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is None  # Empty != Zero — never fabricate a closed position


def test_redrive_none_health_is_unavailable_not_closed() -> None:
    # A provider/mock returning None (rather than raising) must be treated as
    # UNAVAILABLE, never as a measured-zero closed market — else a live position
    # would be silently stranded (Gemini review).
    market = _FakeMarket(health=None, prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")})
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is None


def test_redrive_measured_zero_is_closed_position() -> None:
    # A clean read of an all-zero market is a genuinely CLOSED position (not None).
    market = _FakeMarket(
        health=_Health(Decimal("0"), Decimal("0"), None),
        prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.has_exposure() is False


def test_redrive_missing_price_leaves_amount_none() -> None:
    # Collateral priced, debt token not ⇒ debt_amount None (unmeasured), not 0.
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("1700"), Decimal("1.7")),
        prices={"wstETH": Decimal("3400")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.collateral_amount == Decimal("1")
    assert live.debt_amount is None


def test_redrive_price_override_takes_precedence() -> None:
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("0"), None),
        prices={"wstETH": Decimal("9999")},  # snapshot price would be wrong
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
        collateral_price_usd=Decimal("3400"),
    )
    assert live is not None
    assert live.collateral_amount == Decimal("1")  # uses the override, not 9999


# ---------------------------------------------------------------------------
# chain_verify_lp_open
#
# VIB-5631: the read is PROTOCOL-SCOPED (the position's own NPM only) and
# TRI-STATE via the gateway's QueryPositionLiquidity (burned NFT folds to a
# MEASURED liquidity=0; a read fault is None/unmeasured). NPM token ids are
# per-contract counters, so probing OTHER protocols' NPMs for the same uint
# matches an unrelated position — the false-FAILED teardown bug.
# ---------------------------------------------------------------------------


def _lp(position_id: str = "555", chain: str = "arbitrum", protocol: str = "uniswap_v3") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
    )


def _npm(protocol: str, chain: str) -> str:
    """Resolve a connector-registered NPM address (single source: AddressRegistry)."""
    from almanak.connectors._strategy_base.address_registry import AddressRegistry

    address = AddressRegistry.resolve_contract_address(protocol, chain, ("position_manager", "nft"))
    assert address, f"expected a registered NPM for {protocol} on {chain}"
    return address


class _FakeGatewayClient:
    """GatewayClient double for the protocol-scoped tri-state liquidity read.

    ``liquidity_by_npm`` maps a lowercased NPM address to the liquidity the
    gateway would report for the queried token id (``None`` = read fault). Any
    NPM NOT in the map fails the test loudly — the read must never consult a
    foreign protocol's NPM.
    """

    is_connected = True

    def __init__(self, liquidity_by_npm: dict[str, int | None], *, raises: Exception | None = None):
        self._by_npm = {k.lower(): v for k, v in liquidity_by_npm.items()}
        self._raises = raises
        self.queried_npms: list[str] = []

    def query_position_liquidity(self, *, chain, position_manager, token_id, block=None):
        self.queried_npms.append(position_manager.lower())
        if self._raises is not None:
            raise self._raises
        assert position_manager.lower() in self._by_npm, (
            f"query_position_liquidity consulted an unexpected NPM {position_manager} — "
            "the read must be scoped to the position's own protocol NPM (VIB-5631)"
        )
        return self._by_npm[position_manager.lower()]


@pytest.mark.asyncio
async def test_chain_verify_none_without_gateway() -> None:
    assert await chain_verify_lp_open(gateway_client=None, position=_lp()) is None


@pytest.mark.asyncio
async def test_chain_verify_none_for_non_int_token_id() -> None:
    client = _FakeGatewayClient({})
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("pool0xABC:555")) is None
    assert client.queried_npms == []


@pytest.mark.asyncio
async def test_chain_verify_open_when_liquidity_positive() -> None:
    client = _FakeGatewayClient({_npm("uniswap_v3", "arbitrum"): 12345})
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("555")) is True


@pytest.mark.asyncio
async def test_chain_verify_measured_closed_when_liquidity_zero() -> None:
    """liquidity == 0 is a MEASURED closure: the gateway folds the burned-NFT
    'Invalid token ID' revert into 0, and a fully-decreased unburned shell also
    reads 0 — both are the closed signal, never conflated with a read fault."""
    client = _FakeGatewayClient({_npm("uniswap_v3", "arbitrum"): 0})
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("555")) is False


@pytest.mark.asyncio
async def test_chain_verify_read_fault_is_none_not_closed() -> None:
    # query_position_liquidity returns None on a gateway/RPC fault — unknown,
    # never 'closed' (Empty != Zero).
    client = _FakeGatewayClient({_npm("uniswap_v3", "arbitrum"): None})
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("555")) is None


@pytest.mark.asyncio
async def test_chain_verify_read_raise_is_none_not_closed() -> None:
    client = _FakeGatewayClient({}, raises=RuntimeError("gateway exploded"))
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("555")) is None


@pytest.mark.asyncio
async def test_chain_verify_none_for_non_npm_protocol() -> None:
    """A non-V3-family LP (e.g. a UniV4 lp_v4 / registry 'lp' label) has no NPM
    to scope to — unverifiable HERE, and no other protocol's NPM is probed."""
    client = _FakeGatewayClient({})
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("555", protocol="lp_v4")) is None
    assert client.queried_npms == []


@pytest.mark.asyncio
async def test_chain_verify_none_when_protocol_has_no_npm_on_chain() -> None:
    # agni_finance is a V3_NPM family member but deploys on mantle, not ethereum.
    client = _FakeGatewayClient({})
    position = _lp("555", chain="ethereum", protocol="agni_finance")
    assert await chain_verify_lp_open(gateway_client=client, position=position) is None
    assert client.queried_npms == []


@pytest.mark.asyncio
async def test_chain_verify_scopes_to_own_npm_never_foreign_vib5631() -> None:
    """THE VIB-5631 regression: a burned sushiswap_v3 NFT (own NPM measures 0)
    must verify MEASURED-CLOSED even while uniswap_v3's / pancakeswap_v3's
    ethereum NPMs hold unrelated, live positions under the SAME token id.
    Pre-fix, the all-NPM walk returned True off the foreign NPM and the
    teardown was flipped to FAILED on a provably-closed position."""
    sushi_npm = _npm("sushiswap_v3", "ethereum")
    client = _FakeGatewayClient(
        {
            sushi_npm: 0,  # burned: gateway folds 'Invalid token ID' -> 0
            _npm("uniswap_v3", "ethereum"): 999_999,  # a stranger's live token 3014
            _npm("pancakeswap_v3", "ethereum"): 777,  # ditto
        }
    )
    position = _lp("3014", chain="ethereum", protocol="sushiswap_v3")
    assert await chain_verify_lp_open(gateway_client=client, position=position) is False
    assert client.queried_npms == [sushi_npm.lower()]


def test_live_lending_position_dust_threshold() -> None:
    p = LiveLendingPosition(
        collateral_value_usd=Decimal("0.005"),
        debt_value_usd=Decimal("0"),
        health_factor=None,
        collateral_amount=None,
        debt_amount=None,
    )
    assert p.has_exposure(dust_usd=Decimal("0.01")) is False
    assert p.has_exposure(dust_usd=Decimal("0.001")) is True
