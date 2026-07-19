"""Shared lending teardown post-condition template (VIB-5795).

Covers the template's trichotomy (closed / MEASURED residual / unmeasured), the
per-``position_type`` dispatch (the debt-leg trap), the fault-dominance leg
combiner, the low-level read helpers, and the manifest-driven registration —
capability-derived over ``CONNECTOR_REGISTRY``, no hardcoded slug lists.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.connectors._strategy_base import vault_post_condition as vpc
from almanak.connectors._strategy_base.lending_post_condition import (
    _LENDING_ASSET_DUST_WEI,
    combine_leg_reads,
    read_erc4626_owned_assets,
    read_uint_address_call,
    verify_lending_closure,
)
from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition

_WALLET = "0x" + "11" * 20
_VAULT = "0x" + "22" * 20


def _position(
    *,
    protocol: str = "euler_v2",
    position_type: str = "SUPPLY",
    chain: str | None = "ethereum",
    details: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        position_id=f"{protocol}-{position_type.lower()}-X-{chain}",
        chain=chain,
        position_type=position_type,
        details={"asset": "WETH", "type": "collateral"} if details is None else details,
    )


def _reader(value):
    """A leg reader stub returning ``value`` and recording its calls."""

    calls: list[tuple] = []

    def read(gateway_client, chain, asset, wallet_address, block):
        calls.append((chain, asset, wallet_address, block))
        return value

    read.calls = calls
    return read


class TestCombineLegReads:
    def test_no_targets_is_none(self):
        assert combine_leg_reads([]) is None

    def test_all_measured_sums_including_zeros(self):
        assert combine_leg_reads([0, 0]) == 0
        assert combine_leg_reads([3, 0, 4]) == 7

    def test_fault_with_sub_dust_partial_is_none(self):
        # A faulted target could hold the whole position; a small measured
        # partial must NOT prove closure.
        assert combine_leg_reads([None, _LENDING_ASSET_DUST_WEI]) is None
        assert combine_leg_reads([None]) is None

    def test_fault_with_decisive_partial_returns_residual(self):
        # A measured strand above dust is decisive evidence of non-closure even
        # when a sibling read faulted — must stay FAILED, not degrade to
        # UNVERIFIED.
        assert combine_leg_reads([None, _LENDING_ASSET_DUST_WEI + 1]) == _LENDING_ASSET_DUST_WEI + 1

    def test_fault_dominance_boundary_is_strictly_above_dust(self):
        assert combine_leg_reads([None, _LENDING_ASSET_DUST_WEI]) is None
        assert combine_leg_reads([None, _LENDING_ASSET_DUST_WEI + 1]) is not None


class TestVerifyLendingClosureGuards:
    def test_missing_chain_is_unmeasured(self):
        result = verify_lending_closure(
            _position(chain=None),
            _WALLET,
            object(),
            None,
            read_supply=_reader(0),
            read_debt=_reader(0),
        )
        assert result.unmeasured is True
        assert result.closed is False
        assert result.error and "chain" in result.error

    def test_missing_gateway_client_is_unmeasured(self):
        result = verify_lending_closure(
            _position(),
            _WALLET,
            None,
            None,
            read_supply=_reader(0),
            read_debt=_reader(0),
        )
        assert result.unmeasured is True
        assert result.error and "gateway_client" in result.error

    def test_non_mapping_details_is_unmeasured_never_raises(self):
        # A truthy non-dict details (list / str) must degrade honestly, not
        # raise off .get() — the hook contract is "never raises".
        for details in (["bad"], "bad", 42):
            result = verify_lending_closure(
                _position(details=details),  # type: ignore[arg-type]
                _WALLET,
                object(),
                None,
                read_supply=_reader(0),
                read_debt=_reader(0),
            )
            assert result.unmeasured is True, details
            assert result.closed is False

    def test_missing_asset_detail_is_unmeasured(self):
        for details in ({}, {"asset": ""}, {"asset": "   "}, {"asset": 42}):
            result = verify_lending_closure(
                _position(details=details),
                _WALLET,
                object(),
                None,
                read_supply=_reader(0),
                read_debt=_reader(0),
            )
            assert result.unmeasured is True, details
            assert result.error and "asset" in result.error

    def test_unknown_position_type_is_unmeasured_not_silently_closed(self):
        # NOT the closed=True skip: an unexpected type on a lending slug is a
        # false-green vector if waved through.
        result = verify_lending_closure(
            _position(position_type="LP"),
            _WALLET,
            object(),
            None,
            read_supply=_reader(0),
            read_debt=_reader(0),
        )
        assert result.unmeasured is True
        assert result.closed is False
        assert result.error and "LP" in result.error


class TestVerifyLendingClosureDispatch:
    def test_supply_routes_to_supply_reader_only(self):
        supply, debt = _reader(0), _reader(0)
        result = verify_lending_closure(
            _position(position_type="SUPPLY"),
            _WALLET,
            object(),
            123,
            read_supply=supply,
            read_debt=debt,
        )
        assert result.closed is True
        assert len(supply.calls) == 1
        assert debt.calls == []
        # The reader receives the position's chain/asset and the pinned block.
        assert supply.calls[0] == ("ethereum", "WETH", _WALLET, 123)

    def test_borrow_routes_to_debt_reader_only(self):
        supply, debt = _reader(0), _reader(0)
        result = verify_lending_closure(
            _position(position_type="BORROW", details={"asset": "USDC"}),
            _WALLET,
            object(),
            None,
            read_supply=supply,
            read_debt=debt,
        )
        assert result.closed is True
        assert supply.calls == []
        assert len(debt.calls) == 1

    def test_debt_residual_is_measured_open_never_greened_by_supply_read(self):
        # THE debt-leg trap: residual debt must surface as a measured residual
        # even though the supply reader would report closed.
        result = verify_lending_closure(
            _position(position_type="BORROW", details={"asset": "USDC"}),
            _WALLET,
            object(),
            None,
            read_supply=_reader(0),
            read_debt=_reader(5_000_000),
        )
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual == {"asset": "USDC", "leg": "debt", "residual_wei": 5_000_000}


class TestVerifyLendingClosureTrichotomy:
    def test_dust_boundary(self):
        at_dust = verify_lending_closure(
            _position(),
            _WALLET,
            object(),
            None,
            read_supply=_reader(_LENDING_ASSET_DUST_WEI),
            read_debt=_reader(0),
        )
        assert at_dust.closed is True

        above_dust = verify_lending_closure(
            _position(),
            _WALLET,
            object(),
            None,
            read_supply=_reader(_LENDING_ASSET_DUST_WEI + 1),
            read_debt=_reader(0),
        )
        assert above_dust.closed is False
        assert above_dust.unmeasured is False
        assert above_dust.residual["residual_wei"] == _LENDING_ASSET_DUST_WEI + 1

    def test_reader_none_is_unmeasured_with_named_context(self):
        result = verify_lending_closure(
            _position(protocol="silo_v2", chain="avalanche", details={"asset": "USDC"}),
            _WALLET,
            object(),
            None,
            read_supply=_reader(None),
            read_debt=_reader(0),
        )
        assert result.unmeasured is True
        assert result.closed is False
        assert result.residual == {}
        # Operator-actionable error: names protocol, leg, asset, and chain.
        assert result.error is not None
        for token in ("silo_v2", "supply", "USDC", "avalanche"):
            assert token in result.error

    def test_reader_raise_is_unmeasured_never_propagates(self):
        def boom(gateway_client, chain, asset, wallet_address, block):
            raise RuntimeError("gateway down")

        result = verify_lending_closure(
            _position(),
            _WALLET,
            object(),
            None,
            read_supply=boom,
            read_debt=_reader(0),
        )
        assert result.unmeasured is True
        assert result.closed is False


class _Erc4626Gateway:
    """Scripted gateway double for the low-level read helpers."""

    def __init__(self, *, balance, convert_raw=None):
        self._balance = balance
        self._convert_raw = convert_raw
        self.balance_calls = 0
        self.balance_kwargs: list[dict] = []
        self.eth_calls: list[dict] = []

    def query_erc20_balance(self, **kwargs):
        self.balance_calls += 1
        self.balance_kwargs.append(kwargs)
        return self._balance

    def eth_call(self, **kwargs):
        self.eth_calls.append(kwargs)
        return self._convert_raw


class TestReadErc4626OwnedAssets:
    def test_zero_shares_short_circuits_without_convert_call(self):
        gateway = _Erc4626Gateway(balance=0)
        assert read_erc4626_owned_assets(gateway, "ethereum", _VAULT, _WALLET, None) == 0
        assert gateway.eth_calls == []

    def test_invalid_vault_address_is_none_without_any_read(self):
        gateway = _Erc4626Gateway(balance=0)
        assert read_erc4626_owned_assets(gateway, "ethereum", "not-an-address", _WALLET, None) is None
        assert gateway.balance_calls == 0

    def test_balance_fault_exhausts_retry_then_none(self):
        gateway = _Erc4626Gateway(balance=None)
        assert read_erc4626_owned_assets(gateway, "ethereum", _VAULT, _WALLET, None) is None
        assert gateway.balance_calls == vpc._READ_ATTEMPTS

    def test_non_numeric_balance_is_none(self):
        gateway = _Erc4626Gateway(balance="garbage")
        assert read_erc4626_owned_assets(gateway, "ethereum", _VAULT, _WALLET, None) is None

    def test_malformed_convert_return_is_none_not_a_value(self):
        gateway = _Erc4626Gateway(balance=1000, convert_raw="0xdeadbeef")
        assert read_erc4626_owned_assets(gateway, "ethereum", _VAULT, _WALLET, None) is None

    def test_value_decodes_and_call_is_block_pinned(self):
        gateway = _Erc4626Gateway(balance=1000, convert_raw="0x" + format(1, "064x"))
        assert read_erc4626_owned_assets(gateway, "ethereum", _VAULT, _WALLET, 456) == 1
        # BOTH reads must pin the same block — mixing chain states between the
        # shares read and the conversion could misvalue the closure.
        assert gateway.balance_kwargs[0]["block"] == 456
        assert gateway.eth_calls[0]["block"] == 456
        assert gateway.eth_calls[0]["data"].startswith(vpc._CONVERT_TO_ASSETS_SELECTOR)


class TestReadUintAddressCall:
    def test_invalid_target_is_none(self):
        gateway = _Erc4626Gateway(balance=0)
        assert read_uint_address_call(gateway, "ethereum", "0xnope", "0xd283e75f", _WALLET, None) is None
        assert gateway.eth_calls == []

    def test_calldata_is_selector_plus_padded_wallet(self):
        gateway = _Erc4626Gateway(balance=0, convert_raw="0x" + format(7, "064x"))
        value = read_uint_address_call(gateway, "ethereum", _VAULT, "0xd283e75f", _WALLET, 99)
        assert value == 7
        call = gateway.eth_calls[0]
        assert call["to"] == _VAULT
        assert call["data"] == "0xd283e75f" + f"{int(_WALLET, 16):064x}"
        assert call["block"] == 99


class TestRegistration:
    def test_manifest_hooks_registered_for_every_declaring_lending_slug(self):
        """Capability-derived: enumerate CONNECTOR_REGISTRY live, no name lists.

        Every LENDING connector that declares a manifest
        ``teardown_post_condition`` must resolve to that hook under every slug
        it can emit; every LENDING connector that does NOT declare one must
        have NO hook silently registered for it (there is deliberately no
        LENDING-kind framework default — one read shape cannot cover
        ERC-4626 / paired-silo / Compound-fork / aToken venues).
        """
        from almanak.connectors._base.types import ProtocolKind
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.teardown.post_conditions import _connector_teardown_slugs

        declaring = 0
        for connector in CONNECTOR_REGISTRY.all():
            if connector.kind is not ProtocolKind.LENDING:
                continue
            if connector.teardown_post_condition is not None:
                declaring += 1
                expected = connector.teardown_post_condition.load()
                for slug in _connector_teardown_slugs(connector):
                    assert get_teardown_post_condition(slug) is expected, slug
            else:
                for slug in _connector_teardown_slugs(connector):
                    assert get_teardown_post_condition(slug) is None, (
                        f"{connector.name}: no manifest hook declared but slug {slug!r} "
                        "resolves — a LENDING default was introduced without updating "
                        "the VIB-5795 design rationale"
                    )
        assert declaring >= 3  # euler_v2, silo_v2, benqi at minimum

    def test_vib5795_acceptance_slugs_resolve(self):
        """Ticket acceptance: the three named protocols are hook-covered."""
        for slug, name in (
            ("euler_v2", "euler_v2_teardown_post_condition"),
            ("silo_v2", "silo_v2_teardown_post_condition"),
            ("benqi", "benqi_teardown_post_condition"),
        ):
            hook = get_teardown_post_condition(slug)
            assert hook is not None, slug
            assert getattr(hook, "__name__", "") == name
