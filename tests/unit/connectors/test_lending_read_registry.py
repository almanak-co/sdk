"""Characterization + contract tests for the lending-read capability seam.

Pins the protocol-agnostic refactor of
``almanak.framework.valuation.lending_position_reader`` (item #4 / VIB-4851
chain-protocol coupling). Before this change the framework reader hardcoded the
Aave V3 ``pool_data_provider`` contract kind and protocol identifier; now the
read (target address + calldata + decoder) is resolved through the strategy-side
:class:`LendingReadRegistry` from a connector-published
:class:`~almanak.connectors._strategy_base.lending_read_base.LendingReadSpec`.

The **oracle** for behaviour preservation is the old aave-hardcoded logic,
reproduced inline here as ``_legacy_*`` helpers. Every test asserts the new
registry-driven path produces byte-for-byte identical results so an accounting
auditor sees no change in valuation inputs.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    AAVE_FORK_RESERVE_READ,
    LendingPositionOnChain,
    LendingReadSpec,
)
from almanak.connectors._strategy_base.lending_read_registry import (
    LendingReadPlan,
    LendingReadRegistry,
)
from almanak.framework.valuation.lending_position_reader import (
    LendingPositionReader,
    _parse_user_reserve_data_hex,
)

# ---------------------------------------------------------------------------
# Oracle — the pre-refactor aave-hardcoded logic, verbatim.
# ---------------------------------------------------------------------------

_LEGACY_SELECTOR = "0x28dd2d01"  # getUserReserveData(address asset, address user)

# The old module built this dict at import from the aave_v3 address table.
_LEGACY_AAVE_V3_POOL_DATA_PROVIDER: dict[str, str] = {
    chain: provider
    for chain in AddressRegistry.address_supported_chains("aave_v3")
    if (provider := AddressRegistry.addresses_for("aave_v3", chain).get("pool_data_provider"))
}


def _legacy_pad_address(address: str) -> str:
    return address.lower().replace("0x", "").zfill(64)


def _legacy_calldata(asset_address: str, wallet_address: str) -> str:
    return _LEGACY_SELECTOR + _legacy_pad_address(asset_address) + _legacy_pad_address(wallet_address)


def _build_reserve_hex(
    *,
    atoken_balance: int = 0,
    stable_debt: int = 0,
    variable_debt: int = 0,
    liquidity_rate: int = 0,
    collateral_enabled: bool = True,
) -> str:
    words = [
        atoken_balance,
        stable_debt,
        variable_debt,
        0,  # principalStableDebt
        0,  # scaledVariableDebt
        0,  # stableBorrowRate
        liquidity_rate,
        0,  # stableRateLastUpdated
        1 if collateral_enabled else 0,
    ]
    return "0x" + "".join(format(w, "064x") for w in words)


# ---------------------------------------------------------------------------
# Registry contract
# ---------------------------------------------------------------------------


def test_default_protocol_matches_legacy_hardcoded_protocol():
    # The framework reader's no-protocol path must resolve to the same family
    # the old code hardcoded ("aave_v3").
    assert LendingReadRegistry.default_protocol() == "aave_v3"


def test_supported_protocols_cover_the_aave_fork_family():
    assert set(LendingReadRegistry.supported_protocols()) == {"aave_v3", "spark"}


@pytest.mark.parametrize("protocol", ["aave_v3", "spark", "aave"])
def test_known_protocols_recognised(protocol: str):
    assert LendingReadRegistry.has(protocol)


@pytest.mark.parametrize("protocol", ["compound_v3", "morpho_blue", "uniswap_v3", "unknown"])
def test_non_lending_or_unknown_protocols_not_recognised(protocol: str):
    assert not LendingReadRegistry.has(protocol)


@pytest.mark.parametrize(
    ("protocol", "expected"),
    [
        ("aave_v3", "aave_v3"),
        ("AAVE_V3", "aave_v3"),
        ("aave", "aave_v3"),  # alias resolves to canonical key
        ("spark", "spark"),
    ],
)
def test_canonical_resolves_supported_protocols(protocol: str, expected: str):
    assert LendingReadRegistry.canonical(protocol) == expected


@pytest.mark.parametrize("protocol", ["compound_v3", "morpho_blue", "uniswap_v3", "unknown"])
def test_canonical_returns_none_for_protocols_without_a_lending_read(protocol: str):
    assert LendingReadRegistry.canonical(protocol) is None


@pytest.mark.parametrize("protocol", [None, 123, "", b"aave_v3", 3.14])
def test_canonical_is_total_for_invalid_input(protocol):
    # Loosely typed / missing strategy metadata must never crash canonical();
    # it returns None so callers can use ``canonical(p) or fallback`` directly.
    assert LendingReadRegistry.canonical(protocol) is None


def test_aave_alias_normalises_to_aave_v3():
    plan_alias = LendingReadRegistry.resolve("aave", "arbitrum", "0xAsset", "0xWallet")
    plan_canon = LendingReadRegistry.resolve("aave_v3", "arbitrum", "0xAsset", "0xWallet")
    assert plan_alias is not None and plan_canon is not None
    assert plan_alias.target_address == plan_canon.target_address


def test_all_aave_fork_specs_share_the_canonical_read():
    # Each fork connector opts in by publishing the shared spec instance.
    for protocol in ("aave_v3", "spark"):
        spec = LendingReadRegistry._load_spec(protocol)
        assert isinstance(spec, LendingReadSpec)
        assert spec is AAVE_FORK_RESERVE_READ
        assert spec.contract_kinds == ("pool_data_provider",)


@pytest.mark.parametrize(
    ("protocol", "chain"),
    [("aave_v3", "ethereum"), ("spark", "ethereum")],
)
def test_each_protocol_key_resolves_a_usable_plan(protocol: str, chain: str):
    # Every registered protocol key (otherwise only seen via
    # has()/_load_spec()) must dispatch through resolve() to a usable plan: the
    # spec's data-provider target + the canonical getUserReserveData calldata.
    asset = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    wallet = "0x" + "1" * 40
    plan = LendingReadRegistry.resolve(protocol, chain, asset, wallet)
    assert isinstance(plan, LendingReadPlan)
    expected_provider = AddressRegistry.addresses_for(protocol, chain)["pool_data_provider"]
    assert plan.target_address == expected_provider
    assert plan.calldata == _legacy_calldata(asset, wallet)
    assert plan.parse_result is AAVE_FORK_RESERVE_READ.parse_result


# ---------------------------------------------------------------------------
# Behaviour preservation — registry vs legacy oracle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chain", sorted(_LEGACY_AAVE_V3_POOL_DATA_PROVIDER))
def test_resolved_target_matches_legacy_pool_data_provider(chain: str):
    plan = LendingReadRegistry.resolve("aave_v3", chain, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "0x" + "1" * 40)
    assert plan is not None
    assert plan.target_address == _LEGACY_AAVE_V3_POOL_DATA_PROVIDER[chain]


def test_resolved_calldata_is_byte_identical_to_legacy():
    asset = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    wallet = "0xABCDEF0123456789abcdef0123456789ABCDEF01"
    plan = LendingReadRegistry.resolve("aave_v3", "arbitrum", asset, wallet)
    assert plan is not None
    assert plan.calldata == _legacy_calldata(asset, wallet)
    assert plan.calldata.startswith(_LEGACY_SELECTOR)


def test_unsupported_chain_resolves_to_none():
    assert LendingReadRegistry.resolve("aave_v3", "solana", "0xa", "0xb") is None


def test_unknown_protocol_resolves_to_none():
    assert LendingReadRegistry.resolve("compound_v3", "arbitrum", "0xa", "0xb") is None


def test_plan_decoder_is_the_connector_parser():
    plan = LendingReadRegistry.resolve("aave_v3", "arbitrum", "0xa", "0xb")
    assert plan is not None
    assert plan.parse_result is AAVE_FORK_RESERVE_READ.parse_result
    assert isinstance(plan, LendingReadPlan)


# ---------------------------------------------------------------------------
# End-to-end reader path with a mocked gateway — identical to legacy output
# ---------------------------------------------------------------------------


def _mock_gateway_returning(result_hex: str) -> MagicMock:
    """Build a mock gateway whose _rpc_stub.Call returns ``result_hex``."""
    gateway = MagicMock()
    response = MagicMock()
    response.success = True
    # Gateway wraps the eth_call result as a JSON string of the hex.
    import json

    response.result = json.dumps(result_hex)
    gateway._rpc_stub.Call.return_value = response
    gateway.config.timeout = 10
    return gateway


def test_reader_default_protocol_matches_legacy_parse():
    result_hex = _build_reserve_hex(
        atoken_balance=1_500_000_000,
        variable_debt=250_000_000,
        liquidity_rate=30_000_000_000_000_000_000_000_000,
    )
    gateway = _mock_gateway_returning(result_hex)
    reader = LendingPositionReader(gateway_client=gateway)

    asset = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    # New path (no protocol -> registry default).
    got = reader.read_position("arbitrum", asset, "0x" + "1" * 40)
    # Oracle: the legacy parser on the same bytes.
    expected = _parse_user_reserve_data_hex(result_hex, asset)

    assert got == expected
    assert got is not None
    assert got.current_atoken_balance == 1_500_000_000
    assert got.total_debt == 250_000_000
    assert got.is_active is True

    # And the gateway saw the legacy calldata against the legacy data provider.
    call_kwargs = gateway._rpc_stub.Call.call_args
    rpc_request = call_kwargs.args[0]
    assert rpc_request.method == "eth_call"
    assert _legacy_calldata(asset, "0x" + "1" * 40) in rpc_request.params
    assert _LEGACY_AAVE_V3_POOL_DATA_PROVIDER["arbitrum"] in rpc_request.params


def test_reader_explicit_protocol_routes_to_that_connector():
    # Spark on ethereum must hit Spark's data provider, not Aave's.
    result_hex = _build_reserve_hex(atoken_balance=42)
    gateway = _mock_gateway_returning(result_hex)
    reader = LendingPositionReader(gateway_client=gateway)

    got = reader.read_position("ethereum", "0xAsset", "0xWallet", protocol="spark")
    assert got is not None and got.current_atoken_balance == 42

    rpc_request = gateway._rpc_stub.Call.call_args.args[0]
    spark_provider = AddressRegistry.addresses_for("spark", "ethereum")["pool_data_provider"]
    assert spark_provider in rpc_request.params


def test_reader_no_gateway_returns_none():
    reader = LendingPositionReader(gateway_client=None)
    assert reader.read_position("arbitrum", "0xa", "0xb") is None


def test_reader_unknown_chain_returns_none_without_calling_gateway():
    gateway = _mock_gateway_returning(_build_reserve_hex())
    reader = LendingPositionReader(gateway_client=gateway)
    assert reader.read_position("solana", "0xa", "0xb") is None
    gateway._rpc_stub.Call.assert_not_called()


def test_read_positions_filters_inactive_like_legacy():
    active = _build_reserve_hex(atoken_balance=1_000)
    empty = _build_reserve_hex()
    gateway = MagicMock()

    def _call(req, timeout=None):
        resp = MagicMock()
        resp.success = True
        import json

        # First asset active, second empty.
        resp.result = json.dumps(active if "aaaa" in req.params else empty)
        return resp

    gateway._rpc_stub.Call.side_effect = _call
    gateway.config.timeout = 10
    reader = LendingPositionReader(gateway_client=gateway)

    positions = reader.read_positions(
        "arbitrum",
        ["0xaaaa000000000000000000000000000000000000", "0xbbbb000000000000000000000000000000000000"],
        "0xWallet",
    )
    assert len(positions) == 1
    assert positions[0].current_atoken_balance == 1_000


def test_lending_position_on_chain_is_re_exported_from_reader():
    # Public surface preserved: callers/tests import the dataclass from the
    # framework reader module.
    from almanak.framework.valuation.lending_position_reader import (
        LendingPositionOnChain as ReExported,
    )

    assert ReExported is LendingPositionOnChain


class TestCapabilityAccessors:
    """The VIB-4851 B2 capability bits framework consumers dispatch on."""

    def test_publishes_market_table_truth_table(self):
        for protocol in ("morpho_blue", "compound_v3", "silo_v2", "euler_v2", "benqi"):
            assert LendingReadRegistry.publishes_market_table(protocol), protocol
        for protocol in ("aave_v3", "spark", "aave", "definitely_not_a_protocol"):
            assert not LendingReadRegistry.publishes_market_table(protocol), protocol

    def test_declares_valuation_roles_truth_table(self):
        # Non-USD-native per-market protocols declare roles; USD-native ones do not.
        for protocol in ("morpho_blue", "silo_v2", "euler_v2"):
            assert LendingReadRegistry.declares_valuation_roles(protocol), protocol
        for protocol in ("benqi", "aave_v3", "spark", "definitely_not_a_protocol"):
            assert not LendingReadRegistry.declares_valuation_roles(protocol), protocol


class TestNormalizeProtocol:
    """The public alias-aware folding consumers use instead of local tables."""

    def test_manifest_aliases_resolve(self):
        for alias, canonical in (
            ("aave", "aave_v3"),
            ("aavev3", "aave_v3"),
            ("morpho", "morpho_blue"),
            ("morphoblue", "morpho_blue"),
            ("comet", "compound_v3"),
            ("compound", "compound_v3"),
            ("compoundv3", "compound_v3"),
        ):
            assert LendingReadRegistry.normalize_protocol(alias) == canonical, alias

    def test_folding_handles_case_hyphens_whitespace(self):
        assert LendingReadRegistry.normalize_protocol("Aave-V3") == "aave_v3"
        assert LendingReadRegistry.normalize_protocol("  Morpho-Blue  ") == "morpho_blue"
        assert LendingReadRegistry.normalize_protocol("COMPOUND-V3") == "compound_v3"

    def test_unknown_passes_through_folded(self):
        # No silent swallowing of typos: downstream capability checks fail closed.
        assert LendingReadRegistry.normalize_protocol("Definitely-Not_A Protocol") == "definitely_not_a protocol"

    def test_total_on_none_and_non_str(self):
        assert LendingReadRegistry.normalize_protocol(None) == ""
        assert LendingReadRegistry.normalize_protocol(123) == ""  # type: ignore[arg-type]


class TestAcceptsIsCollateral:
    """Plan 027 Step 5: the registry owns which lending venues take the
    ``is_collateral`` flag (replaces the inline ``{"morpho", "morpho_blue"}``
    set-membership guard in the executor and the ax CLI withdraw path)."""

    @pytest.mark.parametrize(
        "protocol",
        ["morpho_blue", "morpho", "morphoblue", "Morpho-Blue", "MORPHO", "morpho-blue"],
    )
    def test_morpho_and_aliases_accept_is_collateral(self, protocol: str) -> None:
        # The canonical key, manifest aliases, and case/hyphen folding all
        # resolve to the morpho_blue decl that sets accepts_is_collateral=True.
        # (Whitespace folding is the caller's responsibility -- see the method
        # docstring -- and is covered at the executor / ax CLI call sites.)
        assert LendingReadRegistry.accepts_is_collateral(protocol) is True

    @pytest.mark.parametrize(
        "protocol",
        ["aave_v3", "spark", "aave", "compound_v3", "comet", "definitely_not_a_protocol", ""],
    )
    def test_other_and_unknown_protocols_fail_closed(self, protocol: str) -> None:
        # Every lending venue that does NOT declare the flag -- and any unknown
        # key -- returns False (fail closed); the gate must never default open.
        assert LendingReadRegistry.accepts_is_collateral(protocol) is False


class TestIsTokenKeyed:
    """VIB-5493: the registry owns which lending venues are supply-only,
    token-keyed surfaces (Fluid fTokens — one position per underlying token,
    no ``market_id``). The teardown lending guard's ``_position_key`` splits
    per token ONLY for these, so two distinct Fluid supplies on one chain are
    two positions instead of collapsing to one ``(fluid, chain, "")`` key."""

    def test_dispatch_populates_token_keyed_protocols_from_manifest(self) -> None:
        # The manifest-derived dispatch map collects exactly the canonical keys
        # whose ``LendingReadDecl`` declares ``token_keyed=True`` (Fluid fTokens).
        token_keyed = LendingReadRegistry._dispatch().token_keyed_protocols
        assert isinstance(token_keyed, frozenset)
        assert "fluid" in token_keyed
        # Account/vault-keyed venues must NOT leak into the token-keyed set.
        assert token_keyed.isdisjoint({"aave_v3", "spark", "morpho_blue", "compound_v3", "fluid_vault"})

    def test_canonical_key_is_token_keyed(self) -> None:
        assert LendingReadRegistry.is_token_keyed("fluid") is True

    def test_alias_resolves_to_token_keyed(self) -> None:
        # The ``fluid_lending`` alias must resolve to the same canonical decl,
        # so a loosely-spelled protocol identifier still splits per token.
        assert LendingReadRegistry.is_token_keyed("fluid_lending") is True

    @pytest.mark.parametrize("protocol", ["FLUID", "Fluid", "fluid-lending", "  fluid  "])
    def test_case_hyphen_whitespace_folding_resolves(self, protocol: str) -> None:
        # ``is_token_keyed`` normalises via ``normalize_protocol`` (fold + alias),
        # so display-cased / hyphenated / padded spellings still resolve.
        assert LendingReadRegistry.is_token_keyed(protocol) is True

    @pytest.mark.parametrize(
        "protocol",
        ["aave_v3", "spark", "aave", "morpho_blue", "compound_v3", "fluid_vault", "definitely_not_a_protocol"],
    )
    def test_account_and_vault_keyed_protocols_fail_closed(self, protocol: str) -> None:
        # Account-keyed (Aave family, market_id="") and vault/market-keyed
        # (Morpho / Compound / fluid_vault) venues stay grouped per account;
        # unknown keys fail closed onto the safe account-keyed grouping.
        assert LendingReadRegistry.is_token_keyed(protocol) is False

    @pytest.mark.parametrize("protocol", [None, "", 123, 3.14, b"fluid"])
    def test_total_on_none_and_invalid_input(self, protocol) -> None:
        # Loosely typed / missing strategy metadata must never crash; it
        # normalises to a non-token-keyed (False) answer so callers fail closed.
        assert LendingReadRegistry.is_token_keyed(protocol) is False
