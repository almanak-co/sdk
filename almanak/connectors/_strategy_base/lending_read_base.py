"""Strategy-side shared infrastructure for connector lending-position reads.

The framework's :class:`~almanak.framework.valuation.lending_position_reader.LendingPositionReader`
needs to read a wallet's current on-chain supply/debt for a single reserve so
valuation, position discovery, and ``amount="all"`` resolution can reprice
lending positions. *How* that read is performed — which on-chain contract holds
the per-user reserve data, the function selector, the calldata layout, and the
return decoding — is **connector knowledge**, not framework knowledge.

This module owns the strategy-side half every lending connector that exposes a
"read a single reserve position" capability shares:

* :class:`LendingPositionOnChain` — the canonical decoded result the framework
  consumes (re-exported by the framework reader for backward compatibility).
* :class:`LendingReadSpec` — the per-capability descriptor a connector publishes:
  the contract-kind it reads from (resolved through ``AddressRegistry``), the
  selector, the calldata encoder, and the return decoder.
* :data:`AAVE_FORK_RESERVE_READ` — the concrete spec for the Aave V3 fork
  family (Aave V3, Spark). Both forks expose the identical
  ``getUserReserveData(address asset, address user)`` ABI against their own
  ``pool_data_provider`` contract, so they share one spec; only the per-chain
  data-provider address (owned by each connector's ``addresses.py``) differs.

VIB-4929 adds the **aggregate account-state** read capability alongside the
single-reserve one. Where a single-reserve read answers "supply/debt for *one*
reserve", an account-state read answers "total collateral / total debt / health
factor across the *whole* position" — the inputs the framework's lending
valuation, position-health, and ``amount="all"`` paths need. It mirrors the
single-reserve seam exactly:

* :class:`EthCall` — one ``(to, data)`` read the framework reader executes.
* :class:`AccountStateQuery` — the resolved request the planner consumes
  (chain, wallet, optional market id, optional block, and the registry-resolved
  position-manager address the calls target).
* :class:`LendingAccountState` — the canonical decoded aggregate result. Every
  field is optional where a protocol lacks the concept (Empty ≠ Zero — ``None``
  is "unmeasured", never a fabricated zero).
* :class:`AccountStateReadSpec` — the per-capability descriptor a connector
  publishes: which contract kinds to resolve the target from, a pure
  ``build_calls`` planner, and a pure ``reduce_calls`` reducer.
* :data:`AAVE_FORK_ACCOUNT_STATE_READ` — the concrete spec for the Aave V3 fork
  family. Both forks expose the identical
  ``getUserAccountData(address user)`` / ``getUserEMode(address user)`` ABI
  against their own ``pool`` contract, and that data is **already USD-denominated
  on-chain by the protocol's own oracle** — so the Aave family needs no external
  price injection (the price-oracle seam Morpho/Compound require is deferred to a
  later PR). Only the per-chain pool address (owned by each connector's
  ``addresses.py``) differs, so the forks share one spec.

Gateway-boundary note: this module performs **no** network egress. It only
*describes* a read (selector + calldata + decoder) as pure data + pure
functions; the gateway-routed ``eth_call`` that executes the read stays in the
framework reader, which owns the gateway client.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "AAVE_FORK_ACCOUNT_STATE_READ",
    "AAVE_FORK_RESERVE_READ",
    "MORPHO_BLUE_ACCOUNT_STATE_READ",
    "AccountStateQuery",
    "AccountStateReadSpec",
    "EthCall",
    "LendingAccountState",
    "LendingPositionOnChain",
    "LendingReadSpec",
    "build_compound_asset_info_calldata",
    "build_compound_borrow_balance_calldata",
    "build_compound_collateral_balance_calldata",
    "build_compound_get_price_calldata",
    "decode_uint_hex",
    "normalize_market_id_hex",
    "pad_address",
    "parse_account_state_hex",
    "parse_user_emode_hex",
    "parse_user_reserve_data_hex",
]


@dataclass
class LendingPositionOnChain:
    """On-chain state of a lending position for a single reserve asset.

    Canonical home for the result the framework lending reader returns. Decoded
    from a connector's per-user reserve read (e.g. Aave-fork
    ``PoolDataProvider.getUserReserveData(asset, user)``). All amounts are in the
    reserve asset's native wei.
    """

    asset_address: str
    current_atoken_balance: int  # Supply + accrued interest (wei)
    current_stable_debt: int  # Stable rate debt (wei)
    current_variable_debt: int  # Variable rate debt (wei)
    liquidity_rate: int  # Supply APY in ray (1e27)
    usage_as_collateral_enabled: bool

    @property
    def is_active(self) -> bool:
        """Position has any supply or debt."""
        return self.current_atoken_balance > 0 or self.total_debt > 0

    @property
    def total_debt(self) -> int:
        """Total debt = stable + variable."""
        return self.current_stable_debt + self.current_variable_debt


@dataclass(frozen=True)
class LendingReadSpec:
    """Connector-published descriptor for a single-reserve lending read.

    Carries the protocol-specific knowledge the framework reader must NOT
    hardcode:

    Attributes:
        contract_kinds: Ordered contract-kind names (the connector's private
            ``addresses.py`` vocabulary) to resolve the read target from, tried
            in order via ``AddressRegistry.resolve_contract_address``. For the
            Aave fork family this is ``("pool_data_provider",)``.
        build_calldata: ``(asset_address, wallet_address) -> hex calldata`` for
            the read (selector + ABI-encoded args), without a ``0x`` prefix
            requirement on the result (the framework reader passes it verbatim).
        parse_result: ``(result_hex, asset_address) -> LendingPositionOnChain |
            None`` decoder for the read's return data.
    """

    contract_kinds: tuple[str, ...]
    build_calldata: Callable[[str, str], str]
    parse_result: Callable[[str, str], LendingPositionOnChain | None]


# ---------------------------------------------------------------------------
# Shared ABI helpers (Aave V2 / V3 fork family)
# ---------------------------------------------------------------------------


def pad_address(address: str) -> str:
    """Left-pad an address to 32 bytes (64 hex chars), no ``0x`` prefix."""
    addr = address.lower().replace("0x", "")
    return addr.zfill(64)


def decode_uint_hex(hex_data: str, word_index: int) -> int:
    """Decode a uint256 from ABI-encoded hex at the given 32-byte word index."""
    # Strip any 0x/0X prefix first so the word offset is correct for word_index > 0
    # (``int(..., 16)`` tolerates the prefix at index 0, but the slice would not).
    data = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data
    start = word_index * 64
    return int(data[start : start + 64], 16)


# Function selector for getUserReserveData(address asset, address user)
_GET_USER_RESERVE_DATA_SELECTOR = "0x28dd2d01"


def _build_get_user_reserve_data_calldata(asset_address: str, wallet_address: str) -> str:
    """Build calldata for ``getUserReserveData(address asset, address user)``."""
    return _GET_USER_RESERVE_DATA_SELECTOR + pad_address(asset_address) + pad_address(wallet_address)


def parse_user_reserve_data_hex(
    hex_data: str,
    asset_address: str,
) -> LendingPositionOnChain | None:
    """Parse hex response from Aave-fork ``getUserReserveData``.

    Expected ABI layout (9 words * 32 bytes = 576 hex chars):
    [0] currentATokenBalance (uint256)
    [1] currentStableDebt (uint256)
    [2] currentVariableDebt (uint256)
    [3] principalStableDebt (uint256) -- not used
    [4] scaledVariableDebt (uint256)  -- not used
    [5] stableBorrowRate (uint256)    -- not used
    [6] liquidityRate (uint256)
    [7] stableRateLastUpdated (uint40 padded) -- not used
    [8] usageAsCollateralEnabled (bool padded)
    """
    # Strip a leading 0x/0X prefix case-insensitively. A bare ``.replace("0x", "")``
    # would miss the upper-case form and could mangle a mid-string match.
    data = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data

    # 9 words * 64 hex chars = 576 minimum
    if len(data) < 576:
        logger.warning("getUserReserveData response too short: %d chars", len(data))
        return None

    try:
        atoken_balance = decode_uint_hex(data, 0)
        stable_debt = decode_uint_hex(data, 1)
        variable_debt = decode_uint_hex(data, 2)
        liquidity_rate = decode_uint_hex(data, 6)
        collateral_enabled = decode_uint_hex(data, 8) != 0

        return LendingPositionOnChain(
            asset_address=asset_address,
            current_atoken_balance=atoken_balance,
            current_stable_debt=stable_debt,
            current_variable_debt=variable_debt,
            liquidity_rate=liquidity_rate,
            usage_as_collateral_enabled=collateral_enabled,
        )
    except Exception:
        logger.debug("Failed to parse user reserve data hex", exc_info=True)
        return None


#: Read capability shared by every Aave V3 fork (Aave V3, Spark).
#: The forks expose the identical ``getUserReserveData`` ABI against their
#: own ``pool_data_provider`` contract; only the per-chain address (owned by
#: each connector's ``addresses.py``) differs.
AAVE_FORK_RESERVE_READ = LendingReadSpec(
    contract_kinds=("pool_data_provider",),
    build_calldata=_build_get_user_reserve_data_calldata,
    parse_result=parse_user_reserve_data_hex,
)


# ===========================================================================
# Aggregate account-state read capability (VIB-4929)
# ===========================================================================


@dataclass(frozen=True)
class EthCall:
    """A single read the framework reader executes via the gateway.

    Pure data: the target contract and the hex calldata. The strategy-side
    planner emits these; the framework reader owns the gateway-routed
    ``eth_call`` that turns each one into a return blob (Gateway-boundary rule).

    Attributes:
        to: Contract address to call (the registry-resolved read target).
        data: Hex calldata (selector + ABI-encoded args), ``0x``-prefixed.
    """

    to: str
    data: str


@dataclass(frozen=True)
class LendingAccountState:
    """Canonical decoded aggregate account state for one lending position.

    The result the framework's lending valuation / position-health /
    ``amount="all"`` paths consume. Every field is optional because protocols
    differ in what they expose natively: the Aave V3 family reports all of them
    on-chain, but a protocol without an on-chain health-factor or e-mode concept
    leaves those ``None``.

    Empty ≠ Zero (AGENTS.md §Accounting): ``None`` means *unmeasured*; a real
    measured zero is ``Decimal("0")`` / ``0``. Never substitute one for the
    other.

    Attributes:
        collateral_usd: Total collateral value in USD (oracle-denominated by the
            protocol on-chain for the Aave family).
        debt_usd: Total debt value in USD.
        health_factor: Normalised health factor (``1.0`` = at the liquidation
            threshold). ``None`` when the protocol has no on-chain HF or the read
            failed.
        liquidation_threshold_bps: Weighted current liquidation threshold in
            basis points (e.g. ``8500`` → 85 %). ``None`` when unavailable.
        e_mode_category: Aave efficiency-mode category id (``0`` = no e-mode;
            ``1..255`` = a configured category). ``None`` when the protocol has no
            e-mode concept or the secondary read failed — distinct from a measured
            ``0``.
        lltv: Liquidation loan-to-value as a fraction (e.g. ``Decimal("0.86")`` →
            86 %). The Morpho-family analogue of Aave's
            ``liquidation_threshold_bps`` — but a distinct concept (a per-market
            constant, not the weighted-average current threshold), so it is NOT
            overloaded onto ``liquidation_threshold_bps``. ``None`` for the Aave
            family (which carries the threshold in bps) and whenever unmeasured.
        interest_rate_mode: Aave intent-layer rate mode (``"variable"`` on Aave V3;
            stable is deprecated) overlaid by the framework consumer for
            BORROW/REPAY intents. It is **not decoded from any on-chain read** —
            it is intent metadata threaded onto the decoded state via
            ``dataclasses.replace`` so it lands in ``pre_state_json`` /
            ``post_state_json``. ``None`` for SUPPLY/WITHDRAW (no rate mode at the
            collateral side), for the post-state read, and for non-Aave families.
            Empty ≠ Zero: ``None`` is "not applicable / unmeasured".
        family: Explicit protocol-family discriminator the serializer gates
            Aave-only keys on **structurally** (not by protocol-name string).
            Set to ``"aave"`` by the Aave-fork reducer; ``None`` for every other
            family. Lets ``lending_state_to_dict`` emit the Aave-only
            ``e_mode_category`` / ``interest_rate_mode`` keys (even when their
            values are ``None``) without re-deriving "is this Aave?" from a
            protocol string. Empty ≠ Zero: ``None`` is "not the Aave family".
    """

    collateral_usd: Decimal | None
    debt_usd: Decimal | None
    health_factor: Decimal | None
    liquidation_threshold_bps: int | None
    e_mode_category: int | None
    lltv: Decimal | None = None
    interest_rate_mode: str | None = None
    family: str | None = None


@dataclass(frozen=True)
class AccountStateQuery:
    """A fully-resolved aggregate account-state read request.

    Built by the strategy-side registry (which resolves the per-chain
    ``position_manager_address`` through ``AddressRegistry``) and consumed by a
    connector's pure ``build_calls`` / ``reduce_calls`` functions. Carrying the
    resolved target here keeps the spec itself pure (no address-table lookup)
    while letting the planner emit fully-formed :class:`EthCall` targets.

    Attributes:
        chain: Chain identifier (e.g. ``"arbitrum"``).
        wallet_address: User wallet whose aggregate position is read.
        position_manager_address: Registry-resolved contract the reads target
            (the Aave-family ``pool``; the Compound ``comet``; etc.). Defaults
            to ``""`` so callers may omit it; the registry rebinds it via
            ``dataclasses.replace`` during planning.
        market_id: Optional per-market identifier for protocols whose account
            state is scoped to a single market (Morpho Blue, Compound V3).
            ``None`` for whole-account protocols like the Aave family.
        block: Optional block to pin the read to. ``None`` → ``"latest"``.
            Post-execution captures MUST pin to the receipt block.
        prices: Injected ``{token_symbol: USD price}`` valuation inputs for
            non-USD-native protocols (Morpho / Compound), whose on-chain reads
            return raw token amounts rather than USD-denominated values. ``None``
            for the Aave family (its oracle denominates collateral/debt in USD
            on-chain, so no injection is needed). Empty ≠ Zero (AGENTS.md
            §Accounting): a ``None`` price for a required token makes the reducer
            fail closed (returns ``None``) rather than fabricate a zero.
        decimals: Injected ``{token_symbol: decimals}`` map used to scale the raw
            on-chain amounts to human units. Same non-USD-native rationale and
            ``None``-for-Aave default as ``prices``.
        market_params: Injected per-market parameters the reducer needs but cannot
            read on-chain cheaply (e.g. Morpho's ``{"lltv": <1e18-scaled int>}``).
            ``None`` for protocols that need none.
        collateral_token: Injected collateral-token symbol the reducer looks up in
            ``prices`` / ``decimals``. ``None`` for whole-account protocols.
        loan_token: Injected loan/borrow-token symbol the reducer looks up in
            ``prices`` / ``decimals``. ``None`` for whole-account protocols.
    """

    chain: str
    wallet_address: str
    position_manager_address: str = ""
    market_id: str | None = None
    block: int | str | None = None
    # Injected valuation inputs for non-USD-native protocols (Morpho / Compound);
    # all None for Aave (USD-denominated on-chain). Empty ≠ Zero: a None price ⇒
    # the reducer fails closed.
    prices: Mapping[str, Decimal] | None = None
    decimals: Mapping[str, int] | None = None
    market_params: Mapping[str, Any] | None = None
    collateral_token: str | None = None
    loan_token: str | None = None
    # Injected collateral-token *address* for protocols whose read targets a
    # collateral position by address (Compound V3 ``userCollateral(user, asset)``).
    # ``None`` for whole-account / market-id-only protocols. Resolved + injected by
    # the framework reader alongside the collateral price/decimals.
    collateral_address: str | None = None


@dataclass(frozen=True)
class AccountStateReadSpec:
    """Connector-published descriptor for an aggregate account-state read.

    The account-state analogue of :class:`LendingReadSpec`. Carries the
    protocol-specific knowledge the framework reader must NOT hardcode:

    Attributes:
        contract_kinds: Ordered contract-kind names (the connector's private
            ``addresses.py`` vocabulary) the registry resolves the read target
            from, tried in order via ``AddressRegistry.resolve_contract_address``.
            For the Aave fork family this is ``("pool",)``.
        build_calls: ``AccountStateQuery -> list[EthCall]`` planner. Pure: emits
            the ordered reads (selector + ABI-encoded args, each with its target)
            the framework reader will execute. The result ordering is the
            contract ``reduce_calls`` decodes against.
        reduce_calls: ``(AccountStateQuery, list[str | None]) -> LendingAccountState
            | None`` reducer. Pure: decodes the return blobs (in the same order
            ``build_calls`` emitted them; ``None`` for a failed read) into the
            canonical aggregate state, or ``None`` when the required reads are
            missing/malformed so the framework reader fails closed.
        valuation_role_keys: Connector-declared ``(query_field, market_params_key)``
            pairs naming which tokens the framework consumer must price + inject
            (VIB-4929 PR-3a). For each pair the registry's
            :meth:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.valuation_roles`
            looks up ``market_params()[market_params_key]`` to get the token
            symbol, and the framework reader resolves its USD price + decimals and
            sets ``AccountStateQuery.<query_field>``. Morpho declares
            ``(("collateral_token","collateral_token"),("loan_token","loan_token"))``;
            the Aave family declares ``()`` because its on-chain reads are already
            USD-denominated (no external price injection needed). Empty tuple =
            "this protocol needs no injected valuation".
    """

    contract_kinds: tuple[str, ...]
    build_calls: Callable[[AccountStateQuery], list[EthCall]]
    reduce_calls: Callable[[AccountStateQuery, list[str | None]], LendingAccountState | None]
    valuation_role_keys: tuple[tuple[str, str], ...] = ()
    # Connector-declared market-id normaliser (VIB-4929 PR-3b). ``None`` → the
    # registry's default Morpho-style 32-byte ``zfill(64)`` normalisation. Compound
    # V3 sets ``str.lower`` because its market ids are base-asset symbols, not hashes.
    normalize_market_id: Callable[[str], str] | None = None
    # Connector-declared per-read intent-input extractor (VIB-4929 PR-3b). ``None`` →
    # the default :meth:`query_inputs_from_intent` (``market_id`` only). Compound V3
    # sets one that also names the intent-derived collateral token. An empty
    # ``contract_kinds`` (above) signals a *market-scoped* read target (the per-market
    # Comet), which the registry binds from the market table rather than AddressRegistry.
    query_inputs_fn: Callable[[Any], dict[str, Any]] | None = None

    def query_inputs_from_intent(self, intent: Any) -> dict[str, Any]:
        """Extract the per-read query inputs this protocol needs from an intent.

        The framework consumer calls this (via
        :meth:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.query_inputs`)
        to derive the keyword inputs that vary per protocol — without the
        framework hardcoding which intent attributes a protocol reads. The
        result is splatted into
        :func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`.

        Default (covers the Aave family and Morpho Blue whole-account read):
        ``{"market_id": intent.market_id or None}`` — ``None`` for whole-account
        protocols (Aave) where the intent carries no ``market_id``; the market id
        for per-market protocols (Morpho). Per-market protocols whose Comet/market
        selection needs more than ``market_id`` (Compound V3, PR-3b) override this.
        """
        if self.query_inputs_fn is not None:
            return self.query_inputs_fn(intent)
        return {"market_id": getattr(intent, "market_id", None)}


# ---------------------------------------------------------------------------
# Aave V3 fork family account-state ABI (Aave V3, Spark)
# ---------------------------------------------------------------------------
#
# The decode below is byte-identical to ``read_aave_account_state`` in
# ``almanak/framework/accounting/lending_accounting.py`` (the PR-1 equivalence
# gate, VIB-4929). The Aave oracle denominates collateral/debt in USD on-chain,
# so this spec needs no external price injection.

# getUserAccountData(address user) → (totalCollateralBase, totalDebtBase,
#   availableBorrowsBase, currentLiquidationThreshold, ltv, healthFactor)
_AAVE_GET_ACCOUNT_DATA_SELECTOR = "0xbf92857c"
# getUserEMode(address user) → uint256 category (0 = none; 1..255 = configured)
_AAVE_GET_USER_EMODE_SELECTOR = "0xeddf1b79"
_AAVE_USD_SCALE = Decimal("1e8")  # 8-decimal USD base unit
_AAVE_HF_SCALE = Decimal("1e18")  # 1.0 HF = 1e18
# Empty positions return a sentinel HF (uint256 max); cap so it serialises sanely.
_AAVE_HF_CAP = Decimal("999999")


def parse_account_state_hex(hex_data: str | None) -> tuple[Decimal, Decimal, int, Decimal] | None:
    """Decode an Aave-fork ``getUserAccountData`` return blob.

    Returns ``(collateral_usd, debt_usd, liquidation_threshold_bps,
    health_factor)`` or ``None`` when the blob is missing/too short. Mirrors the
    ``read_aave_account_state`` decode byte-for-byte:

      [0] totalCollateralBase  (uint256, 1e8 USD)
      [1] totalDebtBase        (uint256, 1e8 USD)
      [2] availableBorrowsBase (uint256, 1e8 USD) -- not used
      [3] currentLiquidationThreshold (uint256, bps, e.g. 8500)
      [4] ltv                  (uint256, bps) -- not used
      [5] healthFactor         (uint256, 1e18; capped at 999999)
    """
    if not hex_data:
        return None
    # Lowercase before stripping so an uppercase ``0X`` prefix is handled (matches
    # the framework reader's robustness).
    raw = hex_data.lower().replace("0x", "")
    if len(raw) < 6 * 64:  # expect ≥ 6 words
        return None
    try:
        collateral_usd = Decimal(decode_uint_hex(raw, 0)) / _AAVE_USD_SCALE
        debt_usd = Decimal(decode_uint_hex(raw, 1)) / _AAVE_USD_SCALE
        liquidation_threshold_bps = decode_uint_hex(raw, 3)
        hf_raw = decode_uint_hex(raw, 5)
        health_factor = min(Decimal(hf_raw) / _AAVE_HF_SCALE, _AAVE_HF_CAP)
    except (ValueError, ArithmeticError):
        logger.debug("Failed to parse getUserAccountData hex", exc_info=True)
        return None
    return collateral_usd, debt_usd, liquidation_threshold_bps, health_factor


def parse_user_emode_hex(hex_data: str | None) -> int | None:
    """Decode an Aave-fork ``getUserEMode`` return blob into a category id.

    Returns an ``int`` in ``0..255`` (``0`` = not in any e-mode — a real value),
    or ``None`` when the blob is missing/too short or the decoded value lies
    outside the documented uint8 range (a sign the response was the wrong shape).
    Empty ≠ Zero — ``None`` is "unmeasured", distinct from a measured ``0``.
    """
    if not hex_data:
        return None
    raw = hex_data.lower().replace("0x", "")
    if len(raw) < 64:  # need at least one uint256 word
        return None
    try:
        value = decode_uint_hex(raw, 0)
    except ValueError:
        logger.debug("Failed to parse getUserEMode hex", exc_info=True)
        return None
    # Aave V3 stores category ids as uint8 (CategoryId in EModeLogic.sol). Out of
    # range ⇒ wrong-shape response; treat as unmeasured rather than trust it.
    if not 0 <= value <= 255:
        logger.debug("getUserEMode decoded value %d outside uint8 range", value)
        return None
    return value


def _build_aave_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the Aave-fork aggregate reads: ``getUserAccountData`` + ``getUserEMode``.

    Both target the same ``pool`` contract (``query.position_manager_address``).
    The e-mode read is emitted second so ``_reduce_aave_account_state`` decodes it
    as a best-effort enrichment; a failed e-mode read does not void the rest.
    """
    wallet = pad_address(query.wallet_address)
    return [
        EthCall(to=query.position_manager_address, data=_AAVE_GET_ACCOUNT_DATA_SELECTOR + wallet),
        EthCall(to=query.position_manager_address, data=_AAVE_GET_USER_EMODE_SELECTOR + wallet),
    ]


def _reduce_aave_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode Aave-fork ``[getUserAccountData, getUserEMode]`` results.

    Byte-identical to ``read_aave_account_state``: the primary
    ``getUserAccountData`` blob is required (``None`` if it fails/short); the
    secondary ``getUserEMode`` blob is best-effort, decoding to ``None`` (not
    fabricated ``0``) when absent or malformed.
    """
    account_hex = results[0] if results else None
    parsed = parse_account_state_hex(account_hex)
    if parsed is None:
        return None
    collateral_usd, debt_usd, liquidation_threshold_bps, health_factor = parsed

    emode_hex = results[1] if len(results) > 1 else None
    e_mode_category = parse_user_emode_hex(emode_hex)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=liquidation_threshold_bps,
        e_mode_category=e_mode_category,
        # Structural discriminator the serializer gates the Aave-only
        # e_mode_category / interest_rate_mode keys on — NOT a protocol-name
        # string. interest_rate_mode is intent metadata the framework consumer
        # overlays later (it is never decoded here).
        family="aave",
    )


#: Aggregate account-state read shared by every Aave V3 fork (Aave V3, Spark).
#: Both forks expose the identical ``getUserAccountData`` / ``getUserEMode`` ABI
#: against their own ``pool`` contract, and that data is USD-denominated on-chain
#: by each fork's oracle (no external price injection needed). Only the per-chain
#: pool address (owned by each connector's ``addresses.py``) differs.
AAVE_FORK_ACCOUNT_STATE_READ = AccountStateReadSpec(
    contract_kinds=("pool",),
    build_calls=_build_aave_account_state_calls,
    reduce_calls=_reduce_aave_account_state,
    # USD-denominated on-chain by Aave's oracle ⇒ no external price injection.
    valuation_role_keys=(),
)


# ---------------------------------------------------------------------------
# Morpho Blue account-state ABI (VIB-4929 PR-3a)
# ---------------------------------------------------------------------------
#
# Morpho Blue is NOT USD-native: its on-chain reads return raw token amounts and
# share totals, never USD. So unlike the Aave spec, this reducer consumes the
# injected price/decimals/market-params seam on :class:`AccountStateQuery` to
# value the position. The decode below is byte-identical to
# ``read_morpho_blue_account_state`` in
# ``almanak/framework/accounting/lending_accounting.py`` (the consumer this PR
# migrates onto the spec); the framework function keeps the gateway round-trip
# + block-pinning, the spec stays pure.

# position(bytes32 id, address user) → (uint256 supplyShares,
#   uint128 borrowShares, uint128 collateral)
_MORPHO_POSITION_SELECTOR = "0x93c52062"
# market(bytes32 id) → (uint128 totalSupplyAssets, uint128 totalSupplyShares,
#   uint128 totalBorrowAssets, uint128 totalBorrowShares, uint128 lastUpdate,
#   uint128 fee)
_MORPHO_MARKET_SELECTOR = "0x5c60e39a"
_MORPHO_LLTV_SCALE = Decimal("1e18")  # lltv is 1e18-scaled
# No-debt / undefined-HF sentinel, also the serialisation cap for huge HFs.
_MORPHO_HF_SENTINEL = Decimal("999999")


def normalize_market_id_hex(market_id: str) -> str:
    """Return a 32-byte Morpho market id as 64 lowercase hex chars (no ``0x``).

    Mirrors the framework reader's ``_normalize_market_id_hex`` byte-for-byte —
    the Morpho ``build_calls`` planner needs it to encode the ``position`` /
    ``market`` calldata. Public (no leading underscore) because it is a connector
    primitive other strategy-side callers may reuse.
    """
    raw = market_id.lower().replace("0x", "")
    return raw.zfill(64)


def _build_morpho_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the two Morpho reads: ``position(id, user)`` then ``market(id)``.

    Both target the resolved Morpho singleton (``query.position_manager_address``)
    and are scoped to ``query.market_id``. The order is the contract
    :func:`_reduce_morpho_account_state` decodes against (position first, market
    second).

    Fails closed (returns ``[]``) when ``query.market_id`` is missing: Morpho's
    account state is per-market, so a missing market id has no well-defined read.
    Normalising ``None`` to the all-zero bytes32 would otherwise issue live
    ``position`` / ``market`` RPCs against market id ``0x00…00`` and decode a
    fabricated empty state (CodeRabbit 2026-06). With no calls, the planner emits
    an empty plan and the reducer (which requires a non-empty position blob) also
    fails closed.
    """
    if not query.market_id:
        return []
    market_hex = normalize_market_id_hex(query.market_id)
    user_hex = pad_address(query.wallet_address)
    morpho = query.position_manager_address
    return [
        EthCall(to=morpho, data=_MORPHO_POSITION_SELECTOR + market_hex + user_hex),
        EthCall(to=morpho, data=_MORPHO_MARKET_SELECTOR + market_hex),
    ]


def _reduce_morpho_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode Morpho ``[position, market]`` blobs into aggregate account state.

    Byte-identical to ``read_morpho_blue_account_state``'s decode. Pure: values
    the position from the injected ``query.prices`` / ``query.decimals`` /
    ``query.market_params`` (Morpho is not USD-native), never touching the gateway
    or an oracle. Fails closed (returns ``None``) on a missing / short blob, a
    missing required injected input, or a ``None`` price (Empty ≠ Zero).
    """
    position_hex = results[0] if results else None
    market_hex = results[1] if len(results) > 1 else None

    # Required injected inputs — fail closed (not fabricate) when any is missing.
    collateral_token = query.collateral_token
    loan_token = query.loan_token
    prices = query.prices
    decimals = query.decimals
    market_params = query.market_params
    if collateral_token is None or loan_token is None or prices is None or decimals is None or market_params is None:
        return None
    if collateral_token not in decimals or loan_token not in decimals:
        return None
    lltv_raw = market_params.get("lltv")
    if lltv_raw is None:
        return None

    # ── position(id, user) → borrowShares (word 1), collateral (word 2) ──────
    if not position_hex:
        return None
    pos = position_hex[2:] if position_hex[:2].lower() == "0x" else position_hex
    if len(pos) < 3 * 64:  # 3 words minimum
        return None
    # ── market(id) → totalBorrowAssets (word 2), totalBorrowShares (word 3) ──
    if not market_hex:
        return None
    mkt = market_hex[2:] if market_hex[:2].lower() == "0x" else market_hex
    if len(mkt) < 6 * 64:  # 6 words minimum
        return None

    try:
        borrow_shares = decode_uint_hex(pos, 1)
        collateral_raw = decode_uint_hex(pos, 2)
        total_borrow_assets = decode_uint_hex(mkt, 2)
        total_borrow_shares = decode_uint_hex(mkt, 3)
    except (ValueError, ArithmeticError):
        logger.debug("Failed to decode Morpho position/market hex", exc_info=True)
        return None

    # ── shares → assets (round UP to be conservative — never under-count debt) ─
    # Exact integer ceil-division — byte-identical to the framework reader's
    # ``(shares * total_assets + total_shares - 1) // total_shares``. Done on ints
    # (not Decimal) so 128-bit share totals cannot lose precision at the default
    # 28-digit Decimal context.
    if borrow_shares == 0 or total_borrow_shares == 0:
        borrow_assets = 0
    else:
        borrow_assets = (borrow_shares * total_borrow_assets + total_borrow_shares - 1) // total_borrow_shares

    # ── raw → human, then USD via the injected prices (Empty ≠ Zero) ─────────
    collateral_amount = Decimal(collateral_raw) / Decimal(10 ** decimals[collateral_token])
    borrow_amount = Decimal(borrow_assets) / Decimal(10 ** decimals[loan_token])

    collateral_price = prices.get(collateral_token)
    loan_price = prices.get(loan_token)
    if collateral_price is None or loan_price is None:
        return None

    collateral_usd = collateral_amount * collateral_price
    debt_usd = borrow_amount * loan_price

    # ── lltv + health factor (no-debt sentinel, capped) ──────────────────────
    # Fail closed on a malformed injected lltv (bad catalogue entry / upstream
    # injection bug) rather than letting Decimal() abort the whole read with an
    # uncaught exception — the reducer's contract is "return None on bad input"
    # (CodeRabbit 2026-06). Empty ≠ Zero: a None/garbage lltv is unmeasured.
    try:
        lltv = Decimal(lltv_raw) / _MORPHO_LLTV_SCALE
    except (TypeError, ValueError, ArithmeticError):
        logger.debug("Invalid Morpho lltv value: %r", lltv_raw, exc_info=True)
        return None
    if borrow_shares == 0 or debt_usd == 0:
        health_factor = _MORPHO_HF_SENTINEL
    else:
        health_factor = (collateral_usd * lltv) / debt_usd
    health_factor = min(health_factor, _MORPHO_HF_SENTINEL)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,  # Morpho carries the threshold as lltv, not bps.
        e_mode_category=None,  # Morpho has no e-mode concept.
        lltv=lltv,
    )


#: Aggregate account-state read for Morpho Blue (VIB-4929 PR-3a).
#: Morpho deploys a single per-chain singleton (contract kind ``morpho``, owned
#: by ``morpho_blue/addresses.py``) that holds every market's per-user position
#: and the market totals. Unlike the Aave family, Morpho is not USD-native — the
#: reducer values the position from the injected price/decimals/market-params
#: seam on :class:`AccountStateQuery`, staying pure (no gateway, no oracle).
MORPHO_BLUE_ACCOUNT_STATE_READ = AccountStateReadSpec(
    contract_kinds=("morpho",),
    build_calls=_build_morpho_account_state_calls,
    reduce_calls=_reduce_morpho_account_state,
    # Morpho is not USD-native: the framework consumer must price both legs and
    # inject them. Each pair is (AccountStateQuery field to set, MORPHO_MARKETS
    # params key holding the token symbol). The market id fully determines the
    # (collateral, loan) pair (it is keccak(loan, collateral, oracle, irm, lltv)),
    # so valuation reads the symbols from the market catalogue, not the intent.
    valuation_role_keys=(
        ("collateral_token", "collateral_token"),
        ("loan_token", "loan_token"),
    ),
)


# ---------------------------------------------------------------------------
# Compound V3 (Comet) account-state ABI (VIB-4929 PR-3b)
# ---------------------------------------------------------------------------
#
# Compound V3 differs from the Aave family and Morpho on three axes the
# connector-declared spec hooks resolve (so the framework stays generic):
#
#   * Per-market target. The read target is a per-market Comet, not a single
#     per-chain contract — so this spec declares EMPTY ``contract_kinds`` (the
#     "target is market-scoped" signal) and the registry binds
#     ``query.position_manager_address`` from the market table's ``comet_address``
#     instead of via ``AddressRegistry``.
#   * Symbol market ids. ``market_id`` is a base-asset symbol ("usdc"/"weth"),
#     not a 32-byte hash — so this spec declares ``normalize_market_id=str.lower``.
#   * Intent-derived collateral. The base/borrow leg is market-derived (priced via
#     ``valuation_role_keys``); the collateral leg is intent-derived (any approved
#     collateral can back a Comet) — so this spec declares a ``query_inputs_fn``
#     that names the collateral symbol, and the framework reader prices + resolves
#     the address of ``query.collateral_token``.
#
# The decode below is byte-identical to ``read_compound_v3_account_state`` in
# ``almanak/framework/accounting/lending_accounting.py`` (the consumer this PR
# migrates onto the spec). The spec stays pure (no gateway, oracle, resolver); the
# framework reader owns the gateway round-trip + price/decimals/address resolution.

# userCollateral(address user, address asset) → (uint128 balance, uint128 reserved)
_COMPOUND_V3_USER_COLLATERAL_SELECTOR = "0x2b92a07d"
# borrowBalanceOf(address user) → uint256
_COMPOUND_V3_BORROW_BALANCE_SELECTOR = "0x374c49b4"
# balanceOf(address user) → uint256 (supplied base-asset balance)
_COMPOUND_V3_BALANCE_OF_SELECTOR = "0x70a08231"
# No-debt / base-asset-supply HF sentinel, also the serialisation cap.
_COMPOUND_HF_SENTINEL = Decimal("999999")


# ---------------------------------------------------------------------------
# Compound V3 (Comet) MULTI-COLLATERAL account-health ABI (VIB-4851 PR-2)
# ---------------------------------------------------------------------------
#
# The VIB-4929 single-leg seam above answers "supply/debt for ONE collateral leg"
# (``userCollateral(user, asset)`` — selector ``0x2b92a07d``). The product-owner
# choice for the position-HEALTH gate keeps the multi-collateral summed health
# factor ``HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd``,
# which needs a DIFFERENT set of Comet reads that iterate every approved
# collateral and read its on-chain price + scale + liquidation factor:
#
#   * ``collateralBalanceOf(user, asset)`` — the 2-arg balance form (uint128),
#     DISTINCT from the seam's ``userCollateral`` (which also returns a reserved
#     word). Selector ``0x5c2549ee``.
#   * ``getAssetInfoByAddress(asset)`` — the 8-field AssetInfo struct carrying the
#     per-collateral price feed, scale, and liquidation factor (read ON-CHAIN, never
#     from a catalogue — see ``read_compound_v3_market_health``). Selector ``0x3b3bec2e``.
#   * ``getPrice(priceFeed)`` — the Comet-oracle USD price (8-decimal). Selector ``0x41976e09``.
#
# ``borrowBalanceOf`` is shared with the seam (``_COMPOUND_V3_BORROW_BALANCE_SELECTOR``).
# These primitives are pure ABI data + a pure struct decoder; the gateway-routed
# ``eth_call`` that executes them lives in the framework reader (Gateway-boundary rule).

# collateralBalanceOf(address user, address asset) → uint128 (the 2-arg balance form;
# NOT ``userCollateral``, which additionally returns a reserved word).
_COMPOUND_V3_COLLATERAL_BALANCE_OF_SELECTOR = "0x5c2549ee"
# getAssetInfoByAddress(address asset) → AssetInfo (8-field struct, see _parse_asset_info_hex).
_COMPOUND_V3_GET_ASSET_INFO_SELECTOR = "0x3b3bec2e"
# getPrice(address priceFeed) → uint256 (USD price, 8 decimals).
_COMPOUND_V3_GET_PRICE_SELECTOR = "0x41976e09"


def build_compound_collateral_balance_calldata(user_address: str, asset_address: str) -> str:
    """Build calldata for ``collateralBalanceOf(address user, address asset)``."""
    return _COMPOUND_V3_COLLATERAL_BALANCE_OF_SELECTOR + pad_address(user_address) + pad_address(asset_address)


def build_compound_asset_info_calldata(asset_address: str) -> str:
    """Build calldata for ``getAssetInfoByAddress(address asset)``."""
    return _COMPOUND_V3_GET_ASSET_INFO_SELECTOR + pad_address(asset_address)


def build_compound_get_price_calldata(price_feed: str) -> str:
    """Build calldata for ``getPrice(address priceFeed)``."""
    return _COMPOUND_V3_GET_PRICE_SELECTOR + pad_address(price_feed)


def build_compound_borrow_balance_calldata(user_address: str) -> str:
    """Build calldata for ``borrowBalanceOf(address user)`` (shared with the seam)."""
    return _COMPOUND_V3_BORROW_BALANCE_SELECTOR + pad_address(user_address)


def _parse_asset_info_hex(hex_data: str | None) -> tuple[str, int, int] | None:
    """Decode an ``getAssetInfoByAddress`` AssetInfo struct return blob.

    Returns ``(price_feed, scale, liquidate_cf)`` — the three fields the
    multi-collateral health read needs — or ``None`` when the blob is
    missing/short/malformed (fail-closed, Empty ≠ Zero).

    The 8-field struct layout (matches the legacy inline Comet ABI in
    ``position_health._get_compound_health``):
      [0] offset            (uint8 padded)
      [1] asset             (address padded)
      [2] priceFeed         (address padded)  ← returned (checksum-agnostic hex)
      [3] scale             (uint64 padded)   ← returned
      [4] borrowCollateralFactor    (uint64 padded) — not used
      [5] liquidateCollateralFactor (uint64 padded) ← returned
      [6] liquidationFactor (uint64 padded)   — not used
      [7] supplyCap         (uint128 padded)  — not used
    """
    if not hex_data:
        return None
    raw = hex_data[2:] if hex_data[:2].lower() == "0x" else hex_data
    if len(raw) < 8 * 64:  # 8 words minimum
        return None
    try:
        # priceFeed is an address packed in word 2: take the trailing 40 hex chars
        # and re-prefix with 0x so it is a valid address literal for the price read.
        price_feed = "0x" + raw[2 * 64 + 24 : 3 * 64]
        if int(price_feed, 16) == 0:
            # Zero price-feed address => uninitialised / invalid asset. Fail closed
            # (Empty != Zero) rather than read getPrice against the zero address.
            return None
        scale = decode_uint_hex(raw, 3)
        liquidate_cf = decode_uint_hex(raw, 5)
    except (ValueError, ArithmeticError):
        logger.debug("Failed to parse Compound getAssetInfoByAddress hex", exc_info=True)
        return None
    return price_feed, scale, liquidate_cf


def _compound_query_inputs_from_intent(intent: Any) -> dict[str, Any]:
    """Derive Compound V3's per-read inputs from an intent.

    Byte-identical to the selection ``_capture_compound_v3_pre_state`` and the
    Compound arm of ``build_lending_accounting_event`` performed:

    * SUPPLY / WITHDRAW: the collateral being supplied is ``intent.token``; the
      Comet is selected by ``intent.market_id`` (required — a missing market id
      stays ``None`` and the read fails closed downstream, never guessing a Comet
      from the collateral symbol).
    * BORROW / REPAY / DELEVERAGE: the collateral is ``intent.collateral_token``;
      the Comet key falls back to the borrow token (``intent.borrow_token`` or
      ``intent.token``) when ``intent.market_id`` is absent — the legacy
      ``(market_id or borrow_token)`` Comet-key behaviour.
    """
    it = getattr(intent, "intent_type", None)
    if it is None:
        intent_type = ""
    else:
        intent_type = it.value if hasattr(it, "value") else str(it)
    market_id = getattr(intent, "market_id", None)
    if intent_type in ("SUPPLY", "WITHDRAW"):
        collateral_token = getattr(intent, "token", None)
    else:
        collateral_token = getattr(intent, "collateral_token", None)
        if not market_id:
            market_id = getattr(intent, "borrow_token", None) or getattr(intent, "token", None)
    return {"market_id": market_id, "collateral_token": collateral_token}


def _compound_health_factor(
    collateral_usd: Decimal,
    debt_usd: Decimal,
    collateral_token: str,
    params: Mapping[str, Any],
) -> Decimal | None:
    """HF = min((collateral_usd * LCF) / debt_usd, sentinel); sentinel when no debt.

    LCF (``liquidation_collateral_factor``) is read from the injected market
    params' ``collaterals`` map, case-insensitively (so mixed-case symbols like
    ``wstETH`` resolve). Returns ``None`` (unmeasured, Empty ≠ Zero) when the LCF
    is absent — byte-identical to ``read_compound_v3_account_state``.
    """
    if debt_usd == 0:
        return _COMPOUND_HF_SENTINEL
    collaterals = params.get("collaterals") or {}
    col_upper = collateral_token.upper()
    entry = collaterals.get(col_upper)
    if entry is None:
        for k, v in collaterals.items():
            if k.upper() == col_upper:
                entry = v
                break
    lcf = entry.get("liquidation_collateral_factor") if entry else None
    if lcf is None:
        return None
    return min((collateral_usd * lcf) / debt_usd, _COMPOUND_HF_SENTINEL)


def _build_compound_account_state_calls(query: AccountStateQuery) -> list[EthCall]:
    """Emit the Compound V3 reads against the per-market Comet.

    Two shapes (byte-identical to ``read_compound_v3_account_state``):

    * Base-asset supply (collateral == the market's base token): ``balanceOf(user)``
      then ``borrowBalanceOf(user)``.
    * Collateral / borrow (collateral != base): ``userCollateral(user, collateral)``
      then ``borrowBalanceOf(user)``.

    Fails closed (returns ``[]``) when the per-market Comet target was not bound
    (``query.position_manager_address`` empty — no Comet for this market) or, on
    the collateral path, when the collateral address was not injected. The order
    is the contract the reducer decodes against (primary read, then borrowBalanceOf).
    """
    comet = query.position_manager_address
    if not comet:
        return []
    params = query.market_params or {}
    base_token = params.get("base_token")
    collateral_token = query.collateral_token
    user_hex = pad_address(query.wallet_address)
    borrow_call = EthCall(to=comet, data=_COMPOUND_V3_BORROW_BALANCE_SELECTOR + user_hex)

    is_base_asset_supply = bool(collateral_token and base_token and collateral_token.upper() == str(base_token).upper())
    if is_base_asset_supply:
        return [EthCall(to=comet, data=_COMPOUND_V3_BALANCE_OF_SELECTOR + user_hex), borrow_call]

    # Collateral path needs the collateral token *address* (framework-injected).
    if not query.collateral_address:
        return []
    collateral_hex = pad_address(query.collateral_address)
    return [
        EthCall(to=comet, data=_COMPOUND_V3_USER_COLLATERAL_SELECTOR + user_hex + collateral_hex),
        borrow_call,
    ]


def _decode_compound_word(blob: str, min_hex_len: int) -> int | None:
    """Decode word 0 of a Comet read blob, or ``None`` on a short / malformed blob.

    ``min_hex_len`` is the minimum hex length required: 64 for a single uint256
    (``balanceOf`` / ``borrowBalanceOf``), 128 for ``userCollateral``'s
    ``(uint128 balance, uint128 reserved)`` two-word return. Fail-closed (``None``),
    never a fabricated ``0`` (Empty ≠ Zero).
    """
    if len(blob) < min_hex_len:
        return None
    try:
        return decode_uint_hex(blob, 0)
    except (ValueError, ArithmeticError):
        return None


def _reduce_compound_account_state(
    query: AccountStateQuery,
    results: list[str | None],
) -> LendingAccountState | None:
    """Decode Compound V3 ``[balanceOf|userCollateral, borrowBalanceOf]`` blobs.

    Byte-identical to ``read_compound_v3_account_state``'s decode + HF math. Pure:
    values the position from the injected ``query.prices`` / ``query.decimals`` /
    ``query.market_params``. Fails closed (returns ``None``) on a missing/short
    blob, a missing required injected input, or a ``None`` price (Empty ≠ Zero).
    """
    params = query.market_params or {}
    base_token = params.get("base_token")
    collateral_token = query.collateral_token
    prices = query.prices
    decimals = query.decimals
    if not base_token or not collateral_token or prices is None or decimals is None:
        return None
    if base_token not in prices or base_token not in decimals:
        return None

    primary_hex = results[0] if results else None
    borrow_hex = results[1] if len(results) > 1 else None
    if not primary_hex or not borrow_hex:
        return None
    primary = primary_hex[2:] if primary_hex[:2].lower() == "0x" else primary_hex
    borrow = borrow_hex[2:] if borrow_hex[:2].lower() == "0x" else borrow_hex
    borrow_balance_raw = _decode_compound_word(borrow, 64)
    if borrow_balance_raw is None:
        return None

    base_decimals = decimals[base_token]
    base_price = prices[base_token]
    if base_price is None:
        return None
    debt_usd = (Decimal(borrow_balance_raw) / Decimal(10**base_decimals)) * base_price

    is_base_asset_supply = collateral_token.upper() == str(base_token).upper()
    if is_base_asset_supply:
        # balanceOf(user) → supplied base balance; base decimals/price for both legs.
        supplied_raw = _decode_compound_word(primary, 64)
        if supplied_raw is None:
            return None
        collateral_usd = (Decimal(supplied_raw) / Decimal(10**base_decimals)) * base_price
        health_factor: Decimal | None = _COMPOUND_HF_SENTINEL
    else:
        # userCollateral(user, collateral) → (uint128 balance, uint128 reserved):
        # two words, so require ≥128 hex chars (the legacy guard).
        if collateral_token not in prices or collateral_token not in decimals:
            return None
        collateral_price = prices[collateral_token]
        if collateral_price is None:
            return None
        # userCollateral → (uint128 balance, uint128 reserved): two words, ≥128 hex.
        collateral_raw = _decode_compound_word(primary, 128)
        if collateral_raw is None:
            return None
        collateral_usd = (Decimal(collateral_raw) / Decimal(10 ** decimals[collateral_token])) * collateral_price
        health_factor = _compound_health_factor(collateral_usd, debt_usd, collateral_token, params)

    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,  # Compound uses per-asset LCFs, not a single threshold.
        e_mode_category=None,  # No e-mode concept.
        lltv=None,  # Not a Morpho-style per-market lltv.
    )


#: Aggregate account-state read for Compound V3 (VIB-4929 PR-3b). The read target
#: is the per-market Comet (empty ``contract_kinds`` → market-scoped, bound by the
#: registry from the market table's ``comet_address``). Compound is not USD-native:
#: the base/borrow leg is priced via ``valuation_role_keys`` (market-derived), the
#: collateral leg by the framework reader from the ``query_inputs_fn``-named intent
#: token. ``normalize_market_id=str.lower`` because Compound market ids are
#: base-asset symbols, not 32-byte hashes.
COMPOUND_V3_ACCOUNT_STATE_READ = AccountStateReadSpec(
    contract_kinds=(),
    build_calls=_build_compound_account_state_calls,
    reduce_calls=_reduce_compound_account_state,
    valuation_role_keys=(("loan_token", "base_token"),),
    normalize_market_id=str.lower,
    query_inputs_fn=_compound_query_inputs_from_intent,
)
