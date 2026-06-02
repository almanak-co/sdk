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
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

logger = logging.getLogger(__name__)

__all__ = [
    "AAVE_FORK_ACCOUNT_STATE_READ",
    "AAVE_FORK_RESERVE_READ",
    "AccountStateQuery",
    "AccountStateReadSpec",
    "EthCall",
    "LendingAccountState",
    "LendingPositionOnChain",
    "LendingReadSpec",
    "decode_uint_hex",
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
    """

    collateral_usd: Decimal | None
    debt_usd: Decimal | None
    health_factor: Decimal | None
    liquidation_threshold_bps: int | None
    e_mode_category: int | None


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
    """

    chain: str
    wallet_address: str
    position_manager_address: str = ""
    market_id: str | None = None
    block: int | str | None = None


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
    """

    contract_kinds: tuple[str, ...]
    build_calls: Callable[[AccountStateQuery], list[EthCall]]
    reduce_calls: Callable[[AccountStateQuery, list[str | None]], LendingAccountState | None]


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
)
