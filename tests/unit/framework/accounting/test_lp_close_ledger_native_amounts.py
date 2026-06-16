"""Regression contract (VIB-5132): V4 native LP_CLOSE ledger amounts are HUMAN units.

Sibling of ``test_lp_ledger_raw_wei_repro.py`` (the LP_OPEN human-units pin,
VIB-5036). An LP_CLOSE had an ``LP_OPEN`` extraction branch but no symmetric
``LP_CLOSE`` branch: a **V4 native** close returns its ETH leg as raw ETH
(TAKE_PAIR, no ERC-20 Transfer), so the receipt parser produces no
``swap_amounts`` and the row fell through to ``_extract_from_intent_fallback`` →
``amount_in=""`` / ``amount_out=""``. The measured proceeds live on
``LPCloseData.amount{0,1}_collected`` (VIB-5117 stamps the native principal),
but nothing read them back into the ledger's top-level amount columns.

These tests pin the fix:

* ``transaction_ledger.amount_in / amount_out`` is a HUMAN-units column (blueprint
  27 §18.4 asserts ``amount_in_human`` / ``amount_out_human``). The new
  ``_extract_from_lp_close`` scales ``LPCloseData.amount{0,1}_collected`` (raw
  on-chain ints) to human units, mirroring ``_extract_from_lp_open``.
* **Empty != Zero**: a ``None`` collected leg (unmeasured) stays ``""``; a
  measured ``0`` becomes the human ``"0"``.
* The native-leg amounts must be read POST-stamp — ``build_ledger_entry`` defers
  ``_extract_tokens_and_amounts`` to after the LP-close native stamps.
* A V3 close that DOES emit ``swap_amounts`` is unchanged (still swap-driven).

If these revert, the V4-native column-completeness gap (VIB-5132) has regressed.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.execution.extracted_data import LPCloseData, SwapAmounts
from almanak.framework.observability.ledger import build_ledger_entry

# V4 PoolKey native-ETH sentinel (currency == zero address) and base USDC.
_V4_NATIVE = "0x" + "0" * 40
_BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

# A small WETH/USDC native-V4 close: 0.0015 ETH (18 dp) + 3.00 USDC (6 dp).
_ETH_RAW = 1_500_000_000_000_000  # 1.5e15 wei = 0.0015 ETH
_USDC_RAW = 3_000_000  # 3e6 = 3 USDC


class _FakeV4CloseResult:
    """Minimal ``result`` carrying a V4-native ``LPCloseData``.

    ``swap_amounts`` MUST be falsy so the dispatcher routes through
    ``_extract_from_lp_close`` rather than the SWAP/V3 path.
    """

    swap_amounts = None
    success = True
    tx_hash = "0xlpclose"
    total_gas_cost_wei = 0

    def __init__(
        self,
        *,
        amount0: int | None,
        amount1: int | None,
    ) -> None:
        self.extracted_data: dict[str, Any] = {
            "lp_close_data": LPCloseData(
                amount0_collected=amount0,
                amount1_collected=amount1,
                currency0=_V4_NATIVE,
                currency1=_BASE_USDC,
            )
        }

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


class _FakeV4CloseIntent:
    """A V4-native LP_CLOSE intent (token symbols only used as a fallback)."""

    intent_type = "LP_CLOSE"
    token0 = "ETH"
    token1 = "USDC"
    pool = "ETH/USDC"


def test_v4_native_lp_close_persists_human_not_empty() -> None:
    """REGRESSION CONTRACT (VIB-5132): a V4 native close stores HUMAN amounts.

    Pre-fix, ``amount_in`` / ``amount_out`` were ``""`` (no swap_amounts → intent
    fallback). The writer now reads the post-stamp ``LPCloseData.amount{0,1}_
    collected`` and scales to human units (ETH 18-dp, USDC 6-dp).
    """
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeV4CloseIntent(),
        result=_FakeV4CloseResult(amount0=_ETH_RAW, amount1=_USDC_RAW),
        chain="base",
        success=True,
    )

    assert entry is not None
    # HUMAN units — the fix. 1.5e15 raw ETH / 1e18 = 0.0015; 3e6 USDC / 1e6 = 3.
    assert entry.amount_in == "0.0015", f"expected human ETH amount, got {entry.amount_in!r}"
    assert entry.amount_out == "3", f"expected human USDC amount, got {entry.amount_out!r}"
    # NOT empty (the VIB-5132 bug) and NOT raw wei.
    assert entry.amount_in not in ("", str(_ETH_RAW))
    assert entry.amount_out not in ("", str(_USDC_RAW))
    # Native sentinel resolves to the chain's native symbol.
    assert entry.token_in == "ETH"
    assert entry.token_out == "USDC"


def test_v4_native_lp_close_unmeasured_leg_stays_empty() -> None:
    """Empty != Zero: a ``None`` (unmeasured) native leg stays ``""``.

    A native leg whose runner stamp never ran is honestly unmeasured. The
    ERC-20 leg is still scaled to human units.
    """
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeV4CloseIntent(),
        result=_FakeV4CloseResult(amount0=None, amount1=_USDC_RAW),
        chain="base",
        success=True,
    )

    assert entry is not None
    assert entry.amount_in == "", f"unmeasured leg must stay empty, got {entry.amount_in!r}"
    assert entry.amount_out == "3"


def test_v4_native_lp_close_measured_zero_leg_is_zero() -> None:
    """Empty != Zero: a measured ``0`` leg becomes the human ``"0"`` (not "")."""
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeV4CloseIntent(),
        result=_FakeV4CloseResult(amount0=0, amount1=_USDC_RAW),
        chain="base",
        success=True,
    )

    assert entry is not None
    assert entry.amount_in == "0", f"measured-zero leg must be '0', got {entry.amount_in!r}"
    assert entry.amount_out == "3"


class _FakeV4UnresolvableCloseResult:
    """A V4 ERC-20/ERC-20 close whose currency0 address is NOT in the offline
    catalogue (off-registry token), so ``_resolve_lp_close_symbol`` returns "".

    ``currency0`` (PoolKey index 0) is the unresolvable token; ``currency1`` is
    USDC. The intent (below) lists tokens in the OPPOSITE (user) order — token0=USDC.
    A naive ``or intent.token0`` fallback would scale ``amount0_collected`` (raw,
    18-dp-magnitude) with USDC's 6 decimals → a materially wrong number.
    """

    swap_amounts = None
    success = True
    tx_hash = "0xv4unresolvable"
    total_gas_cost_wei = 0

    # An off-registry ERC-20 (not in the bundled catalogue) — resolver returns "".
    _OFF_REGISTRY = "0x1111111111111111111111111111111111111111"

    def __init__(self) -> None:
        self.extracted_data: dict[str, Any] = {
            "lp_close_data": LPCloseData(
                amount0_collected=2_000_000_000_000_000_000,  # 2e18 raw (18-dp token)
                amount1_collected=_USDC_RAW,  # 3e6 = 3 USDC
                currency0=self._OFF_REGISTRY,
                currency1=_BASE_USDC,
            )
        }

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


class _FakeV4UnresolvableCloseIntent:
    """Intent lists tokens in the OPPOSITE order to PoolKey (token0=USDC)."""

    intent_type = "LP_CLOSE"
    token0 = "USDC"  # PoolKey index 0 is the off-registry token, NOT USDC
    token1 = "TKN"
    pool = "USDC/TKN"


def test_v4_unresolvable_currency_does_not_misscale_with_intent_order() -> None:
    """CORRECTNESS (codex P2): a present-but-unresolvable V4 currency must NOT be
    scaled with the misordered intent-token fallback.

    PoolKey index 0 is an off-registry token; the intent lists token0=USDC (the
    opposite order). Scaling ``amount0_collected`` (18-dp magnitude) with USDC's 6
    decimals would persist a wildly wrong number. The fix leaves the unresolved
    leg unmeasured ("") rather than fabricating a misscaled amount. The resolvable
    USDC leg (currency1) is still scaled correctly.
    """
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeV4UnresolvableCloseIntent(),
        result=_FakeV4UnresolvableCloseResult(),
        chain="base",
        success=True,
    )

    assert entry is not None
    # The unresolvable leg must NOT be the USDC-misscaled value (2e18 / 1e6 = 2e12).
    assert entry.amount_in != "2000000000000", f"misscaled with wrong decimals: {entry.amount_in!r}"
    # It stays unmeasured (Empty != Zero) — never a fabricated number.
    assert entry.amount_in == "", f"unresolvable leg must stay empty, got {entry.amount_in!r}"
    # The LABEL must also NOT be the misordered intent token (USDC is PoolKey idx 1
    # here); a present-but-unresolved V4 leg carries no user-ordered symbol.
    assert entry.token_in != "USDC", f"misordered intent-token label leaked: {entry.token_in!r}"
    assert entry.token_in == "", f"unresolved present-currency label must stay empty, got {entry.token_in!r}"
    # The address-resolvable USDC leg is still scaled to human units.
    assert entry.amount_out == "3"


class _FakeCurrencyAbsentCloseResult:
    """An LP_CLOSE with ``LPCloseData`` but NO PoolKey currencies (V3-style /
    legacy data: ``currency0`` / ``currency1`` are ``None``).

    ``swap_amounts`` is falsy, so the dispatcher routes through
    ``_extract_from_lp_close`` and must take the currency-ABSENT fallback: symbols
    come from the intent (user order is safe — there is no PoolKey re-sort).
    """

    swap_amounts = None
    success = True
    tx_hash = "0xcurrencyabsent"
    total_gas_cost_wei = 0

    def __init__(self) -> None:
        self.extracted_data: dict[str, Any] = {
            "lp_close_data": LPCloseData(
                amount0_collected=500_000_000_000_000_000,  # 0.5 WETH (18 dp)
                amount1_collected=_USDC_RAW,  # 3 USDC (6 dp)
                currency0=None,
                currency1=None,
            )
        }

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


class _FakeIntentTokenAttrsIntent:
    """currency-absent close whose symbols come from token0/token1 intent attrs."""

    intent_type = "LP_CLOSE"
    token0 = "WETH"
    token1 = "USDC"
    pool = ""  # force the intent-attr branch (not the pool-string parse)


class _FakePoolStringOnlyIntent:
    """currency-absent close with NO token attrs — symbols parsed from ``pool``."""

    intent_type = "LP_CLOSE"
    pool = "WETH/USDC"

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return ""


def test_currency_absent_close_uses_intent_token_attrs() -> None:
    """currency-ABSENT leg (V3-style data) is safe to label from the intent's
    user-ordered token0/token1 — no PoolKey re-sort to misalign."""
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeIntentTokenAttrsIntent(),
        result=_FakeCurrencyAbsentCloseResult(),
        chain="base",
        success=True,
    )
    assert entry is not None
    assert entry.token_in == "WETH"
    assert entry.token_out == "USDC"
    assert entry.amount_in == "0.5"
    assert entry.amount_out == "3"


def test_currency_absent_close_falls_back_to_pool_string() -> None:
    """currency-ABSENT leg with no token attrs parses symbols from ``pool``."""
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakePoolStringOnlyIntent(),
        result=_FakeCurrencyAbsentCloseResult(),
        chain="base",
        success=True,
    )
    assert entry is not None
    assert entry.token_in == "WETH"
    assert entry.token_out == "USDC"
    assert entry.amount_in == "0.5"
    assert entry.amount_out == "3"


class _FakeV3CloseResult:
    """A V3 close that DID emit ``swap_amounts`` (the unchanged path)."""

    success = True
    tx_hash = "0xv3close"
    total_gas_cost_wei = 0

    def __init__(self) -> None:
        self.swap_amounts = SwapAmounts(
            amount_in=500_000_000_000_000_000,  # 0.5 WETH raw (18 dp)
            amount_out=1_500_000_000,  # 1500 USDC raw (6 dp)
            amount_in_decimal=Decimal("0.5"),
            amount_out_decimal=Decimal("1500"),
            effective_price=Decimal("3000"),
            slippage_bps=10,
            token_in="WETH",
            token_out="USDC",
        )
        # Even if LPCloseData is also present, the swap_amounts short-circuit wins.
        self.extracted_data: dict[str, Any] = {
            "lp_close_data": LPCloseData(amount0_collected=999, amount1_collected=999)
        }

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - stub default
        return None


class _FakeV3CloseIntent:
    intent_type = "LP_CLOSE"
    token0 = "WETH"
    token1 = "USDC"
    pool = "WETH/USDC"


def test_v3_lp_close_with_swap_amounts_unchanged() -> None:
    """REGRESSION: a V3 close with ``swap_amounts`` is still swap-driven.

    The new LP_CLOSE branch sits AFTER the ``swap_amounts`` short-circuit, so a
    V3 ERC-20 close (whose receipt parser emits SwapAmounts) is untouched — its
    amounts come from the swap path, not from LPCloseData.
    """
    entry = build_ledger_entry(
        deployment_id="deployment:5132abcdef01",
        cycle_id="cycle-1",
        intent=_FakeV3CloseIntent(),
        result=_FakeV3CloseResult(),
        chain="base",
        success=True,
    )

    assert entry is not None
    # Driven by swap_amounts, NOT the LPCloseData (999) values.
    assert entry.amount_in == "0.5"
    assert entry.amount_out == "1500"
    assert entry.token_in == "WETH"
    assert entry.token_out == "USDC"
    assert entry.effective_price == "3000"
