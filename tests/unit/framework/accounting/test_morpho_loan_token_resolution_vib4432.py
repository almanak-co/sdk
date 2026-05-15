"""VIB-4432 / GH #2148 — Morpho Blue loan_token must come from the market
registry for SUPPLY / WITHDRAW intents, NOT from ``intent.token`` (which is
the collateral asset for those intent types).

Before the fix, ``_capture_morpho_blue_pre_state`` seeded ``loan_token_sym``
from ``intent.borrow_token or intent.token`` unconditionally. For SUPPLY /
WITHDRAW, ``borrow_token`` is ``None`` and ``intent.token`` is the
collateral — so the seed short-circuited the registry lookup in
``_resolve_morpho_market_params`` and the wrong symbol was passed downstream
to ``read_morpho_blue_account_state``, producing wrong ``debt_usd`` and
``health_factor``.

Coverage:

1. ``_derive_morpho_token_symbols`` — direct unit test of the helper for
   all 5 intent types.
2. ``_capture_morpho_blue_pre_state`` — integration with the helper:
   SUPPLY/WITHDRAW resolve ``loan_token`` from ``MORPHO_MARKETS``;
   BORROW/REPAY/DELEVERAGE behaviour unchanged.
"""

from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

from almanak.framework.accounting import lending_accounting
from almanak.framework.accounting.lending_accounting import _derive_morpho_token_symbols

# Real WBTC/USDC market on Ethereum, 86 % LLTV. ``loan_token=USDC`` (decimals=6).
_WBTC_USDC_MARKET_ID = "0x3a85e619751152991742810df6ec69ce473daef99e28a64ab2340d7b7ccfee49"
_WALLET = "0x0000000000000000000000000000000000000001"


def _make_intent(
    *,
    intent_type: str,
    token: str | None = None,
    collateral_token: str | None = None,
    borrow_token: str | None = None,
    use_as_collateral: bool = True,
    is_collateral: bool = True,
) -> MagicMock:
    intent = MagicMock(
        spec=[
            "intent_type",
            "token",
            "collateral_token",
            "borrow_token",
            "market_id",
            "protocol",
            "use_as_collateral",
            "is_collateral",
        ]
    )
    intent.intent_type.value = intent_type
    intent.token = token
    intent.collateral_token = collateral_token
    intent.borrow_token = borrow_token
    intent.market_id = _WBTC_USDC_MARKET_ID
    intent.protocol = "morpho_blue"
    intent.use_as_collateral = use_as_collateral
    intent.is_collateral = is_collateral
    return intent


# ─── 1. Helper unit tests (all 5 intent types) ───────────────────────────────


class TestDeriveMorphoTokenSymbols:
    """Direct unit tests on ``_derive_morpho_token_symbols`` covering every
    Morpho-relevant intent type."""

    def test_supply_intent_token_is_collateral(self) -> None:
        """SUPPLY: ``intent.token`` is the collateral. ``loan_token_sym`` MUST be
        ``None`` so the market registry fills it."""
        intent = _make_intent(intent_type="SUPPLY", token="WBTC")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="SUPPLY")
        assert collateral == "WBTC"
        assert loan is None  # ← the bug-fix invariant

    def test_supply_intent_respects_explicit_collateral_token(self) -> None:
        """SUPPLY: if ``intent.collateral_token`` is set, prefer it over
        ``intent.token``."""
        intent = _make_intent(intent_type="SUPPLY", token="WBTC", collateral_token="wstETH")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="SUPPLY")
        assert collateral == "wstETH"
        assert loan is None

    def test_withdraw_intent_token_is_collateral(self) -> None:
        """WITHDRAW behaves identically to SUPPLY — ``intent.token`` is the
        collateral asset being withdrawn."""
        intent = _make_intent(intent_type="WITHDRAW", token="wstETH")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="WITHDRAW")
        assert collateral == "wstETH"
        assert loan is None

    def test_borrow_intent_uses_borrow_token(self) -> None:
        """BORROW: ``intent.borrow_token`` is the loan asset."""
        intent = _make_intent(intent_type="BORROW", collateral_token="wstETH", borrow_token="USDC")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="BORROW")
        assert collateral == "wstETH"
        assert loan == "USDC"

    def test_repay_intent_falls_back_to_token(self) -> None:
        """REPAY: ``RepayIntent`` uses ``token`` (not ``borrow_token``) for the
        loan asset. The helper falls back to ``intent.token`` when
        ``borrow_token`` is unset."""
        intent = _make_intent(intent_type="REPAY", token="USDC")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="REPAY")
        assert loan == "USDC"
        assert collateral is None  # no collateral_token on RepayIntent

    def test_deleverage_intent_uses_borrow_token_then_token(self) -> None:
        """DELEVERAGE: same shape as REPAY for loan-token resolution."""
        intent = _make_intent(intent_type="DELEVERAGE", token="USDC")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="DELEVERAGE")
        assert loan == "USDC"

    def test_intent_type_case_insensitive(self) -> None:
        """Helper accepts lower-case intent type strings (defensive)."""
        intent = _make_intent(intent_type="supply", token="WBTC")
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="supply")
        assert collateral == "WBTC"
        assert loan is None

    # ─── Codex PR #2321 P1: respect use_as_collateral / is_collateral flags ──

    def test_loan_side_supply_when_use_as_collateral_false(self) -> None:
        """Codex P1 (PR #2321): ``SupplyIntent.use_as_collateral=False`` routes
        through ``morpho_adapter.supply()`` — loan-side deposit. ``intent.token``
        is the LOAN asset (not collateral). The helper must NOT seed collateral
        from intent.token in this case."""
        intent = _make_intent(
            intent_type="SUPPLY",
            token="USDC",  # loan-side supply: token is the loan asset
            use_as_collateral=False,
        )
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="SUPPLY")
        assert loan == "USDC", (
            f"Loan-side SUPPLY: intent.token should be loan_token. Got loan={loan!r}"
        )
        assert collateral is None, (
            f"Loan-side SUPPLY: collateral must stay None so registry fills it. "
            f"Got collateral={collateral!r}"
        )

    def test_loan_side_withdraw_when_is_collateral_false(self) -> None:
        """Symmetric Codex P1 case for WITHDRAW.
        ``WithdrawIntent.is_collateral=False`` is loan-side withdraw."""
        intent = _make_intent(
            intent_type="WITHDRAW",
            token="USDC",
            is_collateral=False,
        )
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="WITHDRAW")
        assert loan == "USDC"
        assert collateral is None

    def test_collateral_supply_default_use_as_collateral_true(self) -> None:
        """Sanity guard: when ``use_as_collateral`` is True (default), behaviour
        is unchanged from the original VIB-4432 fix."""
        intent = _make_intent(intent_type="SUPPLY", token="WBTC", use_as_collateral=True)
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="SUPPLY")
        assert collateral == "WBTC"
        assert loan is None

    def test_helper_tolerates_missing_use_as_collateral_attribute(self) -> None:
        """Defensive: intents without ``use_as_collateral`` (e.g. test mocks)
        default to the collateral branch — preserves backward compat for
        callers that haven't been updated."""
        intent = MagicMock(spec=["intent_type", "token", "collateral_token", "borrow_token", "market_id", "protocol"])
        intent.intent_type.value = "SUPPLY"
        intent.token = "WBTC"
        intent.collateral_token = None
        intent.borrow_token = None
        intent.market_id = _WBTC_USDC_MARKET_ID
        intent.protocol = "morpho_blue"
        # No use_as_collateral attribute on this mock at all.
        collateral, loan = _derive_morpho_token_symbols(intent=intent, intent_type_str="SUPPLY")
        assert collateral == "WBTC"
        assert loan is None


# ─── 2. Pre-state integration tests ──────────────────────────────────────────


class TestMorphoPreStateLoanTokenResolution:
    """``_capture_morpho_blue_pre_state`` integration: verify the helper is
    correctly wired and the downstream call receives the right symbols."""

    def _capture_pre_state_call(self, intent: MagicMock) -> dict:
        captured: dict = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            return None  # short-circuit; we only assert on kwargs

        with mock.patch.object(lending_accounting, "read_morpho_blue_account_state", side_effect=_capture):
            lending_accounting._capture_morpho_blue_pre_state(
                intent=intent,
                chain="ethereum",
                wallet_address=_WALLET,
                gateway_client=MagicMock(),
                price_oracle=None,
            )
        return captured

    def test_supply_resolves_loan_token_from_registry_not_intent_token(self) -> None:
        """The bug repro. SUPPLY with WBTC collateral against WBTC/USDC market
        MUST resolve ``loan_token='USDC'`` (decimals=6), not ``loan_token='WBTC'``
        (decimals=8). Before the fix this captured ``loan_token='WBTC'``."""
        intent = _make_intent(intent_type="SUPPLY", token="WBTC")
        captured = self._capture_pre_state_call(intent)

        assert captured.get("collateral_token") == "WBTC", (
            f"Expected collateral_token='WBTC', got {captured.get('collateral_token')!r}"
        )
        assert captured.get("loan_token") == "USDC", (
            f"VIB-4432 regression: expected loan_token='USDC' from MORPHO_MARKETS, got "
            f"{captured.get('loan_token')!r} — likely re-introduced the intent.token short-circuit"
        )
        assert captured.get("collateral_decimals") == 8
        assert captured.get("loan_decimals") == 6

    def test_withdraw_resolves_loan_token_from_registry(self) -> None:
        """WITHDRAW behaves identically to SUPPLY for token resolution."""
        intent = _make_intent(intent_type="WITHDRAW", token="WBTC")
        captured = self._capture_pre_state_call(intent)

        assert captured.get("collateral_token") == "WBTC"
        assert captured.get("loan_token") == "USDC"
        assert captured.get("loan_decimals") == 6

    def test_borrow_uses_intent_borrow_token_unchanged(self) -> None:
        """Regression guard for the BORROW path — must not regress while
        fixing SUPPLY/WITHDRAW. ``intent.borrow_token='USDC'`` is honoured."""
        intent = _make_intent(intent_type="BORROW", collateral_token="WBTC", borrow_token="USDC")
        captured = self._capture_pre_state_call(intent)

        assert captured.get("collateral_token") == "WBTC"
        assert captured.get("loan_token") == "USDC"

    def test_repay_uses_intent_token_as_loan(self) -> None:
        """Regression guard for REPAY. ``RepayIntent.token`` is the loan
        asset; ``borrow_token`` is unset; resolution falls back to ``token``."""
        intent = _make_intent(intent_type="REPAY", token="USDC")
        captured = self._capture_pre_state_call(intent)

        # Pre-fix and post-fix both pass — this is a regression guard, NOT a
        # bug reproducer. REPAY's intent.token IS the loan; the fix preserves
        # that fall-through.
        assert captured.get("loan_token") == "USDC"
