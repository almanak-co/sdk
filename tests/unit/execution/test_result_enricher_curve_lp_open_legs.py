"""VIB-3587 — Curve LP_OPEN declares its money legs via the PrimitiveMoneyLeg contract.

Curve is a multi-coin (2/3/4) LP venue with single-sided and non-leading-coin
deposits. The legacy ledger path mapped ``LPOpenData.amount0`` / ``amount1``
(the pool's FIRST TWO coins) positionally onto ``token_in`` / ``token_out``,
which is structurally wrong for Curve:

* a single-sided deposit of coin 0 left coin 1 carrying a fabricated zero leg
  (and vice-versa) — a measured-zero where the coin was simply UNFUNDED;
* a deposit of coin index 2+ (USDT in 3pool) was dropped entirely.

The connector now DECLARES one INPUT leg per FUNDED pool coin
(``CurveReceiptParser.extract_primitive_money_legs``), built from the
AddLiquidity event's pool-coin-ordered ``token_amounts`` joined to the pool's
``coin_addresses``. The connector-owned parser attribute
``CurveReceiptParser.EXTRA_EXTRACTIONS_BY_INTENT = {"LP_OPEN": ("primitive_money_legs",)}``
(merged generically by ``ResultEnricher._with_parser_extra_extractions``, keeping
the protocol name out of the framework — guarded by the chain/protocol coupling
ratchet) surfaces it under ``extracted_data["primitive_money_legs"]`` — the seam
the ledger dispatcher prefers over the legacy guesser.

This is the enricher→dispatcher integration half (the parser-unit half lives in
``tests/framework/connectors/curve/test_receipt_parser.py``). It exercises the
REAL parser + REAL ResultEnricher + REAL ledger dispatcher (no SimpleNamespace).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from almanak.connectors.curve.receipt_parser import CurveReceiptParser

# Ethereum 3pool — DAI (idx 0, 18dp) / USDC (idx 1, 6dp) / USDT (idx 2, 6dp).
_POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
_PROVIDER = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
_ADD_LIQUIDITY_3 = "0x423f6495a08fc652425cf4ed0d1f9e37e571d9b9529b1c1c23cce780b2e7df0d"


def _add_liquidity_3_log(amounts: list[int]) -> dict:
    """AddLiquidity(address,uint256[3],uint256[3],uint256,uint256) for 3pool."""
    fees = [0, 0, 0]
    invariant, supply = 1, 10**18
    data = (
        "".join(f"{a:064x}" for a in amounts)
        + "".join(f"{f:064x}" for f in fees)
        + f"{invariant:064x}{supply:064x}"
    )
    return {
        "address": _POOL,
        "topics": [_ADD_LIQUIDITY_3, f"0x000000000000000000000000{_PROVIDER[2:].lower()}"],
        "data": f"0x{data}",
        "logIndex": 1,
    }


@dataclass
class _FakeReceipt:
    tx_hash: str = "0xlpopen"
    block_number: int = 100
    gas_used: int = 200_000
    status: int = 1
    logs: list = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "block_number": self.block_number,
            "gas_used": self.gas_used,
            "status": self.status,
            "logs": self.logs,
            "contract_address": None,
        }


@dataclass
class _FakeTxResult:
    success: bool = True
    tx_hash: str = "0xlpopen"
    receipt: _FakeReceipt | None = None
    gas_used: int = 200_000


@dataclass
class _FakeExecResult:
    success: bool = True
    transaction_results: list = field(default_factory=list)
    extracted_data: dict = field(default_factory=dict)
    extraction_warnings: list = field(default_factory=list)
    # Core ExecutionResult slots the LP_OPEN enrich path reads via attribute
    # access (``_has_extracted``); default to None (= not yet extracted).
    position_id: Any = None
    swap_amounts: Any = None
    lp_open_data: Any = None
    lp_close_data: Any = None
    liquidity: Any = None
    primitive_money_legs: Any = None


@dataclass
class _FakeContext:
    chain: str = "ethereum"
    protocol: str | None = "curve"


@dataclass
class _FakeIntent:
    intent_type: str = "LP_OPEN"
    protocol: str | None = "curve"
    pool: str = "3pool"


class _PinnedRegistry:
    def __init__(self, parser: Any) -> None:
        self._parser = parser

    def get(self, protocol, chain=None, **kwargs):  # noqa: ARG002
        return self._parser


def _enrich(amounts: list[int]) -> Any:
    from almanak.framework.execution.result_enricher import ResultEnricher

    parser = CurveReceiptParser(chain="ethereum")
    enricher = ResultEnricher(parser_registry=_PinnedRegistry(parser), live_mode=False)
    result = _FakeExecResult(
        transaction_results=[_FakeTxResult(receipt=_FakeReceipt(logs=[_add_liquidity_3_log(amounts)]))]
    )
    return enricher.enrich(result, _FakeIntent(), _FakeContext())


class TestCurveLpOpenDeclaredLegs:
    def test_overlay_surfaces_single_funded_leg(self) -> None:
        """Single-sided USDC: the overlay surfaces ONE typed INPUT leg (USDC).

        The unfunded DAI/USDT coins are ABSENT (Empty != Zero) — not measured-zero
        legs.
        """
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        enriched = _enrich([0, 100_000_000, 0])  # USDC only
        legs = enriched.extracted_data.get("primitive_money_legs")
        assert isinstance(legs, PrimitiveMoneyLegs)
        assert [(leg.role.value, leg.token) for leg in legs.legs] == [("input", "USDC")]

    def test_single_sided_books_funded_coin_not_zero_leg(self) -> None:
        """End-to-end through the dispatcher: single-sided USDC books
        token_in=USDC/amount_in=100 with NO fabricated zero leg on token_out.

        This is the VIB-3587 fix: the legacy path booked amount_in='0' (the
        unfunded DAI, coin idx 0) and lost USDC's symbol.
        """
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([0, 100_000_000, 0])
        assert _extract_tokens_and_amounts(_FakeIntent(), enriched, chain="ethereum") == (
            "USDC",
            "",  # token_out empty — the other coins were unfunded, not zero
            "100",
            "",  # amount_out empty — NOT a fabricated '0'
            "",
            None,
        )

    def test_non_leading_coin_usdt_idx2_is_surfaced(self) -> None:
        """Single-sided USDT (coin idx 2) — dropped entirely by the legacy
        amount0/amount1 path — is now booked on token_in."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([0, 0, 50_000_000])  # USDT only
        token_in, token_out, amount_in, amount_out, *_ = _extract_tokens_and_amounts(
            _FakeIntent(), enriched, chain="ethereum"
        )
        assert (token_in, token_out, amount_in, amount_out) == ("USDT", "", "50", "")

    def test_two_sided_dai_usdc_lane_symmetric(self) -> None:
        """Two-sided DAI+USDC stays lane-symmetric with the legacy projection."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([10 * 10**18, 10 * 10**6, 0])
        assert _extract_tokens_and_amounts(_FakeIntent(), enriched, chain="ethereum") == (
            "DAI",
            "USDC",
            "10",
            "10",
            "",
            None,
        )

    def test_no_capability_warning_for_primitive_money_legs(self) -> None:
        """Curve parser implements ``extract_primitive_money_legs`` so the overlay
        field must not emit a SUPPORTED_EXTRACTIONS capability warning."""
        enriched = _enrich([0, 100_000_000, 0])
        offenders = [w for w in enriched.extraction_warnings if "primitive_money_legs" in w]
        assert not offenders, f"Unexpected capability warning: {offenders}"

    def test_no_intent_fallback_warn_for_curve_lp_open(self, caplog) -> None:
        """The declared-legs path drives the row, so the intent_fallback WARN /
        metric must NOT fire for a single-sided Curve LP_OPEN."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        enriched = _enrich([0, 100_000_000, 0])
        with caplog.at_level(logging.WARNING):
            _extract_tokens_and_amounts(_FakeIntent(), enriched, chain="ethereum")
        assert "intent_fallback" not in caplog.text
