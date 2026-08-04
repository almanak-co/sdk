"""VIB-6383 — TraderJoe Liquidity Book LP_CLOSE booked the token pair FLIPPED and
each leg scaled with the other token's decimals ($228bn on a $2.80 position).

Mechanism (verified against the real run, not assumed)
------------------------------------------------------
The LB receipt parser emits ``amount0``/``amount1`` in the pool's own
``getTokenX()``/``getTokenY()`` order and says so explicitly (``_lbpair_transfers``:
"Emission order is preserved (NOT sorted by token address)"). The accounting
handler then re-pairs the labels:

* ``_v4_realign_token_pair`` runs first, but no-ops because LB never populated
  ``currency0``/``currency1``;
* ``_v3_realign_token_pair`` therefore falls through to
  ``realign_token_pair_by_address`` — an **address sort**, which is a
  V3-family property (VIB-5851 / VIB-5983). LB ``tokenX``/``tokenY`` are fixed at
  pool creation and are NOT address-sorted.

On ``WAVAX/USDT/20`` the two orders disagree — USDT ``0x9702…`` sorts BELOW WAVAX
``0xB31f…`` while the pool's ``tokenX`` is WAVAX — so the labels flip while the
amounts stay in pool order, and ``_resolve_lp_amounts`` scales each raw int with
its counterpart's decimals.

Why the sibling lanes were right (lane asymmetry, not a global defect)
---------------------------------------------------------------------
* **LP_OPEN** — ``_v3_realign_token_pair`` short-circuits on declared
  ``primitive_money_legs``; that gate is ``LP_OPEN``-only, so LP_CLOSE fell through.
* **``position_events``** — its VIB-5988 declared-money-legs branch fires for BOTH
  lanes and returns before the address sort.

So the flip is isolated to the accounting LP_CLOSE writer, exactly as the ticket
says. The fix binds the pair to receipt truth at the **parser**: stamp
``currency0``/``currency1`` from the same Transfer legs the amounts are read from,
which re-uses the existing V4 branch-out (``_v4_realign_token_pair``) instead of
adding another special case in the handler.

Ground truth — batch ``20260803-0430-noneth8``, leg ``traderjoe-lp-avax``
------------------------------------------------------------------------
Mainnet Avalanche, pool ``0x87EB2F90d7D0034571f343fb7429AE22C1Bd9F72``
(``getTokenX()`` = WAVAX, ``getTokenY()`` = USDT — re-read on-chain 2026-08-03 at
block 91930573). From ``transaction_ledger.extracted_data_json``:

    lp_close_data.amount0_collected = 228032393198215910   (WAVAX, 18 dec)
    lp_close_data.amount1_collected = 1334492              (USDT, 6 dec)
    currency0 = null, currency1 = null                     <- the gap
    primitive_money_legs = [OUT WAVAX 0.22803239319821591, OUT USDT 1.334492]

The declared legs are the independent chain-truth witness: the parser builds each
one from that transfer's OWN emitting contract address, so they are not derived
from the pair labels under test. The observed corrupt ``accounting_events`` row was

    token0 = USDT, amount0 = 228032393198.21591      (= 228032393198215910 / 1e6)
    token1 = WAVAX, amount1 = 1.334492E-12           (= 1334492 / 1e18)
    realized_pnl_usd = 228032393195.4188476195085979

THE TOLERANCE TRAP (VIB-6383, VIB-6399)
---------------------------------------
The Accountant Test's epsilon scales off ``notional_traded``, which this bug
inflated to $228bn, giving ``eps_threshold = $570,080,983`` and ``eps_vacuous:
True``. **The bug manufactured the tolerance that would have caught it.** Every
assertion below is therefore an absolute band around receipt truth, and none of
them compares LP_CLOSE against LP_OPEN — the ticket states explicitly that both
sides can be wrong identically.

These tests FAIL on unmodified ``main`` (mutation-checked: run this file at
``6abb60095f`` and ``test_lb_close_*_pool_order_differs_from_address_order``
reports the ~$228bn payload) and PASS with the parser fix.
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors.traderjoe_v2.receipt_parser import (
    EVENT_TOPICS,
    TraderJoeV2ReceiptParser,
)
from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.observability.ledger import serialize_extracted_data

# ── Real mainnet identities (Avalanche C-Chain) ──────────────────────────────
# Using the REAL addresses matters: the defect only manifests when the pool's
# tokenX/tokenY order disagrees with the numeric address order, and that
# disagreement is a fact about these specific contracts.
WAVAX = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"  # 18 dec
USDT = "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7"  # 6 dec  -> sorts BELOW WAVAX
USDC = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"  # 6 dec  -> sorts ABOVE WAVAX

LBPAIR_WAVAX_USDT = "0x87EB2F90d7D0034571f343fb7429AE22C1Bd9F72"
LBPAIR_WAVAX_USDC = "0xD446eb1660F766d533BeCeEf890Df7A69d26f7d1"

WALLET = "0xD739d9ecF38190F1EbFa537D955229Da8872d6f5"

# Raw withdrawal amounts from the real LP_CLOSE receipt.
WAVAX_RAW = 228_032_393_198_215_910  # 0.22803239319821591 WAVAX
USDT_RAW = 1_334_492  # 1.334492 USDT

WAVAX_HUMAN = Decimal("0.22803239319821591")
STABLE_HUMAN = Decimal("1.334492")

WAVAX_PRICE = Decimal("6.44281683")  # price_inputs_json of the real close row

# Receipt truth: 0.22803239319821591 * 6.44281683 + 1.334492 = $2.80.
EXPECTED_BASIS = float(WAVAX_HUMAN * WAVAX_PRICE + STABLE_HUMAN)
BASIS_TOLERANCE = 0.01

_DECIMALS = {WAVAX.lower(): ("WAVAX", 18), USDT.lower(): ("USDT", 6), USDC.lower(): ("USDC", 6)}


def _topic_addr(addr: str) -> str:
    return "0x" + "00" * 12 + addr[2:].lower()


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _bins_data(bin_ids: list[int]) -> str:
    """``WithdrawnFromBins`` data layout: [ids_off][amounts_off][ids_len][ids…][amounts_len]."""
    amounts_offset = 0x40 + 32 + len(bin_ids) * 32
    return (
        "0x"
        + _uint256_hex(0x40)
        + _uint256_hex(amounts_offset)
        + _uint256_hex(len(bin_ids))
        + "".join(_uint256_hex(b) for b in bin_ids)
        + _uint256_hex(0)
    )


def _make_log(topic0: str, contract: str, topics: list[str], data: str) -> dict:
    return {"topics": [topic0, *topics], "address": contract, "data": data, "logIndex": 0}


def _lb_close_receipt(pool: str, token_x: str, x_raw: int, token_y: str, y_raw: int) -> dict:
    """A TraderJoe LB LP_CLOSE receipt: ``WithdrawnFromBins`` + the two
    LBPair -> wallet principal ``Transfer`` legs, emitted tokenX then tokenY
    (the LBRouter's real emission order).
    """
    return {
        "status": 1,
        "transactionHash": "0x" + "d1" * 32,
        "blockNumber": 91_913_113,
        "gasUsed": 319_048,
        "logs": [
            _make_log(
                EVENT_TOPICS["WithdrawnFromBins"],
                pool,
                [_topic_addr(WALLET), _topic_addr(WALLET)],
                _bins_data([8375706, 8375707, 8375708]),
            ),
            _make_log(
                EVENT_TOPICS["Transfer"],
                token_x,
                [_topic_addr(pool), _topic_addr(WALLET)],
                "0x" + _uint256_hex(x_raw),
            ),
            _make_log(
                EVENT_TOPICS["Transfer"],
                token_y,
                [_topic_addr(pool), _topic_addr(WALLET)],
                "0x" + _uint256_hex(y_raw),
            ),
        ],
    }


class _Resolver:
    """Resolve by address OR by symbol, returning address + decimals for both.

    Three call sites need this one object: ``_resolve_lp_amounts`` (symbol ->
    decimals), ``_v3_realign_token_pair`` -> ``realign_token_pair_by_address``
    (symbol -> address, ``skip_gateway=True``), and ``_v4_realign_token_pair``
    (address -> symbol).
    """

    def resolve(self, key, **_kwargs):  # noqa: ANN001, ANN003
        k = str(key).lower()
        if k in _DECIMALS:
            sym, dec = _DECIMALS[k]
            return SimpleNamespace(symbol=sym, address=key, decimals=dec)
        for addr, (sym, dec) in _DECIMALS.items():
            if sym == str(key).upper():
                return SimpleNamespace(symbol=sym, address=addr, decimals=dec)
        return None


@pytest.fixture
def _patched_resolver(monkeypatch):
    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Resolver(),
    )
    monkeypatch.setattr(
        "almanak.connectors.traderjoe_v2.receipt_parser.get_token_resolver",
        lambda: _Resolver(),
    )


def _close_rows(pool: str, lp_close, token_x_symbol: str, token_y_symbol: str) -> tuple[dict, dict]:
    """Build the (outbox, ledger) pair the way the real run wrote them.

    ``token_in``/``token_out`` carry the pool label order (WAVAX, USDT) because
    the LP_CLOSE ledger row is projected from the connector's declared money
    legs — verified against ``transaction_ledger`` in the 20260803 bundle.
    """
    ledger = {
        "id": "le-close",
        "deployment_id": "deployment:5b0f09ed6ccd",
        "cycle_id": "c1",
        "intent_type": "LP_CLOSE",
        "protocol": "traderjoe_v2",
        "chain": "avalanche",
        "execution_mode": "paper",
        "tx_hash": "0x" + "d1" * 32,
        "token_in": token_x_symbol,
        "token_out": token_y_symbol,
        "amount_in": str(WAVAX_HUMAN),
        "amount_out": str(STABLE_HUMAN),
        "timestamp": "2026-08-03T09:59:39+00:00",
        "extracted_data_json": serialize_extracted_data({"lp_close_data": lp_close}),
        "price_inputs_json": json.dumps(
            {token_x_symbol: str(WAVAX_PRICE), token_y_symbol: "1.00"},
        ),
    }
    outbox = {
        "outbox_id": "ob-close",
        "deployment_id": "deployment:5b0f09ed6ccd",
        "cycle_id": "c1",
        "position_key": (
            f"lp:traderjoe_v2:avalanche:{WALLET.lower()}:"
            f"{token_x_symbol.lower()}/{token_y_symbol.lower()}/20"
        ),
        "wallet_address": WALLET,
    }
    return outbox, ledger


class TestLBParserBindsPairToReceiptTruth:
    """Layer 1 — the parser must declare WHICH token each amount belongs to."""

    def test_close_stamps_currency_pair_from_the_transfer_legs(self, _patched_resolver) -> None:
        """``currency0``/``currency1`` must name the contracts that emitted the
        legs ``amount0_collected``/``amount1_collected`` were read from.

        Without them the handler has no chain-truth signal for LB pair order and
        falls through to the V3 address sort. RED on unmodified main (both None).
        """
        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        receipt = _lb_close_receipt(LBPAIR_WAVAX_USDT, WAVAX, WAVAX_RAW, USDT, USDT_RAW)

        data = parser.extract_lp_close_data(receipt)

        assert data is not None
        assert int(data.amount0_collected) == WAVAX_RAW
        assert int(data.amount1_collected) == USDT_RAW
        assert (data.currency0 or "").lower() == WAVAX.lower(), (
            "currency0 must be the contract that emitted the amount0 leg"
        )
        assert (data.currency1 or "").lower() == USDT.lower(), (
            "currency1 must be the contract that emitted the amount1 leg"
        )

    def test_open_stamps_currency_pair_from_the_transfer_legs(self, _patched_resolver) -> None:
        """Lane symmetry — the OPEN side carries the same binding.

        LP_OPEN is currently correct only because a *different* gate (declared
        money legs) happens to fire first. Binding it here too means the pair is
        right for the reason it should be right, not by luck of gate ordering.
        """
        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "2c" * 32,
            "blockNumber": 91_894_828,
            "gasUsed": 500_000,
            "logs": [
                _make_log(
                    EVENT_TOPICS["DepositedToBins"],
                    LBPAIR_WAVAX_USDT,
                    [_topic_addr(WALLET), _topic_addr(WALLET)],
                    _bins_data([8375706, 8375707]),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    WAVAX,
                    [_topic_addr(WALLET), _topic_addr(LBPAIR_WAVAX_USDT)],
                    "0x" + _uint256_hex(228_000_000_000_000_000),
                ),
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    USDT,
                    [_topic_addr(WALLET), _topic_addr(LBPAIR_WAVAX_USDT)],
                    "0x" + _uint256_hex(1_330_000),
                ),
            ],
        }

        data = parser.extract_lp_open_data(receipt)

        assert data is not None
        assert (data.currency0 or "").lower() == WAVAX.lower()
        assert (data.currency1 or "").lower() == USDT.lower()


class TestLBCloseAccountingPayload:
    """Layer 2 — end-to-end: the real parser feeds the real handler.

    The ``LPCloseData`` under test is produced by ``TraderJoeV2ReceiptParser``,
    never hand-written, so a fix that greens only a hand-rolled stand-in cannot
    pass this.
    """

    @staticmethod
    def _payload(pool: str, token_y: str, token_y_symbol: str, _patched):  # noqa: ANN205
        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        receipt = _lb_close_receipt(pool, WAVAX, WAVAX_RAW, token_y, USDT_RAW)
        lp_close = parser.extract_lp_close_data(receipt)
        assert lp_close is not None
        outbox, ledger = _close_rows(pool, lp_close, "WAVAX", token_y_symbol)
        return handle_lp(outbox, ledger)

    def test_lb_close_when_pool_order_differs_from_address_order(self, _patched_resolver) -> None:
        """THE REGRESSION. ``WAVAX/USDT/20``: tokenX = WAVAX but USDT is the lower
        address, so the V3 sort flips the labels onto pool-ordered amounts.

        RED on unmodified main: token0 = USDT, amount0 = 228032393198.21591.
        """
        event = self._payload(LBPAIR_WAVAX_USDT, USDT, "USDT", _patched_resolver)

        assert event is not None
        assert event.token0 == "WAVAX", (
            f"token0 must be the pool's tokenX (WAVAX), got {event.token0!r} — "
            "labels flipped onto pool-ordered amounts"
        )
        assert event.token1 == "USDT"
        assert event.amount0 == pytest.approx(WAVAX_HUMAN, abs=Decimal("1e-15")), (
            f"amount0 {event.amount0} != receipt truth {WAVAX_HUMAN} — "
            "WAVAX's 18-dec raw was scaled with USDT's 6 decimals"
        )
        assert event.amount1 == pytest.approx(STABLE_HUMAN, abs=Decimal("1e-9"))

    def test_lb_close_cost_basis_stays_within_a_dollar_of_receipt_truth(
        self, _patched_resolver
    ) -> None:
        """An ABSOLUTE band, not an epsilon derived from the payload.

        A tolerance computed from ``notional_traded`` is manufactured by the very
        corruption it is meant to bound (VIB-6383 / VIB-6399): the observed run
        booked $228bn and still scored green at ``eps_threshold = $570,080,983``.
        """
        event = self._payload(LBPAIR_WAVAX_USDT, USDT, "USDT", _patched_resolver)

        assert event is not None
        assert event.cost_basis_usd is not None, "basis must be measured, not None"
        basis = float(event.cost_basis_usd)
        assert abs(basis - EXPECTED_BASIS) < BASIS_TOLERANCE, (
            f"cost_basis_usd {basis} != receipt truth ~${EXPECTED_BASIS:.4f} "
            "(the observed defect booked $228,032,393,198.22)"
        )

    def test_lb_close_when_pool_order_matches_address_order_is_unchanged(
        self, _patched_resolver
    ) -> None:
        """MUST-NOT-CHANGE control. ``WAVAX/USDC/20``: tokenX = WAVAX AND WAVAX is
        the lower address, so the V3 sort was already a no-op here.

        Pairing this with the flip case is what makes the pair falsifiable — a
        "fix" that simply disabled all realignment would pass the case above and
        would still have to pass this one. GREEN both before and after.
        """
        event = self._payload(LBPAIR_WAVAX_USDC, USDC, "USDC", _patched_resolver)

        assert event is not None
        assert event.token0 == "WAVAX"
        assert event.token1 == "USDC"
        assert event.amount0 == pytest.approx(WAVAX_HUMAN, abs=Decimal("1e-15"))
        assert event.amount1 == pytest.approx(STABLE_HUMAN, abs=Decimal("1e-9"))
        assert event.cost_basis_usd is not None
        assert abs(float(event.cost_basis_usd) - EXPECTED_BASIS) < BASIS_TOLERANCE


class TestLBCloseSingleLegPartialObservation:
    """VIB-6383 residual — a SINGLE-SIDED close must not fall back to the address sort.

    Raised as M1 by Grok on the #3578 high-risk panel, IN-SCOPE under clauses (1)
    and (3), and confirmed in code before being acted on.

    A Liquidity Book position whose price has moved out of range holds 100% of one
    token, so its close emits exactly ONE LBPair -> wallet Transfer. The parser then
    stamps ``currency0=<WAVAX>, currency1=None`` — an honest partial observation
    (Empty != Zero: the second leg was not observed, so it is ``None``, not ``""``).

    But the handler's two realign gates both key on BOTH currencies being present:

      * ``_v4_realign_token_pair``  -> ``if not c0 or not c1: return``  (no-ops)
      * ``_v3_realign_token_pair``  -> ``if currency0 and currency1: return``
                                       (does NOT short-circuit)

    so the V3 ADDRESS SORT still runs on a Liquidity Book pair — reproducing exactly
    the defect this PR exists to prevent, on the single-sided path.

    This is why the PR's claim that identity is "bound so the flip cannot happen" was
    too strong before the fix: it held for two-leg closes only.
    """

    def test_single_leg_close_does_not_reach_the_address_sort(self, _patched_resolver) -> None:
        """One WAVAX-only withdrawal on a pool whose order is the INVERSE of address
        order. Was XFAIL(strict) as the executable record of VIB-6471; FIXED by the
        positional placement this test now pins.

        The defect: a single-sided LB close stamps one currency, and both realign gates
        keyed on BOTH being present, so the V3 address sort ran on a Liquidity Book pair
        and reproduced the ``228032393198.21591`` signature. Relaxing that gate to ``or``
        was tried and reverted — it reopened the identical defect for the whole V3 family,
        which stamps a partial pair on every ordinary out-of-range single-sided close.

        The fix is neither polarity of that boolean. ``lp_leg_identity.identity_is_complete``
        distinguishes "slot moved nothing, identity moot" from "identity undeterminable",
        so a partial-but-trustworthy observation is placed POSITIONALLY by
        ``place_token_pair_by_observed_identity`` — matching addresses, never sorting them,
        which is why it holds for LB and Curve as well as the address-sorted families.
        """
        parser = TraderJoeV2ReceiptParser(chain="avalanche")
        receipt = {
            "status": 1,
            "transactionHash": "0x" + "5e" * 32,
            "blockNumber": 91_913_200,
            "gasUsed": 300_000,
            "logs": [
                _make_log(
                    EVENT_TOPICS["WithdrawnFromBins"],
                    LBPAIR_WAVAX_USDT,
                    [_topic_addr(WALLET), _topic_addr(WALLET)],
                    _bins_data([8375706]),
                ),
                # ONE leg only — the out-of-range single-sided case.
                _make_log(
                    EVENT_TOPICS["Transfer"],
                    WAVAX,
                    [_topic_addr(LBPAIR_WAVAX_USDT), _topic_addr(WALLET)],
                    "0x" + _uint256_hex(WAVAX_RAW),
                ),
            ],
        }

        lp_close = parser.extract_lp_close_data(receipt)
        assert lp_close is not None
        assert (lp_close.currency0 or "").lower() == WAVAX.lower()
        assert lp_close.currency1 is None, (
            "an unobserved second leg must be None (unmeasured), not a fabricated address"
        )

        outbox, ledger = _close_rows(LBPAIR_WAVAX_USDT, lp_close, "WAVAX", "USDT")
        event = handle_lp(outbox, ledger)

        assert event is not None
        assert event.token0 == "WAVAX", (
            f"token0 is {event.token0!r} — a PARTIAL currency stamp let the V3 address "
            "sort flip a Liquidity Book pair, the same class as the $228bn defect"
        )
        assert event.token1 == "USDT"
        assert event.amount0 == pytest.approx(WAVAX_HUMAN, abs=Decimal("1e-15")), (
            f"amount0 {event.amount0} != {WAVAX_HUMAN} — WAVAX's 18-dec raw was scaled "
            "with USDT's 6 decimals"
        )
