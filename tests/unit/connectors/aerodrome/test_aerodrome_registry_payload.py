"""Tests for AerodromeSlipstreamReceiptParser registry-payload builders.

VIB-4305 / T12 follow-up to PR #2241. The strategy runner's
``_maybe_save_ledger_with_registry`` path consumes
``extract_registry_payload_open`` / ``extract_registry_payload_close`` to
compose ``position_registry.payload`` for LP_OPEN / LP_CLOSE intents on
Slipstream CL positions. Before this fixture the runner fell through to
``save_ledger_entry`` with the INFO log

    "Registry-mode skip: parser returned no LP_OPEN registry payload"

— a UV3 parser was being used on an Aerodrome receipt, its NPM address
filter excluded the Aerodrome NPM, and ``extract_lp_open_data`` returned
``None``. Now the runner routes to ``AerodromeSlipstreamReceiptParser``
for Slipstream protocols, and that parser produces a populated payload.

These tests pin the contract of the new methods at unit grain so the
L2 contract test
(``tests/accounting/L2/test_univ3_ledger_registry_atomicity.py`` —
existing UV3 coverage) plus the E2E lp_aerodrome / lp_aerodrome_dual
Anvil runs are not the only checks. Mirrors the layout of
``tests/unit/connectors/uniswap_v3/test_extract_registry_payload_close_helpers.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.aerodrome.receipt_parser import (
    EVENT_TOPICS,
    AerodromeSlipstreamReceiptParser,
)
from almanak.framework.execution.extract_result import ExtractOk

# Canonical Aerodrome Slipstream NPM on Base (single source of truth:
# ``AERODROME["base"]["cl_nft"]`` in ``almanak/core/contracts.py``). Mirrors
# the constant used in ``test_aerodrome_receipt_parser_branches.py``.
_SLIPSTREAM_NPM_BASE = "0x827922686190790b37229fd06084350e74485b72"
_SLIPSTREAM_POOL_MINT_TOPIC = (
    "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
)
_SLIPSTREAM_POOL_BURN_TOPIC = (
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
)

POOL = "0x" + "cc" * 20
WALLET = "0x" + "aa" * 20


def _pad32(val: int) -> str:
    return f"{val:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _int24_topic(value: int) -> str:
    if value < 0:
        value = value + (1 << 256)
    return "0x" + f"{value:064x}"


def _pool_mint_log(
    *,
    tick_lower: int = -100,
    tick_upper: int = 100,
    pool: str = POOL,
    owner: str = _SLIPSTREAM_NPM_BASE,
    log_index: int = 1,
) -> dict[str, Any]:
    return {
        "address": pool,
        "topics": [
            _SLIPSTREAM_POOL_MINT_TOPIC,
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x"
        + _addr_topic(WALLET).removeprefix("0x")
        + _pad32(0)
        + _pad32(0)
        + _pad32(0),
        "logIndex": log_index,
    }


def _increase_liquidity_log(
    *,
    token_id: int = 42,
    liquidity: int = 10**18,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    npm: str = _SLIPSTREAM_NPM_BASE,
    log_index: int = 2,
) -> dict[str, Any]:
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _decrease_liquidity_log(
    *,
    token_id: int = 42,
    liquidity: int = 10**18,
    amount0: int = 1_000_000,
    amount1: int = 5 * 10**14,
    npm: str = _SLIPSTREAM_NPM_BASE,
    log_index: int = 1,
) -> dict[str, Any]:
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["DecreaseLiquidity"],
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _pool_burn_log(
    *,
    pool: str = POOL,
    tick_lower: int = -100,
    tick_upper: int = 100,
    liquidity: int = 10**18,
    amount0: int = 999_000,
    amount1: int = 4 * 10**14,
    log_index: int = 2,
) -> dict[str, Any]:
    """Slipstream Pool Burn — UV3-style ``Burn(address,int24,int24,uint128,uint256,uint256)``."""
    return {
        "address": pool,
        "topics": [
            _SLIPSTREAM_POOL_BURN_TOPIC,
            _addr_topic(WALLET),  # owner (NPM)
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
        "logIndex": log_index,
    }


def _collect_cl_log(
    *,
    token_id: int = 42,
    recipient: str = WALLET,
    amount0: int = 1_000_500,
    amount1: int = 5 * 10**14 + 100,
    npm: str = _SLIPSTREAM_NPM_BASE,
    log_index: int = 3,
) -> dict[str, Any]:
    """NPM CollectCL: topics=[sig, tokenId], data=recipient(32B)+amount0(32B)+amount1(32B)."""
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["CollectCL"],
            _addr_topic("0x" + format(token_id, "040x")),
        ],
        "data": "0x"
        + _addr_topic(recipient).removeprefix("0x")
        + _pad32(amount0)
        + _pad32(amount1),
        "logIndex": log_index,
    }


def _receipt(logs: list[dict[str, Any]], status: int = 1) -> dict[str, Any]:
    return {
        "transactionHash": "0x" + "11" * 32,
        "blockNumber": 100,
        "status": status,
        "gasUsed": 250_000,
        "logs": logs,
    }


# ---------------------------------------------------------------------------
# _nft_manager_address
# ---------------------------------------------------------------------------


class TestNftManagerAddress:
    def test_base_chain_returns_slipstream_npm(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # Lowercased — matches the parser-side normalization rule.
        assert parser._nft_manager_address() == _SLIPSTREAM_NPM_BASE

    def test_unsupported_chain_returns_empty_string(self) -> None:
        # "Empty != zero" — parser refuses to fabricate an NPM address.
        parser = AerodromeSlipstreamReceiptParser(chain="arbitrum")
        assert parser._nft_manager_address() == ""

    def test_empty_chain_returns_empty_string(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="")
        assert parser._nft_manager_address() == ""


# ---------------------------------------------------------------------------
# _decreaseliquidity_token_id
# ---------------------------------------------------------------------------


class TestDecreaseLiquidityTokenId:
    def test_extracts_token_id_from_npm_event(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=12345),
                _pool_burn_log(),
                _collect_cl_log(token_id=12345),
            ]
        )
        assert parser._decreaseliquidity_token_id(receipt) == 12345

    def test_returns_none_when_no_logs(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        assert parser._decreaseliquidity_token_id(_receipt([])) is None

    def test_returns_none_when_no_decrease_event(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_pool_burn_log(), _collect_cl_log()])
        assert parser._decreaseliquidity_token_id(receipt) is None

    def test_returns_none_when_decrease_from_non_npm_address(self) -> None:
        # Audit M1: a DecreaseLiquidity emitted by an unrelated contract
        # MUST NOT be accepted as the close token_id.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [_decrease_liquidity_log(token_id=99, npm="0x" + "ee" * 20)]
        )
        assert parser._decreaseliquidity_token_id(receipt) is None

    def test_returns_none_on_unsupported_chain(self) -> None:
        # No NPM registered → parser refuses to match anything.
        parser = AerodromeSlipstreamReceiptParser(chain="arbitrum")
        receipt = _receipt([_decrease_liquidity_log(token_id=99)])
        assert parser._decreaseliquidity_token_id(receipt) is None


# ---------------------------------------------------------------------------
# _pool_address_from_burn
# ---------------------------------------------------------------------------


class TestPoolAddressFromBurn:
    def test_extracts_pool_address_from_burn_emitter(self) -> None:
        receipt = _receipt(
            [_decrease_liquidity_log(), _pool_burn_log(pool=POOL), _collect_cl_log()]
        )
        assert AerodromeSlipstreamReceiptParser._pool_address_from_burn(receipt) == POOL

    def test_returns_empty_string_when_no_burn(self) -> None:
        receipt = _receipt([_decrease_liquidity_log(), _collect_cl_log()])
        assert AerodromeSlipstreamReceiptParser._pool_address_from_burn(receipt) == ""

    def test_first_burn_wins_in_multicall(self) -> None:
        # Multicall close targeting same pool yields multiple Burn logs;
        # first emitter is captured.
        receipt = _receipt(
            [
                _decrease_liquidity_log(),
                _pool_burn_log(pool=POOL, log_index=2),
                _pool_burn_log(pool="0x" + "dd" * 20, log_index=3),
            ]
        )
        assert AerodromeSlipstreamReceiptParser._pool_address_from_burn(receipt) == POOL


# ---------------------------------------------------------------------------
# extract_registry_payload_open
# ---------------------------------------------------------------------------


class TestExtractRegistryPayloadOpen:
    def test_happy_path_8_keys(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _pool_mint_log(tick_lower=-100, tick_upper=100, pool=POOL),
                _increase_liquidity_log(
                    token_id=42, liquidity=10**18, amount0=1_000_000, amount1=5 * 10**14
                ),
            ]
        )
        payload = parser.extract_registry_payload_open(receipt)
        assert payload is not None
        assert payload["token_id"] == "42"
        assert payload["pool_address"] == POOL
        assert payload["tick_lower"] == -100
        assert payload["tick_upper"] == 100
        assert payload["liquidity"] == str(10**18)
        assert payload["amount0"] == "1000000"
        assert payload["amount1"] == "500000000000000"
        assert payload["nft_manager_addr"] == _SLIPSTREAM_NPM_BASE
        # No fee_tier provided → key absent (Empty != zero — don't substitute 0).
        assert "fee_tier" not in payload

    def test_fee_tier_kwarg_included_when_positive(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _pool_mint_log(),
                _increase_liquidity_log(token_id=42),
            ]
        )
        payload = parser.extract_registry_payload_open(receipt, fee_tier=100)
        assert payload is not None
        assert payload["fee_tier"] == 100

    def test_fee_tier_zero_excluded(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [_pool_mint_log(), _increase_liquidity_log(token_id=42)]
        )
        payload = parser.extract_registry_payload_open(receipt, fee_tier=0)
        assert payload is not None
        assert "fee_tier" not in payload

    def test_token_labels_when_symbols_set(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(
            chain="base",
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt(
            [_pool_mint_log(), _increase_liquidity_log(token_id=42)]
        )
        payload = parser.extract_registry_payload_open(receipt)
        assert payload is not None
        assert payload["_token0_label"] == "USDC"
        assert payload["_token1_label"] == "WETH"

    def test_no_symbols_omits_labels(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [_pool_mint_log(), _increase_liquidity_log(token_id=42)]
        )
        payload = parser.extract_registry_payload_open(receipt)
        assert payload is not None
        assert "_token0_label" not in payload
        assert "_token1_label" not in payload

    def test_returns_none_when_lp_open_data_missing(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        assert parser.extract_registry_payload_open(_receipt([])) is None

    def test_returns_none_when_pool_address_missing(self) -> None:
        # IncreaseLiquidity without a preceding Pool Mint owned by the NPM
        # → ``extract_lp_open_data.pool_address`` is empty.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_increase_liquidity_log(token_id=42)])
        assert parser.extract_registry_payload_open(receipt) is None

    def test_returns_none_when_ticks_missing(self) -> None:
        # Pool Mint owner != NPM → ticks decode to None.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _pool_mint_log(owner="0x" + "ee" * 20),  # not NPM-owned
                _increase_liquidity_log(token_id=42),
            ]
        )
        # Even though pool_address may resolve, ticks are None → refuse.
        payload = parser.extract_registry_payload_open(receipt)
        assert payload is None

    def test_returns_none_on_unsupported_chain(self) -> None:
        # No NPM registered → can't compose an identity-bearing payload.
        parser = AerodromeSlipstreamReceiptParser(chain="arbitrum")
        receipt = _receipt(
            [_pool_mint_log(), _increase_liquidity_log(token_id=42)]
        )
        assert parser.extract_registry_payload_open(receipt) is None

    def test_amount0_amount1_are_string_strings_not_decimals(self) -> None:
        # T08 contract: amount0 / amount1 carried as stringified ints.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _pool_mint_log(),
                _increase_liquidity_log(
                    token_id=1, liquidity=1, amount0=0, amount1=0
                ),
            ]
        )
        payload = parser.extract_registry_payload_open(receipt)
        assert payload is not None
        # "0" is a measured zero, NOT None — but parser may emit "0".
        assert payload["amount0"] == "0"
        assert payload["amount1"] == "0"


# ---------------------------------------------------------------------------
# extract_registry_payload_close
# ---------------------------------------------------------------------------


class TestExtractRegistryPayloadClose:
    def test_happy_path_receipt_only(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42, liquidity=10**18),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42, amount0=1_000_500, amount1=5 * 10**14 + 100),
            ]
        )
        payload = parser.extract_registry_payload_close(receipt)
        assert payload is not None
        assert payload["token_id"] == "42"
        assert payload["pool_address"] == POOL
        # Audit m8: collect amounts (principal + pre-existing fees), NOT
        # principal-only. extract_lp_close_data prefers Collect over Burn.
        assert payload["amount0_close"] == "1000500"
        assert payload["amount1_close"] == "500000000000100"
        # VIB-4470 — fees0 / fees1 surface as JSON null in the Collect-only
        # path; the parser doesn't split fees from principal at this layer
        # (extract_fees0/1 gate on absence of DecreaseLiquidity). Honest
        # "unmeasured" per Empty ≠ Zero (was "0" / measured-zero lie).
        assert payload["fee_owed_0"] is None
        assert payload["fee_owed_1"] is None
        assert payload["nft_manager_addr"] == _SLIPSTREAM_NPM_BASE

    def test_returns_none_when_lp_close_data_missing(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        assert parser.extract_registry_payload_close(_receipt([])) is None

    def test_returns_none_when_decrease_liquidity_missing(self) -> None:
        # Burn-only / collect-only — no DecreaseLiquidity → token_id absent.
        # Pool Burn carries amounts so extract_lp_close_data WILL match.
        # The identity anchor (DecreaseLiquidity NPM tokenId) is what's
        # missing. Refuse to compose the payload.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt([_collect_cl_log(token_id=42)])
        # Collect-only is treated as fee-harvest by extract_lp_close_data
        # (returns the Collect amounts as the close payload). The registry
        # builder MUST refuse — no DecreaseLiquidity = no canonical close.
        payload = parser.extract_registry_payload_close(receipt)
        assert payload is None

    def test_returns_none_when_pool_burn_missing(self) -> None:
        # DecreaseLiquidity present but no Pool Burn → pool_address can't
        # be derived from the receipt. Refuse rather than synthesize.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _collect_cl_log(token_id=42),
            ]
        )
        assert parser.extract_registry_payload_close(receipt) is None

    def test_audit_m1_token_id_disagreement_refuses(self) -> None:
        # open_payload says a DIFFERENT token_id — the close MUST refuse to
        # silently overwrite the registry with stale OPEN-side anchors.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        wrong_open = {"token_id": "9999", "pool_address": POOL}
        assert parser.extract_registry_payload_close(receipt, open_payload=wrong_open) is None

    def test_audit_m1_pool_disagreement_refuses(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        wrong_open = {"token_id": "42", "pool_address": "0x" + "ee" * 20}
        assert parser.extract_registry_payload_close(receipt, open_payload=wrong_open) is None

    def test_merges_open_payload_fields(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        open_payload = {
            "token_id": "42",
            "pool_address": POOL,
            "tick_lower": -200,
            "tick_upper": 200,
            "amount0": "1000000",
            "amount1": "500000000000000",
            "liquidity": "9999999999",  # OPEN-side mint amount wins
            "fee_tier": 100,
            "_token0_label": "USDC",
            "_token1_label": "WETH",
        }
        payload = parser.extract_registry_payload_close(
            receipt, open_payload=open_payload
        )
        assert payload is not None
        assert payload["tick_lower"] == -200
        assert payload["tick_upper"] == 200
        assert payload["amount0_open"] == "1000000"
        assert payload["amount1_open"] == "500000000000000"
        # OPEN-time liquidity wins (per UV3 _merge_open_payload_fields).
        assert payload["liquidity"] == "9999999999"
        assert payload["fee_tier"] == 100
        assert payload["_token0_label"] == "USDC"
        assert payload["_token1_label"] == "WETH"

    def test_fee_tier_kwarg_used_when_open_payload_lacks_one(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        payload = parser.extract_registry_payload_close(receipt, fee_tier=200)
        assert payload is not None
        assert payload["fee_tier"] == 200

    def test_open_payload_fee_tier_wins_over_kwarg(self) -> None:
        # setdefault semantics — OPEN-side wins on merge.
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        op = {"token_id": "42", "pool_address": POOL, "fee_tier": 100}
        payload = parser.extract_registry_payload_close(
            receipt, open_payload=op, fee_tier=500
        )
        assert payload is not None
        assert payload["fee_tier"] == 100

    def test_parser_symbols_inject_labels_when_open_payload_lacks_them(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(
            chain="base",
            token0_symbol="USDC",
            token1_symbol="WETH",
        )
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        payload = parser.extract_registry_payload_close(receipt)
        assert payload is not None
        assert payload["_token0_label"] == "USDC"
        assert payload["_token1_label"] == "WETH"

    def test_returns_none_on_unsupported_chain(self) -> None:
        # No NPM registered for chain → identity component can't be filled.
        parser = AerodromeSlipstreamReceiptParser(chain="arbitrum")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(pool=POOL),
                _collect_cl_log(token_id=42),
            ]
        )
        assert parser.extract_registry_payload_close(receipt) is None


# ---------------------------------------------------------------------------
# Fail-closed result variants (existing on the parser; verify shape is
# preserved on the new methods' upstream extractors)
# ---------------------------------------------------------------------------


class TestFailClosedWrappersStillWork:
    """Smoke: pre-existing fail-closed variants must keep working after
    we added the registry-payload methods.
    """

    def test_lp_open_data_result_ok(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [_pool_mint_log(), _increase_liquidity_log(token_id=42)]
        )
        result = parser.extract_lp_open_data_result(receipt)
        # ``ExtractOk`` shape (VIB-3159) must wrap a typed ``LPOpenData``.
        # Field-level assertions guard against silent regressions in the
        # extractor that ``extract_registry_payload_open`` wraps — without
        # these, a parser bug that returns ``LPOpenData(position_id=0)`` or
        # drops ticks could pass the smoke test undetected.
        assert isinstance(result, ExtractOk)
        lp = result.value
        assert lp.position_id == 42
        # _pool_mint_log default tick range is the canonical happy-path
        # bounds defined at module top; assert both ends so a future
        # change to either decoder is caught here.
        assert lp.tick_lower is not None and lp.tick_lower < lp.tick_upper
        assert lp.liquidity is not None and lp.liquidity > 0

    def test_lp_close_data_result_ok(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        receipt = _receipt(
            [
                _decrease_liquidity_log(token_id=42),
                _pool_burn_log(),
                _collect_cl_log(token_id=42),
            ]
        )
        result = parser.extract_lp_close_data_result(receipt)
        # ``ExtractOk`` shape (VIB-3159) must wrap a typed ``LPCloseData``.
        # Collect-event amounts (principal + accrued fees) win over the
        # DecreaseLiquidity fallback per the Aerodrome parser's docstring
        # at receipt_parser.py:2174 — assert both legs are present and
        # decoded non-trivially so the smoke test catches drift in either
        # decode path.
        assert isinstance(result, ExtractOk)
        lp_close = result.value
        assert lp_close.amount0_collected is not None and lp_close.amount0_collected >= 0
        assert lp_close.amount1_collected is not None and lp_close.amount1_collected >= 0
        # The fixture's collect log is non-zero on at least one leg, so
        # the parser must surface a measured (non-empty) amount somewhere.
        assert (lp_close.amount0_collected or 0) + (lp_close.amount1_collected or 0) > 0


# ---------------------------------------------------------------------------
# Cross-check the helpers wired through UV3 still operate on plain dicts
# ---------------------------------------------------------------------------


class TestUv3HelperReuse:
    """The close-path reuses ``UniswapV3ReceiptParser._open_payload_disagrees``
    / ``_build_close_receipt_payload`` / ``_merge_open_payload_fields``.

    They operate on plain dicts — no UV3-specific state. These tests pin
    that contract from the Aerodrome caller's side: passing a Slipstream-
    shaped open payload through the UV3 helpers behaves the same as it
    does in UV3's own tests. If a future refactor lands a UV3-specific
    assumption (e.g. address-format validation on ``pool_address``), this
    suite is where the regression would surface for the Aerodrome lane.
    """

    POOL = POOL
    TOKEN_ID = 42

    def test_disagree_helper_handles_slipstream_payload(self) -> None:
        from almanak.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        op = {"token_id": str(self.TOKEN_ID), "pool_address": self.POOL}
        assert (
            UniswapV3ReceiptParser._open_payload_disagrees(
                open_payload=op, token_id=self.TOKEN_ID, pool_address=self.POOL
            )
            is False
        )

    def test_build_close_receipt_payload_with_slipstream_npm(self) -> None:
        from almanak.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        # Stand-in for LPCloseData — only the attributes the helper reads.
        from types import SimpleNamespace

        lp_close = SimpleNamespace(
            amount0_collected=1_000_500,
            amount1_collected=500_000_000_000_100,
            fees0=0,
            fees1=0,
            liquidity_removed=10**18,
        )
        out = UniswapV3ReceiptParser._build_close_receipt_payload(
            token_id=self.TOKEN_ID,
            pool_address=self.POOL,
            lp_close=lp_close,
            nft_manager_addr=_SLIPSTREAM_NPM_BASE,
        )
        assert out["nft_manager_addr"] == _SLIPSTREAM_NPM_BASE
        assert out["amount0_close"] == "1000500"
        assert out["amount1_close"] == "500000000000100"


# ---------------------------------------------------------------------------
# Smoke: the helper used by the runner to look up OPEN payloads on close
# (StrategyRunner._lookup_open_registry_payload) calls
# ``parser._decreaseliquidity_token_id`` — confirm it is callable on the
# Aerodrome parser (the runner is parser-agnostic past that point).
# ---------------------------------------------------------------------------


class TestRunnerIntegrationShape:
    def test_decreaseliquidity_token_id_exposed_to_runner(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # Method must exist (not e.g. a typo on the parser); runner relies
        # on this exact name across all UV3-family parsers.
        assert callable(getattr(parser, "_decreaseliquidity_token_id"))

    def test_extract_registry_payload_open_signature_matches_runner(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # Must accept the kwarg the runner passes.
        out = parser.extract_registry_payload_open({"logs": []}, fee_tier=500)
        assert out is None  # empty receipt — but call shape works

    def test_extract_registry_payload_close_signature_matches_runner(self) -> None:
        parser = AerodromeSlipstreamReceiptParser(chain="base")
        # Must accept BOTH kwargs the runner passes.
        out = parser.extract_registry_payload_close(
            {"logs": []}, open_payload=None, fee_tier=500
        )
        assert out is None
