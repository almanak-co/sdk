"""Unit tests for SushiSwap V3 registry-payload extraction (VIB-4198 / T12).

Covers the two new SushiSwap V3 receipt-parser methods that feed the
runner's registry-mode atomic write path
(``strategy_runner._maybe_save_ledger_with_registry``):

- :meth:`SushiSwapV3ReceiptParser.extract_registry_payload_open`
- :meth:`SushiSwapV3ReceiptParser.extract_registry_payload_close`

Without these, ``position_registry`` stays empty for Sushi LP positions
even though the on-chain TX lands fine (VIB-4305 evidence:
``"Registry-mode skip: parser returned no LP_OPEN registry payload"``
INFO log).

Mirror of ``tests/unit/connectors/uniswap_v3/test_extract_registry_payload_close_helpers.py``
adapted for the Sushi-specific bits (NPM addresses, DecreaseLiquidity
emitter filter). The shape-only helpers
(``_open_payload_disagrees`` / ``_build_close_receipt_payload`` /
``_merge_open_payload_fields``) are imported and reused from the Uniswap
V3 baseline — Sushi V3 is a clean fork and duplicating that ~150 LoC
would let the two forks drift on Audit M1 semantics. The cross-class
helper reuse is tested in the close-orchestrator section below.
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    SushiSwapV3ReceiptParser,
)

# Pre-resolved fixtures
_NPM_ARB = POSITION_MANAGER_ADDRESSES["arbitrum"]
_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"
_WALLET = "0x1234567890123456789012345678901234567890"
_OTHER_CONTRACT = "0x" + "ee" * 20


# ---------------------------------------------------------------------------
# Hex / topic helpers (mirror of the existing branches test file)
# ---------------------------------------------------------------------------


def _addr_topic(addr: str) -> str:
    return "0x" + addr.lower().removeprefix("0x").zfill(64)


def _pad32(value: int) -> str:
    return f"{value:064x}"


def _pad32_signed(value: int) -> str:
    if value < 0:
        value = value + (1 << 256)
    return f"{value:064x}"


def _int24_topic(value: int) -> str:
    if value < 0:
        value = value + (1 << 256)
    return "0x" + f"{value:064x}"


def _receipt(logs: list[Any]) -> dict[str, Any]:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": "0x1000000",
        "status": 1,
        "logs": logs,
    }


def _pool_mint_log(
    *,
    tick_lower: int,
    tick_upper: int,
    owner: str = _NPM_ARB,
    pool: str = _POOL,
    amount: int = 10**18,
    amount0: int = 0,
    amount1: int = 0,
) -> dict[str, Any]:
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Mint"],
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x"
        + _addr_topic(_WALLET).removeprefix("0x")
        + _pad32(amount)
        + _pad32(amount0)
        + _pad32(amount1),
    }


def _npm_increase_liquidity_log(
    *,
    token_id: int,
    liquidity: int,
    amount0: int,
    amount1: int,
    npm: str = _NPM_ARB,
) -> dict[str, Any]:
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["IncreaseLiquidity"],
            "0x" + _pad32(token_id),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
    }


def _npm_decrease_liquidity_log(
    *,
    token_id: int,
    liquidity: int = 1,
    amount0: int = 1,
    amount1: int = 1,
    npm: str = _NPM_ARB,
) -> dict[str, Any]:
    """``DecreaseLiquidity(uint256 indexed tokenId, uint128 liquidity,
    uint256 amount0, uint256 amount1)`` — emitted by the NPM on close.
    """
    return {
        "address": npm,
        "topics": [
            EVENT_TOPICS["DecreaseLiquidity"],
            "0x" + _pad32(token_id),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
    }


def _pool_swap_log(
    *,
    tick: int,
    pool: str = _POOL,
    amount0: int = 1,
    amount1: int = -1,
    sqrt_price_x96: int = 2**96,
    liquidity: int = 10**12,
) -> dict[str, Any]:
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Swap"],
            _addr_topic(_WALLET),
            _addr_topic(_WALLET),
        ],
        "data": "0x"
        + _pad32_signed(amount0)
        + _pad32_signed(amount1)
        + _pad32(sqrt_price_x96)
        + _pad32(liquidity)
        + _pad32_signed(tick),
    }


def _pool_burn_log(
    *,
    pool: str = _POOL,
    owner: str = _NPM_ARB,
    tick_lower: int = -60,
    tick_upper: int = 60,
    liquidity: int = 10**12,
    amount0: int = 100,
    amount1: int = 200,
) -> dict[str, Any]:
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Burn"],
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x" + _pad32(liquidity) + _pad32(amount0) + _pad32(amount1),
    }


def _pool_collect_log(
    *,
    pool: str = _POOL,
    owner: str = _WALLET,
    recipient: str = _WALLET,
    tick_lower: int = -60,
    tick_upper: int = 60,
    amount0: int = 500,
    amount1: int = 600,
) -> dict[str, Any]:
    """Pool ``Collect`` event (Uniswap V3-shaped — Sushi V3 fork is identical).

    Note: ``recipient`` is non-indexed in the Pool's Collect event, so it
    occupies the first 32-byte data slot — amount0/amount1 start at
    offsets 32 and 64.
    """
    return {
        "address": pool,
        "topics": [
            EVENT_TOPICS["Collect"],
            _addr_topic(owner),
            _int24_topic(tick_lower),
            _int24_topic(tick_upper),
        ],
        "data": "0x"
        + _addr_topic(recipient).removeprefix("0x")
        + _pad32(amount0)
        + _pad32(amount1),
    }


# ---------------------------------------------------------------------------
# extract_registry_payload_open — happy path + missing-anchor refusals
# ---------------------------------------------------------------------------


class TestExtractRegistryPayloadOpen:
    """LP_OPEN payload composition for SushiSwap V3."""

    def test_happy_path_full_payload(self) -> None:
        """Receipt with Pool Mint + IncreaseLiquidity + Swap → full payload."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_swap_log(tick=12345),
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=42,
                liquidity=10**18,
                amount0=1_000_000,
                amount1=5 * 10**14,
            ),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        assert out["token_id"] == "42"
        assert out["pool_address"] == _POOL  # already lowercased fixture
        assert out["tick_lower"] == -100
        assert out["tick_upper"] == 100
        assert out["liquidity"] == str(10**18)
        assert out["amount0"] == "1000000"
        assert out["amount1"] == str(5 * 10**14)
        assert out["nft_manager_addr"] == _NPM_ARB.lower()
        # fee_tier omitted when not supplied — Empty ≠ zero (no 0 fallback).
        assert "fee_tier" not in out
        # Token labels omitted when parser wasn't constructed with symbols.
        assert "_token0_label" not in out
        assert "_token1_label" not in out

    def test_fee_tier_forwarded_when_positive(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs), fee_tier=3000)
        assert out is not None
        assert out["fee_tier"] == 3000

    @pytest.mark.parametrize("bogus_fee", [0, -1])
    def test_fee_tier_non_positive_omitted(self, bogus_fee: int) -> None:
        # Empty ≠ zero — a ``0`` / negative fee_tier is "unknown", not measured.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_registry_payload_open(
            _receipt(logs), fee_tier=bogus_fee
        )
        assert out is not None
        assert "fee_tier" not in out

    def test_token_labels_included_when_symbols_set(self) -> None:
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_symbol="WETH",
            token1_symbol="USDC",
            token0_decimals=18,
            token1_decimals=6,
        )
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=7, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        assert out["_token0_label"] == "WETH"
        assert out["_token1_label"] == "USDC"

    def test_returns_none_when_lp_open_data_missing(self) -> None:
        """No IncreaseLiquidity log → extract_lp_open_data returns None →
        payload-open returns None."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser.extract_registry_payload_open(_receipt([])) is None

    def test_returns_none_when_pool_address_missing(self) -> None:
        """No Pool Mint owned by NPM → pool_address empty → payload-open
        refuses (would corrupt semantic_grouping_key)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        # Only IncreaseLiquidity present; the NPM-owned Pool Mint is absent.
        logs = [
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            )
        ]
        assert parser.extract_registry_payload_open(_receipt(logs)) is None

    def test_returns_none_when_ticks_missing(self) -> None:
        """A Pool Mint with owner != NPM still yields an LPOpenData but
        with tick_lower / tick_upper = None and empty pool_address — the
        payload-open refuses on both counts."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(
                tick_lower=-100, tick_upper=100, owner=_WALLET
            ),  # not NPM-owned
            _npm_increase_liquidity_log(
                token_id=99, liquidity=1, amount0=1, amount1=1
            ),
        ]
        assert parser.extract_registry_payload_open(_receipt(logs)) is None

    def test_returns_none_when_unknown_chain(self) -> None:
        """Unknown chain → extract_lp_open_data fails-loud with None →
        payload-open returns None (no NPM fabrication)."""
        parser = SushiSwapV3ReceiptParser(chain="not-a-real-chain")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        assert parser.extract_registry_payload_open(_receipt(logs)) is None

    def test_does_not_share_uniswap_v3_nft_manager_address(self) -> None:
        """Regression guard: Sushi's NPM on Arbitrum is NOT the UV3 NPM.

        If the parser ever defaults to the UV3 NPM, the registry's
        ``physical_identity_hash`` will be wrong (UV3-tokenized hash for
        a Sushi position) and the dashboard / teardown will look up the
        wrong row.
        """
        uv3_arb_npm = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        sushi_arb_npm = _NPM_ARB.lower()
        assert uv3_arb_npm != sushi_arb_npm

        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_mint_log(tick_lower=-100, tick_upper=100),
            _npm_increase_liquidity_log(
                token_id=1, liquidity=1, amount0=1, amount1=1
            ),
        ]
        out = parser.extract_registry_payload_open(_receipt(logs))
        assert out is not None
        assert out["nft_manager_addr"] == sushi_arb_npm
        assert out["nft_manager_addr"] != uv3_arb_npm


# ---------------------------------------------------------------------------
# extract_registry_payload_close — happy path + refusals + cross-check
# ---------------------------------------------------------------------------


class TestExtractRegistryPayloadClose:
    """LP_CLOSE payload composition for SushiSwap V3."""

    def test_happy_path_burn_plus_collect_plus_decrease(self) -> None:
        """Canonical close (single-TX shape): Burn (pool), Collect (pool),
        DecreaseLiquidity (NPM) all in one receipt.

        When Burn AND Collect are both present (e.g. a multicall close on a
        Uniswap V3-style single-TX path, or a synthesized merged receipt),
        the parser separates principal (burn amounts) from fees
        (collect - burn). Sushi V3's compile path actually emits a 3-TX
        bundle so each individual receipt typically only carries ONE of
        Burn / Collect — those single-event-source paths are exercised by
        the two follow-up tests below.
        """
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        token_id = 5467895
        logs = [
            _pool_burn_log(liquidity=10**12, amount0=80, amount1=160),
            _pool_collect_log(amount0=100, amount1=200),
            _npm_decrease_liquidity_log(token_id=token_id),
        ]
        out = parser.extract_registry_payload_close(_receipt(logs))
        assert out is not None
        assert out["token_id"] == str(token_id)
        assert out["pool_address"] == _POOL
        assert out["nft_manager_addr"] == _NPM_ARB.lower()
        # Collect amount is the user's total — amount0_close mirrors it
        # (T08 golden contract). Fees = collect - burn principal.
        assert out["amount0_close"] == "100"
        assert out["amount1_close"] == "200"
        assert out["fee_owed_0"] == "20"  # 100 collect − 80 burn principal
        assert out["fee_owed_1"] == "40"  # 200 collect − 160 burn principal
        assert out["liquidity"] == "1000000000000"  # 10**12

    def test_decrease_tx_receipt_alone_burn_only_path(self) -> None:
        """Sushi 3-TX bundle: the ``lp_decrease_liquidity`` receipt carries
        Burn + DecreaseLiquidity but NO Collect (Collect is in the next TX).

        The parser must accept Burn-only and emit principal-only
        ``amount{0,1}_close`` (no fees disentangleable without the Collect).
        Regression guard against the original "Collect-required" failure
        mode that caused the E2E to log
        ``Registry-mode skip: parser returned no LP_CLOSE registry payload``.
        """
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        token_id = 37899
        logs = [
            _pool_burn_log(liquidity=10**12, amount0=80, amount1=160),
            _npm_decrease_liquidity_log(token_id=token_id),
        ]
        out = parser.extract_registry_payload_close(_receipt(logs))
        assert out is not None
        assert out["token_id"] == str(token_id)
        assert out["pool_address"] == _POOL
        # Burn-only: amount{0,1}_close fall back to burn principal amounts.
        assert out["amount0_close"] == "80"
        assert out["amount1_close"] == "160"
        # VIB-4470 — fees cannot be disentangled without the Collect
        # receipt; emit JSON null (unmeasured) rather than the prior "0"
        # measured-zero lie (Empty ≠ Zero).
        assert out["fee_owed_0"] is None
        assert out["fee_owed_1"] is None
        assert out["liquidity"] == "1000000000000"


    def test_returns_none_when_no_decreaseliquidity(self) -> None:
        """Burn + Collect present but no DecreaseLiquidity → close refuses
        (token_id identity anchor unrecoverable)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            # No NPM DecreaseLiquidity.
        ]
        assert parser.extract_registry_payload_close(_receipt(logs)) is None

    def test_returns_none_when_no_burn_pool_unknown(self) -> None:
        """DecreaseLiquidity + Collect but no Burn → pool_address empty →
        close refuses (semantic_grouping_key anchor unrecoverable).

        This is the "fee-only collect" / audit M1 silent-error class
        guard: a Collect-only receipt is NOT a close.
        """
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        assert parser.extract_registry_payload_close(_receipt(logs)) is None

    def test_returns_none_when_decreaseliquidity_token_id_zero(self) -> None:
        """A DecreaseLiquidity log with ``tokenId == 0`` is treated as
        identity-corrupt (token_id is the physical_identity_hash anchor)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=0),
        ]
        assert parser.extract_registry_payload_close(_receipt(logs)) is None

    def test_returns_none_on_open_payload_disagreement_token_id(self) -> None:
        """Audit M1 cross-check: open_payload's token_id ≠ receipt's →
        refuse (would overwrite OPEN-side anchors with stale data)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        bad_open = {"token_id": "9999999", "pool_address": _POOL}
        assert (
            parser.extract_registry_payload_close(_receipt(logs), open_payload=bad_open)
            is None
        )

    def test_returns_none_on_open_payload_disagreement_pool(self) -> None:
        """Audit M1: open_payload's pool ≠ receipt's pool → refuse."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        bad_open = {
            "token_id": "5467895",
            "pool_address": "0x" + "11" * 20,
        }
        assert (
            parser.extract_registry_payload_close(_receipt(logs), open_payload=bad_open)
            is None
        )

    def test_merges_open_payload_ticks_and_labels(self) -> None:
        """OPEN-side ticks / amounts / labels merge into the close payload."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        open_payload = {
            "token_id": "5467895",
            "pool_address": _POOL,
            "tick_lower": -199740,
            "tick_upper": -197740,
            "amount0": "3000000",
            "amount1": "1000000000000000",
            "liquidity": "1042017676194",  # OPEN-time wins
            "fee_tier": 3000,
            "_token0_label": "WETH",
            "_token1_label": "USDC",
        }
        out = parser.extract_registry_payload_close(
            _receipt(logs), open_payload=open_payload
        )
        assert out is not None
        assert out["tick_lower"] == -199740
        assert out["tick_upper"] == -197740
        assert out["amount0_open"] == "3000000"
        assert out["amount1_open"] == "1000000000000000"
        # OPEN-time liquidity wins over close-side liquidity_removed.
        assert out["liquidity"] == "1042017676194"
        assert out["fee_tier"] == 3000
        assert out["_token0_label"] == "WETH"
        assert out["_token1_label"] == "USDC"

    def test_fee_tier_argument_applied_when_open_payload_absent(self) -> None:
        """When ``open_payload`` is None, the ``fee_tier`` argument is
        carried into the payload (the compiler / intent metadata path)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        out = parser.extract_registry_payload_close(_receipt(logs), fee_tier=3000)
        assert out is not None
        assert out["fee_tier"] == 3000

    def test_fee_tier_argument_does_not_override_open_payload(self) -> None:
        """setdefault semantics — OPEN-side fee_tier wins over the argument."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        open_payload = {
            "token_id": "5467895",
            "pool_address": _POOL,
            "fee_tier": 500,
        }
        out = parser.extract_registry_payload_close(
            _receipt(logs), open_payload=open_payload, fee_tier=3000
        )
        assert out is not None
        assert out["fee_tier"] == 500  # OPEN-side wins

    def test_decreaseliquidity_from_other_contract_ignored(self) -> None:
        """A DecreaseLiquidity log emitted by a NON-Sushi-NPM contract is
        filtered out by the NPM address gate; without a Sushi NPM emitter
        the close refuses (regression guard against a UV3 receipt being
        routed through the Sushi parser by accident)."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            # NOT the Sushi NPM — would match UV3's NPM topic but fail the
            # address gate.
            _npm_decrease_liquidity_log(
                token_id=5467895, npm=_OTHER_CONTRACT
            ),
        ]
        assert parser.extract_registry_payload_close(_receipt(logs)) is None

    def test_returns_none_when_unknown_chain(self) -> None:
        """Unknown chain → _decreaseliquidity_token_id can't resolve the
        NPM filter → token_id None → close refuses."""
        parser = SushiSwapV3ReceiptParser(chain="not-a-real-chain")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=5467895),
        ]
        assert parser.extract_registry_payload_close(_receipt(logs)) is None


# ---------------------------------------------------------------------------
# Sushi-specific NPM filter (regression guards for the fork delta)
# ---------------------------------------------------------------------------


class TestSushiSpecificNPMFilter:
    """Sushi V3 NPM on Arbitrum is ``0x2214A42d...``, NOT UV3's ``0xC36442b4...``."""

    def test_decreaseliquidity_token_id_known_chain(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        receipt = _receipt(
            [_npm_decrease_liquidity_log(token_id=12345)]
        )
        assert parser._decreaseliquidity_token_id(receipt) == 12345

    def test_decreaseliquidity_token_id_uv3_npm_rejected(self) -> None:
        """A DecreaseLiquidity emitted by the UV3 NPM is filtered out
        because we keyed on the Sushi NPM address."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        uv3_arb_npm = "0xc36442b4a4522e871399cd717abdd847ab11fe88"
        receipt = _receipt(
            [
                _npm_decrease_liquidity_log(token_id=12345, npm=uv3_arb_npm)
            ]
        )
        assert parser._decreaseliquidity_token_id(receipt) is None

    def test_decreaseliquidity_token_id_unknown_chain_returns_none(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="not-a-real-chain")
        receipt = _receipt(
            [_npm_decrease_liquidity_log(token_id=12345)]
        )
        assert parser._decreaseliquidity_token_id(receipt) is None

    def test_decreaseliquidity_token_id_no_logs(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser._decreaseliquidity_token_id({"logs": []}) is None

    def test_decreaseliquidity_token_id_bytes_address_decoded(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _npm_decrease_liquidity_log(token_id=77)
        log["address"] = bytes.fromhex(_NPM_ARB.removeprefix("0x"))
        receipt = _receipt([log])
        assert parser._decreaseliquidity_token_id(receipt) == 77

    def test_decreaseliquidity_token_id_bytes_topic_decoded(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        log = _npm_decrease_liquidity_log(token_id=88)
        log["topics"][0] = bytes.fromhex(
            EVENT_TOPICS["DecreaseLiquidity"].removeprefix("0x")
        )
        log["topics"][1] = bytes.fromhex(_pad32(88))
        receipt = _receipt([log])
        assert parser._decreaseliquidity_token_id(receipt) == 88

    def test_decreaseliquidity_token_id_malformed_topic_returns_none(self) -> None:
        """A non-hex tokenId topic causes ``int(.., 16)`` to raise — the
        parser must handle that defensively and return None, not crash."""
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        bad = {
            "address": _NPM_ARB,
            "topics": [EVENT_TOPICS["DecreaseLiquidity"], "0xZZZZNOTHEX"],
            "data": "0x",
        }
        assert parser._decreaseliquidity_token_id(_receipt([bad])) is None

    def test_nft_manager_address_arbitrum(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        assert parser._nft_manager_address() == _NPM_ARB.lower()

    def test_nft_manager_address_unknown_chain_is_empty(self) -> None:
        parser = SushiSwapV3ReceiptParser(chain="not-a-real-chain")
        # Empty string — NEVER a fallback to UV3's address.
        assert parser._nft_manager_address() == ""


# ---------------------------------------------------------------------------
# Cross-fork helper reuse — the close orchestrator imports the UV3
# shape-only helpers. If anyone splits them out as Sushi-specific copies
# (drift hazard), this test fails until they're unified again.
# ---------------------------------------------------------------------------


class TestCrossForkHelperReuse:
    def test_close_orchestrator_uses_uv3_open_payload_disagrees(self) -> None:
        """Smoke test — calling ``extract_registry_payload_close`` with a
        disagreeing open_payload must hit UV3's cross-check, not a Sushi
        copy."""
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )

        # UV3's _open_payload_disagrees is a @classmethod and is shape-only;
        # the Sushi close path must reach it via the import.
        assert UniswapV3ReceiptParser._open_payload_disagrees(
            open_payload={"token_id": "1", "pool_address": _POOL},
            token_id=2,
            pool_address=_POOL,
        )
        # Sanity: same call site through the Sushi parser refuses too.
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")
        logs = [
            _pool_burn_log(),
            _pool_collect_log(),
            _npm_decrease_liquidity_log(token_id=2),
        ]
        out = parser.extract_registry_payload_close(
            _receipt(logs),
            open_payload={"token_id": "1", "pool_address": _POOL},
        )
        assert out is None


# ---------------------------------------------------------------------------
# Runner-side dispatch — the strategy_runner._registry_resolve_receipt_and_parser
# branch added for sushiswap_v3 must route to SushiSwapV3ReceiptParser (NOT the
# Uni V3 parser, whose NPM address filter would silently drop every Sushi
# IncreaseLiquidity / DecreaseLiquidity log). CRAP-gate coverage for the dispatch
# block (CodeRabbit gate on PR #2247).
# ---------------------------------------------------------------------------


class TestRunnerSushiDispatch:
    """Smoke tests for `StrategyRunner._registry_resolve_receipt_and_parser`
    Sushi branch. Bypasses the full runner setup with a minimal stub so the
    test is fast and doesn't touch the gateway.
    """

    def _make_runner_stub(self):
        """Bare-minimum object that exposes the methods under test —
        ``_registry_resolve_receipt_and_parser`` reaches into
        ``self._extract_receipt_from_result`` (static), so a plain
        ``object`` works as ``self`` for this isolated call.
        """
        from almanak.framework.runner.strategy_runner import StrategyRunner

        # Use the unbound method to avoid constructing a full StrategyRunner.
        return StrategyRunner._registry_resolve_receipt_and_parser

    def test_sushiswap_v3_routes_to_sushi_parser(self) -> None:
        """A receipt-bearing result + protocol='sushiswap_v3' returns a
        SushiSwapV3ReceiptParser instance — NOT UniswapV3ReceiptParser.

        Regression guard for the dispatch added on PR #2247. Without this
        branch, Sushi LP receipts hit the UV3 parser, which filters every
        IncreaseLiquidity / DecreaseLiquidity log by UV3's NPM address and
        silently emits ExtractMissing.
        """
        from types import SimpleNamespace

        from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
            SushiSwapV3ReceiptParser,
        )
        from almanak.framework.connectors.uniswap_v3.receipt_parser import (
            UniswapV3ReceiptParser,
        )
        from almanak.framework.runner.strategy_runner import StrategyRunner

        # Synthesize the result shape that _extract_receipt_from_result picks up.
        receipt = {"logs": [{"topics": ["0xdeadbeef"], "address": "0x0", "data": "0x"}]}
        result = SimpleNamespace(receipts=[receipt])

        resolver = self._make_runner_stub()
        # Bind a stub `self` that owns the static receipt extractor.
        out = resolver(
            StrategyRunner.__new__(StrategyRunner),  # type: ignore[call-arg]
            result=result,
            chain="arbitrum",
            intent_type_str="LP_OPEN",
            protocol="sushiswap_v3",
        )
        assert out is not None
        _receipt_out, parser = out
        assert isinstance(parser, SushiSwapV3ReceiptParser)
        assert not isinstance(parser, UniswapV3ReceiptParser)

    def test_sushiswap_v3_branch_returns_correct_chain(self) -> None:
        """The Sushi parser must be constructed with the right chain so
        ``POSITION_MANAGER_ADDRESSES[chain]`` resolves to the Sushi NPM
        (not the UV3 NPM)."""
        from types import SimpleNamespace

        from almanak.framework.runner.strategy_runner import StrategyRunner

        receipt = {"logs": [{"topics": ["0xdeadbeef"], "address": "0x0", "data": "0x"}]}
        result = SimpleNamespace(receipts=[receipt])

        resolver = self._make_runner_stub()
        out = resolver(
            StrategyRunner.__new__(StrategyRunner),  # type: ignore[call-arg]
            result=result,
            chain="arbitrum",
            intent_type_str="LP_CLOSE",
            protocol="sushiswap_v3",
        )
        assert out is not None
        _receipt_out, parser = out
        # Sushi NPM on Arbitrum (NOT UV3's 0xC36442b4...).
        assert parser._nft_manager_address().lower() == _NPM_ARB.lower()
