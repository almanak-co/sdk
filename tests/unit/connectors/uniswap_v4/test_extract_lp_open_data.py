"""Tests for ``UniswapV4ReceiptParser.extract_lp_open_data`` (VIB-4474 / T05).

Anvil-shaped mint receipt round-trip: the canonical V4 LP_OPEN receipt has

    1. ``ModifyLiquidity(pool_id, sender=PositionManager, ticks, +liquidity, salt=bytes32(tokenId))``
    2. ``Transfer(from=0x0, to=wallet, tokenId)`` emitted by the PositionManager NFT
    3. ERC-20 ``Transfer(...)`` events landing on the PoolManager (deposits)

The parser must:

- return an ``LPOpenData`` with ``pool_address`` = the 32-byte pool_id (66-char
  lowercase hex), ``position_hash`` = the canonical v4-core key, and amounts
  attributed by sorted token-address order;
- drop (return ``None`` + WARN) when ``ModifyLiquidity.sender`` is not in
  ``POSITION_MANAGER_ADDRESS_SET``;
- drop (return ``None`` + WARN) when ``salt != bytes32(tokenId)``;
- accept non-zero salt that MATCHES bytes32(tokenId) -- that is the canonical
  V4 path.
"""

from __future__ import annotations

import logging

import pytest

from almanak.core.contracts import UNISWAP_V4
from almanak.connectors.uniswap_v4.hooks import compute_position_hash
from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    UniswapV4ReceiptParser,
)

# =============================================================================
# Receipt builders -- shaped after real Anvil mint receipts
# =============================================================================


def _hex32(value: int) -> str:
    """0x-prefixed 32-byte hex from int (lowercase, padded)."""
    return "0x" + format(value, "064x")


def _hex32_int24(value: int) -> str:
    """0x-prefixed 32-byte hex from a signed int24 (two's complement)."""
    if value < 0:
        value = (1 << 256) + value
    return "0x" + format(value, "064x")


def _build_modify_liquidity_log(
    *,
    pool_id: str,
    sender: str,
    tick_lower: int,
    tick_upper: int,
    liquidity_delta: int,
    salt: str,
    pool_manager: str,
) -> dict:
    """Encode a V4 ModifyLiquidity event as the parser sees it.

    Indexed: pool_id (topic[1]), sender (topic[2] padded as address).
    Data: tickLower + tickUpper + liquidityDelta + salt (each 32 bytes).
    """
    sender_padded = "0x" + sender.lower().replace("0x", "").zfill(64)
    # Liquidity delta is int256 — handle negative for completeness.
    if liquidity_delta < 0:
        liquidity_delta_bytes = (1 << 256) + liquidity_delta
    else:
        liquidity_delta_bytes = liquidity_delta
    data = (
        "0x"
        + format(
            (1 << 256) + tick_lower if tick_lower < 0 else tick_lower,
            "064x",
        )
        + format(
            (1 << 256) + tick_upper if tick_upper < 0 else tick_upper,
            "064x",
        )
        + format(liquidity_delta_bytes, "064x")
        + salt.lower().replace("0x", "").zfill(64)
    )
    return {
        "address": pool_manager,
        "topics": [
            EVENT_TOPICS["ModifyLiquidity"],
            pool_id,
            sender_padded,
        ],
        "data": data,
    }


def _build_erc721_mint_log(
    *,
    position_manager: str,
    wallet: str,
    token_id: int,
) -> dict:
    return {
        "address": position_manager,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + "0" * 64,  # from = zero address
            "0x" + wallet.lower().replace("0x", "").zfill(64),
            "0x" + format(token_id, "064x"),
        ],
        "data": "0x",
    }


def _build_erc20_transfer_log(
    *,
    token: str,
    from_addr: str,
    to_addr: str,
    amount: int,
) -> dict:
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.lower().replace("0x", "").zfill(64),
            "0x" + to_addr.lower().replace("0x", "").zfill(64),
        ],
        "data": _hex32(amount),
    }


# =============================================================================
# Fixtures: canonical Base USDC/WETH mint at a negative-range tick band
# =============================================================================


CHAIN = "base"
BASE_PM = UNISWAP_V4[CHAIN]["position_manager"]
BASE_POOL_MANAGER = UNISWAP_V4[CHAIN]["pool_manager"]
WALLET = "0x1111111111111111111111111111111111111111"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"  # 6 decimals
WETH_BASE = "0x4200000000000000000000000000000000000006"  # 18 decimals
TOKEN_ID = 4242
POOL_ID = _hex32(0xABCD)
TICK_LOWER = -887220
TICK_UPPER = -100
LIQUIDITY_DELTA = 10**15
USDC_AMOUNT = 1_000_000_000  # 1000 USDC
WETH_AMOUNT = 5 * 10**17  # 0.5 WETH


def _canonical_mint_receipt() -> dict:
    salt = _hex32(TOKEN_ID)
    return {
        "transactionHash": "0xabcdef0123456789",
        "logs": [
            _build_modify_liquidity_log(
                pool_id=POOL_ID,
                sender=BASE_PM,
                tick_lower=TICK_LOWER,
                tick_upper=TICK_UPPER,
                liquidity_delta=LIQUIDITY_DELTA,
                salt=salt,
                pool_manager=BASE_POOL_MANAGER,
            ),
            _build_erc721_mint_log(
                position_manager=BASE_PM,
                wallet=WALLET,
                token_id=TOKEN_ID,
            ),
            _build_erc20_transfer_log(
                token=USDC_BASE,
                from_addr=WALLET,
                to_addr=BASE_POOL_MANAGER,
                amount=USDC_AMOUNT,
            ),
            _build_erc20_transfer_log(
                token=WETH_BASE,
                from_addr=WALLET,
                to_addr=BASE_POOL_MANAGER,
                amount=WETH_AMOUNT,
            ),
        ],
    }


# =============================================================================
# Tests
# =============================================================================


class TestCanonicalMint:
    def test_returns_lp_open_data(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())

        assert data is not None
        assert data.position_id == TOKEN_ID
        assert data.tick_lower == TICK_LOWER
        assert data.tick_upper == TICK_UPPER
        assert data.liquidity == LIQUIDITY_DELTA

    def test_pool_address_is_32_byte_pool_id_lowercase(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())

        assert data is not None
        assert data.pool_address.startswith("0x")
        assert len(data.pool_address) == 66
        assert data.pool_address == data.pool_address.lower()
        assert data.pool_address == POOL_ID.lower()

    def test_position_hash_matches_canonical(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())

        assert data is not None
        expected = compute_position_hash(
            owner=BASE_PM,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            salt=_hex32(TOKEN_ID),
        )
        assert data.position_hash == expected
        assert data.position_hash is not None
        assert len(data.position_hash) == 66

    def test_amounts_sorted_by_token_address(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())

        assert data is not None
        # WETH_BASE = 0x42... < USDC_BASE = 0x83... so amount0 = WETH amount
        assert int(WETH_BASE, 16) < int(USDC_BASE, 16)
        assert data.amount0 == WETH_AMOUNT
        assert data.amount1 == USDC_AMOUNT


class TestNonAllowlistedSender:
    """V0 supports PositionManager-mediated flow only. Hook/router-initiated
    mints WARN + drop until VIB-4484 lifts the constraint."""

    def test_unknown_sender_dropped_with_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender="0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )

        parser = UniswapV4ReceiptParser(chain=CHAIN)
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
            result = parser.extract_lp_open_data(receipt)

        assert result is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "non_position_manager_sender" in joined
        assert POOL_ID.lower() in joined.lower()

    def test_random_eoa_sender_dropped(self) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender="0x0000000000000000000000000000000000000001",
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        assert parser.extract_lp_open_data(receipt) is None

    def test_cross_chain_pm_is_accepted(self) -> None:
        """A receipt whose ModifyLiquidity.sender is ANY chain's V4 PositionManager
        passes the allowlist (mirrors extract_position_id fallback). The chain
        binding is by ``pool_manager`` filter on the ModifyLiquidity event
        address, not by sender."""
        eth_pm = UNISWAP_V4["ethereum"]["position_manager"]
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=eth_pm,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )
        # Position id extraction still needs the NFT mint to come from the
        # chain's PM (already satisfied by the canonical fixture); the
        # position_hash is re-derived against the ModifyLiquidity sender so
        # the cross-chain PM's address is what lands in the hash.
        #
        # CodeRabbit nit: assert deterministic acceptance (not "either
        # accepted or rejected"). Result MUST be non-None with the
        # cross-chain PM bound into the hash so a future regression that
        # silently rejects cross-chain PMs surfaces here.
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        result = parser.extract_lp_open_data(receipt)
        assert result is not None, "cross-chain PositionManager (any chain's V4 PM) MUST be accepted by the allowlist"
        assert result.position_hash == compute_position_hash(
            owner=eth_pm,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            salt=_hex32(TOKEN_ID),
        )


class TestSaltTokenIdConsistency:
    def test_mismatched_salt_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=BASE_PM,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID + 1),  # WRONG: salt != bytes32(tokenId)
            pool_manager=BASE_POOL_MANAGER,
        )

        parser = UniswapV4ReceiptParser(chain=CHAIN)
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
            result = parser.extract_lp_open_data(receipt)

        assert result is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "salt_tokenid_mismatch" in joined

    def test_zero_salt_with_nonzero_token_id_dropped(self) -> None:
        """salt=bytes32(0) but tokenId != 0 is the failure case."""
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=BASE_PM,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(0),
            pool_manager=BASE_POOL_MANAGER,
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        assert parser.extract_lp_open_data(receipt) is None

    def test_nonzero_salt_matching_token_id_accepted(self) -> None:
        """CANONICAL PATH: non-zero salt that equals bytes32(tokenId) PASSES.

        This is the v4-periphery PositionManager._mint() shape -- the test
        guards against an over-eager 'reject any non-zero salt' regression
        (explicit anti-pattern in the VIB-4474 spec)."""
        # The default fixture has tokenId = 4242 (non-zero), salt = hex(4242)
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())

        assert data is not None
        assert data.position_id == 4242
        # Confirm salt is genuinely non-zero
        assert _hex32(TOKEN_ID) != _hex32(0)


class TestBurnEventsIgnored:
    """A receipt with only a negative-liquidity ModifyLiquidity (burn) is NOT
    an LP_OPEN — returns None."""

    def test_burn_only_receipt_returns_none(self) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=BASE_PM,
            tick_lower=TICK_LOWER,
            tick_upper=TICK_UPPER,
            liquidity_delta=-LIQUIDITY_DELTA,  # burn
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        assert parser.extract_lp_open_data(receipt) is None


class TestEmptyReceipt:
    def test_empty_logs_returns_none(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        assert parser.extract_lp_open_data({"logs": []}) is None

    def test_no_modify_liquidity_returns_none(self) -> None:
        """ERC-721 mint present but no ModifyLiquidity → not an LP_OPEN."""
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        receipt = {
            "logs": [
                _build_erc721_mint_log(
                    position_manager=BASE_PM,
                    wallet=WALLET,
                    token_id=TOKEN_ID,
                ),
            ],
        }
        assert parser.extract_lp_open_data(receipt) is None


class TestMissingPositionId:
    def test_modify_liquidity_without_nft_mint_dropped(self, caplog: pytest.LogCaptureFixture) -> None:
        """ModifyLiquidity but no ERC-721 mint → drop + WARN (missing tokenId)."""
        receipt = _canonical_mint_receipt()
        # Remove the ERC-721 mint log
        receipt["logs"] = [receipt["logs"][0], receipt["logs"][2], receipt["logs"][3]]

        parser = UniswapV4ReceiptParser(chain=CHAIN)
        with caplog.at_level(logging.WARNING, logger="almanak.connectors.uniswap_v4.receipt_parser"):
            result = parser.extract_lp_open_data(receipt)

        assert result is None
        joined = " ".join(rec.message for rec in caplog.records)
        assert "missing_position_id" in joined


class TestNegativeTickBands:
    """NIT-1 spec requirement: extraction must work for negative tick ranges
    (USDC/WETH on Base usually has tickLower around -887220)."""

    def test_min_tick_lower(self) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=BASE_PM,
            tick_lower=-887272,
            tick_upper=-100,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.tick_lower == -887272
        assert data.tick_upper == -100
        assert data.position_hash == compute_position_hash(
            owner=BASE_PM,
            tick_lower=-887272,
            tick_upper=-100,
            salt=_hex32(TOKEN_ID),
        )

    def test_full_range(self) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"][0] = _build_modify_liquidity_log(
            pool_id=POOL_ID,
            sender=BASE_PM,
            tick_lower=-887272,
            tick_upper=887272,
            liquidity_delta=LIQUIDITY_DELTA,
            salt=_hex32(TOKEN_ID),
            pool_manager=BASE_POOL_MANAGER,
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.tick_lower == -887272
        assert data.tick_upper == 887272


class TestSingleTokenDeposit:
    """Sometimes only one currency is deposited (e.g. boundary tick, single-
    sided liquidity). VIB-4535: single-sided opens are resolved via the
    gateway PoolKey lookup -- symmetric with T07's close-side
    ``extract_lp_close_data``. Without a lookup callable the parser DROPS
    (mirror of close-side ``missing_pool_key_lookup`` disposition); with a
    lookup callable the missing leg is stamped as measured zero and both
    currencies are populated from the canonical PoolKey.
    """

    def test_single_token_without_lookup_drops(self) -> None:
        """VIB-4535: without ``pool_key_lookup`` injected the parser cannot
        resolve the missing currency leg and MUST drop (no warn-and-write).
        """
        receipt = _canonical_mint_receipt()
        # Drop the WETH (= currency0) transfer; only USDC lands in pool.
        # Receipt order: [0] ModifyLiquidity, [1] ERC-721 mint, [2] USDC, [3] WETH.
        receipt["logs"] = [
            receipt["logs"][0],
            receipt["logs"][1],
            receipt["logs"][2],  # USDC transfer only (WETH dropped)
        ]
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        assert parser.extract_lp_open_data(receipt) is None

    def test_no_token_transfers_amounts_both_none(self) -> None:
        receipt = _canonical_mint_receipt()
        receipt["logs"] = receipt["logs"][:2]  # drop both ERC-20 transfers
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(receipt)
        # VIB-4535: the PoolKey-lookup branch fires only when
        # ``amount0 is not None and amount1 is None``. When zero transfers
        # are observed the helper returns ``(None, None, None, None)`` and
        # the parser preserves the both-unmeasured shape (Empty != Zero).
        assert data is not None
        assert data.amount0 is None
        assert data.amount1 is None


class TestCompanionSwapTick:
    def test_current_tick_from_companion_swap(self) -> None:
        """When a Swap on the same pool fires in the same tx, current_tick
        is recovered from the Swap event's post-swap tick."""
        receipt = _canonical_mint_receipt()
        # Append a Swap event for the same pool_id with tick=12345
        swap_data = (
            "0x"
            + format(0, "064x")  # amount0
            + format(0, "064x")  # amount1
            + format(2**96, "064x")  # sqrtPriceX96
            + format(10**18, "064x")  # liquidity
            + format(12345, "064x")  # tick
            + format(3000, "064x")  # fee
        )
        receipt["logs"].append(
            {
                "address": BASE_POOL_MANAGER,
                "topics": [
                    EVENT_TOPICS["Swap"],
                    POOL_ID,
                    "0x" + "0" * 24 + BASE_PM.lower().replace("0x", ""),
                ],
                "data": swap_data,
            }
        )
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(receipt)
        assert data is not None
        assert data.current_tick == 12345

    def test_no_companion_swap_current_tick_none(self) -> None:
        parser = UniswapV4ReceiptParser(chain=CHAIN)
        data = parser.extract_lp_open_data(_canonical_mint_receipt())
        assert data is not None
        assert data.current_tick is None
