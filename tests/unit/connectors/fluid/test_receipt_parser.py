"""Unit tests for FluidReceiptParser swap extraction (synthetic receipts).

Token identification must be SENDER-INDEPENDENT: under Zodiac Safe execution
``receipt.from`` is the relayer EOA while the ERC-20 transfer legs involve
the Safe, so a wallet-keyed heuristic misses both legs. Fluid custodies all
pool funds in the central Liquidity layer (Phase-0 report §V1, VIB-5028:
input leg = payer → Liquidity, output leg = Liquidity → recipient), so the
parser matches Transfer counterparties against the deterministic Liquidity
address first and the receipt sender only as a secondary signal.

Decimals are seeded into the parser's cache so the tests stay hermetic (no
token-resolver / network dependency). The real-fork behaviour is covered by
``tests/intents/*/test_fluid_swap.py``.
"""

from decimal import Decimal
from unittest.mock import patch

from almanak.connectors.fluid.receipt_parser import (
    _FLUID_NATIVE_SENTINEL,
    DEPOSIT_4626_TOPIC,
    ERC721_TRANSFER_TOPIC,
    SWAP_TOPIC,
    WITHDRAW_4626_TOPIC,
    FluidReceiptParser,
)

# Real arbitrum addresses (parser compares lowercased strings only).
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
POOL = "0x1234567890AbcdEF1234567890aBcdef12345678"
LIQUIDITY = "0x52Aa899454998Be5b000Ad077a46Bbe360F4e497"  # Fluid Liquidity layer
SAFE = "0xAAAAaaaAAaAaAAaaAAAAaAAAaaaaAAaAAAAAaaaA"
RELAYER = "0xBBbBBBbbbBBBbbbbBbBbbbbBBbBbbbbBbBbbbbBB"

AMOUNT_IN = 50_000_000  # 50 USDC (6 dp)
AMOUNT_OUT = 49_975_000  # 49.975 USDT (6 dp)


def _word(value: int) -> str:
    return f"{value:064x}"


def _addr_topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr[2:].lower()


def _swap_log(to: str, swap0to1: bool = True, amount_in: int = AMOUNT_IN, amount_out: int = AMOUNT_OUT) -> dict:
    data = "0x" + _word(int(swap0to1)) + _word(amount_in) + _word(amount_out) + "0" * 24 + to[2:].lower()
    return {"address": POOL, "topics": [SWAP_TOPIC], "data": data}


def _transfer_log(token: str, from_addr: str, to_addr: str, amount: int) -> dict:
    return {
        "address": token,
        "topics": [ERC721_TRANSFER_TOPIC, _addr_topic(from_addr), _addr_topic(to_addr)],
        "data": "0x" + _word(amount),
    }


def _receipt(sender: str, logs: list[dict]) -> dict:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 1_000_000,
        "status": 1,
        "from": sender,
        "logs": logs,
    }


def _parser() -> FluidReceiptParser:
    parser = FluidReceiptParser(chain="arbitrum")
    parser._decimals_cache[f"arbitrum:{USDC.lower()}"] = 6
    parser._decimals_cache[f"arbitrum:{USDT.lower()}"] = 6
    return parser


class TestSwapAmountsEoaExecution:
    def test_both_legs_resolved(self):
        receipt = _receipt(
            SAFE,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in == AMOUNT_IN
        assert amounts.amount_out == AMOUNT_OUT
        assert amounts.amount_in_decimal == Decimal("50")
        assert amounts.amount_out_decimal == Decimal("49.975")


class TestSwapAmountsSafeExecution:
    """receipt.from is the relayer EOA; transfers involve the Safe only."""

    def test_both_legs_resolved_sender_independent(self):
        receipt = _receipt(
            RELAYER,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None, "Safe execution must not break token identification"
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in == AMOUNT_IN
        assert amounts.amount_out == AMOUNT_OUT

    def test_native_out_falls_back_to_sentinel(self):
        # Native output leg produces no ERC-20 Transfer; pools are strictly
        # per-pair so the missing leg is guaranteed native — and the fallback
        # must not depend on swap.to matching receipt.from.
        receipt = _receipt(
            RELAYER,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE, amount_out=10**16),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in.lower() == USDC.lower()
        assert amounts.token_out == _FLUID_NATIVE_SENTINEL
        assert amounts.amount_out_decimal == Decimal("0.01")  # 18 dp native

    def test_native_in_falls_back_to_sentinel(self):
        receipt = _receipt(
            RELAYER,
            [
                _swap_log(to=SAFE, amount_in=10**16),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        amounts = _parser().extract_swap_amounts(receipt)
        assert amounts is not None
        assert amounts.token_in == _FLUID_NATIVE_SENTINEL
        assert amounts.token_out.lower() == USDT.lower()
        assert amounts.amount_in_decimal == Decimal("0.01")


class TestSwapAmountsFailClosed:
    def test_no_swap_event_returns_none(self):
        receipt = _receipt(SAFE, [_transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN)])
        assert _parser().extract_swap_amounts(receipt) is None

    def test_unresolvable_decimals_returns_none(self):
        # Both legs identified but decimals unknown -> None (fail-closed),
        # never raw-wei amounts dressed as decimals.
        receipt = _receipt(
            SAFE,
            [
                _transfer_log(USDC, SAFE, LIQUIDITY, AMOUNT_IN),
                _swap_log(to=SAFE),
                _transfer_log(USDT, LIQUIDITY, SAFE, AMOUNT_OUT),
            ],
        )
        parser = FluidReceiptParser(chain="arbitrum")  # cache NOT seeded
        with patch("almanak.framework.data.tokens.get_token_resolver", side_effect=RuntimeError("offline")):
            assert parser.extract_swap_amounts(receipt) is None


# =============================================================================
# fToken lending (ERC-4626) — VIB-5030
# =============================================================================

FTOKEN = "0xf42f5795D9ac7e9D757dB633D693cD548Cfd9169"  # base fUSDC
SUPPLY_ASSETS = 50_000_000  # 50 USDC (6 dp)
SUPPLY_SHARES = 49_990_000
WITHDRAW_ASSETS = 20_000_000  # 20 USDC (6 dp)
WITHDRAW_SHARES = 19_996_000


def _deposit_log(assets: int = SUPPLY_ASSETS, shares: int = SUPPLY_SHARES) -> dict:
    # Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
    return {
        "address": FTOKEN,
        "topics": [DEPOSIT_4626_TOPIC, _addr_topic(SAFE), _addr_topic(SAFE)],
        "data": "0x" + _word(assets) + _word(shares),
    }


def _withdraw_log(assets: int = WITHDRAW_ASSETS, shares: int = WITHDRAW_SHARES) -> dict:
    # Withdraw(address indexed sender, address indexed receiver,
    #          address indexed owner, uint256 assets, uint256 shares)
    return {
        "address": FTOKEN,
        "topics": [WITHDRAW_4626_TOPIC, _addr_topic(SAFE), _addr_topic(SAFE), _addr_topic(SAFE)],
        "data": "0x" + _word(assets) + _word(shares),
    }


class TestLending4626Events:
    """D3.F4 — exact ``assets`` extraction; parse failures NEVER fabricate amounts."""

    def test_supply_deposit_event_exact_assets(self):
        receipt = _receipt(
            SAFE,
            [
                _transfer_log(USDC, SAFE, FTOKEN, SUPPLY_ASSETS),
                _deposit_log(),
            ],
        )
        assert _parser().extract_supply_amount(receipt) == SUPPLY_ASSETS

    def test_lending_withdraw_event_exact_assets(self):
        receipt = _receipt(
            SAFE,
            [
                _withdraw_log(),
                _transfer_log(USDC, FTOKEN, SAFE, WITHDRAW_ASSETS),
            ],
        )
        assert _parser().extract_withdraw_amount(receipt) == WITHDRAW_ASSETS

    def test_supply_missing_deposit_event_returns_none(self):
        # A bare ERC-20 transfer is not proof of a supply — fail closed.
        receipt = _receipt(SAFE, [_transfer_log(USDC, SAFE, FTOKEN, SUPPLY_ASSETS)])
        assert _parser().extract_supply_amount(receipt) is None

    def test_lending_withdraw_missing_event_returns_none(self):
        # A Deposit log must never satisfy a withdraw extraction (and vice versa).
        receipt = _receipt(SAFE, [_deposit_log()])
        assert _parser().extract_withdraw_amount(receipt) is None

    def test_supply_withdraw_event_returns_none(self):
        # The vice-versa direction: a Withdraw log must never satisfy a
        # supply extraction (CodeRabbit 2026-06-11 — bidirectional guard).
        receipt = _receipt(SAFE, [_withdraw_log()])
        assert _parser().extract_supply_amount(receipt) is None

    def test_supply_multiple_deposit_events_ambiguous_returns_none(self):
        # MORE THAN ONE matching ERC-4626 event = ambiguous attribution
        # (bundler/multicall receipt) — fail closed, never the first amount.
        receipt = _receipt(SAFE, [_deposit_log(), _deposit_log(assets=1_000_000, shares=999_000)])
        assert _parser().extract_supply_amount(receipt) is None

    def test_withdraw_multiple_events_ambiguous_returns_none(self):
        receipt = _receipt(SAFE, [_withdraw_log(), _withdraw_log(assets=1_000_000, shares=999_000)])
        assert _parser().extract_withdraw_amount(receipt) is None

    def test_supply_malformed_short_data_returns_none(self):
        malformed = {
            "address": FTOKEN,
            "topics": [DEPOSIT_4626_TOPIC, _addr_topic(SAFE), _addr_topic(SAFE)],
            "data": "0x1234",  # < one 32-byte word — undecodable assets
        }
        receipt = _receipt(SAFE, [malformed])
        assert _parser().extract_supply_amount(receipt) is None

    def test_lending_reverted_receipt_returns_none(self):
        receipt = _receipt(SAFE, [_deposit_log(), _withdraw_log()])
        receipt["status"] = 0
        parser = _parser()
        assert parser.extract_supply_amount(receipt) is None
        assert parser.extract_withdraw_amount(receipt) is None


# =============================================================================
# Vault NFT-CDP receipts (VIB-5031) — factory-gated mint + signed deltas
# =============================================================================

from almanak.connectors.fluid.receipt_parser import (  # noqa: E402
    FACTORY_NEW_POSITION_MINTED_TOPIC,
    LIQUIDITY_LOG_OPERATE_TOPIC,
    VAULT_LOG_OPERATE_TOPIC,
    FluidVaultReceiptParser,
)

VAULT = "0xeabbfca72f8a8bf14c4ac59e69ecb2eb69f0811c"  # arbitrum vault id 1
VAULT_FACTORY = "0x324c5dc1fc42c7a4d43d92df1eba58a54d13bf2d"  # ERC-721 home
FOREIGN_NFT_CONTRACT = "0xc36442b4a4522e871399cd717abdd847ab11fe88"  # e.g. Uni V3 NPM
NFT_ID = 12542
COL_DELTA = 10**18  # +1 ETH
DEBT_DELTA = 500_000_000  # +500 USDC


def _signed_word(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def _vault_operate_log(
    nft_id: int = NFT_ID,
    col_delta: int = COL_DELTA,
    debt_delta: int = DEBT_DELTA,
    vault: str = VAULT,
) -> dict:
    # LogOperate(address user_, uint256 nftId_, int256 colAmt_, int256 debtAmt_,
    # address to_) — ZERO indexed params, all five words in data (verified D2).
    data = (
        "0x"
        + _addr_topic(SAFE)[2:]
        + _word(nft_id)
        + _signed_word(col_delta)
        + _signed_word(debt_delta)
        + _addr_topic(SAFE)[2:]
    )
    return {"address": vault, "topics": [VAULT_LOG_OPERATE_TOPIC], "data": data}


def _factory_mint_log(nft_id: int = NFT_ID, emitter: str = VAULT_FACTORY) -> dict:
    return {
        "address": emitter,
        "topics": [
            ERC721_TRANSFER_TOPIC,
            _addr_topic("0x" + "0" * 40),
            _addr_topic(SAFE),
            "0x" + _word(nft_id),
        ],
        "data": "0x",
    }


def _new_position_minted_log(nft_id: int = NFT_ID) -> dict:
    return {
        "address": VAULT_FACTORY,
        "topics": [
            FACTORY_NEW_POSITION_MINTED_TOPIC,
            _addr_topic(VAULT),
            _addr_topic(SAFE),
            "0x" + _word(nft_id),
        ],
        "data": "0x",
    }


def _liquidity_log_operate() -> dict:
    # The Liquidity-layer LogOperate (different signature, 2 indexed topics)
    # that appears twice in real vault receipts — corroboration only.
    return {
        "address": LIQUIDITY,
        "topics": [LIQUIDITY_LOG_OPERATE_TOPIC, _addr_topic(VAULT), _addr_topic(USDC)],
        "data": "0x" + _word(0) * 6,
    }


def _vault_open_receipt(**operate_kwargs) -> dict:
    # The verified open receipt topology (report D2): factory Transfer mint,
    # NewPositionMinted, two Liquidity LogOperate legs, ERC-20 Transfer,
    # then the vault's own LogOperate.
    return _receipt(
        SAFE,
        [
            _factory_mint_log(),
            _new_position_minted_log(),
            _liquidity_log_operate(),
            _transfer_log(USDC, LIQUIDITY, SAFE, DEBT_DELTA),
            _liquidity_log_operate(),
            _vault_operate_log(**operate_kwargs),
        ],
    )


def _vault_parser() -> FluidVaultReceiptParser:
    return FluidVaultReceiptParser(chain="arbitrum")


class TestVaultOperateDecoding:
    """D3.F5 / D1.S1 — signed deltas are the receipt-truth amounts."""

    def test_vault_operate_open_decodes_nft_and_signed_deltas(self):
        result = _vault_parser().parse_receipt(_vault_open_receipt())
        assert result.success
        assert result.minted_nft_id == NFT_ID
        assert len(result.operate_events) == 1
        event = result.operate_events[0]
        assert event.nft_id == NFT_ID
        assert event.col_delta == COL_DELTA
        assert event.debt_delta == DEBT_DELTA
        assert event.vault == VAULT

    def test_vault_operate_negative_delta_twos_complement(self):
        # The verified live repay: operate(12542, 0, -200000000) decoded
        # debtAmt 0xff...f4143e00 — exactly two's complement.
        receipt = _receipt(SAFE, [_vault_operate_log(col_delta=0, debt_delta=-200_000_000)])
        parser = _vault_parser()
        event = parser.parse_receipt(receipt).operate_events[0]
        assert event.col_delta == 0
        assert event.debt_delta == -200_000_000
        assert parser.extract_repay_amount(receipt) == 200_000_000

    def test_vault_operate_amount_extraction_by_direction(self):
        parser = _vault_parser()
        supply = _receipt(SAFE, [_vault_operate_log(col_delta=COL_DELTA, debt_delta=0)])
        assert parser.extract_supply_amount(supply) == COL_DELTA
        assert parser.extract_withdraw_amount(supply) is None
        assert parser.extract_repay_amount(supply) is None

        borrow = _vault_open_receipt()
        assert parser.extract_borrow_amount(borrow) == DEBT_DELTA

        withdraw = _receipt(SAFE, [_vault_operate_log(col_delta=-(10**17), debt_delta=0)])
        assert parser.extract_withdraw_amount(withdraw) == 10**17
        assert parser.extract_supply_amount(withdraw) is None

    def test_vault_operate_lending_data_string_encoded(self):
        data = _vault_parser().extract_lending_data(_vault_open_receipt())
        assert data == {
            "nft_id": str(NFT_ID),
            "vault": VAULT,
            "col_delta": str(COL_DELTA),
            "debt_delta": str(DEBT_DELTA),
        }

    def test_vault_operate_full_close_negative_pair(self):
        # Full-close sentinel pair resolves on-chain to true negative deltas;
        # the parser surfaces the receipt-truth absolute amounts.
        parser = _vault_parser()
        repay = _receipt(SAFE, [_vault_operate_log(col_delta=0, debt_delta=-DEBT_DELTA)])
        withdraw = _receipt(SAFE, [_vault_operate_log(col_delta=-COL_DELTA, debt_delta=0)])
        assert parser.extract_repay_amount(repay) == DEBT_DELTA
        assert parser.extract_withdraw_amount(withdraw) == COL_DELTA


class TestVaultFactoryGating:
    """D3.F5 — nftId capture is factory-gated; foreign mints ignored."""

    def test_factory_emitted_mint_captured(self):
        result = _vault_parser().parse_receipt(_vault_open_receipt())
        assert result.minted_nft_id == NFT_ID
        assert _vault_parser().extract_position_id(_vault_open_receipt()) == NFT_ID

    def test_foreign_mint_ignored_by_nft_extraction(self):
        # A bundled/Zodiac receipt can contain an unrelated ERC-721 mint —
        # it must NEVER be captured as the Fluid nftId.
        foreign_only = _receipt(SAFE, [_factory_mint_log(nft_id=999_999, emitter=FOREIGN_NFT_CONTRACT)])
        parser = _vault_parser()
        assert parser.parse_receipt(foreign_only).minted_nft_id is None
        assert parser.extract_position_id(foreign_only) is None

    def test_foreign_mint_alongside_factory_mint_factory_wins(self):
        receipt = _receipt(
            SAFE,
            [
                _factory_mint_log(nft_id=999_999, emitter=FOREIGN_NFT_CONTRACT),
                _factory_mint_log(nft_id=NFT_ID),
                _vault_operate_log(),
            ],
        )
        assert _vault_parser().extract_position_id(receipt) == NFT_ID

    def test_no_mint_falls_back_to_vault_operate_nft(self):
        receipt = _receipt(SAFE, [_vault_operate_log(col_delta=0, debt_delta=-1_000_000)])
        assert _vault_parser().extract_position_id(receipt) == NFT_ID

    def test_mint_operate_nft_mismatch_ambiguous_not_captured(self):
        # Mint <-> operate correlation: a factory mint whose tokenId does
        # NOT match the vault LogOperate's nonzero nftId belongs to a
        # different position in the same (bundled) receipt — attribution is
        # ambiguous, so the mint is dropped (fail closed) and position
        # extraction falls back to the vault's own receipt-truth nftId.
        receipt = _receipt(SAFE, [_factory_mint_log(nft_id=777), _vault_operate_log(nft_id=NFT_ID)])
        parser = _vault_parser()
        result = parser.parse_receipt(receipt)
        assert result.minted_nft_id is None
        assert parser.extract_position_id(receipt) == NFT_ID

    def test_mint_operate_nft_match_captured(self):
        receipt = _receipt(SAFE, [_factory_mint_log(nft_id=NFT_ID), _vault_operate_log(nft_id=NFT_ID)])
        result = _vault_parser().parse_receipt(receipt)
        assert result.minted_nft_id == NFT_ID

    def test_legacy_dex_parser_mint_fallback_also_factory_gated(self):
        # The Phase-1 parser's permissive any-mint fallback is now strictly
        # narrowed to factory-emitted mints (ADR §5 tightening).
        foreign = _receipt(SAFE, [_factory_mint_log(nft_id=777, emitter=FOREIGN_NFT_CONTRACT)])
        genuine = _receipt(SAFE, [_factory_mint_log(nft_id=NFT_ID)])
        parser = _parser()
        assert parser.extract_position_id(foreign) is None
        assert parser.extract_position_id(genuine) == NFT_ID


class TestVaultFailClosed:
    """D3.F5 — truncated/foreign payloads rejected, never partially decoded."""

    def test_vault_operate_truncated_payload_rejected(self):
        # 4 words instead of 5 — must be rejected, not partially decoded.
        log = _vault_operate_log()
        log["data"] = log["data"][: 2 + 4 * 64]
        receipt = _receipt(SAFE, [log])
        parser = _vault_parser()
        assert parser.parse_receipt(receipt).operate_events == []
        assert parser.extract_lending_data(receipt) is None
        assert parser.extract_borrow_amount(receipt) is None

    def test_vault_operate_oversized_payload_rejected(self):
        log = _vault_operate_log()
        log["data"] = log["data"] + _word(0)
        receipt = _receipt(SAFE, [log])
        assert _vault_parser().extract_lending_data(receipt) is None

    def test_no_vault_operate_event_yields_none_never_zeros(self):
        # Liquidity-layer LogOperate legs alone (corroboration only) must
        # never be decoded as vault amounts or a fabricated nftId.
        receipt = _receipt(SAFE, [_liquidity_log_operate(), _liquidity_log_operate()])
        parser = _vault_parser()
        assert parser.extract_position_id(receipt) is None
        assert parser.extract_lending_data(receipt) is None
        assert parser.extract_supply_amount(receipt) is None

    def test_multiple_vault_operate_events_ambiguous_fail_closed(self):
        receipt = _receipt(SAFE, [_vault_operate_log(), _vault_operate_log(nft_id=7)])
        parser = _vault_parser()
        assert parser.extract_lending_data(receipt) is None
        assert parser.extract_borrow_amount(receipt) is None
        assert parser.extract_position_id(receipt) is None

    def test_multiple_vault_operate_events_with_mint_position_id_fail_closed(self):
        # extract_position_id honours the SAME multi-operate ambiguity rule
        # as _single_operate_event: a surviving factory mint must not rescue
        # a bundler/multicall receipt the parser cannot attribute.
        receipt = _receipt(
            SAFE,
            [_factory_mint_log(nft_id=NFT_ID), _vault_operate_log(nft_id=NFT_ID), _vault_operate_log(nft_id=NFT_ID)],
        )
        assert _vault_parser().extract_position_id(receipt) is None

    def test_reverted_vault_receipt_fails_closed(self):
        receipt = _vault_open_receipt()
        receipt["status"] = 0
        parser = _vault_parser()
        assert not parser.parse_receipt(receipt).success
        assert parser.extract_lending_data(receipt) is None

    def test_zero_nft_id_is_mint_sentinel_never_a_position_id(self):
        # LogOperate carrying nftId 0 (the operate() MINT SENTINEL) with no
        # factory mint in the receipt: 0 is never a real position id, so
        # extract_position_id fails closed and extract_lending_data OMITS the
        # nft_id field while still emitting the receipt-truth deltas.
        receipt = _receipt(SAFE, [_vault_operate_log(nft_id=0)])
        parser = _vault_parser()
        assert parser.extract_position_id(receipt) is None
        data = parser.extract_lending_data(receipt)
        assert data is not None, "deltas are receipt-truth even when the id is unresolved"
        assert "nft_id" not in data, "0 is the mint sentinel — fail closed, never stamp it"
        assert data["col_delta"] == str(COL_DELTA)
        assert data["debt_delta"] == str(DEBT_DELTA)

    def test_zero_nft_id_with_factory_mint_resolves_from_the_mint(self):
        # The same sentinel-0 LogOperate WITH a factory mint: the mint is the
        # authoritative id (the correlation rule only compares NONZERO
        # operate ids, so the sentinel never vetoes the mint).
        receipt = _receipt(SAFE, [_factory_mint_log(nft_id=NFT_ID), _vault_operate_log(nft_id=0)])
        assert _vault_parser().extract_position_id(receipt) == NFT_ID


class TestVaultRunnerHookStamping:
    """The §6.3 nftId home: extracted_data_json via the runner-hook seam."""

    def _result_with_receipts(self, receipts: list[dict]):
        from types import SimpleNamespace

        return SimpleNamespace(
            extracted_data={},
            transaction_results=[SimpleNamespace(success=True, receipt=receipt) for receipt in receipts],
        )

    def test_vault_operate_data_stamped_into_extracted_data(self):
        from almanak.connectors.fluid.runner_hooks import (
            FLUID_VAULT_OPERATE_KEY,
            FluidVaultRunnerHookConnector,
        )

        result = self._result_with_receipts([_vault_open_receipt()])
        FluidVaultRunnerHookConnector().enrich_result(result, gateway_client=None, chain="arbitrum")
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] == {
            "nft_id": str(NFT_ID),
            "vault": VAULT,
            "col_delta": str(COL_DELTA),
            "debt_delta": str(DEBT_DELTA),
        }
        assert result.extracted_data["nft_id"] == str(NFT_ID)

    def test_non_vault_receipts_stamp_nothing(self):
        from almanak.connectors.fluid.runner_hooks import FluidVaultRunnerHookConnector

        result = self._result_with_receipts([_receipt(SAFE, [_transfer_log(USDC, SAFE, LIQUIDITY, 1)])])
        FluidVaultRunnerHookConnector().enrich_result(result, gateway_client=None, chain="arbitrum")
        assert result.extracted_data == {}

    def test_existing_stamp_never_overwritten(self):
        from almanak.connectors.fluid.runner_hooks import (
            FLUID_VAULT_OPERATE_KEY,
            FluidVaultRunnerHookConnector,
        )

        sentinel = {"nft_id": "1"}
        result = self._result_with_receipts([_vault_open_receipt()])
        result.extracted_data[FLUID_VAULT_OPERATE_KEY] = sentinel
        FluidVaultRunnerHookConnector().enrich_result(result, gateway_client=None, chain="arbitrum")
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] is sentinel
