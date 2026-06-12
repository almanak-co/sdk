"""Direct unit coverage for the Fluid vault runner hook (VIB-5031).

The hook is the ONLY writer of the §6.3 ``FluidVaultOperateData`` stamp into
``result.extracted_data`` (-> ``transaction_ledger.extracted_data_json``), so
its receipt-normalization and stamping branches get first-class tests here:

- vault receipt with a single operate event  -> stamped (dict AND to_dict shapes)
- non-vault receipt                          -> untouched
- ambiguous / failed parse                   -> NOT stamped (no zero/fabricated values)
- malformed receipt dict                     -> no raise, no stamp

End-to-end stamping through the parser fixtures also lives in
``test_receipt_parser.py::TestVaultRunnerHookStamping``; on-chain behaviour in
``tests/intents/{arbitrum,base}/test_fluid_vault_lending.py``.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.connectors.fluid.receipt_parser import VAULT_LOG_OPERATE_TOPIC
from almanak.connectors.fluid.runner_hooks import (
    FLUID_VAULT_OPERATE_KEY,
    FluidVaultRunnerHookConnector,
)

VAULT = "0xeabbfca72f8a8bf14c4ac59e69ecb2eb69f0811c"  # arbitrum vault id 1
WALLET = "0xAAAAaaaAAaAaAAaaAAAAaAAAaaaaAAaAAAAAaaaA"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NFT_ID = 12542
COL_DELTA = 10**18  # +1 ETH
DEBT_DELTA = 500_000_000  # +500 USDC


def _word(value: int) -> str:
    return f"{value:064x}"


def _signed_word(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def _addr_word(addr: str) -> str:
    return "0" * 24 + addr[2:].lower()


def _vault_operate_log(nft_id: int = NFT_ID, col_delta: int = COL_DELTA, debt_delta: int = DEBT_DELTA) -> dict:
    # LogOperate(address user_, uint256 nftId_, int256 colAmt_, int256 debtAmt_,
    # address to_) — ZERO indexed params, all five words in data.
    data = (
        "0x"
        + _addr_word(WALLET)
        + _word(nft_id)
        + _signed_word(col_delta)
        + _signed_word(debt_delta)
        + _addr_word(WALLET)
    )
    return {"address": VAULT, "topics": [VAULT_LOG_OPERATE_TOPIC], "data": data}


def _erc20_transfer_log() -> dict:
    return {
        "address": USDC,
        "topics": [ERC20_TRANSFER_TOPIC, "0x" + _addr_word(VAULT), "0x" + _addr_word(WALLET)],
        "data": "0x" + _word(DEBT_DELTA),
    }


def _receipt(logs: list[dict], status: int = 1) -> dict:
    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 1_000_000,
        "status": status,
        "from": WALLET,
        "logs": logs,
    }


def _result(receipts: list, successes: list[bool] | None = None, extracted_data=None):
    successes = successes if successes is not None else [True] * len(receipts)
    return SimpleNamespace(
        extracted_data={} if extracted_data is None else extracted_data,
        transaction_results=[
            SimpleNamespace(success=ok, receipt=receipt) for ok, receipt in zip(successes, receipts, strict=True)
        ],
    )


def _enrich(result) -> None:
    FluidVaultRunnerHookConnector().enrich_result(result, gateway_client=None, chain="arbitrum")


EXPECTED_STAMP = {
    "nft_id": str(NFT_ID),
    "vault": VAULT,
    "col_delta": str(COL_DELTA),
    "debt_delta": str(DEBT_DELTA),
}


class TestStampingHappyPath:
    def test_vault_receipt_with_operate_event_stamps_operate_data_and_nft_id(self):
        result = _result([_receipt([_vault_operate_log()])])
        _enrich(result)
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] == EXPECTED_STAMP
        assert result.extracted_data["nft_id"] == str(NFT_ID)

    def test_receipt_object_normalized_via_to_dict(self):
        receipt_obj = SimpleNamespace(to_dict=lambda: _receipt([_vault_operate_log()]))
        result = SimpleNamespace(
            extracted_data={},
            transaction_results=[SimpleNamespace(success=True, receipt=receipt_obj)],
        )
        _enrich(result)
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] == EXPECTED_STAMP

    def test_generic_nft_id_key_not_clobbered_when_already_claimed(self):
        result = _result([_receipt([_vault_operate_log()])], extracted_data={"nft_id": "777"})
        _enrich(result)
        assert result.extracted_data["nft_id"] == "777"
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] == EXPECTED_STAMP


class TestNonVaultReceiptsUntouched:
    def test_non_vault_receipt_untouched(self):
        result = _result([_receipt([_erc20_transfer_log()])])
        _enrich(result)
        assert result.extracted_data == {}

    def test_failed_transaction_results_skipped(self):
        # A vault receipt on a FAILED tx result must not be stamped.
        result = _result([_receipt([_vault_operate_log()])], successes=[False])
        _enrich(result)
        assert result.extracted_data == {}

    def test_none_receipt_skipped(self):
        result = SimpleNamespace(
            extracted_data={},
            transaction_results=[SimpleNamespace(success=True, receipt=None)],
        )
        _enrich(result)
        assert result.extracted_data == {}

    def test_existing_stamp_never_overwritten(self):
        sentinel = {"nft_id": "1"}
        result = _result([_receipt([_vault_operate_log()])], extracted_data={FLUID_VAULT_OPERATE_KEY: sentinel})
        _enrich(result)
        assert result.extracted_data[FLUID_VAULT_OPERATE_KEY] is sentinel


class TestAmbiguousOrFailedParseNotStamped:
    def test_two_operate_events_in_one_receipt_ambiguous_not_stamped(self):
        result = _result([_receipt([_vault_operate_log(), _vault_operate_log(nft_id=7)])])
        _enrich(result)
        assert result.extracted_data == {}

    def test_two_vault_receipts_ambiguous_not_stamped(self):
        result = _result([_receipt([_vault_operate_log()]), _receipt([_vault_operate_log(nft_id=7)])])
        _enrich(result)
        assert result.extracted_data == {}

    def test_reverted_vault_receipt_not_stamped(self):
        result = _result([_receipt([_vault_operate_log()], status=0)])
        _enrich(result)
        assert result.extracted_data == {}

    def test_truncated_operate_payload_not_stamped(self):
        log = _vault_operate_log()
        log["data"] = log["data"][: 2 + 4 * 64]  # 4 words instead of 5
        result = _result([_receipt([log])])
        _enrich(result)
        assert result.extracted_data == {}

    def test_zero_nft_id_mint_sentinel_never_stamped_as_position_id(self):
        # The parser omits nft_id for the sentinel; the hook must not
        # fabricate one — deltas are still receipt-truth.
        result = _result([_receipt([_vault_operate_log(nft_id=0)])])
        _enrich(result)
        stamp = result.extracted_data[FLUID_VAULT_OPERATE_KEY]
        assert "nft_id" not in stamp
        assert stamp["col_delta"] == str(COL_DELTA)
        assert "nft_id" not in result.extracted_data, "no zero/fabricated generic nft_id"


class TestMalformedReceiptsNeverRaise:
    def test_logs_not_a_list_no_raise_no_stamp(self):
        result = _result([{"status": 1, "logs": None}])
        _enrich(result)
        assert result.extracted_data == {}

    def test_log_entries_not_dicts_no_raise_no_stamp(self):
        result = _result([{"status": 1, "logs": ["garbage", 42, None]}])
        _enrich(result)
        assert result.extracted_data == {}

    def test_topics_not_a_list_no_raise_no_stamp(self):
        result = _result([{"status": 1, "logs": [{"topics": "0xdeadbeef", "data": "0x"}]}])
        _enrich(result)
        assert result.extracted_data == {}

    def test_vault_topic_with_malformed_sibling_log_no_raise_no_stamp(self):
        # The vault topic IS present, so the hook proceeds to the parser —
        # the malformed sibling log blows up the parse, which the hook must
        # swallow (fail-open contract): no raise, no stamp.
        result = _result([_receipt([_vault_operate_log(), "garbage"])])
        _enrich(result)
        assert result.extracted_data == {}

    def test_extracted_data_not_a_dict_no_raise(self):
        result = SimpleNamespace(
            extracted_data=None,
            transaction_results=[SimpleNamespace(success=True, receipt=_receipt([_vault_operate_log()]))],
        )
        _enrich(result)
        assert result.extracted_data is None

    def test_no_transaction_results_no_raise(self):
        result = SimpleNamespace(extracted_data={}, transaction_results=None)
        _enrich(result)
        assert result.extracted_data == {}
