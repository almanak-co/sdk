"""Tests for Solana RPC client."""

import json
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.execution.solana.rpc import (
    ConfirmationResult,
    SolanaRpcClient,
    SolanaRpcConfig,
    SolanaRpcError,
    TransactionReceipt,
    _commitment_met,
    _parse_transaction_response,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config():
    return SolanaRpcConfig(rpc_url="https://api.mainnet-beta.solana.com")


@pytest.fixture
def client(config):
    return SolanaRpcClient(config)


# ---------------------------------------------------------------------------
# _commitment_met tests
# ---------------------------------------------------------------------------


class TestCommitmentMet:
    def test_confirmed_meets_confirmed(self):
        assert _commitment_met("confirmed", "confirmed") is True

    def test_finalized_meets_confirmed(self):
        assert _commitment_met("finalized", "confirmed") is True

    def test_processed_does_not_meet_confirmed(self):
        assert _commitment_met("processed", "confirmed") is False

    def test_confirmed_does_not_meet_finalized(self):
        assert _commitment_met("confirmed", "finalized") is False

    def test_finalized_meets_finalized(self):
        assert _commitment_met("finalized", "finalized") is True

    def test_unknown_does_not_meet(self):
        assert _commitment_met("unknown", "confirmed") is False


# ---------------------------------------------------------------------------
# _parse_transaction_response tests
# ---------------------------------------------------------------------------


class TestParseTransactionResponse:
    def test_successful_transaction(self):
        tx_data = {
            "slot": 123456789,
            "blockTime": 1700000000,
            "meta": {
                "fee": 5000,
                "err": None,
                "logMessages": ["Program log: swap ok"],
                "preTokenBalances": [{"owner": "w1", "mint": "m1", "uiTokenAmount": {"amount": "100"}}],
                "postTokenBalances": [{"owner": "w1", "mint": "m1", "uiTokenAmount": {"amount": "200"}}],
            },
        }
        receipt = _parse_transaction_response("sig123", tx_data)
        assert receipt.signature == "sig123"
        assert receipt.slot == 123456789
        assert receipt.block_time == 1700000000
        assert receipt.fee_lamports == 5000
        assert receipt.success is True
        assert receipt.err is None
        assert len(receipt.logs) == 1
        assert len(receipt.pre_token_balances) == 1
        assert len(receipt.post_token_balances) == 1

    def test_failed_transaction(self):
        tx_data = {
            "slot": 100,
            "meta": {
                "fee": 5000,
                "err": {"InstructionError": [0, "Custom"]},
                "logMessages": [],
                "preTokenBalances": [],
                "postTokenBalances": [],
            },
        }
        receipt = _parse_transaction_response("sig_fail", tx_data)
        assert receipt.success is False
        assert receipt.err == {"InstructionError": [0, "Custom"]}

    def test_missing_meta(self):
        tx_data = {"slot": 50}
        receipt = _parse_transaction_response("sig_empty", tx_data)
        assert receipt.success is True
        assert receipt.fee_lamports == 0


# ---------------------------------------------------------------------------
# SolanaRpcClient sync tests
# ---------------------------------------------------------------------------


class TestSolanaRpcClientSync:
    def test_rpc_call_success(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {"jsonrpc": "2.0", "id": 1, "result": "ok"}
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "post", return_value=mock_response):
            result = client._rpc_call("getHealth")
            assert result == "ok"

    def test_rpc_call_error(self, client):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32600, "message": "Invalid request"},
        }
        mock_response.raise_for_status = MagicMock()

        with patch.object(client._session, "post", return_value=mock_response):
            with pytest.raises(SolanaRpcError, match="getHealth"):
                client._rpc_call("getHealth")

    def test_rpc_call_increments_id(self, client):
        assert client._next_id() == 1
        assert client._next_id() == 2
        assert client._next_id() == 3


# ---------------------------------------------------------------------------
# SolanaRpcClient async tests
# ---------------------------------------------------------------------------


class TestSolanaRpcClientAsync:
    @pytest.mark.asyncio
    async def test_get_health(self, client):
        with patch.object(client, "_rpc_call", return_value="ok"):
            result = await client.get_health()
            assert result is True

    @pytest.mark.asyncio
    async def test_get_health_failure(self, client):
        with patch.object(client, "_rpc_call", side_effect=Exception("down")):
            result = await client.get_health()
            assert result is False

    @pytest.mark.asyncio
    async def test_get_latest_blockhash(self, client):
        with patch.object(
            client,
            "_rpc_call",
            return_value={
                "value": {
                    "blockhash": "FakeBlockhash123",
                    "lastValidBlockHeight": 280000000,
                }
            },
        ):
            blockhash, height = await client.get_latest_blockhash()
            assert blockhash == "FakeBlockhash123"
            assert height == 280000000

    @pytest.mark.asyncio
    async def test_send_transaction(self, client):
        with patch.object(client, "_rpc_call", return_value="SigABC123"):
            sig = await client.send_transaction("base64txdata")
            assert sig == "SigABC123"

    @pytest.mark.asyncio
    async def test_get_signature_statuses(self, client):
        with patch.object(
            client,
            "_rpc_call",
            return_value={"value": [{"confirmationStatus": "confirmed", "slot": 100}]},
        ):
            statuses = await client.get_signature_statuses(["sig1"])
            assert statuses[0]["confirmationStatus"] == "confirmed"

    @pytest.mark.asyncio
    async def test_get_transaction(self, client):
        tx_data = {"slot": 100, "meta": {"fee": 5000, "err": None}}
        with patch.object(client, "_rpc_call", return_value=tx_data):
            result = await client.get_transaction("sig1")
            assert result["slot"] == 100


# ---------------------------------------------------------------------------
# TransactionReceipt tests
# ---------------------------------------------------------------------------


class TestTransactionReceipt:
    def test_to_dict(self):
        receipt = TransactionReceipt(
            signature="sig1",
            slot=100,
            block_time=1700000000,
            fee_lamports=5000,
            success=True,
            logs=["log1"],
        )
        d = receipt.to_dict()
        assert d["signature"] == "sig1"
        assert d["slot"] == 100
        assert d["fee_lamports"] == 5000
        assert d["success"] is True

    def test_defaults(self):
        receipt = TransactionReceipt(signature="sig2")
        assert receipt.slot == 0
        assert receipt.success is True
        assert receipt.logs == []
        assert receipt.pre_token_balances == []
