"""Unit tests for ``almanak ax vault`` — ERC-4626 vault promotion step.

The vault counterpart of ``ax lending-market``. Exit-code contract:
0 vault verified / 1 not a vault / 2 invalid input / 4 gateway unavailable.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from almanak.cli.cli import almanak
from almanak.framework.agent_tools.errors import AgentErrorCode, ToolErrorPayload, get_error_category
from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus

VAULT = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def _vault_identity(version: str | None = "v1") -> ToolResponse:
    return ToolResponse(
        status="success",
        data={
            "address": VAULT,
            "kind": "erc4626_vault",
            "family": "erc4626",
            "protocol": "metamorpho" if version else None,
            "vault_version": version,
            "symbol": "steakUSDC",
            "decimals": 18,
            "underlying_asset": USDC,
            "underlying_symbol": "USDC",
            "underlying_decimals": 6,
            "total_assets": 429_847_842_881_293,
            "factory_verified": "unverified",
            "identified_via": "abi-probe",
            "notes": [],
        },
    )


def _executor(identity: ToolResponse):
    mock_executor, mock_client = MagicMock(), MagicMock()
    mock_client.wait_for_ready.return_value = True

    async def execute(tool_name, args):
        assert tool_name == "resolve_pool_address"
        assert args == {"address": VAULT, "chain": "base"}
        return identity

    mock_executor.execute = execute
    return mock_executor, mock_client


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _quiet_advisories(monkeypatch: pytest.MonkeyPatch, *, listed: bool | None = True) -> None:
    """Stub the advisory listing + allocation steps (they need a live gateway)."""
    monkeypatch.setattr(
        "almanak.framework.cli.ax._vault_listing",
        lambda ctx, address, chain, symbol: {
            "listed": listed,
            "source": "morpho_vault",
            "resolved_address": address,
            "note": None,
        },
    )
    monkeypatch.setattr(
        "almanak.framework.cli.ax._vault_allocation",
        lambda ctx, address, chain, version: {
            "source": "on-chain",
            "markets": [],
            "adapters": [],
            "liquidity_adapter": None,
            "note": None,
        },
    )


class TestAxVaultFound:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_v1_vault_json_is_deployable(self, mock_get_exec, runner, monkeypatch):
        mock_get_exec.return_value = _executor(_vault_identity("v1"))
        _quiet_advisories(monkeypatch)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "found"
        assert payload["kind"] == "erc4626_vault"
        assert payload["vault_version"] == "v1"
        assert payload["underlying_symbol"] == "USDC"
        assert payload["listed"] is True
        assert payload["deployable"] is True
        assert payload["verified_on_chain"] is True
        assert 'Intent.vault_deposit(protocol="metamorpho", vault_address="' + VAULT in payload["intent"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_v2_vault_is_reported_but_not_deployable(self, mock_get_exec, runner, monkeypatch):
        mock_get_exec.return_value = _executor(_vault_identity("v2"))
        _quiet_advisories(monkeypatch)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["vault_version"] == "v2"
        assert payload["deployable"] is False
        assert "maxRedeem" in payload["deployable_note"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_human_output_names_generation_and_intent(self, mock_get_exec, runner, monkeypatch):
        mock_get_exec.return_value = _executor(_vault_identity("v1"))
        _quiet_advisories(monkeypatch)
        result = runner.invoke(almanak, ["ax", "-c", "base", "vault", VAULT])
        assert result.exit_code == 0, result.output
        assert "ERC-4626 vault verified on-chain [MetaMorpho v1]" in result.output
        assert "steakUSDC" in result.output
        assert "listed        yes" in result.output
        assert "deployable    yes" in result.output
        assert "vault_deposit" in result.output

    @patch("almanak.framework.cli.ax._get_executor")
    def test_symbol_collision_is_never_reported_as_listed(self, mock_get_exec, runner, monkeypatch):
        mock_get_exec.return_value = _executor(_vault_identity("v1"))
        _quiet_advisories(monkeypatch, listed=None)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["listed"] is None
        assert "listed        unknown" in runner.invoke(almanak, ["ax", "-c", "base", "vault", VAULT]).output


class TestVaultListingVerdicts:
    """``_vault_listing`` maps the gateway's address-keyed Morpho index onto a three-valued verdict."""

    def _run(self, monkeypatch, response, *, capture_request: bool = False):
        from types import SimpleNamespace

        from almanak.framework.cli import ax as ax_mod

        channel = MagicMock(name="channel")
        seen: dict = {}

        def resolve_token(request, timeout):  # noqa: ARG001
            seen["token"] = request.token
            seen["chain"] = request.chain
            return response

        monkeypatch.setattr(ax_mod, "_acquire_gateway_channel", lambda ctx: (channel, None))
        monkeypatch.setattr(
            "almanak.gateway.proto.gateway_pb2_grpc.TokenServiceStub",
            lambda ch: SimpleNamespace(ResolveToken=resolve_token),
        )
        verdict = ax_mod._vault_listing(MagicMock(), VAULT, "base", "steakUSDC")
        return (verdict, seen) if capture_request else verdict

    def test_queries_the_gateway_by_exact_vault_address(self, monkeypatch):
        from types import SimpleNamespace

        verdict, seen = self._run(
            monkeypatch,
            SimpleNamespace(success=True, address=VAULT, source="morpho_vault"),
            capture_request=True,
        )
        assert seen["token"] == VAULT
        assert seen["chain"] == "base"
        assert verdict["listed"] is True

    def test_same_address_from_morpho_index_is_listed(self, monkeypatch):
        from types import SimpleNamespace

        verdict = self._run(monkeypatch, SimpleNamespace(success=True, address=VAULT, source="morpho_vault"))
        assert verdict["listed"] is True

    def test_non_morpho_source_is_unknown_not_false(self, monkeypatch):
        from types import SimpleNamespace

        verdict = self._run(monkeypatch, SimpleNamespace(success=True, address=VAULT, source="static"))
        assert verdict["listed"] is None
        assert "static" in verdict["note"]

    def test_different_resolved_address_is_unknown_not_false(self, monkeypatch):
        from types import SimpleNamespace

        other = "0xbeefe94c8ad530842bfe7d8b397938ffc1cb83b2"
        verdict = self._run(monkeypatch, SimpleNamespace(success=True, address=other, source="morpho_vault"))
        assert verdict["listed"] is None
        assert other in verdict["note"]

    def test_address_miss_is_not_listed(self, monkeypatch):
        from types import SimpleNamespace

        verdict = self._run(monkeypatch, SimpleNamespace(success=False, address="", source=""))
        assert verdict["listed"] is False


class TestAxVaultFailures:
    @patch("almanak.framework.cli.ax._get_executor")
    def test_plain_erc20_exits_1_not_a_vault(self, mock_get_exec, runner):
        identity = ToolResponse(
            status="success",
            data={"address": VAULT, "kind": "erc20", "symbol": "WETH", "decimals": 18, "notes": []},
        )
        mock_get_exec.return_value = _executor(identity)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "not_a_vault"
        assert "kind='erc20'" in payload["error"]

    @patch("almanak.framework.cli.ax._get_executor")
    def test_pool_exits_1_and_points_to_ax_pool(self, mock_get_exec, runner):
        identity = ToolResponse(
            status="success",
            data={"address": VAULT, "kind": "pool", "protocol": "uniswap_v3", "notes": []},
        )
        mock_get_exec.return_value = _executor(identity)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert "uniswap_v3" in payload["error"]
        assert "ax lending-market" in payload["error"]

    def test_malformed_address_exits_2(self, runner):
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", "steakUSDC"])
        assert result.exit_code == 2, result.output
        assert json.loads(result.output)["status"] == "invalid"

    @patch("almanak.framework.cli.ax._get_executor")
    def test_probe_error_exits_4_inconclusive(self, mock_get_exec, runner):
        identity = ToolResponse(
            status=ToolResponseStatus.ERROR,
            error=ToolErrorPayload(
                error_code=AgentErrorCode.RPC_FAILED,
                message="Identity probes could not answer",
                recoverable=True,
                error_category=get_error_category(AgentErrorCode.RPC_FAILED),
            ),
        )
        mock_get_exec.return_value = _executor(identity)
        result = runner.invoke(almanak, ["ax", "-c", "base", "--json", "vault", VAULT])
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        assert "could not verify" in payload["hint"]


# ``_vault_allocation`` and its helpers: enumerate the vault's holdings from the
# contract via a fake ``client.eth_call`` and verify markets over ONE channel.

from types import SimpleNamespace  # noqa: E402

from almanak.connectors._strategy_base.pool_identity_base import (  # noqa: E402
    METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR,
    MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR,
)
from almanak.framework.cli import ax as ax_mod  # noqa: E402

_MID = ["0x" + f"{i:02x}" * 32 for i in (1, 2, 3)]
_ADAPTER = "0x" + "ad" * 20
_LIQ_ADAPTER = "0x" + "1a" * 20


def _word_hex(value: int) -> str:
    return "0x" + f"{value:064x}"


def _client_with(script: dict[str, str]):
    """Fake gateway client: ``eth_call(chain, to, data)`` answers by calldata; unscripted reverts."""
    client = MagicMock()

    def eth_call(chain, to, data):
        if data not in script:
            raise RuntimeError("execution reverted")
        return script[data]

    client.eth_call = eth_call
    return client


def _v1_script(*market_ids: str, length: int | None = None) -> dict[str, str]:
    script = {
        METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR: _word_hex(length if length is not None else len(market_ids))
    }
    for i, mid in enumerate(market_ids):
        script[ax_mod._MORPHO_V1_WITHDRAW_QUEUE + f"{i:064x}"] = mid
    return script


def _v2_script(*adapters: str, liquidity: str | None = _LIQ_ADAPTER) -> dict[str, str]:
    script = {MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR: _word_hex(len(adapters))}
    for i, adapter in enumerate(adapters):
        script[ax_mod._MORPHO_V2_ADAPTERS + f"{i:064x}"] = "0x" + "0" * 24 + adapter[2:]
    if liquidity:
        script[ax_mod._MORPHO_V2_LIQUIDITY_ADAPTER] = "0x" + "0" * 24 + liquidity[2:]
    return script


def _market_response(*, verified: bool = True, success: bool = True, error: str = ""):
    market = SimpleNamespace(
        verified=verified,
        collateral_symbol="cbETH",
        collateral_token="0x" + "cb" * 20,
        loan_symbol="USDC",
        loan_token=USDC,
        lltv_bps=8600,
    )
    return SimpleNamespace(success=success, error=error, market=market)


class TestWordReader:
    def test_reverting_or_empty_reads_are_none(self):
        word = ax_mod._make_word_reader(_client_with({"0xaa": "0x"}), "base", VAULT)
        assert word("0xaa") is None  # empty payload
        assert word("0xbb") is None  # reverted
        word = ax_mod._make_word_reader(_client_with({"0xaa" + f"{3:064x}": _word_hex(7)}), "base", VAULT)
        assert word("0xaa", 3) == _word_hex(7)


class TestEnumerateWords:
    def test_length_not_answering_is_none_not_empty(self):
        word = ax_mod._make_word_reader(_client_with({}), "base", VAULT)
        assert ax_mod._enumerate_words(word, "0x11", "0x22", 64) == (None, False, 0)

    def test_unread_entries_are_counted_not_silently_dropped(self):
        script = _v1_script(*_MID)
        del script[ax_mod._MORPHO_V1_WITHDRAW_QUEUE + f"{1:064x}"]  # entry 1 reverts
        word = ax_mod._make_word_reader(_client_with(script), "base", VAULT)
        entries, truncated, unread = ax_mod._enumerate_words(
            word, METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR, ax_mod._MORPHO_V1_WITHDRAW_QUEUE, 64
        )
        assert entries == [_MID[0], _MID[2]]
        assert truncated is False
        assert unread == 1
        assert ax_mod._partial_listing_note("withdrawQueue", False, 1) == (
            "1 withdrawQueue entry did not answer and is missing from this listing"
        )
        assert ax_mod._partial_listing_note("withdrawQueue", False, 2) == (
            "2 withdrawQueue entries did not answer and are missing from this listing"
        )
        assert ax_mod._partial_listing_note("withdrawQueue", False, 0) is None

    def test_entries_are_right_aligned_and_truncation_is_reported(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_MAX_ENUMERATED_ENTRIES", 2)
        word = ax_mod._make_word_reader(_client_with(_v1_script(*_MID, length=3)), "base", VAULT)
        entries, truncated, unread = ax_mod._enumerate_words(
            word, METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR, ax_mod._MORPHO_V1_WITHDRAW_QUEUE, 64
        )
        assert entries == _MID[:2]
        assert truncated is True
        assert unread == 0


class TestMarketRecord:
    def test_verified_response_maps_params(self):
        record = ax_mod._market_record(_MID[0], _market_response())
        assert record == {
            "market_id": _MID[0],
            "verified": True,
            "collateral_symbol": "cbETH",
            "collateral_token": "0x" + "cb" * 20,
            "loan_symbol": "USDC",
            "loan_token": USDC,
            "lltv_bps": 8600,
        }

    def test_unverified_and_failed_responses_never_fabricate(self):
        assert ax_mod._market_record(_MID[0], _market_response(verified=False)) == {
            "market_id": _MID[0],
            "verified": False,
            "error": "unverified",
        }
        assert ax_mod._market_record(_MID[0], _market_response(success=False, error="MISMATCH"))["error"] == "MISMATCH"


class TestVerifyMarkets:
    def _stub(self, monkeypatch, responses):
        channel = MagicMock(name="channel")
        monkeypatch.setattr(ax_mod, "_acquire_gateway_channel", lambda ctx: (channel, None))
        stub = MagicMock()
        stub.GetLendingMarket.side_effect = responses
        monkeypatch.setattr("almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub", lambda ch: stub)
        return channel, stub

    def test_all_markets_share_one_channel_and_the_channel_is_closed(self, monkeypatch):
        channel, stub = self._stub(monkeypatch, [_market_response(), _market_response(verified=False)])
        records = ax_mod._verify_markets(MagicMock(), "base", _MID[:2])
        assert [r["verified"] for r in records] == [True, False]
        assert stub.GetLendingMarket.call_count == 2
        assert all(call.kwargs["timeout"] == 30.0 for call in stub.GetLendingMarket.call_args_list)
        assert stub.GetLendingMarket.call_args_list[0].args[0].protocol == "morpho_blue"
        channel.close.assert_called_once()

    def test_one_failing_rpc_does_not_stop_the_others(self, monkeypatch):
        channel, _ = self._stub(monkeypatch, [RuntimeError("deadline exceeded"), _market_response()])
        records = ax_mod._verify_markets(MagicMock(), "base", _MID[:2])
        assert records[0] == {"market_id": _MID[0], "verified": False, "error": "deadline exceeded"}
        assert records[1]["verified"] is True
        channel.close.assert_called_once()

    def test_gateway_unavailable_marks_every_market_unverified(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_acquire_gateway_channel", lambda ctx: (None, "connection refused"))
        records = ax_mod._verify_markets(MagicMock(), "base", _MID)
        assert all(r["verified"] is False and "connection refused" in r["error"] for r in records)

    def test_no_ids_is_a_no_op(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_acquire_gateway_channel", lambda ctx: pytest.fail("must not open a channel"))
        assert ax_mod._verify_markets(MagicMock(), "base", []) == []


class TestVaultAllocation:
    def test_non_morpho_generation_is_not_enumerated(self):
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", None)
        assert result["markets"] == [] and "only implemented for Morpho" in result["note"]

    def test_gateway_unavailable_is_advisory(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (_ for _ in ()).throw(RuntimeError("no gateway")))
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v1")
        assert result["note"] == "gateway unavailable: no gateway"

    def test_v1_enumerates_withdraw_queue_and_verifies_each_market(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(_v1_script(*_MID))))
        seen = {}
        monkeypatch.setattr(
            ax_mod,
            "_verify_markets",
            lambda ctx, chain, ids: seen.setdefault("ids", ids) and [{"market_id": i, "verified": True} for i in ids],
        )
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v1")
        assert seen["ids"] == _MID
        assert [m["market_id"] for m in result["markets"]] == _MID
        assert result["note"] is None

    def test_v1_reports_truncation_instead_of_a_silently_partial_list(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_MAX_ENUMERATED_ENTRIES", 2)
        monkeypatch.setattr(
            ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(_v1_script(*_MID, length=3)))
        )
        monkeypatch.setattr(
            ax_mod, "_verify_markets", lambda ctx, chain, ids: [{"market_id": i, "verified": True} for i in ids]
        )
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v1")
        assert len(result["markets"]) == 2
        assert "truncated" in result["note"]

    def test_v1_unread_entry_is_reported_as_a_partial_listing(self, monkeypatch):
        script = _v1_script(*_MID)
        del script[ax_mod._MORPHO_V1_WITHDRAW_QUEUE + f"{2:064x}"]
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(script)))
        monkeypatch.setattr(
            ax_mod, "_verify_markets", lambda ctx, chain, ids: [{"market_id": i, "verified": True} for i in ids]
        )
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v1")
        assert len(result["markets"]) == 2
        assert "did not answer" in result["note"]

    def test_v1_length_not_answering_is_reported(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with({})))
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v1")
        assert result["note"] == "withdrawQueueLength() did not answer"

    def test_v2_unread_adapter_entry_is_reported_as_a_partial_listing(self, monkeypatch):
        script = _v2_script(_ADAPTER, "0x" + "bb" * 20)
        del script[ax_mod._MORPHO_V2_ADAPTERS + f"{1:064x}"]  # adapters(1) reverts
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(script)))
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v2")
        assert result["adapters"] == [_ADAPTER]
        assert "1 adapters entry did not answer and is missing" in result["note"]

    def test_v2_truncated_adapter_list_is_reported(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_MAX_ENUMERATED_ENTRIES", 1)
        monkeypatch.setattr(
            ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(_v2_script(_ADAPTER, "0x" + "bb" * 20)))
        )
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v2")
        assert result["adapters"] == [_ADAPTER]
        assert "truncated" in result["note"]

    def test_v2_reports_adapters_and_liquidity_adapter(self, monkeypatch):
        monkeypatch.setattr(ax_mod, "_get_executor", lambda ctx: (MagicMock(), _client_with(_v2_script(_ADAPTER))))
        result = ax_mod._vault_allocation(MagicMock(), VAULT, "base", "v2")
        assert result["adapters"] == [_ADAPTER]
        assert result["liquidity_adapter"] == _LIQ_ADAPTER
        assert "behind adapters" in result["note"]
        assert result["markets"] == []


class TestRenderVaultHuman:
    """Every branch of the human renderer: markets (verified + unverified), adapters, notes, no intent."""

    def _record(self, **overrides) -> dict:
        record = {
            "address": VAULT,
            "chain": "base",
            "vault_version": "v1",
            "symbol": "steakUSDC",
            "decimals": 18,
            "underlying_symbol": "USDC",
            "underlying_asset": USDC,
            "underlying_decimals": 6,
            "total_assets": 1,
            "listed": True,
            "listing": {"note": None},
            "allocation": {},
            "deployable": True,
            "deployable_note": "ok",
            "intent": "Intent.vault_deposit(...)",
        }
        record.update(overrides)
        return record

    def _render(self, record: dict) -> str:
        from click.testing import CliRunner

        with CliRunner().isolation() as (out, _err):
            ax_mod._render_vault_human(record)
            return out.getvalue().decode()

    def test_v1_markets_verified_and_unverified(self):
        out = self._render(
            self._record(
                allocation={
                    "markets": [
                        {
                            "market_id": _MID[0],
                            "verified": True,
                            "collateral_symbol": "cbETH",
                            "loan_symbol": "USDC",
                            "lltv_bps": 8600,
                        },
                        {"market_id": _MID[1], "verified": False, "error": "MISMATCH"},
                    ],
                    "adapters": [],
                    "liquidity_adapter": None,
                    "note": None,
                }
            )
        )
        assert "[MetaMorpho v1]" in out
        assert "markets (withdrawQueue, verified via GetLendingMarket):" in out
        assert f"{_MID[0]}  cbETH/USDC  lltv 86.00%" in out
        assert f"{_MID[1]}  UNVERIFIED — MISMATCH" in out
        assert "listed        yes" in out
        assert "deployable    yes — ok" in out

    def test_v2_adapters_liquidity_note_and_unlisted_with_note(self):
        out = self._render(
            self._record(
                vault_version="v2",
                listed=False,
                listing={"note": "symbol resolved via 'token_registry'"},
                allocation={
                    "markets": [],
                    "adapters": [_ADAPTER],
                    "liquidity_adapter": _LIQ_ADAPTER,
                    "note": "behind adapters",
                },
                deployable=False,
                deployable_note="not yet",
            )
        )
        assert "[Morpho Vault V2]" in out
        assert "listed        no — symbol resolved via 'token_registry'" in out
        assert f"adapters      {_ADAPTER}" in out
        assert f"liquidity     {_LIQ_ADAPTER}" in out
        assert "note          behind adapters" in out
        assert "deployable    NO — not yet" in out

    def test_generic_vault_has_unknown_listing_and_no_intent(self):
        out = self._render(self._record(vault_version=None, listed=None, listing={}, intent=None, symbol=None))
        assert "[generic ERC-4626]" in out
        assert "symbol        ?" in out
        assert "listed        unknown" in out
        assert "no Almanak vault connector fingerprint" in out
