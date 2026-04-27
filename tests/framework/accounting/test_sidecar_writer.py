"""Unit tests for AccountingSidecarWriter (VIB-3454).

Covers:
- Path resolution: sidecar lands in ~/.almanak/accounting/<strategy_id>.jsonl
- Parent directories are created when they don't exist
- Appended line is valid JSON with the correct schema fields
- Two successive appends both appear in the file (append, not overwrite)
- Failures (e.g. unwritable directory) are swallowed and logged at WARNING
- None / missing fields on intent/result produce null values, not crashes
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.accounting.sidecar import AccountingSidecarWriter, _sidecar_path


# ---------------------------------------------------------------------------
# Minimal stubs
# ---------------------------------------------------------------------------


def _swap_intent(
    from_token: str = "USDC",
    to_token: str = "WETH",
    amount_usd: Decimal = Decimal("100"),
    protocol: str = "uniswap_v3",
) -> Any:
    class _IntentType:
        value = "SWAP"

    return SimpleNamespace(
        intent_type=_IntentType(),
        from_token=from_token,
        to_token=to_token,
        amount_usd=amount_usd,
        protocol=protocol,
    )


def _swap_amounts(
    token_in: str = "USDC",
    token_out: str = "WETH",
    amount_in_decimal: Decimal = Decimal("100"),
    amount_out_decimal: Decimal = Decimal("0.04"),
) -> Any:
    return SimpleNamespace(
        token_in=token_in,
        token_out=token_out,
        amount_in_decimal=amount_in_decimal,
        amount_out_decimal=amount_out_decimal,
        amount_in_decimal_resolved=True,
        amount_out_decimal_resolved=True,
    )


def _tx_result(tx_hash: str = "0xabc123") -> Any:
    return SimpleNamespace(tx_hash=tx_hash)


def _execution_result(
    *,
    tx_hash: str = "0xabc123",
    gas_cost_usd: Decimal | None = Decimal("0.5"),
    with_swap_amounts: bool = True,
    position_id: str | None = None,
) -> Any:
    return SimpleNamespace(
        transaction_results=[_tx_result(tx_hash)],
        gas_cost_usd=gas_cost_usd,
        swap_amounts=_swap_amounts() if with_swap_amounts else None,
        position_id=position_id,
    )


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def test_sidecar_path_resolves_under_home() -> None:
    path = _sidecar_path("my_strategy")
    assert path == Path.home() / ".almanak" / "accounting" / "my_strategy.jsonl"


def test_sidecar_path_uses_strategy_id_as_stem() -> None:
    path = _sidecar_path("arb_lp_v2")
    assert path.stem == "arb_lp_v2"
    assert path.suffix == ".jsonl"


# ---------------------------------------------------------------------------
# Happy-path append
# ---------------------------------------------------------------------------


def test_append_creates_file_with_valid_json(tmp_path: Path) -> None:
    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="strat1",
            intent=_swap_intent(),
            result=_execution_result(),
            chain="arbitrum",
        )

    sidecar = tmp_path / "strat1.jsonl"
    assert sidecar.exists()
    line = json.loads(sidecar.read_text().strip())

    assert line["strategy_id"] == "strat1"
    assert line["intent_type"] == "SWAP"
    assert line["chain"] == "arbitrum"
    assert line["tx_hash"] == "0xabc123"
    assert line["gas_usd"] == "0.5"
    assert line["token_in"] == "USDC"
    assert line["amount_in"] == "100"
    assert line["token_out"] == "WETH"
    assert line["amount_out"] == "0.04"
    assert line["protocol"] == "uniswap_v3"
    # cost_basis_usd is not yet computed at runner level
    assert line["cost_basis_usd"] is None


def test_append_two_lines_both_appear(tmp_path: Path) -> None:
    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="strat2",
            intent=_swap_intent(),
            result=_execution_result(tx_hash="0xfirst"),
            chain="base",
        )
        writer.append(
            strategy_id="strat2",
            intent=_swap_intent(from_token="WETH", to_token="USDC"),
            result=_execution_result(tx_hash="0xsecond"),
            chain="base",
        )

    sidecar = tmp_path / "strat2.jsonl"
    lines = [json.loads(ln) for ln in sidecar.read_text().splitlines() if ln.strip()]
    assert len(lines) == 2
    assert lines[0]["tx_hash"] == "0xfirst"
    assert lines[1]["tx_hash"] == "0xsecond"


def test_append_creates_parent_dirs_if_missing(tmp_path: Path) -> None:
    deep_dir = tmp_path / "a" / "b" / "c"
    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", deep_dir):
        writer.append(
            strategy_id="s",
            intent=_swap_intent(),
            result=_execution_result(),
            chain="optimism",
        )

    assert (deep_dir / "s.jsonl").exists()


# ---------------------------------------------------------------------------
# Null / missing field handling
# ---------------------------------------------------------------------------


def test_append_null_fields_when_result_is_none(tmp_path: Path) -> None:
    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="s_null",
            intent=_swap_intent(),
            result=None,
            chain="ethereum",
        )

    line = json.loads((tmp_path / "s_null.jsonl").read_text().strip())
    assert line["tx_hash"] is None
    assert line["gas_usd"] is None
    assert line["position_id"] is None
    # token_in falls back to intent.from_token
    assert line["token_in"] == "USDC"
    assert line["amount_in"] is not None  # intent.amount_usd fallback


def test_append_no_swap_amounts_uses_intent_fallback(tmp_path: Path) -> None:
    writer = AccountingSidecarWriter()
    result = _execution_result(with_swap_amounts=False)
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="s_fallback",
            intent=_swap_intent(from_token="DAI", amount_usd=Decimal("50")),
            result=result,
            chain="polygon",
        )

    line = json.loads((tmp_path / "s_fallback.jsonl").read_text().strip())
    assert line["token_in"] == "DAI"
    assert line["amount_in"] == "50"  # from intent.amount_usd fallback
    assert line["token_out"] == "WETH"


def test_append_position_id_populated(tmp_path: Path) -> None:
    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="s_pos",
            intent=_swap_intent(),
            result=_execution_result(position_id="0xdeadbeef"),
            chain="arbitrum",
        )

    line = json.loads((tmp_path / "s_pos.jsonl").read_text().strip())
    assert line["position_id"] == "0xdeadbeef"


# ---------------------------------------------------------------------------
# Best-effort: failures are swallowed and logged
# ---------------------------------------------------------------------------


def test_append_swallows_io_error_and_logs_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    writer = AccountingSidecarWriter()
    # Patch mkdir to raise so we trigger an I/O error
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        with patch("pathlib.Path.mkdir", side_effect=OSError("disk full")):
            with caplog.at_level(logging.WARNING, logger="almanak.framework.accounting.sidecar"):
                # Must not raise
                writer.append(
                    strategy_id="s_err",
                    intent=_swap_intent(),
                    result=_execution_result(),
                    chain="arbitrum",
                )

    assert any("AccountingSidecarWriter" in r.message for r in caplog.records)


def test_append_timestamp_is_iso8601(tmp_path: Path) -> None:
    from datetime import datetime

    writer = AccountingSidecarWriter()
    with patch("almanak.framework.accounting.sidecar._SIDECAR_DIR", tmp_path):
        writer.append(
            strategy_id="s_ts",
            intent=_swap_intent(),
            result=_execution_result(),
            chain="arbitrum",
        )

    line = json.loads((tmp_path / "s_ts.jsonl").read_text().strip())
    ts = datetime.fromisoformat(line["timestamp"])
    assert ts.tzinfo is not None  # must be UTC-aware
