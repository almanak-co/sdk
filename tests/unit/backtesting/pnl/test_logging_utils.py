"""Tests for PnL backtesting logging utilities."""

import json
import logging
import sys
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.backtesting.pnl.logging_utils import JSONLogFormatter, log_trade_execution

LOGGER_NAME = "tests.unit.backtesting.pnl.logging_utils"
TRADE_TIME = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def _make_log_record(**kwargs) -> logging.LogRecord:
    record = logging.LogRecord(
        name=LOGGER_NAME,
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="trade %s",
        args=("executed",),
        exc_info=None,
    )
    for name, value in kwargs.items():
        setattr(record, name, value)
    return record


class TestJSONLogFormatter:
    def test_format_includes_standard_and_backtest_fields(self) -> None:
        record = _make_log_record(
            backtest_id="bt-12345678",
            phase="execution",
            duration_seconds=1.25,
            extra={"position_id": "pos-1"},
        )

        payload = json.loads(JSONLogFormatter().format(record))

        assert payload["level"] == "INFO"
        assert payload["logger"] == LOGGER_NAME
        assert payload["message"] == "trade executed"
        assert payload["backtest_id"] == "bt-12345678"
        assert payload["phase"] == "execution"
        assert payload["duration_seconds"] == 1.25
        assert payload["extra"] == {"position_id": "pos-1"}
        assert payload["timestamp"].endswith("+00:00")

    def test_format_omits_empty_optional_fields(self) -> None:
        record = _make_log_record(extra={})

        payload = json.loads(JSONLogFormatter().format(record))

        assert "backtest_id" not in payload
        assert "phase" not in payload
        assert "duration_seconds" not in payload
        assert "extra" not in payload
        assert "exception" not in payload

    def test_format_includes_exception_text(self) -> None:
        try:
            raise ValueError("bad fill")
        except ValueError:
            record = _make_log_record()
            record.exc_info = sys.exc_info()

        payload = json.loads(JSONLogFormatter().format(record))

        assert "ValueError: bad fill" in payload["exception"]


def test_json_trade_log_preserves_measured_zero_optional_fields(caplog):
    """Measured zero optional values should not be serialized as missing."""
    logger = logging.getLogger(LOGGER_NAME)

    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        log_trade_execution(
            logger=logger,
            backtest_id="bt-12345678",
            timestamp=TRADE_TIME,
            intent_type="SWAP",
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            amount_usd=Decimal("100"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            executed_price=Decimal("0"),
            mev_cost_usd=Decimal("0"),
            json_format=True,
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["executed_price"] == "0"
    assert payload["mev_cost_usd"] == "0"


def test_json_trade_log_keeps_none_optional_fields_missing(caplog):
    """None still represents unmeasured/disabled optional values."""
    logger = logging.getLogger(LOGGER_NAME)

    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        log_trade_execution(
            logger=logger,
            backtest_id="bt-12345678",
            timestamp=TRADE_TIME,
            intent_type="SWAP",
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            amount_usd=Decimal("100"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            executed_price=None,
            mev_cost_usd=None,
            json_format=True,
        )

    payload = json.loads(caplog.records[-1].message)

    assert payload["executed_price"] is None
    assert payload["mev_cost_usd"] is None


def test_text_trade_log_preserves_measured_zero_optional_fields(caplog):
    """Text trade logs should include measured zero price/MEV values when supplied."""
    logger = logging.getLogger(LOGGER_NAME)

    with caplog.at_level(logging.DEBUG, logger=LOGGER_NAME):
        log_trade_execution(
            logger=logger,
            backtest_id="bt-12345678",
            timestamp=TRADE_TIME,
            intent_type="SWAP",
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            amount_usd=Decimal("100"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            executed_price=Decimal("0"),
            mev_cost_usd=Decimal("0"),
            json_format=False,
        )

    message = caplog.records[-1].message

    assert "price=0.000000" in message
    assert "mev=$0.00" in message
