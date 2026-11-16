"""One cash ledger with connector-owned measurements and explicit cost contributions."""

import json
from typing import Any

from almanak.connectors.aave_v3.economic_reporting import report as lending_report
from almanak.connectors.aerodrome.economic_reporting import report as aerodrome_report
from almanak.connectors.aerodrome_slipstream.economic_reporting import (
    report as slipstream_report,
)
from almanak.connectors.composed.aave_v4_economic import report as aave_v4_report
from almanak.connectors.composed.economic_book import MODEL
from almanak.connectors.composed.economic_model import component_state
from almanak.connectors.composed.v4_reporting import position_only
from almanak.connectors.compound_v3.economic_reporting import report as compound_report
from almanak.connectors.curve.economic_reporting import report as curve_report
from almanak.connectors.dex_economic_reporting import payer_reports as dex_payers
from almanak.connectors.gmx_v2.economic_reporting import report as gmx_report
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.economic_reporting import report as morpho_report
from almanak.connectors.pancakeswap_v3.economic_reporting import report as pancake_report
from almanak.connectors.sparklend.economic_reporting import report as spark_report
from almanak.connectors.uniswap_v3.economic_reporting import report as exchange_report
from almanak.connectors.uniswap_v4.economic_binding import UniswapEconomic
from almanak.effects import WalletAsset


def payer_reports(components):
    payers: dict[WalletAsset, dict[str, Any]] = {}
    for connector, report in components.items():
        for contribution in report.pop("gas_payers"):
            key = WalletAsset.model_validate_json(json.dumps(contribution["wallet"]))
            payer = payers.setdefault(
                key,
                {"wallet": contribution["wallet"], "fund_accounts": set(), "contributions": []},
            )
            payer["fund_accounts"].update(contribution["fund_accounts"])
            payer["contributions"].append(
                {
                    "connector": connector,
                    **{k: contribution[k] for k in ("cost", "measurement_status")},
                }
            )
    return [{**payer, "fund_accounts": sorted(payer["fund_accounts"])} for payer in payers.values()]


def report(state, model):
    components = {}
    shared_fields = {
        "run_id",
        "mode",
        "simulation_time",
        "window",
        "closed",
        "pending",
        "completed_intents",
        "delivered_outcomes",
        "delivered_events",
        "decision_interval",
        "decision_evaluations",
        "nominal_decision_opportunities",
        "wallets",
        "strategy_sha256",
    }
    for connector, driver in model.components.items():
        native_report = {
            ConnectorId("aave_v3"): lending_report,
            ConnectorId("aave_v4"): aave_v4_report,
            ConnectorId("sparklend"): spark_report,
            ConnectorId("uniswap_v3"): exchange_report,
            ConnectorId("pancakeswap_v3"): pancake_report,
            ConnectorId("aerodrome"): aerodrome_report,
            ConnectorId("aerodrome_slipstream"): slipstream_report,
            ConnectorId("uniswap_v4"): UniswapEconomic.report,
            ConnectorId("morpho_blue"): morpho_report,
            ConnectorId("compound_v3"): compound_report,
            ConnectorId("curve"): curve_report,
            ConnectorId("gmx_v2"): gmx_report,
        }[connector]
        native = native_report(component_state(state, connector), driver)
        # Component slices share one cash ledger, so a per-component quote equity
        # would value the same shared cash in every component. The composed report
        # drops the run-level valuation layer until it can value the joint run,
        # and resets the remaining valuation fields so no component advertises a
        # quote valuation it does not carry.
        for layer_field in (
            "equity_curve",
            "price_series",
            "metrics",
            "valuation_quote",
            "valuation_prices",
        ):
            native.pop(layer_field, None)
        if "net_quote_return" in native:
            native["net_quote_return"] = None
        if "valuation" in native:
            native["valuation"] = "asset_units_only"
        if connector == ConnectorId("uniswap_v4"):
            native["gas_payers"] = dex_payers(component_state(state, connector), driver)
            native["positions"] = [position_only(position) for position in native["positions"]]
        components[connector.value] = {k: v for k, v in native.items() if k not in shared_fields}
    payers = payer_reports(components)
    return {
        "run_id": state.run_id,
        "model": MODEL,
        "mode": state.mode,
        "simulation_time": int(state.time),
        "window": state.schedule.model_dump(mode="json"),
        "closed": model.closed(state, model.observe(state)),
        "pending": state.pending is not None,
        "completed_intents": len(state.completed),
        "delivered_outcomes": state.delivered,
        "delivered_events": state.events_delivered,
        "decision_interval": state.interval.report(),
        "decision_evaluations": state.evaluations,
        "nominal_decision_opportunities": state.schedule.opportunities(
            state.interval.effective.seconds
        ),
        "wallets": [b.model_dump(mode="json") for b in state.portfolio.balances],
        "gas_payers": payers,
        "components": components,
        "strategy_sha256": model.strategy.sha256,
        "assumptions": [
            "connector currency views project shared fund cash and are not additional assets",
            "closure requires discharged protocol obligations and permits remaining shared cash",
            "payer costs retain connector contributions; excluded costs are not measured zero",
        ],
    }
