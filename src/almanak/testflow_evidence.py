"""Teardown and closure evidence for the test verdict.

What each teardown decision returned, every intent teardown executed in order, and what
each fund holds when the lifecycle ends: the facts that tell a clean closure from one
that held with obligations open or left the fund in another asset than it started in.
"""

from decimal import Decimal
from pathlib import Path

import httpx

BALANCE_OF = "0x70a08231"


def teardown_decisions(root, verdict) -> list[dict]:
    """Each logged teardown decision; the last, when it held, names what its closure did.

    A teardown hold is the strategy's claim that nothing is left to do, which the runtime
    then checks. `closure` is `closed` when the deployment closed on it, else
    `not_closed` with the teardown failure's kind as `refusal` when one was recorded.
    """
    from almanak.decision_facts import teardown_evaluations

    rows = teardown_evaluations(Path(root) / "logs" / "coordinator.log")
    if rows and rows[-1]["result"] == "hold":
        if verdict["closed"]:
            rows[-1]["closure"] = "closed"
        else:
            rows[-1]["closure"] = "not_closed"
            kinds = [f["kind"] for f in verdict["failures"] if f["stage"] == "teardown"]
            if kinds:
                rows[-1]["refusal"] = kinds[0]
    return rows


def teardown_sequence(state) -> list[dict]:
    """Every intent teardown executed completely, in order, repeats included."""
    from almanak.testflow import by_teardown, completed

    rows = []
    for outcome in state.outcomes:
        if not completed(outcome) or not by_teardown(outcome):
            continue
        intent = outcome.decision.intent
        rows.append(
            {
                "intent": getattr(intent, "kind", type(intent).__name__),
                "allocation": getattr(getattr(intent, "target", None), "allocation", None),
            }
        )
    return rows


def holdings(root, state) -> list[dict]:
    """Each fund's balance of every configured asset, read from its fork node at the end.

    The balance is the fund's whole on-chain balance, untracked amounts such as the
    fixture's funding buffer included; a run without a workspace reports none.
    """
    from almanak.workspace import read

    try:
        processes = read(Path(root))["processes"]
    except (OSError, ValueError):
        return []
    config = state.config
    origins = {b.chain: b.origin.chain.chain_id for b in getattr(config, "fork_bindings", ())}
    metadata = getattr(config, "asset_metadata", None) or {}
    rows = []
    for binding in config.accounts.bindings:
        fund = binding.profile.fund
        chain = origins.get(fund.chain, fund.chain.chain_id)
        row: dict = {"account": binding.name, "chain": str(chain), "balances": []}
        entry = processes.get(f"fork-{fund.chain.chain_id}")
        if entry is None:
            row["error"] = "the run has no fork node for this fund"
        else:
            assets = sorted((a for a in metadata if a.chain == chain), key=str)
            try:
                row["balances"] = [
                    held(entry["endpoint"], asset, metadata[asset], str(fund.address))
                    for asset in assets
                ]
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                row["error"] = f"balance read failed: {exc}"
        rows.append(row)
    return rows


def held(endpoint, asset, metadata, address) -> dict:
    from almanak.gateway.fork_fixture import Rpc

    rpc = Rpc(endpoint)
    if asset.namespace == "erc20":
        call = {"to": asset.reference, "data": BALANCE_OF + address[2:].lower().rjust(64, "0")}
        amount = int(rpc("eth_call", [call, "latest"]), 16)
    else:
        amount = int(rpc("eth_getBalance", [address, "latest"]), 16)
    row = {"asset": str(asset), "amount": str(amount)}
    if metadata.symbol is not None:
        row["symbol"] = metadata.symbol
    row["quantity"] = format(Decimal(amount).scaleb(-metadata.decimals).normalize(), "f")
    return row
