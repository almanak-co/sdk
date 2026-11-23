"""Generate a fork operator file from the author configuration.

The generated file has the shape of `examples/operators/*.fork.yaml`: one Alchemy
provider per authored chain, placeholder account bindings (fork provisioning
generates the actual fixture accounts), anchors pinned at one block, Safe/Roles
fixture funds sized from the online check's `funding` block and fork gas ceilings
equal to the authored `strategy.gas`. It holds no credentials.
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path

from almanak.caip import ChainId
from almanak.check_warnings import warning
from almanak.gateway.evm import native_metadata
from almanak.gateway.production_config import op_family

PROVIDER_ENV = "ALCHEMY_API_KEY"
# Token funding exceeds the stated requirement by this much, at least one base unit.
FUND_BUFFER_BPS = 100
OP_NOTE = (
    "{chain} is an OP-family chain: forks cannot bound its L1 data and operator fees, so "
    "authored gas ceilings are refused there. This file runs the fork gasless "
    "(fork.author_gas: unenforced); the authored strategy.gas is retained but not enforced."
)

LAG_NOTE = (
    "{chain} anchors {seconds} seconds behind the chain head: a fork test settles this "
    "strategy's keeper-executed requests with authentic reports the chain has already "
    "published, so fork time must stay behind the head."
)


def command(args) -> int:
    from almanak.checks import author_inputs
    from almanak.presentation import emit

    if args.output is not None and args.output.exists() and not args.force:
        raise ValueError(f"{args.output} exists; pass --force to replace it")
    document, _ = author_inputs(args)
    chains = sorted({r.chain for r in document.inputs.requirements}, key=str)
    if args.block is not None and len(chains) != 1:
        raise ValueError("--block pins one anchor; this strategy spans several chains")
    lags = {chain: anchor_lag(document.inputs, chain) for chain in chains}
    anchors = [
        pinned_anchor(
            chain, args.env_file, args.block, lags[chain], anchor_bindings(document.inputs, chain)
        )
        for chain in chains
    ]
    plan = FixturePlan(document.inputs, chains)
    if args.block is None:
        plan.notes.extend(
            warning("fork_anchor", "fork.anchors", LAG_NOTE.format(chain=chain, seconds=lag))
            for chain, lag in lags.items()
            if lag
        )
    funding = draft_funding(args, plan.document(anchors, None))
    operator = plan.document(anchors, funding)
    validate(document, operator)
    text = render(operator, plan.notes)
    if args.output is None:
        sys.stdout.write(text)
        return 0
    args.output.write_text(text)
    summary = {
        "output": str(args.output.resolve()),
        "anchors": operator["fork"]["anchors"],
        "notes": [note["message"] for note in plan.notes],
    }
    emit(summary, args)
    return 0


def anchor_bindings(inputs, chain) -> list:
    """Each connector's fork binding on `chain`, with its market selections there."""
    from almanak.connectors.composition import connector_from_wire

    groups: dict[str, list] = {}
    for market in inputs.markets.values():
        if market.chain == chain:
            groups.setdefault(str(market.connector), []).append(market)
    return [
        (connector_from_wire({"connector": connector}).fork, selections)
        for connector, selections in groups.items()
    ]


def anchor_lag(inputs, chain) -> int:
    """Seconds behind the chain head a fork of `chain` starts, the most its connectors ask.

    A connector whose requests a fork keeper settles with authentic reports published on
    the chain needs fork time to stay behind the head, or those reports do not exist yet.
    """
    return max(
        (getattr(fork, "anchor_lag_seconds", 0) for fork, _ in anchor_bindings(inputs, chain)),
        default=0,
    )


def pinned_anchor(chain, env_file, block, lag=0, bindings=()) -> dict:
    """The fork anchor: `--block`, the head, or `lag` seconds behind it, moved earlier to
    where a connector's fork binding finds its replayed publications dense."""
    from almanak.gateway.block_time import first_block_at
    from almanak.gateway.read_endpoints import mainnet_reader, read_endpoint

    with mainnet_reader(chain, read_endpoint(chain, env_file)) as reader:
        if block is None and lag:
            head = reader.pin_block("latest")
            start = head.timestamp - lag
            for fork, selections in bindings:
                choose = getattr(fork, "anchor_time", None)
                chosen = None if choose is None else choose(reader, selections, start)
                if chosen is not None:
                    start = min(start, chosen)
            pinned = first_block_at(reader, start, head=head)
        else:
            pinned = reader.pin_block("latest" if block is None else block)
    return {"chain": str(chain), "height": pinned.number, "reference": pinned.hash}


class FixturePlan:
    """Accounts grouped by chain and gas policy; each group shares one fixture operator."""

    def __init__(self, inputs, chains):
        self.inputs = inputs
        self.chains = chains
        self.notes: list[dict] = []
        policies = {
            account: (name, policy)
            for name, policy in inputs.strategy.gas.items()
            for account in policy.accounts
        }
        groups: dict[tuple, list[str]] = {}
        for account, value in inputs.accounts.items():
            policy = policies.get(account)
            key = (str(value.chain), policy[0] if policy else None)
            groups.setdefault(key, []).append(account)
        self.groups = groups
        self.plain = plain_accounts(inputs)
        self.policies = dict(inputs.strategy.gas)
        self.unenforced = op_family(
            tuple(policy.resolve(name) for name, policy in self.policies.items())
        )
        if self.unenforced:
            for chain in sorted({str(p.chain) for p in self.policies.values()}):
                self.notes.append(warning("fork_gas", "strategy.gas", OP_NOTE.format(chain=chain)))

    def operator_name(self, key) -> str:
        names = sorted(self.groups, key=lambda group: (group[0], group[1] or ""))
        return "operator" if len(names) == 1 else f"operator_{names.index(key)}"

    def document(self, anchors, funding) -> dict:
        accounts = list(self.inputs.accounts.items())
        bindings = [
            {
                "name": name,
                "profile": {
                    "kind": "eoa",
                    "fund": {
                        "chain": {"chain_id": str(value.chain), "environment": "mainnet"},
                        "address": "0x" + format(index + 1, "040x"),
                    },
                },
            }
            for index, (name, value) in enumerate(accounts)
        ]
        fork = {
            "anchors": anchors,
            "funds": [
                self.fund(key, name, funding)
                for key, names in self.groups.items()
                for name in names
            ],
            "operators": [
                self.operator(key, funding)
                for key, names in self.groups.items()
                if not set(names) <= self.plain
            ],
        }
        if self.unenforced:
            fork["author_gas"] = "unenforced"
        else:
            ceilings = [self.ceiling(key) for key in self.groups if key[1] is not None]
            if ceilings:
                fork["gas"] = ceilings
        return {
            "schema_version": "operator-gateway-config-v1",
            "modes": ["fork"],
            "networks": [
                {"chain": str(chain), "provider": {"kind": "alchemy", "api_key_env": PROVIDER_ENV}}
                for chain in self.chains
            ],
            "accounts": {"bindings": bindings},
            "fork": fork,
        }

    def fund(self, key, account, funding) -> dict:
        assets = [] if funding is None else funded_assets(funding, account)
        if account in self.plain:
            return {
                "kind": "eoa",
                "account": account,
                "native_balance": self.operator(key, funding)["native_balance"],
                "assets": assets,
            }
        return {
            "kind": "safe_roles",
            "account": account,
            "operator": self.operator_name(key),
            "assets": assets,
        }

    def operator(self, key, funding) -> dict:
        asset, native = native_metadata(ChainId(key[0]))
        amount = 10**native.decimals
        if funding is not None:
            needed = payer_need(funding, self.groups[key])
            amount = max(amount, 2 * needed)
        return {
            "name": self.operator_name(key),
            "native_balance": {"asset": str(asset), "amount": amount},
        }

    def ceiling(self, key) -> dict:
        name = key[1]
        resolved = self.policies[name].resolve(name)
        native = str(resolved.asset)
        return {
            "accounts": self.groups[key],
            "max_gas_price_wei": resolved.max_gas_price_wei,
            "max_gas_limit": resolved.max_gas_limit,
            "max_transaction_fee": {
                "asset": native,
                "amount": resolved.max_transaction_fee.amount,
            },
            "allow_replacement": resolved.replacement is not None,
        } | (
            {}
            if resolved.total_budget is None
            else {"total_budget": {"asset": native, "amount": resolved.total_budget.amount}}
        )


def plain_accounts(inputs) -> set[str]:
    """Accounts the fixture provisions as plain EOAs instead of Safes.

    A fixture account is a Safe with delegated permissions unless a connector that acts
    from it supports only plain accounts on that chain. An account that only receives a
    bridge holds no venue authority, so it follows its chain's sending accounts.
    """
    from almanak.connectors.composition import connector_from_wire

    plain = set()
    for position in (*inputs.strategy.allocations.values(), *inputs.strategy.debts.values()):
        market = inputs.markets[position.market]
        support = connector_from_wire({"connector": str(market.connector)}).support_for(
            market.chain
        )
        if "safe_roles" not in support.account_profiles:
            plain |= {position.account, getattr(position, "destination_account", position.account)}
    return plain


def funded_assets(funding, account) -> list[dict]:
    entry = next(a for a in funding["accounts"] if a["account"] == account)
    totals: dict[str, int] = {}
    for item in entry["assets"]:
        totals[item["asset"]] = totals.get(item["asset"], 0) + int(item["amount_base_units"])
    return [
        {"asset": asset, "amount": amount + max(1, amount * FUND_BUFFER_BPS // 10000)}
        for asset, amount in totals.items()
        if amount > 0
    ]


def payer_need(funding, accounts) -> int:
    """The native a group's payer needs: its entry minimum plus the whole gas budget.

    Without a `total_budget` there is no lifetime amount to fund, so the payer gets only
    its entry minimum; a long fork run that states no budget must fund gas explicitly.
    """
    need = 0
    for payer in funding["payers"]:
        if set(payer["accounts"]) & set(accounts):
            budget = int((payer.get("gas") or {}).get("total_budget_base_units", 0))
            need = max(need, int(payer["entry_minimum_base_units"]) + budget)
    return need


def draft_funding(args, draft) -> dict:
    """The funding block of an online fork check against the unfunded draft."""
    from almanak.checks import CheckFailure, check

    with tempfile.TemporaryDirectory(prefix="almanak-fork-config-") as temporary:
        path = Path(temporary) / "draft.fork.yaml"
        path.write_text(json.dumps(draft))
        namespace = argparse.Namespace(
            **{
                **vars(args),
                "fork": True,
                "production": False,
                "online": True,
                "normalized": False,
                "backtest_config": None,
                "gateway_config": path,
            }
        )
        try:
            report = check(namespace)
        except CheckFailure as failure:
            if "funding" not in failure.detail:
                raise
            return failure.detail["funding"]
    if "funding" not in report:
        raise RuntimeError("the online fork check reported no funding block")
    return report["funding"]


def validate(document, operator) -> None:
    from almanak.configuration.fork_preparation import require_fork_policy
    from almanak.gateway.operator_config import OperatorConfig

    config = OperatorConfig.model_validate_json(json.dumps(operator))
    require_fork_policy(document, config)


def render(operator, notes) -> str:
    import yaml

    header = [
        "# Fork operator file generated by `almanak test config` from the author configuration.",
        "# Provider credentials stay in the selected --env-file. Account bindings are",
        "# placeholders: fork provisioning generates the actual fixture accounts.",
        *(f"# Note: {note['message']}" for note in notes),
    ]
    body = yaml.safe_dump(operator, sort_keys=False, default_flow_style=False)
    return "\n".join(header) + "\n" + body
