"""`almanak markets`: what each market in config.yaml resolves to, for the user to check before
approving a strategy. Each market's contracts, tokens and ids, under the field that names them,
read at one pinned block, with each token's symbol and decimals. Each market resolves through
its connector's author binding (`configuration.authoring`), the resolver `almanak check` uses;
nothing is priced and no history is read, so it is quick.
"""

import json
import re
from pathlib import Path

import yaml

ADDRESS = re.compile(r"^0x(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")


def command(args) -> int:
    record = resolve(args)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(record, indent=1) + "\n")
    summary = {"out": str(args.out), "markets": len(record["markets"])}
    if record.get("skipped"):
        summary["skipped"] = record["skipped"]
    print(json.dumps(summary, indent=1))
    return 0


def resolve(args) -> dict:
    """{"markets": [...], "assets": {...}, "blocks": {chain: {number, hash}}, "skipped": {...}}.

    Each chain is pinned once and all its markets resolve at that block, which the record
    names, so markets on one chain never read different states."""
    from almanak.ax import observation_reader
    from almanak.caip import ChainId

    document = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    resolved: dict[str, dict] = {}
    assets: dict[str, dict] = {}
    blocks: dict[str, dict] = {}
    skipped: dict[str, str] = {}
    chains: dict[str, list] = {}
    for name, market in (document.get("markets") or {}).items():
        if not isinstance(market, dict):
            skipped[name] = "not a market: expected a mapping with connector and chain"
            continue
        binding = bound(market.get("connector"))
        if binding is None:
            skipped[name] = f"{market.get('connector')}: no resolver for this connector"
            continue
        chains.setdefault(str(market.get("chain")), []).append((name, market, binding))
    for chain, markets in chains.items():
        try:
            with observation_reader(args, ChainId(chain)) as reader:
                head = reader.pin_block(args.block)
                blocks[chain] = {"number": head.number, "hash": str(head.hash)}
                for name, market, binding in markets:
                    try:
                        facts = resolution(binding, reader, head, market)
                    except Exception as error:  # one unreadable market must not lose the others
                        skipped[name] = failure(error)
                        continue
                    resolved[name] = identity(market["connector"], chain, facts)
                traded = [
                    c["asset"]
                    for name, _, _ in markets
                    if name in resolved
                    for c in resolved[name]["contracts"]
                    if "asset" in c
                ]
                for asset in [*config_assets(document, chain), *traded]:
                    if asset not in assets:
                        assets[asset] = asset_meta(reader, head, asset)
        except Exception as error:  # an unreachable chain must not lose the others
            for name, _, _ in markets:
                if name not in resolved:
                    skipped.setdefault(name, failure(error))
    assets = {asset: meta for asset, meta in assets.items() if meta}
    record = {"markets": named(resolved, assets), "assets": assets, "blocks": blocks}
    if skipped:
        record["skipped"] = skipped
    return record


def failure(error) -> str:
    return f"{type(error).__name__}: {error}"[:300]


def bound(connector):
    """The connector's author binding, or None when it has none."""
    from almanak.configuration.authoring import binding_for
    from almanak.configuration.yaml import ConfigurationError

    try:
        return binding_for(connector)
    except ConfigurationError:
        return None


def resolution(binding, reader, head, market) -> dict:
    """The market as its connector resolves it: the config entry is its market selection."""
    selection = binding.selection.model_validate_json(json.dumps(market))
    resolved = binding.resolve(reader, selection, head)
    if binding.admit is not None:
        binding.admit(resolved)
    return resolved.summary() if hasattr(resolved, "summary") else resolved.model_dump(mode="json")


def identity(connector: str, chain: str, resolution: dict) -> dict:
    """What a market resolved to, from its reader's own resolution (no per-connector names):
    each contract, token and id under the field that names it (`pool`, `asset`, `oracle`, ...;
    an `address` or `identifier` is named by its parent). Tokens in a list (a pool's
    currencies) count; other listed addresses (every contract a resolver checked) do not.
    The request it answered and the pinned block are left out. A contract in several roles
    (one token as a GMX market's index, long and wrapped native) is listed once with each."""
    found: dict[str, dict] = {}

    def add(address: str, role: str, **extra):
        entry = found.setdefault(address, {"role": role, "address": address, **extra})
        # A generic field (`asset`) repeating a specific one (`loan_asset`) adds no role.
        if not any(r == role or r.endswith(f"_{role}") for r in entry["role"].split(", ")):
            entry["role"] += f", {role}"
        entry.update({k: v for k, v in extra.items() if k not in entry})

    def visit(node, role: str, depth: int, listed: bool):
        if isinstance(node, str):
            if node.startswith(f"{chain}/erc20:"):
                add(node.split("/erc20:")[1].lower(), role, asset=node)
            elif not listed and depth <= 2 and ADDRESS.match(node):
                add(node.lower(), role)
        elif isinstance(node, dict):
            for key, value in node.items():
                if key in ("selection", "block"):
                    continue
                if role and key in ("address", "identifier"):
                    child = role
                else:
                    child = f"{role}.{key}" if role else key
                visit(value, child, depth + 1, listed)
        elif isinstance(node, (list, tuple)):
            for value in node:
                visit(value, role, depth, True)

    visit(resolution, "", 0, False)
    return {"connector": connector, "chain": chain, "contracts": list(found.values())}


def named(resolved: dict[str, dict], assets: dict[str, dict]) -> list[dict]:
    """Each market with its tokens' symbols; names that resolved to the same contracts (two
    legs of one lending market) share one entry."""
    markets: dict[str, dict] = {}
    for name, market in resolved.items():
        contracts = [
            {**c, "symbol": assets[c["asset"]]["symbol"]}
            if (assets.get(c.get("asset")) or {}).get("symbol")
            else c
            for c in market["contracts"]
        ]
        entry = {**market, "contracts": contracts}
        key = json.dumps(entry, sort_keys=True)
        if key in markets:
            markets[key]["name"] += f", {name}"
        else:
            markets[key] = {"name": name, **entry}
    return list(markets.values())


def config_assets(document, chain) -> list[str]:
    """CAIP-19 assets the config names on `chain`: markets, capital and inventory."""
    found = []

    def visit(node):
        if isinstance(node, dict):
            for value in node.values():
                visit(value)
        elif isinstance(node, list):
            for value in node:
                visit(value)
        elif isinstance(node, str) and node.startswith(f"{chain}/erc20:"):
            found.append(node)

    visit(document)
    return list(dict.fromkeys(found))


def asset_meta(reader, head, asset) -> dict:
    from almanak.ax_lending_markets import token_symbol

    try:
        symbol = token_symbol(reader, head, asset)
    except Exception:  # metadata is a label: a failed read never fails the market's evidence
        symbol = None
    try:
        (decimals,) = reader.call(asset.split("/erc20:")[1], "decimals()", head, outputs=("uint8",))
    except Exception:
        decimals = None
    meta: dict[str, str | int] = {}
    if symbol:
        meta["symbol"] = symbol
    if decimals is not None:
        meta["decimals"] = int(decimals)
    return meta
