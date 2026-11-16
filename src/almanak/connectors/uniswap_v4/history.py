"""Archival pool observations and continuous initialized-boundary evidence."""

from eth_abi import decode
from pydantic import model_validator

from almanak.backtest.history import LogScope
from almanak.connectors.uniswap_v4.deployments import historical_addresses, verify_identities
from almanak.connectors.uniswap_v4.hooks import require_reviewed_code
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.pool import pool_id, read_pool
from almanak.connectors.uniswap_v4.reader import fee_growth
from almanak.connectors.uniswap_v4.receipts import MODIFY
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Block, Model, UInt


class BoundaryEvidence(Model):
    target: Target
    first: Block
    last: Block
    initial_gross: tuple[UInt, UInt] | None
    final_gross: tuple[UInt, UInt] | None
    events: tuple[dict, ...]
    contracts: tuple[ContractIdentity, ...]
    inputs: tuple[str, ...]

    @model_validator(mode="after")
    def scope(self):
        if self.first.chain != self.target.fund.chain or self.last.chain != self.first.chain:
            raise ValueError("boundary evidence must retain the target chain and environment")
        if self.last.number < self.first.number:
            raise ValueError("boundary evidence requires an increasing block interval")
        if self.target.band is not None:
            # A band has no fixed boundary to measure; the prepared book reconciles every
            # retained event against each pair of consecutive samples instead.
            if self.initial_gross is not None or self.final_gross is not None:
                raise ValueError("band evidence cannot claim fixed boundary measurements")
            for event in self.events:
                modification(event)
        elif self.target.range is not None:
            if self.initial_gross is None or self.final_gross is None:
                raise ValueError("LP preflight requires both archival tick measurements")
            if continuous_gross(self.target, self.initial_gross, self.events) != self.final_gross:
                raise ValueError("retained boundary events do not reconcile with tick measurements")
        elif self.initial_gross is not None or self.final_gross is not None or self.events:
            raise ValueError("trading preflight cannot claim range evidence")
        return self


def refuse_band(target):
    # The detailed historical model reads the growth of one fixed range from archival
    # state at each action; a band target has none. Economic mode prepares band growth.
    if target.band is not None:
        raise ValueError(
            "banded liquidity allocations are not supported in historical_path backtests; "
            "select the economic model"
        )


def modification(event):
    """The (lower, upper, signed liquidity) change of one retained ModifyLiquidity log."""
    lower, upper, change, _ = decode(
        ("int24", "int24", "int256", "bytes32"), bytes.fromhex(event["data"][2:])
    )
    if lower >= upper:
        raise DataError("missing_history", "liquidity event has an invalid tick interval")
    return lower, upper, change


def log_scope(target):
    return LogScope(address=target.pool.pool_manager, topics=(MODIFY, pool_id(target.pool)))


def contract_identities(reader, target, block):
    authority = AuthorityReader(reader, block)
    observed = tuple(authority.identity(a) for a in sorted(historical_addresses(target)))
    verify_identities(target, observed)
    # History simulates only reviewed hook code; a changed hook refuses the window.
    require_reviewed_code(authority, target.pool)
    return observed


def tick_gross(reader, target, block):
    if target.range is None:
        raise ValueError("boundary evidence requires an explicit range")
    return tuple(
        reader.call(
            target.pool.state_view,
            "getTickLiquidity(bytes32,int24)",
            block,
            inputs=("bytes32", "int24"),
            args=(bytes.fromhex(pool_id(target.pool)[2:]), tick),
            outputs=("uint128", "int128"),
        )[0]
        for tick in (target.range.lower, target.range.upper)
    )


def liquidity_logs(history, target, first, last):
    scope = log_scope(target)
    events = []
    for low in range(first.number + 1, last.number + 1, 2000):
        high = min(last.number, low + 1999)
        chunk = history.reader._rpc(
            "eth_getLogs",
            [
                {
                    "address": scope.address,
                    "topics": list(scope.topics),
                    "fromBlock": hex(low),
                    "toBlock": hex(high),
                }
            ],
        )
        if not isinstance(chunk, list):
            raise DataError(
                "missing_history", "liquidity log evidence is not a complete event list"
            )
        for event in chunk:
            number = int(event["blockNumber"], 16)
            if not low <= number <= high or event.get("removed", False):
                raise DataError(
                    "missing_history", "liquidity log is outside its canonical interval"
                )
            if event["blockHash"] != history.header(number).hash:
                raise DataError("changed_history", "liquidity log has a noncanonical block")
            if event["address"].lower() != scope.address or event["topics"][:2] != list(
                scope.topics
            ):
                raise DataError("source_mismatch", "liquidity event identifies another pool")
        events.extend(chunk)
    return ordered_events(events)


def ordered_events(events):
    def key(event):
        return int(event["blockNumber"], 16), int(event["logIndex"], 16)

    if len({key(e) for e in events}) != len(events):
        raise DataError("missing_history", "historical liquidity evidence contains duplicate logs")
    return tuple(sorted(events, key=key))


def continuous_gross(target, initial, events):
    if min(initial) <= 0:
        raise DataError("unsupported_history", "LP boundaries are not initialized at window start")
    gross = list(initial)
    boundaries = (target.range.lower, target.range.upper)
    for event in events:
        lower, upper, change = modification(event)
        for index, tick in enumerate(boundaries):
            if tick in (lower, upper):
                gross[index] += change
                if not 0 < gross[index] < 2**128:
                    raise DataError(
                        "unsupported_history",
                        "LP boundary cleared or reinitialized during the consumed window",
                    )
    return tuple(gross)


def preflight(history, target, schedule):
    first = history.floor(schedule.start)
    last = history.floor(schedule.deadline)
    history.reader.used.clear()
    history.pin(first)
    contracts = contract_identities(history.reader, target, first)
    read_pool(history.reader, target.pool, first)
    initial = tick_gross(history.reader, target, first) if target.range else None
    events = liquidity_logs(history, target, first, last) if target.liquidity_enabled else ()
    history.pin(last)
    final = tick_gross(history.reader, target, last) if target.range else None
    if target.range and continuous_gross(target, initial, events) != final:
        raise DataError(
            "missing_history", "liquidity logs do not reconcile with archival tick state"
        )
    if contract_identities(history.reader, target, last) != contracts:
        raise DataError(
            "unsupported_revision", "historical contract identity changed during the window"
        )
    return BoundaryEvidence(
        target=target,
        first=first,
        last=last,
        initial_gross=initial,
        final_gross=final,
        events=events,
        contracts=contracts,
        inputs=tuple(sorted(history.reader.used)),
    )


def market(history, target, timestamp):
    refuse_band(target)
    anchor = history.floor(timestamp)
    history.reader.used.clear()
    block = history.pin(anchor)
    contract_identities(history.reader, target, block)
    pool = read_pool(history.reader, target.pool, block)
    growth = fee_growth(history.reader, target, block) if target.range else None
    return pool, growth
