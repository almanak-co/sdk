"""Resumable archive acquisition through the dedicated historical gateway."""

import fcntl
import hashlib
import json
import secrets
import threading
import time
from contextlib import ExitStack

from eth_utils import keccak

from almanak.backtest import progress
from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.collection import bounded_map
from almanak.backtest.config import EconomicBacktestConfig, financial_policy
from almanak.backtest.dataset import DataRequirement, RawEvidence, file_digest
from almanak.backtest.economic_acquisition import accounting_requests, read_rates, read_risk
from almanak.backtest.economic_data import (
    MODEL,
    AccountingSource,
    PriceRecord,
    RateRecord,
    RiskRecord,
    rate_key,
    require_range,
    reserve_key,
)
from almanak.backtest.economic_oracles import OracleReader, artifact_for, contracts_for
from almanak.backtest.evm import EvmHistory
from almanak.backtest.gateway import ContractBinding, GatewayConfig
from almanak.backtest.history import Capture, HistoricalReader
from almanak.backtest.history_logs import MAX_LOG_BLOCKS, HistoricalLog, LogScope, read_logs
from almanak.backtest.prepared import (
    PROVISIONAL,
    AcquiredRange,
    DatasetWriter,
    Preparation,
    load_dataset,
    require_private_directory,
)
from almanak.backtest.pricing import CoinGeckoPrice, ProtocolPrice, price_binding
from almanak.backtest.strategy_prices import (
    gateway_authority,
    series_bindings,
    strategy_requirements,
)
from almanak.backtest.types import UnixTime
from almanak.backtest.vendor import VendorEvidence, price_records, requests_for, requirement_for
from almanak.connectors.aave_v4.deployments import verify_deployments
from almanak.connectors.aave_v4.domain import ReserveObservation
from almanak.gateway.auth import read_token
from almanak.triggers import IntervalSelection

# Files a public root retains in the dataset directory before acquisition starts.
RISK_EVENTS = (
    "Upgraded(address)",
    "SetSpokeImmutables(address,uint16)",
    "UpdateLiquidationConfig((uint128,uint64,uint16))",
    "UpdateReserveConfig(uint256,(uint24,bool,bool,bool,bool))",
    "AddDynamicReserveConfig(uint256,uint32,(uint16,uint32,uint16))",
    "UpdateDynamicReserveConfig(uint256,uint32,(uint16,uint32,uint16))",
    "UpdateReservePriceSource(uint256,address)",
)
ORACLE_EVENTS = {
    "ocr2": (
        "AnswerUpdated(int256,uint256,uint256)",
        "AddedAccess(address)",
        "RemovedAccess(address)",
        "CheckAccessEnabled()",
        "CheckAccessDisabled()",
    ),
    "dual": (
        "AnswerUpdated(int256,uint256,uint256)",
        "SecondaryRoundIdUpdated(uint32)",
        "PrimaryFeedUnlocked(uint32)",
        "CutoffTimeSet(uint32)",
    ),
    "proxy": ("AggregatorConfirmed(address,address)",),
    "cap": ("PriceCapUpdated(int256)",),
    "oracle": ("UpdateReserveSource(uint256,address)", "SetSpoke(address)"),
}


def topics(signatures):
    return tuple("0x" + keccak(text=s).hex() for s in signatures)


def scopes(reserves, first, last, include_oracles=True):
    chains = {r.chain for r in reserves}
    if len(chains) != 1:
        raise ValueError("event acquisition requires one explicit chain per reader")
    result: dict[str, tuple[str, ...]] = {r.spoke: RISK_EVENTS for r in reserves}
    result.update({r.hub: ("Upgraded(address)",) for r in reserves})
    if include_oracles:
        for row in contracts_for(next(iter(chains)).chain_id):
            result[row["address"]] = ORACLE_EVENTS[row["kind"]]
    return tuple(
        LogScope(address=target, topics=topics(signatures), first_block=first, last_block=last)
        for target, signatures in sorted(result.items())
    )


def requirement(key, start, end, unit, sample=None, max_age=None):
    return DataRequirement(
        key=key,
        start=UnixTime(start),
        end=UnixTime(end),
        semantics="sampled" if sample else "changes",
        sample_seconds=sample,
        max_age_seconds=max_age or (sample if sample else end - start) + 60,
        unit=unit,
        visibility="end_of_block",
        payload_revision=MODEL,
    )


def prepare(args):
    directory = args.output.resolve()
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "acquisition.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another coordinator owns this preparation") from None
        try:
            return prepare_locked(args)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def prepare_locked(args):
    from almanak.backtest.inputs import selected

    selection = selected(args)
    directory = args.output.resolve()
    author, settings = selection.resolve(directory, getattr(args, "env_file", None))
    if not isinstance(settings, EconomicBacktestConfig):
        raise ValueError("backtest-prepare requires explicit economic settings")
    IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    reserves = tuple(dict.fromkeys(a.reserve for a in (*author.allocations, *author.debts)))
    for reserve in reserves:
        require_range(reserve, settings.schedule.start, settings.schedule.deadline)
    if {h.chain for h in settings.history} != {r.chain for r in reserves}:
        raise ValueError("every prepared Aave market chain requires its explicit provider")
    validate_sources(settings, reserves)
    validate_strategy_dependencies(args, author, settings, reserves)
    provision(directory, settings, reserves, selection.resolution_facts)
    if (directory / "dataset.json").exists():
        if (job := progress.current()) is not None:
            job.discard(progress.phase_key("preparation"))
        with progress.phase("preparation.reuse", "Verifying cached dataset", 1.0, "phases"):
            manifest, reader = load_dataset(directory)
            reader.close()
            return manifest.model_dump(mode="json")
    if args.env_file is None and not (directory / "observations.sqlite").exists():
        raise ValueError(
            "missing acquisition credentials; supply --env-file or a completed --dataset"
        )
    started = time.perf_counter()
    if (job := progress.current()) is not None:
        job.begin(
            progress.phase_key("preparation.metadata"),
            "Resolving historical requirements",
            1.0,
            "phases",
        )
    acquire(directory, settings, reserves, args.env_file)
    manifest, reader = load_dataset(directory)
    reader.close()
    report = {
        "dataset": manifest.model_dump(mode="json"),
        "elapsed_seconds": time.perf_counter() - started,
        "provider_activity": capture_activity(directory),
        "phase_wall_seconds": phase_totals(directory),
        "cache_state": "cold_or_resumed_acquisition",
    }
    write_json(directory / "preparation-report.json", report)
    if (job := progress.current()) is not None:
        job.finish(progress.phase_key("preparation.finalization"))
    return report


def acquisition_request(settings, reserves, resolution_facts):
    request = {
        "reserves": [r.model_dump(mode="json") for r in reserves],
        "start": int(settings.schedule.start),
        "end": int(settings.schedule.deadline),
        "data": settings.data.model_dump(
            mode="json", exclude={"vendors": {"__all__": {"provider": {"collection"}}}}
        ),
        "providers": [h.model_dump(mode="json", exclude={"collection"}) for h in settings.history],
        "oracle_artifact_sha256": oracle_artifacts(reserves),
        "acquisition_revision": oracle_revision(reserves),
    }
    if resolution_facts is not None:
        # A dataset serves only runs built from the same resolved market facts.
        request["resolution_facts"] = resolution_facts
    return request


def provision(directory, settings, reserves, resolution_facts=None):
    require_private_directory(directory)
    directory.mkdir(parents=True, exist_ok=True)
    request = acquisition_request(settings, reserves, resolution_facts)
    path = directory / "acquisition.json"
    if path.exists():
        if json.loads(path.read_text()) != request:
            raise ValueError("preparation resume cannot change sources, policy or coverage")
    elif any(p.name not in PROVISIONAL for p in directory.iterdir()):
        raise ValueError("preparation requires a new or matching acquisition directory")
    if not path.exists():
        write_json(path, request)
    token = directory / "gateway-auth.token"
    if not token.exists():
        with token.open("x") as output:
            token.chmod(0o600)
            output.write(secrets.token_hex(32))
    prior = directory / "collection-history.jsonl"
    with prior.open("a") as output:
        output.write(
            json.dumps(
                {
                    "time": time.time(),
                    "limits": [h.collection.model_dump(mode="json") for h in settings.history],
                }
            )
            + "\n"
        )
    if (directory / "history.json").exists():
        binding = GatewayConfig.model_validate_json((directory / "history.json").read_text())
        write_json(
            directory / "history.json",
            gateway_authority(
                settings,
                binding.model_copy(
                    update={"providers": settings.history, "vendors": settings.data.vendors}
                ),
            ),
        )
        return
    config = gateway_authority(
        settings,
        GatewayConfig(
            providers=settings.history,
            vendors=settings.data.vendors,
            vendor_requests=tuple(
                dict.fromkeys(
                    r
                    for b in settings.data.prices
                    for r in requests_for(b, settings.schedule.start, settings.schedule.deadline)
                )
            ),
            contracts=tuple(
                contract_binding(source.chain, reserves) for source in settings.history
            ),
        ),
    )
    write_json(directory / "history.json", config)


def reader_for(gateway, chain):
    reader = HistoricalReader(
        chain, gateway.endpoint, read_token(gateway.directory / "gateway-auth.token")
    )
    return EvmHistory(reader)


def oracle_artifacts(reserves):
    return {
        str(chain.chain_id): file_digest(artifact_for(chain.chain_id))
        for chain in dict.fromkeys(r.chain for r in reserves)
    }


def oracle_revision(reserves):
    identities = oracle_artifacts(reserves)
    digest = (
        next(iter(identities.values()))
        if len(identities) == 1
        else hashlib.sha256(
            json.dumps(identities, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    return "archive-oracle-v3-market-evidence-" + digest


def contract_binding(chain, reserves):
    selected = tuple(r for r in reserves if r.chain == chain)
    if not selected:
        raise ValueError("historical provider has no configured Aave market")
    addresses = {a for r in selected for a in (r.spoke, r.hub, r.asset.reference)}
    addresses.update(row["address"] for row in contracts_for(chain.chain_id))
    return ContractBinding(chain=chain, contracts=tuple(sorted(addresses)))


def sessions(gateway, settings, reserves, stack):
    result = []
    for binding in settings.history:
        history = reader_for(gateway, binding.chain)
        stack.callback(history.reader.__exit__)
        result.append(
            Acquisition(
                gateway.directory,
                settings,
                tuple(r for r in reserves if r.chain == binding.chain),
                history,
            )
        )
    return tuple(result)


def freeze_scopes(directory, settings, reserves, env_file):
    boundaries, contracts = {}, []
    binding = GatewayConfig.model_validate_json((directory / "history.json").read_text())
    with GatewayProcess(directory, env_file) as gateway, ExitStack() as stack:
        for session in sessions(gateway, settings, reserves, stack):
            first = session.history.floor(settings.schedule.start)
            last = session.history.floor(settings.schedule.deadline)
            boundaries[session.reader.chain] = (first, last)
            selected = next(c for c in binding.contracts if c.chain == session.reader.chain)
            contracts.append(
                selected.model_copy(
                    update={
                        "logs": scopes(
                            session.reserves, first.number, last.number, needs_oracles(settings)
                        )
                    }
                )
            )
    write_json(
        directory / "history.json",
        gateway_authority(settings, binding.model_copy(update={"contracts": tuple(contracts)})),
    )
    return boundaries


def acquire(directory, settings, reserves, env_file):
    # Each chain resolves the same UTC interval independently before the gateway
    # receives bounded log authority. All products publish in one atomic dataset.
    boundaries = freeze_scopes(directory, settings, reserves, env_file)
    with GatewayProcess(directory, env_file) as gateway, ExitStack() as stack:
        selected = sessions(gateway, settings, reserves, stack)
        metadata = {}
        for session in selected:
            metadata.update(session.metadata(*boundaries[session.reader.chain]))
        requirements, accounting = selected[0].requirements(metadata)
        preparation = Preparation(
            model=MODEL,
            policy_digest=financial_policy(settings.data),
            acquisition_revision=oracle_revision(reserves),
            requirements=requirements,
        )
        writer = DatasetWriter(directory, preparation)
        try:
            collect(selected, boundaries, accounting, writer)
        finally:
            writer.close()


def collect(selected, boundaries, accounting, writer):
    if (job := progress.current()) is not None:
        job.discard("preparation")
    for session in selected:
        local = {
            source: reserve
            for source, reserve in accounting.items()
            if source.chain == session.reader.chain
        }
        session.plan_progress(writer, local, *boundaries[session.reader.chain])
    for session in selected:
        local = {
            source: reserve
            for source, reserve in accounting.items()
            if source.chain == session.reader.chain
        }
        session.collect(writer, local, *boundaries[session.reader.chain])
    selected[0].timed("vendor_prices", selected[0].vendor_inputs, writer)
    # Declared series report their own progress phase, only when a series is missing.
    strategy_prices(selected, writer)
    selected[0].timed("publication", writer.publish)
    if (job := progress.current()) is not None:
        job.begin(
            progress.phase_key("preparation.finalization"),
            "Verifying and closing acquisition",
            1.0,
            "phases",
        )


class Acquisition:
    def __init__(self, directory, settings, reserves, history):
        self.directory, self.settings, self.reserves, self.history = (
            directory,
            settings,
            reserves,
            history,
        )
        self.reader = history.reader
        self.oracle = OracleReader(self.reader)
        self.start, self.end = settings.schedule.start, settings.schedule.deadline
        self.metadata_evidence: tuple[RawEvidence, ...] = ()

    @property
    def event_phase(self):
        return acquisition_phase("event_collection", self.reader.chain)

    def metadata(self, first, last):
        self.reader.used.clear()
        for boundary in (first, last):
            if self.reader.pin_block(boundary.number) != boundary:
                raise ValueError("historical preparation boundary changed between gateway sessions")
        metadata = {}
        for source in self.reserves:
            self.history.pin(first)
            verify_deployments(self.reader, source, first)
            self.history.pin(last)
            verify_deployments(self.reader, source, last)
            self.history.pin(first)
            metadata[source] = read_risk(self.reader, source, first).reserve
        self.metadata_evidence = self.evidence()
        return metadata

    def collect(self, writer, accounting, first, last):
        events, evidence = self.timed("event_collection", self.events, first, last)
        self.reader.used.clear()
        self.validate_upgrades(events)
        # A completed dataset retains the deployment checks as well as the
        # economic observations. Losing the acquisition cache cannot erase them.
        evidence = (*evidence, *self.metadata_evidence, *self.evidence())
        for source, reserve in accounting.items():
            self.timed("accounting_samples", self.sample_rates, writer, source, reserve)
        for reserve in self.reserves:
            self.timed("risk_configuration", self.risk_inputs, writer, reserve, events, evidence)
            if oracle_required(self.settings, reserve):
                self.timed("oracle_prices", self.price_inputs, writer, reserve, events, evidence)

    def timed(self, phase, function, *args):
        started = time.perf_counter()
        try:
            phase_id = (
                acquisition_phase(phase, args[1])
                if phase in ("accounting_samples", "risk_configuration", "oracle_prices")
                else self.event_phase
                if phase == "event_collection"
                else "preparation." + phase
            )
            with progress.activity(phase_id):
                return function(*args)
        finally:
            with (self.directory / "phases.jsonl").open("a") as output:
                output.write(
                    json.dumps({"phase": phase, "wall_seconds": time.perf_counter() - started})
                    + "\n"
                )

    def plan_progress(self, writer, accounting, first, last):
        job = progress.current()
        if job is None:
            return
        chunks = len(range(first.number, last.number + 1, MAX_LOG_BLOCKS))
        logs = scopes(self.reserves, first.number, last.number, needs_oracles(self.settings))
        job.plan(
            progress.phase_key(self.event_phase),
            "Collecting change history",
            float(chunks * len(logs)),
            "log_ranges",
        )
        for source in accounting:
            key = rate_key(source)
            step = self.settings.data.sampling(key).sample_seconds
            total = sample_count(self.start, self.end, self.start, step)
            missing = sum(sample_count(a, b, self.start, step) for a, b in writer.missing(key))
            planned = job.plan(
                progress.phase_key(acquisition_phase("accounting_samples", source)),
                "Collecting accounting samples",
                float(total),
                "samples",
            )
            planned.completed = float(total - missing)
        self.plan_reserve_progress(job, writer)
        requests = sum(
            len(requests_for(b, self.start, self.end))
            for b in self.settings.data.prices
            if isinstance(b.source, CoinGeckoPrice) and writer.missing(b.key)
        )
        job.plan(
            progress.phase_key("preparation.vendor_prices"),
            "Collecting vendor prices",
            float(requests),
            "source_requests",
        )
        job.plan(progress.phase_key("preparation.publication"), "Publishing dataset", 1.0, "phases")
        job.plan(
            progress.phase_key("preparation.finalization"),
            "Verifying and closing acquisition",
            1.0,
            "phases",
        )
        if (metadata := progress.phase_key("preparation.metadata")) in job.phases:
            job.finish(metadata)

    def plan_reserve_progress(self, job, writer):
        for reserve in self.reserves:
            products = [("risk_configuration", "risk", "Collecting risk parameters")]
            if oracle_required(self.settings, reserve):
                products.append(("oracle_prices", "price", "Collecting oracle prices"))
            for name, product, label in products:
                total = None if writer.missing(reserve_key(reserve, product)) else 0.0
                job.plan(
                    progress.phase_key(acquisition_phase(name, reserve)),
                    label,
                    total,
                    "observations",
                    estimated=total is None,
                )

    def requirements(self, metadata):
        requirements: list[DataRequirement] = []
        accounting: dict[AccountingSource, ReserveObservation] = {}
        for source, reserve in metadata.items():
            key = AccountingSource(
                chain=source.chain,
                hub=source.hub,
                asset=source.asset,
                hub_asset_id=reserve.hub_asset_id,
            )
            accounting.setdefault(key, reserve)
            requirements.extend(
                (
                    requirement(
                        reserve_key(source, "risk"),
                        self.start,
                        self.end,
                        "aave_v4_risk_configuration",
                    ),
                )
            )
            if oracle_required(self.settings, source):
                requirements.append(
                    requirement(
                        reserve_key(source, "price"), self.start, self.end, "USD_per_asset_e8"
                    )
                )
        vendor_keys = set()
        for binding in self.settings.data.prices:
            if isinstance(binding.source, CoinGeckoPrice) and binding.key not in vendor_keys:
                vendor_keys.add(binding.key)
                requirements.append(requirement_for(binding, self.start, self.end))
        requirements.extend(strategy_requirements(self.settings))
        for source in accounting:
            requirements.append(
                requirement(
                    rate_key(source),
                    self.start,
                    self.end,
                    "annual_fraction_ray",
                    self.settings.data.sampling(rate_key(source)).sample_seconds,
                    self.settings.data.sampling(rate_key(source)).max_age_seconds,
                )
            )
        keys = {rate_key(source) for source in accounting}
        if any(o.key not in keys for o in self.settings.data.sampling_overrides):
            raise ValueError("sampling override does not match a supported sampled requirement")
        write_json(
            self.directory / "requirements.json",
            {
                "requirements": [r.model_dump(mode="json") for r in requirements],
                "sample_counts": [
                    {
                        "key": r.key.model_dump(mode="json"),
                        "count": (r.end - r.start) // r.sample_seconds + 1,
                    }
                    for r in requirements
                    if r.sample_seconds
                ],
            },
        )
        return tuple(requirements), accounting

    def vendor_inputs(self, writer):
        for binding in self.settings.data.prices:
            if not isinstance(binding.source, CoinGeckoPrice) or not writer.missing(binding.key):
                continue
            requests = requests_for(binding, self.start, self.end)

            def fetch(session, request):
                return VendorEvidence.model_validate_json(
                    json.dumps(session.reader.send("vendor_read", request))
                )

            evidence = tuple(self.parallel(fetch, requests))
            records = price_records(binding, writer.preparation.requirement(binding.key), evidence)
            self.commit(
                writer,
                binding.key,
                self.start,
                self.end,
                records,
                tuple(RawEvidence(body=e.model_dump_json()) for e in evidence),
            )

    def parallel(self, operation, items):
        local = threading.local()
        readers = []
        lock = threading.Lock()

        def run(item):
            if not hasattr(local, "session"):
                reader = HistoricalReader(
                    self.reader.chain, self.reader._endpoint, self.reader.token
                )
                with lock:
                    readers.append(reader)
                local.session = Acquisition(
                    self.directory, self.settings, self.reserves, EvmHistory(reader)
                )
            local.session.reader.used.clear()
            return operation(local.session, item)

        try:
            for result in bounded_map(
                run,
                items,
                next(
                    h.collection.max_in_flight
                    for h in self.settings.history
                    if h.chain == self.reader.chain
                ),
            ):
                progress.advance()
                yield result
        finally:
            for reader in readers:
                reader.__exit__()

    def evidence(self):
        return tuple(
            RawEvidence(body=self.reader.cache[key].model_dump_json())
            for key in sorted(self.reader.used)
        )

    def pin_input(self, block):
        # Header-search cache hits depend on worker assignment. Keep that trace
        # in the archive, while each input retains its pinned source reads only.
        self.reader.used.clear()
        return self.history.pin(block)

    def events(self, first, last):
        work = (
            (scope, start, min(last.number, start + MAX_LOG_BLOCKS - 1))
            for scope in scopes(
                self.reserves, first.number, last.number, needs_oracles(self.settings)
            )
            for start in range(first.number, last.number + 1, MAX_LOG_BLOCKS)
        )

        def read(session, task):
            scope, start, end = task
            rows = read_logs(session.history, scope, start, end)
            return rows, session.evidence()

        result: list[HistoricalLog] = []
        evidence = {}
        for rows, raw in self.parallel(read, work):
            result.extend(rows)
            evidence.update({e.digest: e for e in raw})
        return tuple(sorted(result, key=lambda event: event.order)), tuple(evidence.values())

    def validate_upgrades(self, events):
        upgraded = topics(("Upgraded(address)",))[0]
        for event in events:
            if event.topics[0] == upgraded:
                for reserve in self.reserves:
                    if event.address in (reserve.spoke, reserve.hub):
                        self.history.pin(event.block)
                        verify_deployments(self.reader, reserve, event.block)

    def record(self, cls, key, payload, time, block, unit):
        self.reader.pin_block(block.number)
        evidence = self.evidence()
        return cls(
            key=key,
            payload=payload,
            reference=block,
            unit=unit,
            source_time=UnixTime(block.timestamp),
            available_at=UnixTime(time),
            retrieved_at=max(self.reader.cache[key].retrieved_at for key in self.reader.used),
            evidence=tuple(e.digest for e in evidence),
        ), evidence

    def commit(self, writer, key, start, end, records, evidence):
        unique = {e.digest: e for e in evidence}
        writer.commit_range(
            AcquiredRange(
                key=key, start=UnixTime(start), end=UnixTime(end), evidence=tuple(sorted(unique))
            ),
            tuple(records),
            tuple(unique.values()),
        )

    def sample_rates(self, writer, source, reserve):
        key = rate_key(source)
        interval = self.settings.data.sampling(key).sample_seconds
        for start, end in writer.missing(key):
            for left in range(start, end + 1, 86400):
                right = min(end, left + 86399)
                records, evidence = [], []
                first = left + (self.start - left) % interval

                def read(session, at):
                    block = session.pin_input(session.history.floor(UnixTime(at)))
                    session.reader.prefetch(accounting_requests(reserve, block))
                    return session.record(
                        RateRecord,
                        key,
                        read_rates(session.reader, reserve, block),
                        at,
                        block,
                        "annual_fraction_ray",
                    )

                for row, raw in self.parallel(read, range(first, right + 1, interval)):
                    records.append(row)
                    evidence.extend(raw)
                if not evidence:
                    self.reader.used.clear()
                    self.reader.pin_block(self.history.floor(UnixTime(right)).number)
                    evidence.extend(self.evidence())
                self.commit(writer, key, left, right, records, evidence)

    def risk_inputs(self, writer, reserve, events, coverage):
        key = reserve_key(reserve, "risk")
        if not writer.missing(key):
            return
        times = {self.start}
        times.update(
            e.block.timestamp
            for e in events
            if e.address == reserve.spoke and self.start < e.block.timestamp <= self.end
        )
        records, evidence = [], list(coverage)
        if (job := progress.current()) is not None:
            job.update(total=float(len(times)), estimated=False)
        for at in sorted(times):
            block = self.pin_input(self.history.floor(UnixTime(at)))
            row, raw = self.record(
                RiskRecord,
                key,
                read_risk(self.reader, reserve, block),
                at,
                block,
                "aave_v4_risk_configuration",
            )
            records.append(row)
            evidence.extend(raw)
            progress.advance()
        self.commit(writer, key, self.start, self.end, records, evidence)

    def price_inputs(self, writer, reserve, events, coverage):
        key = reserve_key(reserve, "price")
        if not writer.missing(key):
            return
        times = {self.start}
        times.update(
            e.block.timestamp
            for e in self.price_events(reserve, events)
            if self.start < e.block.timestamp <= self.end
        )
        anchors = {e.block.timestamp: e.block for e in events}
        records, evidence = [], list(coverage)
        done: set[int] = set()

        def read(session, at):
            anchor = anchors.get(at)
            block = session.pin_input(
                session.history.event_floor(anchor)
                if anchor is not None
                else session.history.floor(UnixTime(at))
            )
            price = session.oracle.price(reserve, block)
            row, raw = session.record(PriceRecord, key, price, at, block, "USD_per_asset_e8")
            edges = []
            for deadline in session.oracle.deadlines:
                if deadline <= self.end:
                    edge = session.history.after(UnixTime(deadline - 1)).timestamp
                    if at < edge <= self.end:
                        edges.append(edge)
            return row, raw, edges

        while times:
            pending = sorted(times - done)
            times.clear()
            done.update(pending)
            if (job := progress.current()) is not None:
                job.update(total=float(len(done)), estimated=True)
            for row, raw, edges in self.parallel(read, pending):
                records.append(row)
                evidence.extend(raw)
                times.update(set(edges) - done)
                if job is not None:
                    job.update(total=float(len(done | times)), estimated=True)
        records.sort(key=lambda row: row.available_at)
        self.commit(writer, key, self.start, self.end, records, evidence)

    def price_events(self, reserve, events):
        graph_changes = topics(
            (
                "AggregatorConfirmed(address,address)",
                "UpdateReserveSource(uint256,address)",
                "SetSpoke(address)",
                "SetSpokeImmutables(address,uint16)",
                "Upgraded(address)",
            )
        )
        # A changing source graph retains the conservative complete event set.
        # Otherwise only dependencies of this reserve can change its price.
        if any(event.topics[0] in graph_changes for event in events):
            return events
        block = self.history.pin(self.history.floor(self.start))
        dependencies = self.oracle.price(reserve, block).dependencies
        return tuple(event for event in events if event.address in dependencies)


def strategy_prices(selected, writer):
    """The declared pool series, read through the sessions' gateway like every other input."""
    from almanak.backtest.evm import EvmHistory
    from almanak.connectors.dex_economic_prepare import collect_strategy_prices

    first = selected[0]
    histories = {session.reader.chain: session.history for session in selected}
    with ExitStack() as stack:
        for binding in series_bindings(first.settings):
            if binding.chain not in histories:
                reader = HistoricalReader(binding.chain, first.reader._endpoint, first.reader.token)
                stack.callback(reader.__exit__)
                histories[binding.chain] = EvmHistory(reader)
        collect_strategy_prices(writer, histories, first.settings)


def risk_binding(settings, reserve):
    return price_binding(
        settings.data.prices, "risk", reserve.chain, reserve.asset, reserve_key(reserve, "price")
    )


def oracle_required(settings, reserve):
    key = reserve_key(reserve, "price")
    if settings.data.risk_prices == "spoke_oracle_events":
        return True
    risk = risk_binding(settings, reserve)
    if isinstance(risk.source, ProtocolPrice) and risk.source.key != key:
        raise ValueError("Aave risk requires its own Spoke oracle or explicit vendor approximation")
    return any(
        isinstance(b.source, ProtocolPrice) and b.source.key == key for b in settings.data.prices
    )


def needs_oracles(settings):
    return settings.data.risk_prices == "spoke_oracle_events" or any(
        isinstance(b.source, ProtocolPrice) for b in settings.data.prices
    )


def validate_sources(settings, reserves):
    markets = {reserve_key(r, "price"): r for r in reserves}
    vendors = {v.name for v in settings.data.vendors}
    for reserve in reserves:
        oracle_required(settings, reserve)
    for binding in settings.data.prices:
        if isinstance(binding.source, ProtocolPrice):
            reserve = markets.get(binding.key)
            if reserve is None or reserve.asset != binding.asset:
                raise ValueError("oracle price source differs from its declared asset or market")
        if isinstance(binding.source, CoinGeckoPrice) and binding.source.provider not in vendors:
            raise ValueError("vendor price source requires its explicitly named provider")


def validate_strategy_dependencies(args, author, settings, reserves):
    from almanak.backtest.dependencies import DataDependencies, validate_dependencies
    from almanak.backtest.economic_model import parameters
    from almanak.backtest.inputs import strategy_source
    from almanak.runtime import strategy_worker
    from almanak.strategy import EconomicDataDependencyRequest

    strategy_path = getattr(args, "strategy", None)
    dependencies = DataDependencies()
    if strategy_path is not None:
        interval = IntervalSelection(
            configured=author.decision_trigger,
            override_seconds=settings.schedule.decision_interval_override_seconds,
        )
        dependencies = DataDependencies.model_validate_json(
            strategy_worker(
                EconomicDataDependencyRequest(
                    strategy=strategy_source(args),
                    config=parameters(settings, author, interval),
                )
            )
        )
    validate_dependencies(
        dependencies, settings.data.prices, {reserve_key(r, "price") for r in reserves}
    )
    return dependencies


def capture_activity(directory):
    capture = Capture(directory / "observations.sqlite")
    try:
        return capture.activity()
    finally:
        capture.close()


def phase_totals(directory):
    result: dict[str, float] = {}
    path = directory / "phases.jsonl"
    if path.exists():
        for line in path.read_text().splitlines():
            row = json.loads(line)
            result[row["phase"]] = result.get(row["phase"], 0) + row["wall_seconds"]
    return result


def acquisition_phase(name, source):
    return (
        "preparation."
        + name
        + "."
        + hashlib.sha256(source.model_dump_json().encode()).hexdigest()[:16]
    )


def sample_count(start, end, anchor, interval):
    first = start + (anchor - start) % interval
    return len(range(first, end + 1, interval))
