"""Capture author-hook contracts: harness roots with explicit non-signing account bindings,
public roots with one retained market resolution."""

import argparse
import hashlib
import json
from pathlib import Path

from almanak.configuration.parameter_codec import validation_parameters
from almanak.configuration.parameter_types import parameter_type
from almanak.configuration.unresolved import public_root
from almanak.configuration.yaml import load
from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.models import StrategyAccounts
from almanak.runtime import strategy_worker, validate_strategy_config
from almanak.strategy import DataDependencyRequest, StrategyDefinition
from scripts.capture_config_baseline import canonical, digest, source_revision, write_capture
from scripts.legacy_configuration.author import read_author

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence"


def capture_resolved(config_path: Path, strategy_path: Path, resolution_path: Path):
    """Public roots: the hooks run once over the retained resolution, offline."""
    from almanak.configuration.bootstrap import read_progress
    from almanak.configuration.prepared import build_prepared
    from almanak.configuration.unresolved import parse_inputs

    revision = source_revision(ROOT)
    files = (config_path, strategy_path, resolution_path)
    before = tuple(path.read_bytes() for path in files)
    document = parse_inputs(load(config_path))
    definition = StrategyDefinition.read(strategy_path)
    progress = read_progress(resolution_path)
    prepared = build_prepared(document, definition, progress)
    if before != tuple(path.read_bytes() for path in files):
        raise ValueError("hook capture input changed during evaluation")
    value = {
        "schema": "almanak.author-hook-baseline.v1",
        "source_revision": revision,
        "collector_sha256": digest(Path(__file__).read_bytes()),
        "claims": "Sandboxed hook outputs over a retained resolution; no execution proof.",
        "authored_sha256": digest(before[0]),
        "strategy_sha256": digest(before[1]),
        "resolution_sha256": digest(before[2]),
        "validation_parameters": prepared.parameters.model_dump(mode="json"),
        "bound_parameters": prepared.parameters.model_dump(mode="json"),
        "capabilities": prepared.capabilities.model_dump(mode="json"),
        "dependencies": prepared.dependencies.model_dump(mode="json"),
    }
    return {**value, "content_sha256": digest(canonical(value))}


def capture(config_path: Path, strategy_path: Path, accounts_path: Path):
    revision = source_revision(ROOT)
    files = (config_path, strategy_path, accounts_path)
    before = tuple(path.read_bytes() for path in files)
    if public_root(load(config_path).value):
        raise ValueError("public roots capture hooks with --resolution, not --accounts")
    author = read_author(config_path)
    definition = StrategyDefinition.read(strategy_path)
    accounts = StrategyAccounts.model_validate_json(before[2])
    if any(b.profile.fund.chain.environment != "mainnet" for b in accounts.bindings):
        raise ValueError("hook comparison requires explicit origin account bindings")
    # The registry's parameter type accepts a composed root's owned inventory
    # without execution authority; the codec union would demand a fork or production block.
    identity = ConnectorId(author.connector) if author.connector else None
    parameters = parameter_type(identity).model_validate_json(
        json.dumps(
            {
                **author.parameter_wire(),
                "accounts": accounts.model_dump(mode="json"),
            }
        )
    )
    codec = connector_for(parameters).codec
    validation = validation_parameters(author)
    validate_strategy_config(definition, author)
    dependencies = json.loads(
        strategy_worker(
            DataDependencyRequest(
                strategy=definition,
                config=validation,
            )
        )
    )
    capabilities = codec.capability.evaluate(definition, parameters)
    if before != tuple(path.read_bytes() for path in files):
        raise ValueError("hook capture input changed during evaluation")
    value = {
        "schema": "almanak.author-hook-baseline.v1",
        "source_revision": revision,
        "collector_sha256": digest(Path(__file__).read_bytes()),
        "claims": "Sandboxed hook outputs with supplied accounts; no execution or provider proof.",
        "authored_sha256": digest(before[0]),
        "strategy_sha256": digest(before[1]),
        "accounts_sha256": digest(before[2]),
        "validation_parameters": validation.model_dump(mode="json"),
        "bound_parameters": parameters.model_dump(mode="json"),
        "capabilities": capabilities.model_dump(mode="json"),
        "dependencies": dependencies,
    }
    return {**value, "content_sha256": digest(canonical(value))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--strategy", type=Path, required=True)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--accounts", type=Path, help="harness roots: explicit account bindings")
    inputs.add_argument("--resolution", type=Path, help="public roots: retained resolution")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.output.resolve().parent.is_relative_to(EVIDENCE.resolve()):
        raise ValueError("hook capture must remain under the ignored local evidence directory")
    if args.output.exists():
        raise ValueError("hook capture output already exists")
    if args.resolution is not None:
        result = capture_resolved(args.config, args.strategy, args.resolution)
    else:
        result = capture(args.config, args.strategy, args.accounts)
    write_capture(args.output, result)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
