"""Staged check component for unresolved inputs and one authoritative parameter set."""

from almanak.checks import checked, stage
from almanak.configuration.bootstrap import ResolutionProgress
from almanak.configuration.funding import funding_view
from almanak.configuration.prepared import build_prepared
from almanak.configuration.unresolved import InputDocument
from almanak.sandbox import worker_command
from almanak.strategy import StrategyDefinition

UNADMITTED = "Operator execution ceilings, fixture and installed authority remain unadmitted."


def check_preparation(
    document: InputDocument,
    definition: StrategyDefinition,
    progress: ResolutionProgress | None = None,
    *,
    normalized: bool = False,
    stages: list | None = None,
):
    stages = [stage("configuration")] if stages is None else stages
    if progress is None or progress.result is None:
        return offline_report(document, definition, stages, normalized=normalized)
    report = prepared_report(document, definition, progress, stages, normalized=normalized)
    stages.append(stage("online_admission", "not_checked", UNADMITTED))
    report.pop("_prepared")
    return report


def offline_report(document, definition, stages, *, normalized):
    checked(stages, "sandbox", definition.filename, worker_command)
    stages.extend(
        stage(name, "not_checked", "Matching market resolution is required.")
        for name in (
            "resolution",
            "parameters",
            "strategy",
            "capabilities",
            "data_dependencies",
            "online_admission",
        )
    )
    return {
        "valid": True,
        "ready": False,
        "stages": stages,
        "authored_digest": document.inputs.digest,
        "strategy_digest": definition.sha256,
        **normalized_view(document, None, normalized),
    }


def prepared_report(document, definition, progress, stages, *, normalized):
    """Hook results over one resolution; the caller appends the admission stage."""
    prepared = build_prepared(document, definition, progress, stages)
    parameters = prepared.parameters
    return {
        "valid": True,
        "ready": False,
        "stages": stages,
        "authored_digest": document.inputs.digest,
        "context_digest": progress.context.digest,
        "resolution_digest": progress.result.digest,
        "resolution_facts": progress.result.facts_digest,
        "parameters_digest": prepared.parameters_digest,
        "strategy_digest": definition.sha256,
        "capabilities": prepared.capabilities.model_dump(mode="json"),
        "dependencies": prepared.dependencies.model_dump(mode="json"),
        **normalized_view(document, parameters, normalized),
        **funding_view(parameters, progress.context.mode, prepared.parameters_digest),
        "_prepared": prepared,
    }


def normalized_view(document, parameters, normalized):
    if not normalized:
        return {}
    return {
        "normalized": {
            "authored": document.inputs.model_dump(mode="json"),
            "parameters": parameters.model_dump(mode="json") if parameters else None,
        }
    }
