"""GMX action coverage requires settled requests and the existing authentic keeper fixture."""

import json

from almanak.connectors.gmx_v2.state import Checkpoint, RequestOutcome
from almanak.external_effects import ExternalEffect


def actions(raw, allocations, chain):
    state = Checkpoint.model_validate_json(json.dumps(raw))
    observed = set()
    for outcome in state.outcomes[: state.delivered]:
        if isinstance(outcome, ExternalEffect):
            continue
        target = outcome.operation.target
        if target.allocation not in allocations or target.fund.chain.chain_id != chain:
            continue
        if target != state.config.target(target.allocation):
            continue
        result = outcome.strategy_result()
        if result.status != "completed" or outcome.accounting_issues:
            continue
        if isinstance(outcome, RequestOutcome):
            if outcome.request.status == "executed":
                observed.add(result.intent.kind)
        elif result.intent.kind in ("revoke_position", "revoke_exchange"):
            observed.add(result.intent.kind)
    return observed


def eoa_configuration(config):
    from tests.fork.test_aave_v3_base_eoa import eoa_configuration as convert

    selected = convert(config)
    return selected.model_copy(
        update={
            "funds": tuple(
                fund.model_copy(update={"foundry_account_index": 941078 + index})
                for index, fund in enumerate(selected.funds)
            )
        }
    )


def eoa_lifecycle(*args):
    from tests.fork.test_gmx_requests import settle_eoa

    return settle_eoa(*args)


def cleanup_case(case, source, config, env_file, temporary, artifact):
    """The cleanup driver is internal; a converted reference reaches it through the adapter.

    The case operator file already carries that reference's EOA or distinct-operator
    fixture, so only an unconverted author file needs the harness transforms.
    """
    from tests.fork.test_gmx_cleanup import exercise
    from tests.qa.catalog import converted
    from tests.qa.worker import author_fork_configuration
    from tests.strategy_fork.harness import distinct_operators, resolve_config, retain

    author = author_fork_configuration(case, config, env_file)
    if not converted(case):
        if case.profile == "eoa":
            author = eoa_configuration(author)
        elif case.operator_mode == "distinct":
            author = distinct_operators(author)
    author = resolve_config(author, env_file, {})
    artifact.mkdir()
    record = dict(
        strategy=case.reference,
        account_profile=case.profile,
        operator_mode=case.operator_mode,
        scenario=case.scenario,
        phases=[],
        status="failed",
    )
    try:
        exercise(source, author, env_file, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
