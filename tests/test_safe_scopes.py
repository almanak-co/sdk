"""Submission claims use chain, environment and operator independently of fund ownership."""

from types import SimpleNamespace

import pytest

from almanak.models import Account, AccountBinding, StrategyAccounts
from almanak.state import Lease
from tests.test_aave_debt import borrowing_proof as borrowing_proof


def claim_state(config, identity):
    return SimpleNamespace(config=config, deployment_id=identity)


def other_deployment(config, *, same_operator=True, environment=None):
    bindings = []
    for index, binding in enumerate(config.accounts.bindings):
        original = binding.profile
        chain = original.fund.chain
        if environment is not None:
            chain = chain.model_copy(update={"environment": environment})
        fund = Account(chain=chain, address=f"0x{100 + index:040x}")
        operator = Account(
            chain=chain,
            address=original.operator.address if same_operator else f"0x{200 + index:040x}",
        )
        profile = original.model_copy(update={"fund": fund, "operator": operator})
        bindings.append(AccountBinding(name=binding.name, profile=profile))
    return config.model_copy(update={"accounts": StrategyAccounts(bindings=tuple(bindings))})


def test_shared_operator_conflict_survives_process_release(tmp_path, monkeypatch, borrowing_proof):
    from pathlib import Path

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = borrowing_proof.config
    first = SimpleNamespace(path=tmp_path / "first.sqlite")
    second = SimpleNamespace(path=tmp_path / "second.sqlite")
    Lease(first, claim_state(config, "first")).close()
    with pytest.raises(RuntimeError, match="already claimed"):
        Lease(second, claim_state(other_deployment(config), "second"))
    Lease(first, claim_state(config, "first")).close()


@pytest.mark.parametrize("scope", ["operator", "environment"])
def test_distinct_submission_scopes_do_not_conflict(tmp_path, monkeypatch, borrowing_proof, scope):
    from pathlib import Path

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    config = borrowing_proof.config
    altered = other_deployment(
        config,
        same_operator=scope != "operator",
        environment="other-fork" if scope == "environment" else None,
    )
    first = Lease(SimpleNamespace(path=tmp_path / "first.sqlite"), claim_state(config, "first"))
    try:
        Lease(
            SimpleNamespace(path=tmp_path / "second.sqlite"), claim_state(altered, "second")
        ).close()
    finally:
        first.close()
