"""Teardown full-close threads the strategy's forced-exit consent onto the V2 redeem intent."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.teardown.full_close import VaultExitPolicy, full_close_intents
from almanak.framework.teardown.models import PositionInfo, PositionType

VAULT = "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9"


def _vault_position() -> PositionInfo:
    return PositionInfo(
        position_id=VAULT,
        position_type=PositionType.VAULT,
        protocol="metamorpho",
        chain="base",
        value_usd=Decimal("500"),
        details={"vault_address": VAULT, "asset": "USDC"},
    )


def test_policy_parses_config_block_and_defaults_off() -> None:
    assert VaultExitPolicy.from_config(None) == VaultExitPolicy()
    assert VaultExitPolicy.from_config({}) == VaultExitPolicy(allow_force_deallocate=False, max_penalty_bps=10)
    parsed = VaultExitPolicy.from_config({"allow_force_deallocate": True, "max_penalty_bps": "7"})
    assert parsed == VaultExitPolicy(allow_force_deallocate=True, max_penalty_bps=7)
    assert (
        VaultExitPolicy.from_config({"allow_force_deallocate": True, "max_penalty_bps": 99_999}).max_penalty_bps
        == 10_000
    )
    assert VaultExitPolicy.from_config({"max_penalty_bps": "nope"}).max_penalty_bps == 10


def test_full_close_vault_redeem_is_off_by_default() -> None:
    (intent,) = full_close_intents([_vault_position()])
    assert intent.intent_type.value == "VAULT_REDEEM"
    assert intent.shares == "all"
    assert intent.allow_force_deallocate is False
    assert intent.max_force_deallocate_penalty_bps == 10


def test_full_close_vault_redeem_carries_the_policy() -> None:
    (intent,) = full_close_intents(
        [_vault_position()], vault_exit=VaultExitPolicy(allow_force_deallocate=True, max_penalty_bps=3)
    )
    assert intent.allow_force_deallocate is True
    assert intent.max_force_deallocate_penalty_bps == 3


def test_policy_reads_the_opt_in_strictly() -> None:
    """``bool("false")`` is True — the parser must never turn a string 'false' into a paid exit."""
    assert VaultExitPolicy.from_config({"allow_force_deallocate": "false"}).allow_force_deallocate is False
    assert VaultExitPolicy.from_config({"allow_force_deallocate": "no"}).allow_force_deallocate is False
    assert VaultExitPolicy.from_config({"allow_force_deallocate": 1}).allow_force_deallocate is False
    assert VaultExitPolicy.from_config({"allow_force_deallocate": "TRUE"}).allow_force_deallocate is True
    assert VaultExitPolicy.from_config({"allow_force_deallocate": True, "max_penalty_bps": True}).max_penalty_bps == 10


def test_teardown_full_close_intents_reads_the_strategy_config_block() -> None:
    """The framework helper derives the consent from ``config.json``'s ``vault_exit`` block."""
    from types import SimpleNamespace

    from almanak.framework.strategies.intent_strategy import IntentStrategy
    from almanak.framework.teardown.models import TeardownPositionSummary

    summary = TeardownPositionSummary(deployment_id="d", timestamp=datetime.now(UTC), positions=[_vault_position()])
    config = {"vault_exit": {"allow_force_deallocate": True, "max_penalty_bps": 4}}
    fake_self = SimpleNamespace(
        get_config=lambda key, default=None: config.get(key, default),
        get_open_positions=lambda: summary,
    )
    (intent,) = IntentStrategy.teardown_full_close_intents(fake_self)
    assert intent.intent_type.value == "VAULT_REDEEM"
    assert intent.allow_force_deallocate is True
    assert intent.max_force_deallocate_penalty_bps == 4


def test_teardown_full_close_intents_defaults_off_without_a_config_block() -> None:
    from types import SimpleNamespace

    from almanak.framework.strategies.intent_strategy import IntentStrategy
    from almanak.framework.teardown.models import TeardownPositionSummary

    summary = TeardownPositionSummary(deployment_id="d", timestamp=datetime.now(UTC), positions=[_vault_position()])

    # No ``config`` at all (a ``__new__``-built strategy double) must not raise either.
    def _no_config(key, default=None):
        raise AttributeError("config")

    (intent,) = IntentStrategy.teardown_full_close_intents(
        SimpleNamespace(get_config=_no_config, get_open_positions=lambda: summary)
    )
    assert intent.allow_force_deallocate is False
    assert intent.max_force_deallocate_penalty_bps == 10
