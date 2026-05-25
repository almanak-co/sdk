"""Staking connector compiler registry and dispatch tests."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import pytest

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseStakingCompiler
from almanak.framework.connectors.compiler_registry import get_compiler
from almanak.framework.connectors.ethena.compiler import EthenaCompiler
from almanak.framework.connectors.gimo.compiler import GimoCompiler
from almanak.framework.connectors.lido.compiler import LidoCompiler
from almanak.framework.intents.compiler import (
    CompilationResult,
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import IntentType, StakeIntent, UnstakeIntent

TEST_WALLET = "0x1234567890123456789012345678901234567890"


def test_staking_protocols_are_registered_connector_compilers() -> None:
    assert isinstance(get_compiler("lido"), LidoCompiler)
    assert isinstance(get_compiler("ethena"), EthenaCompiler)
    assert isinstance(get_compiler("gimo"), GimoCompiler)


def test_intent_compiler_no_longer_owns_staking_compile_methods() -> None:
    assert "_compile_stake_intent" not in IntentCompiler.__dict__
    assert "_compile_unstake_intent" not in IntentCompiler.__dict__


@dataclass
class _RecordingStakingCompiler(BaseStakingCompiler):
    protocols: ClassVar[frozenset[str]] = frozenset({"lido"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.STAKE, IntentType.UNSTAKE})
    calls: list[tuple[BaseCompilerContext, Any]]

    def compile_stake(self, ctx: BaseCompilerContext, intent: StakeIntent) -> CompilationResult:
        self.calls.append((ctx, intent))
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)

    def compile_unstake(self, ctx: BaseCompilerContext, intent: UnstakeIntent) -> CompilationResult:
        self.calls.append((ctx, intent))
        return CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)


@pytest.mark.parametrize(
    "intent",
    [
        StakeIntent(protocol="lido", token_in="ETH", amount=Decimal("1")),
        UnstakeIntent(protocol="lido", token_in="stETH", amount=Decimal("1")),
    ],
)
def test_stake_and_unstake_dispatch_through_connector_registry(monkeypatch: pytest.MonkeyPatch, intent: Any) -> None:
    recording_compiler = _RecordingStakingCompiler(calls=[])
    monkeypatch.setattr(
        "almanak.framework.intents.compiler.get_connector_compiler",
        lambda protocol: recording_compiler if protocol == "lido" else None,
    )

    result = IntentCompiler(
        chain="ethereum",
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    ).compile(intent)

    assert result.status == CompilationStatus.SUCCESS
    assert len(recording_compiler.calls) == 1
    ctx, recorded_intent = recording_compiler.calls[0]
    assert isinstance(ctx, BaseCompilerContext)
    assert ctx.chain == "ethereum"
    assert ctx.wallet_address == TEST_WALLET
    assert recorded_intent is intent
