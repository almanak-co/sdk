"""Aerodrome author actions use typed pool targets and retained observations."""

from types import ModuleType
from typing import ClassVar

from almanak.connectors.aerodrome import authoring
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.state import Decision, DecisionInput, OutcomeInput
from almanak.connectors.dex_api import Actions as ExchangeActions
from almanak.dex_intents import CollectLiquidityFees, ExitExchange, RevokeExchange

__all__ = ["Actions", "Decision", "DecisionInput", "OutcomeInput", "Target"]


class Actions(ExchangeActions):
    authoring: ClassVar[ModuleType] = authoring
    collect_intent = CollectLiquidityFees[Target]
    revoke_intent = RevokeExchange[Target]
    exit_intent = ExitExchange[Target]
