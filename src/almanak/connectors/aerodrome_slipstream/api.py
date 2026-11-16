"""Concrete author-facing V3 intent and context types."""

from types import ModuleType
from typing import ClassVar

from almanak.connectors.aerodrome_slipstream import authoring
from almanak.connectors.aerodrome_slipstream.models import Target
from almanak.connectors.aerodrome_slipstream.state import Decision, DecisionInput, OutcomeInput
from almanak.connectors.dex_api import Actions as FamilyActions
from almanak.dex_intents import CollectLiquidityFees, ExitExchange, RevokeExchange

__all__ = ["Actions", "Decision", "DecisionInput", "OutcomeInput", "Target"]


class Actions(FamilyActions):
    authoring: ClassVar[ModuleType] = authoring
    collect_intent = CollectLiquidityFees[Target]
    revoke_intent = RevokeExchange[Target]
    exit_intent = ExitExchange[Target]
