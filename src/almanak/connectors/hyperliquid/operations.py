"""The protocol operations of one HyperCore plan, apart from the intents they serve.

Two kinds reach HyperCore, each with its own path (blueprint 30, sections 3 and 13.2):

- a custody operation is one HyperEVM call the operator makes as the Safe through Roles,
  to the USDC token, CoreDepositWallet or CoreWriter (`corewriter.py`). It is signed and
  submitted as any EVM transaction, and only queues work: HyperCore applies a CoreWriter
  action after the block or drops it, so its effect is a custody request (section 4.4);
- a venue action is one agent-signed `/exchange` action, the retained `trading.Attempt`
  the trading rules planned. It has no transaction, gas or EVM nonce: its nonce lies in
  the agent's own submission scope and its deadline is its signed `expiresAfter`.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.hyperliquid import corewriter
from almanak.connectors.hyperliquid.agents import SUFFIX
from almanak.connectors.hyperliquid.models import Target
from almanak.connectors.hyperliquid.trading import Attempt
from almanak.models import Address, AssetAmount, Model

# An approval's label: one of the deployment's agent names, ` valid_until ` and the
# 13-digit expiry the action 9 grant pins (`agents.AgentNames.label`).
AgentLabel = Annotated[str, Field(pattern=r"^[a-z0-9-]{1,8}" + SUFFIX + r"[0-9]{13}$")]


class Operation(Model):
    target: Target

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def account(self):
        return self.target.account


class UsdcOperation(Operation):
    amount: AssetAmount

    @model_validator(mode="after")
    def units(self):
        if self.amount.asset != self.target.margin_asset:
            raise ValueError("a HyperCore custody amount is its target's USDC")
        return self


class Approve(UsdcOperation):
    """The CoreDepositWallet allowance before a deposit; zero revokes its residual."""

    kind: Literal["hyperliquid_approve"] = "hyperliquid_approve"


class Transfer(UsdcOperation):
    @model_validator(mode="after")
    def positive(self):
        if not self.amount.amount:
            raise ValueError("a HyperCore custody transfer moves a nonzero amount")
        return self


class Deposit(Transfer):
    """CoreDepositWallet `deposit` to the Safe's own HyperCore spot balance."""

    kind: Literal["hyperliquid_deposit"] = "hyperliquid_deposit"


class StandardMode(Operation):
    """CoreWriter action 16: separate spot and perp balances for the Safe."""

    kind: Literal["hyperliquid_standard_mode"] = "hyperliquid_standard_mode"


class ClassTransfer(Transfer):
    """CoreWriter action 7: USDC between the Safe's own spot and perp balances."""

    kind: Literal["hyperliquid_class_transfer"] = "hyperliquid_class_transfer"
    to_perp: bool


class Withdrawal(Transfer):
    """CoreWriter action 13: spot USDC to its system address, paid to the Safe on HyperEVM."""

    kind: Literal["hyperliquid_withdrawal"] = "hyperliquid_withdrawal"


class AgentApproval(Operation):
    """CoreWriter action 9 for a fresh gateway-held key under a deployment name (D9)."""

    kind: Literal["hyperliquid_agent_approval"] = "hyperliquid_agent_approval"
    agent: Address
    label: AgentLabel


CustodyOperation = Annotated[
    Approve | Deposit | StandardMode | ClassTransfer | Withdrawal | AgentApproval,
    Field(discriminator="kind"),
]
# The purposes phase 3 installs; phase 4's resting and protective orders, cancels,
# `scheduleCancel` and margin changes bind in a later change.
VENUE_PURPOSES = ("leverage", "entry", "exit")


class VenueAction(Operation):
    """One agent-signed `/exchange` action: the attempt the trading rules retained."""

    kind: Literal["hyperliquid_venue_action"] = "hyperliquid_venue_action"
    attempt: Attempt

    @model_validator(mode="after")
    def scope(self):
        if self.attempt.purpose not in VENUE_PURPOSES:
            raise ValueError("this Hyperliquid binding admits leverage, entries and exits only")
        if self.attempt.coin != self.target.market.coin:
            raise ValueError("a venue action trades its own target's market")
        return self


ProtocolOperation = Annotated[
    Approve | Deposit | StandardMode | ClassTransfer | Withdrawal | AgentApproval | VenueAction,
    Field(discriminator="kind"),
]


def core_call(operation) -> corewriter.CoreCall:
    """The one HyperEVM call of a custody operation."""
    if isinstance(operation, Approve):
        return corewriter.approval(operation.amount.amount)
    if isinstance(operation, Deposit):
        return corewriter.deposit(operation.amount.amount)
    if isinstance(operation, StandardMode):
        return corewriter.standard_mode(operation.target.fund.address)
    if isinstance(operation, ClassTransfer):
        return corewriter.class_transfer(operation.amount.amount, operation.to_perp)
    if isinstance(operation, Withdrawal):
        return corewriter.withdrawal(operation.amount.amount)
    if isinstance(operation, AgentApproval):
        return corewriter.agent_approval(operation.agent, operation.label)
    raise ValueError("a venue action is no HyperEVM call")
