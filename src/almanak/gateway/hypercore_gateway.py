"""The HyperCore gateway adapter: custody through `EvmGateway`, venue actions beside it
(blueprint 30, section 13.2).

`HyperCoreExecution` extends `EvmGateway` and is mixed in before a concrete one
(`HyperCoreForkGateway`), so the service's steps reach it and the EVM path's own calls
of `self` route back through it. It routes by plan and pending kind:

- a custody plan (one HyperEVM call through Roles) takes the whole EVM path unchanged,
  through `prepare_operation`: authority, simulation, fees, signing and receipt;
- a venue plan holds one attempt the trading rules planned. Admission against current
  reads is its check, since nothing simulates it (section 5.3). Signing re-admits it
  against fresh reads and asks the signer to sign with the Safe's agent key; the body is
  recorded never sent. The service durably clears never-sent, then `submit` posts those
  exact bytes once and records the venue's answer in the pending. A lost answer never
  posts again: a later `submit` reads the attempt's own evidence (`orderStatus` by
  cloid, fills, `activeAssetData`) and records it once it resolves the attempt. Past
  `expiresAfter` with neither, the deployment halts, unless the trading policy states A4
  (`release_unknown_after_expiry`, the fork model's), which releases an `Ioc` order as
  never applied. The receipt is that record and the outcome is derived from it alone,
  so the gateway reproduces every outcome it admits.

Only the gateway reads the venue. A plan's operation rests on reads that do not repeat,
so the gateway retains each operation it planned for the re-preparation that verifies
the recorded plan, and each custody resolution for the commit that records it, as
`EvmGateway` retains a node gas-price reading (`retained`). A gateway restarted between
the two refuses the commit; the coordinator then plans afresh.

Only the adapter reaches `/exchange`: the fork model's, or in production Hyperliquid's own
(`HyperCoreProductionGateway`, section 14), which a connector declaring production reaches.
"""

import threading
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path

from almanak.chains import ChainReference, TransactionReference
from almanak.connectors.hyperliquid import admission, lifecycle, planning, venue_reads
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.custody_evidence import Reads
from almanak.connectors.hyperliquid.fork import MODEL_VENUE, VENUE
from almanak.connectors.hyperliquid.operations import VenueAction
from almanak.connectors.hyperliquid.state import (
    TransactionOutcome,
    VenueOutcome,
    VenuePending,
    VenuePlan,
    effective,
)
from almanak.gateway.credentials import keystore_password
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway, validate_signing_state
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.hypercore_agent_keys import (
    EncryptedAgentKeys,
    LocalAgentKeys,
    SignerAgentKeys,
)
from almanak.gateway.hypercore_exchange import (
    EXPIRED,
    ErrorStatus,
    HyperCoreExchange,
    OrderResponse,
    Refused,
    UnknownOutcome,
)
from almanak.gateway.hypercore_info import (
    ClearinghouseState,
    HyperCoreInfo,
    InfoRequest,
    OpenOrders,
    received_now,
    typed,
)
from almanak.gateway.hypercore_signing import OrderAction, UpdateLeverage, verified
from almanak.gateway.production import ProductionGateway
from almanak.gateway.request_scope import timed
from almanak.gateway.signers import SignerRefused, VerifiedSigner
from almanak.models import AssetAmount
from almanak.transaction_family import is_unsent

# Order errors that leave an `Ioc` order cleanly unapplied: nothing in the book within its
# limit, or nothing left for a reduce-only exit to reduce (Hyperliquid's own answers, which
# the fork model repeats). Any other refusal is a genuine failure that stops new exposure.
CLEAN_ORDER_ERRORS = (
    "Order could not immediately match against any resting orders.",
    "Reduce only order would increase position.",
)
# The least time a never-sent action must have before its `expiresAfter` to be posted.
POST_MARGIN_MS = 1_000
# Venue readings retained for re-verification within one gateway process.
READINGS = 16
TRANSIENT_READS = ("unavailable", "rate_limit")


class HyperCoreVenue:
    """The adapter's HyperCore face: whitelisted `/info` reads, `/exchange` posts of
    recorded bodies and the deployment clock, as `trading_runtime.Venue`."""

    def __init__(self, info, exchange, clock):
        self.info = info
        # None refuses every post.
        self.exchange = exchange
        self.clock = clock

    def fetch(self, request):
        try:
            return self.info.fetch(request)
        except DataError as error:
            if error.kind in TRANSIENT_READS and not error.transient:
                # An outage or a throttle holds the coordinator instead of stopping it.
                raise DataError(error.kind, str(error), transient=True) from error
            raise

    def post(self, body: bytes):
        if self.exchange is None:
            raise DataError("unsupported", "HyperCore /exchange posts wait for D7 outside forks")
        return self.exchange.post(body)

    def now(self) -> int:
        return self.clock()

    def close(self) -> None:
        self.info.close()
        if self.exchange is not None:
            self.exchange.close()


def mainnet_venue(clock=received_now) -> HyperCoreVenue:
    """Hyperliquid's own mainnet `/info` and `/exchange` (section 14.2)."""
    return HyperCoreVenue(HyperCoreInfo(clock=clock), HyperCoreExchange(), clock)


def model_venue(model: str, clock) -> HyperCoreVenue:
    """The synthesized model's loopback `/info` and `/exchange` (blueprint 30, 9.1)."""
    return HyperCoreVenue(
        HyperCoreInfo(model=model, clock=clock), HyperCoreExchange(model=model), clock
    )


class CustodyBackend:
    """The reads, agent keys and clock custody requests resolve with (`custody.Backend`)."""

    def __init__(self, reads: Reads, keys, clock):
        self.reads = reads
        self.keys = keys
        self.clock = clock

    def now(self) -> int:
        return self.clock()


def fields(model, model_type) -> dict:
    return {name: getattr(model, name) for name in model_type.model_fields}


def venue_pending(plan: VenuePlan, signed) -> VenuePending:
    return VenuePending(**fields(plan, VenuePlan), signed=signed, unsent=True)


def clean(attempt) -> bool:
    """Whether an attempt that did not apply left nothing wrong: its own evidence released
    it, the venue refused it only as expired, or an `Ioc` order found nothing to do."""
    answer = attempt.answer
    if answer is None:
        return True
    if isinstance(answer, Refused):
        return answer.response == EXPIRED
    if not isinstance(answer.response, OrderResponse):
        return False
    (status,) = answer.response.data.statuses
    return isinstance(status, ErrorStatus) and status.error.startswith(CLEAN_ORDER_ERRORS)


class HyperCoreExecution(EvmGateway):
    """The EVM path with HyperCore's venue steps beside it; `attach` gives it its venue."""

    def attach(self, venue) -> None:
        self.venue = venue
        self.readings: OrderedDict = OrderedDict()
        self.readings_lock = threading.Lock()
        # Signed bodies this process holds that it has not posted: only these are posted.
        self.unposted: set[str] = set()

    def close(self):
        venue = getattr(self, "venue", None)
        if venue is not None:
            venue.close()
        super().close()

    def retained(self, key, compute):
        """This gateway's own reading for `key`, computed once while it is retained."""
        with self.readings_lock:
            if key in self.readings:
                return self.readings[key]
        value = compute()
        if value is not None:
            with self.readings_lock:
                self.readings[key] = value
                while len(self.readings) > READINGS:
                    self.readings.popitem(last=False)
        return value

    # What the connector's bindings read through the adapter.

    def safe(self, account) -> str:
        return self.config.accounts.binding(account).profile.fund.address

    def base_fee(self, block) -> int:
        """`block`'s base fee in wei per gas, which prices HyperCore's payouts."""
        header = self.reader._rpc("eth_getBlockByNumber", [hex(block.number), False])
        return int(header["baseFeePerGas"], 16)

    def agent_keys(self, account) -> SignerAgentKeys:
        return SignerAgentKeys(self.signers[account], self.safe(account))

    def custody_backend(self, account) -> CustodyBackend:
        reads = Reads(self.chain_reads, self.venue)
        return CustodyBackend(reads, self.agent_keys(account), self.venue.now)

    def chain_reads(self, method, params=()):
        return self.reader._rpc(method, list(params), allow_none=True)

    # Planning.

    def prepare(self, state, intent, before, attempt_id):
        self.require_preparation(state, intent, before)
        key = ("plan", attempt_id, before.block.hash, state.decision.identity)
        planned = {}

        def plan():
            planned["fresh"] = True
            return planning.operation(self, self.config, state, intent, before)

        operation = self.retained(key, plan)
        if not isinstance(operation, VenueAction):
            return self.prepare_operation(state, operation, before, attempt_id)
        self.validate_operation(state, operation, before)
        if planned:
            # Admission is a venue action's check, against current reads; a re-preparation
            # of the same attempt verifies the retained plan, and signing admits it again.
            self.admit_venue(state, operation.attempt)
        return VenuePlan(
            attempt_id=attempt_id, decision=state.decision, operation=operation, before=before
        )

    def admit_venue(self, state, attempt) -> None:
        """Section 5.3 against current reads. A refusal of the agent or the account halts;
        a refusal of the action itself is a market condition the coordinator re-plans on,
        counted against its budget."""
        trading = lifecycle.inserted(state.trading, attempt)
        reader = venue_reads.trader(self.venue, state, trading)
        authority = reader.authority(reader.state(), attempt)
        now, agents = self.venue.now(), state.custody.agents
        admission.require_account(trading, agents, attempt, authority, now)
        try:
            admission.admit(trading, agents, attempt, authority, now, state.book)
        except (DataError, ValueError) as refusal:
            raise DataError(
                "price_movement",
                f"HyperCore admission refused the attempt: {refusal}",
                details=getattr(refusal, "details", None),
            ) from refusal

    # Signing.

    def sign(self, state):
        plan = state.plan
        if not isinstance(plan, VenuePlan):
            return super().sign(state)
        validate_signing_state(state)
        self.audit_history(state)
        self.validate_operation(state, plan.operation, plan.before)
        attempt = plan.operation.attempt
        self.admit_venue(state, attempt)
        signer = self.signers[plan.operation.account]
        try:
            with timed("signer"):
                signed = signer.sign_hyperliquid_action(
                    plan.operation.target.fund.address, attempt.agent, attempt.request
                )
        except (SignerRefused, ValueError, PermissionError) as refusal:
            raise DataError(
                "price_movement", f"the signer refused the HyperCore action: {refusal}"
            ) from refusal
        except Exception as failure:  # noqa: BLE001 - an unreachable signer is retried
            raise DataError(
                "unavailable", "the HyperCore action signer is unavailable", transient=True
            ) from failure
        return venue_pending(plan, signed)

    def verify_signed_admission(self, state, pending):
        """Whether `pending` is the venue plan signed as recorded: the signature recovers to
        the plan's own agent over the digest computed here, and only this gateway's signer
        holds that key, so it signed after its own admission."""
        if not isinstance(pending, VenuePending):
            return super().verify_signed_admission(state, pending)
        validate_signing_state(state)
        plan = state.plan
        if not isinstance(plan, VenuePlan):
            return False
        self.validate_operation(state, plan.operation, plan.before)
        attempt = plan.operation.attempt
        signed = verified(attempt.request, attempt.agent, pending.signed.signature)
        return pending == venue_pending(plan, signed)

    def unsigned_plan(self, pending):
        if isinstance(pending, VenuePending):
            return VenuePlan(**fields(pending, VenuePlan))
        return super().unsigned_plan(pending)

    # The send.

    def authorize_submission(self, state):
        if not isinstance(state.pending, VenuePending):
            super().authorize_submission(state)

    def prepare_submission(self, state):
        pending = state.pending
        if not isinstance(pending, VenuePending):
            return super().prepare_submission(state)
        if not pending.unsent:
            return None
        if self.venue.now() + POST_MARGIN_MS > pending.signed.request.expires_after:
            # Never sent, so it is released and planned afresh rather than posted to fail.
            raise DataError(
                "stale_plan", "the signed HyperCore action expires before it could be posted"
            )
        with self.readings_lock:
            self.unposted.add(pending.signed.action_hash())
        return None

    def submit(self, state):
        pending = state.pending
        if not isinstance(pending, VenuePending):
            return super().submit(state)
        if pending.unsent:
            raise ValueError("signed bytes are sent only after the send is durably recorded")
        if pending.resolved is not None:
            return state, False
        key = pending.signed.action_hash()
        with self.readings_lock:
            first = key in self.unposted
            self.unposted.discard(key)
        if first:
            return self.posted(state), True
        return self.recovered(state), False

    def posted(self, state):
        """Post the recorded body once and record the venue's answer."""
        pending = state.pending
        try:
            answer = self.venue.post(pending.signed.body())
        except UnknownOutcome:
            return state
        attempt_id = pending.operation.attempt.id
        trading = rules.answered(self.sent_document(state), attempt_id, answer)
        return self.recorded(state, trading, answer)

    def recovered(self, state):
        """Resolve a maybe-sent attempt from its own evidence, never from a fresh request."""
        pending = state.pending
        attempt_id = pending.operation.attempt.id
        reader = venue_reads.trader(self.venue, state, self.sent_document(state))
        sent = reader.state().trading.attempt(attempt_id)
        if isinstance(sent.request.action, OrderAction | UpdateLeverage):
            reader.resolve(sent)
        trading = reader.state().trading
        if trading.attempt(attempt_id).resolved:
            return self.recorded(state, trading)
        if self.venue.now() > sent.request.expires_after:
            raise DataError(
                "unknown_outcome",
                "the HyperCore attempt has no evidence of its outcome past its expiry",
                details={"attempt": trading.attempt(attempt_id).model_dump(mode="json")},
            )
        return state

    def sent_document(self, state):
        """The checkpoint's trading document with the pending attempt retained, its body
        recorded and marked maybe sent, as the outcome will retain it."""
        pending = state.pending
        attempt_id = pending.operation.attempt.id
        trading = lifecycle.inserted(state.trading, pending.operation.attempt)
        return rules.sending(rules.signed(trading, attempt_id, pending.signed), attempt_id)

    def recorded(self, state, trading, answer=None):
        pending = state.pending
        resolved = trading.attempt(pending.operation.attempt.id)
        update = {"answer": answer, "resolved": resolved, "resolved_at": self.venue.now()}
        return state.model_copy(update={"pending": pending.model_copy(update=update)})

    def broadcast(self, pending, permissions=None, *, state=None):
        if isinstance(pending, VenuePending):
            raise ValueError("a HyperCore action is posted only through its recorded submission")
        return super().broadcast(pending, permissions, state=state)

    # Resolution.

    def receipt(self, pending):
        if not isinstance(pending, VenuePending):
            return super().receipt(pending)
        if pending.resolved is None:
            return None
        return {"attempt": pending.resolved.id, "status": pending.resolved.status}

    def reconcile(self, pending, receipt):
        if not isinstance(pending, VenuePending):
            return super().reconcile(pending, receipt)
        attempt = pending.resolved
        if attempt is None or pending.resolved_at is None or receipt is None:
            raise DataError("unavailable", "the HyperCore action has no recorded resolution")
        applied = attempt.status == "applied"
        success = applied or clean(attempt)
        complete = success and planning.completes(
            pending.decision.intent, pending.operation, applied=applied
        )
        return VenueOutcome(
            decision=pending.decision,
            before=pending.before,
            after=pending.before,
            success=success,
            complete=complete,
            assets=(),
            operation=pending.operation,
            attempt=attempt,
            resolved_at=effective(pending.resolved_at),
        )

    def transaction_reference(self, pending):
        if not isinstance(pending, VenuePending):
            return super().transaction_reference(pending)
        return TransactionReference(
            chain=pending.operation.target.fund.chain,
            reference=ChainReference(pending.signed.action_hash()),
        )

    def execution_cost(self, pending, receipt):
        if not isinstance(pending, VenuePending):
            return super().execution_cost(pending, receipt)
        # Agent-signed actions use no gas; they count against the account's request budget.
        return AssetAmount(asset=pending.before.native_balance.asset, amount=0)

    def require_nonce_unused(self, pending):
        if not isinstance(pending, VenuePending):
            super().require_nonce_unused(pending)

    def require_releasable(self, pending):
        """Only a venue action never sent is released; one that may have reached the
        venue resolves through its own evidence into an outcome."""
        if not isinstance(pending, VenuePending):
            return super().require_releasable(pending)
        if not is_unsent(pending) or pending.resolved is not None:
            raise ValueError("a HyperCore action that may have been sent is never released")
        return None

    # History and the venue's own state.

    def audit_outcomes(self, state):
        """Each custody transaction's receipt is still canonical; venue actions, custody
        request resolutions and effects pages carry no transaction of ours."""
        self.guard()
        for outcome in state.outcomes:
            if isinstance(outcome, TransactionOutcome):
                self.audit_receipt(outcome)
        if self.connector.requests is not None:
            self.connector.requests.audit(self, state)
        if self.connector.external_effects is not None:
            self.connector.external_effects.audit(self, state)

    def audit_receipt(self, outcome) -> None:
        receipt = self.reader._rpc("eth_getTransactionReceipt", [outcome.tx_hash], allow_none=True)
        expected = outcome.after.block
        if receipt is None or receipt["blockHash"] != expected.hash:
            raise DataError(
                "reorganization", "recorded execution is absent from this chain history"
            )
        if self.reader.pin_block(expected.number).hash != expected.hash:
            raise DataError("reorganization", "recorded execution block is no longer canonical")

    def require_empty_venue(self, state) -> None:
        """The Safe's HyperCore account holds no position, open order or USDC: before the
        deployment adopts it, and once teardown closes it (section 7, step 5). An open
        order of any origin keeps it incomplete; teardown never cancels one it did not
        place."""
        safe = state.custody.safe
        account = self.venue.fetch(InfoRequest(info=ClearinghouseState(user=safe)))
        orders = self.venue.fetch(InfoRequest(info=OpenOrders(user=safe)))
        spot = venue_reads.spot_usdc(self.venue, safe)
        perp = Decimal(typed(account).marginSummary.accountValue)
        positions = typed(account).assetPositions
        if not (positions or typed(orders) or spot or perp):
            return
        stage = "teardown_incomplete" if state.mode == "closed" else "initialization"
        raise DataError(
            stage,
            "the Safe's HyperCore account holds positions, open orders or USDC",
            details={
                "account": account.model_dump(mode="json"),
                "orders": orders.model_dump(mode="json"),
                "spot_usdc": spot,
            },
        )


class AgentKeySigner:
    """An operator's transaction key beside the agent keys the gateway holds (section
    5.4), on forks and in production; `VerifiedSigner` checks both."""

    def __init__(self, account, keys: LocalAgentKeys):
        self.account = account
        self.keys = keys
        self.address = account.address

    def sign_transaction(self, transaction: dict):
        return self.account.sign_transaction(transaction)

    def sign_hyperliquid_action(self, account, agent, action, nonce, expires_after):
        return self.keys.sign_hyperliquid_action(account, agent, action, nonce, expires_after)

    def generate_hyperliquid_agent(self, account) -> str:
        return self.keys.generate_hyperliquid_agent(account)

    def hyperliquid_agents(self, account, addresses):
        return self.keys.hyperliquid_agents(account, addresses)


def agent_signers(config, signers: dict, keys) -> dict:
    """Each account's signer beside the agent keys `keys(safe)` holds for its Safe."""
    held = {}
    for account, signer in signers.items():
        profile = config.accounts.binding(account).profile
        holder = AgentKeySigner(signer, keys(profile.fund.address))
        held[account] = VerifiedSigner(holder, profile.submitter.address)
    return held


class HyperCoreForkGateway(HyperCoreExecution, ForkGateway):
    """An identified HyperEVM fork with the synthesized HyperCore model as its venue."""

    def __init__(
        self, config, endpoint, key_file, *, chain=None, model: str, agent_keys: Path, clock=None
    ):
        super().__init__(config, endpoint, key_file, chain=chain)
        try:
            self.signers = agent_signers(
                config, self.signers, lambda safe: LocalAgentKeys(agent_keys, safe)
            )
            self.attach(model_venue(model, clock or self.fork_clock))
        except BaseException:
            self.close()
            raise

    def fork_clock(self) -> int:
        """The deployment clock on a fork: its head block's time, which the model follows."""
        head = self.reader._rpc("eth_getBlockByNumber", ["latest", False])
        return int(head["timestamp"], 16) * 1000


# The production agent keys' file, beside the operator keystores it shares a password with.
AGENT_KEY_FILE = "hypercore-agent-keys.json"


class HyperCoreProductionGateway(HyperCoreExecution, ProductionGateway):
    """Mainnet HyperEVM with Hyperliquid's own `/info` and `/exchange` as the venue
    (section 14.2). The production keystores sign custody transactions; the gateway
    generates the D9 agent keys and keeps them encrypted beside the keystores, under the
    same password. It is built only for a connector that declares production, which the
    hyperliquid connector does once D7 is accepted (section 14.4)."""

    def __init__(
        self,
        config,
        endpoint,
        key_file=None,
        password_file=None,
        *,
        chain=None,
        password=None,
        signer_file=None,
    ):
        if signer_file is not None:
            raise ValueError(
                "Hyperliquid production signs with local keystores: a remote signer "
                "holds no agent keys"
            )
        # Without keystores the gateway only reads, as for online checks and preflight.
        if key_file is not None:
            password = keystore_password(password_file, password)
        super().__init__(config, endpoint, key_file, chain=chain, password=password)
        try:
            if key_file is not None:
                keys = Path(key_file).parent / AGENT_KEY_FILE
                self.signers = agent_signers(
                    config, self.signers, lambda safe: EncryptedAgentKeys(keys, safe, password)
                )
            self.attach(self.production_venue())
        except BaseException:
            self.close()
            raise

    def production_venue(self) -> HyperCoreVenue:
        return mainnet_venue()


def hypercore_venue(venues: dict) -> dict:
    """A Hyperliquid fork route's HyperCore venue: the `hypercore-model` process's loopback
    endpoint and the agent key file, which the fork workspace writes (`fork_venue.py`)."""
    venue = venues.get(VENUE)
    if set(venues) != {VENUE} or not isinstance(venue, dict) or set(venue) != MODEL_VENUE:
        raise ValueError(
            "a Hyperliquid fork route names its HyperCore model endpoint and agent key file"
        )
    return venue


def fork_adapter(config, endpoint, key_file, *, chain=None, venues):
    """The connector's fork adapter (`fork_venues.fork_adapter`): the HyperEVM fork with
    the model its route names as the HyperCore venue."""
    venue = hypercore_venue(venues)
    return HyperCoreForkGateway(
        config,
        endpoint,
        key_file,
        chain=chain,
        model=venue["model"],
        agent_keys=Path(venue["agent_keys"]),
    )
