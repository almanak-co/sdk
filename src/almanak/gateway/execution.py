"""Durable EVM submission and recovery, with connector-owned economic semantics."""

import threading
import time
from types import SimpleNamespace

from eth_utils import to_checksum_address

from almanak.connectors.composition import connector_for
from almanak.gateway.accounts import (
    inner_revert,
    prepare_call,
    protocol_result,
    protocol_simulation,
    require_plain_eoa,
    revert_diagnosis,
)
from almanak.gateway.ethereum import EXECUTION_RETRY_BUDGET, DataError, EthereumReader, Reverted
from almanak.gateway.evm import chain_number
from almanak.gateway.production_config import execution_gas
from almanak.gateway.production_fees import (
    pinned_gas_price,
    quoted_fees,
    validate_budget,
    validate_receipt_cost,
)
from almanak.gateway.request_scope import once, timed
from almanak.gateway.reverts import MODULE_TRANSACTION_FAILED, describe
from almanak.gateway.roles_storage import ReadPlan
from almanak.gateway.signed_transaction import verify_signed
from almanak.models import Block, Eoa, SafeRoles
from almanak.transaction_family import (
    CURRENT_REFUSALS,
    is_unsent,
    original_plan,
    signed_candidates,
    start_family,
)

# Attempts awaiting revalidation are few; older readings fall back to a fresh read.
NODE_READINGS = 16
# Signing needs at least 1/DEADLINE_MARGIN_DIVISOR of the allocation's `deadline_seconds`
# left on a deadline-bound operation, so inclusion latency cannot outlast the deadline.
DEADLINE_MARGIN_DIVISOR = 2


def plan_operation(gateway, state, intent, before):
    """The next operation of the retained decision.

    A connector whose step needs a venue quote plans through `quoted_operation`: only
    this trusted side reads the venue, and the connector's own `validate_operation`
    re-checks the quoted terms against the pinned snapshot on every later validation.
    """
    adapter = gateway.connector.execution
    quoted = getattr(adapter, "quoted_operation", None)
    if quoted is not None:
        return quoted(gateway, state, intent, before)
    return adapter.operation(gateway.config, state, intent, before)


class EvmGateway:
    def __init__(
        self, config, endpoint: str, signers: dict, *, chain=None, clock=None, local_transport=False
    ):
        self.clock = clock or time.time
        self.config = config
        self.connector.execution.validate_config(config)
        self.signers = signers
        # The last head each account's authority was verified at. It is not authority: a
        # verification is reused only at that same head.
        self.verified_authority: dict[tuple[str, str], Block] = {}
        # Node gas-price suggestions this gateway read while preparing an attempt at a block
        # on a chain whose fee history reports no tips. That suggestion is not
        # block-addressable, so preparing the same attempt at the same block again reuses
        # this gateway's own reading; a caller never supplies it.
        self.node_readings: dict[tuple[str, str], int] = {}
        self.node_readings_lock = threading.Lock()
        self.authority_reads: dict[tuple[str, str], ReadPlan] = {}
        # A provider's transient read failure, such as a just-mined receipt block that a
        # lagging backend does not yet serve, is re-sent within a short budget. A local fork
        # serves its own blocks, so its reads fail at once.
        self.reader = EthereumReader(
            chain or config.accounts.bindings[0].profile.fund.chain,
            endpoint,
            local_transport=local_transport,
            retry_budget=0.0 if local_transport else EXECUTION_RETRY_BUDGET,
        )
        try:
            self.guard()
        except BaseException:
            self.close()
            raise

    @property
    def connector(self):
        return connector_for(self.config)

    def close(self):
        self.reader.__exit__()

    def guard(self):
        raise NotImplementedError("execution requires an explicit environment profile")

    def checkpoint(self):
        from almanak.chains import ChainReference, CheckpointReference

        block = self.reader.pin_block("latest")
        return CheckpointReference(
            chain=block.chain,
            reference=ChainReference(block.hash),
            height=block.number,
            timestamp=block.timestamp,
        )

    def transaction_reference(self, pending):
        from almanak.chains import ChainReference, TransactionReference

        return TransactionReference(
            chain=pending.operation.target.fund.chain, reference=ChainReference(pending.tx_hash)
        )

    def unsigned_plan(self, pending):
        return original_plan(pending)

    def authorize_submission(self, state):
        if (
            state.pending is None
            or execution_gas(self.config) is None
            or state.pending.family is not None
        ):
            return
        # An observed attempt may already have paid gas. Its retained identity
        # needs reconciliation rather than another spending reservation.
        if (
            self.reader._rpc("eth_getTransactionByHash", [state.pending.tx_hash], allow_none=True)
            is not None
        ):
            return
        self.refuse_taken_nonce(state)
        self.validate_fee_budget(state, state.pending, self.payer_balance(state))

    def refuse_taken_nonce(self, state) -> None:
        """Refuse an operator deployment's signed step whose nonce is already taken.

        Once the submitter's count is past the step's nonce the bytes cannot be mined,
        so the payer's balance says nothing about them. The refusal is `nonce_conflict`,
        raised here so that no send depends on a later read agreeing with this one, and
        it is what lets the coordinator ask for the consumed-nonce proof. A strategy
        deployment keeps its fee check first, as before.
        """
        from almanak.operator_deployment import is_operator

        slot = submission_slot(state.pending) if is_operator(state) else None
        if slot is None:
            return
        submitter, nonce = slot
        if int(self.reader._rpc("eth_getTransactionCount", [submitter, "latest"]), 16) > nonce:
            raise DataError(
                "nonce_conflict",
                "unknown execution occupies the recorded nonce; reconcile before continuing",
            )

    def payer_balance(self, state):
        """The gas payer's balance that a signed attempt's fee budget is checked against.

        A connector that observes this attempt in its own way (`own_observer`) is
        read that way here too: this is a check before the attempt's bytes are sent.
        """
        pending = state.pending
        if self.own_observer(pending) is None:
            balances = {o.target: o.native_balance for o in self.observe(state)}
            return balances[pending.operation.target]
        self.guard()
        block = self.reader.pin_block(int(self.reader._rpc("eth_blockNumber", []), 16))
        return self.current_snapshot(pending, block).native_balance

    def execution_cost(self, pending, receipt):
        from almanak.gateway.evm_fees import charged_fees

        pending = receipt_candidate(pending, receipt)
        validate_receipt_cost(self.config, pending, receipt)
        block = self.reader.pin_block(int(receipt["blockNumber"], 16))
        if block.hash != receipt["blockHash"]:
            raise DataError("reorganization", "execution cost receipt is no longer canonical")
        return charged_fees(self, pending, receipt, block)[0]

    def advance_family(self, state, action, identity, index):
        from almanak.gateway.replacement import command

        return command(self, state, action, identity, index)

    def submit(self, state):
        if state.pending.family is not None:
            from almanak.gateway.replacement import record_broadcast

            return record_broadcast(self, state)
        return state, self.broadcast(state.pending, state.permissions)

    def snapshot(self, name, block, reference=None):
        if self.reader.pin_block(block.number) != block:
            raise DataError("reorganization", "snapshot block is no longer canonical")
        return self.connector.execution.snapshot(self, name, block, reference)

    def current_snapshot(self, plan, block):
        """The observation at `block` that a plan is validated against.

        It is the plan's allocation read against the observation the plan was made
        from, unless the connector observes this plan in its own way (`own_observer`).
        """
        observe = self.own_observer(plan)
        if observe is None:
            return self.snapshot(plan.operation.allocation, block, plan.before)
        if self.reader.pin_block(block.number) != block:
            raise DataError("reorganization", "snapshot block is no longer canonical")
        return observe(self, plan, block)

    def own_observer(self, plan):
        """The connector's own current observation of this plan, or None for the shared one.

        A connector whose observation depends on whether a plan's bytes are already
        signed states `current_observer(plan)`, which returns the observation to use or
        None. A transaction family never takes it: fee replacement validates a new
        candidate, which is still to be signed, through the same path as recorded bytes,
        so a family is always observed as unsigned work is.
        """
        select = getattr(self.connector.execution, "current_observer", None)
        if select is None or getattr(plan, "family", None) is not None:
            return None
        return select(plan)

    def observe(self, state=None):
        self.guard()
        block = self.reader.pin_block(int(self.reader._rpc("eth_blockNumber", []), 16))
        owned = {o.target.allocation: o for o in state.owned} if state else {}
        return tuple(
            self.snapshot(a.name, block, owned.get(a.name))
            for a in self.config.positions
            if a.chain == self.reader.chain
        )

    def verify_snapshot(self, observation):
        expected = observation.block
        if self.reader.pin_block(expected.number) != expected:
            raise DataError("reorganization", "observation is no longer canonical")
        return self.snapshot(observation.allocation, expected, observation)

    def audit_history(self, state):
        # Planning and signing audit the checkpoint their request read, then audit it again
        # inside preparation. The memo retains `state`, so its identity cannot be reused.
        once(("audit", self.reader.chain, id(state)), lambda: self.audit_outcomes(state) or state)

    def audit_outcomes(self, state):
        from almanak.external_effects import ExternalEffect

        self.guard()
        for outcome in state.outcomes:
            if isinstance(outcome, ExternalEffect):
                continue
            if outcome.operation.target.fund.chain != self.reader.chain:
                continue
            receipt = self.reader._rpc(
                "eth_getTransactionReceipt", [outcome.tx_hash], allow_none=True
            )
            expected = outcome.after.block
            if receipt is None or receipt["blockHash"] != expected.hash:
                raise DataError(
                    "reorganization", "recorded execution is absent from this chain history"
                )
            if self.reader.pin_block(expected.number).hash != expected.hash:
                raise DataError("reorganization", "recorded execution block is no longer canonical")

        if self.connector.requests is not None:
            self.connector.requests.audit(self, state)
        if self.connector.external_effects is not None:
            self.connector.external_effects.audit(self, state)

    def _sign(self, operation, call, nonce, gas, gas_price):
        transaction = {
            "chainId": chain_number(self.reader.transport_chain),
            "nonce": nonce,
            "gas": gas,
            "gasPrice": gas_price,
            "to": to_checksum_address(call.to),
            "value": call.value,
            "data": call.data,
        }
        with timed("signer"):
            return self.signers[operation.account].sign_transaction(transaction)

    def validate_operation(self, state, operation, before):
        self.guard()
        from almanak.connectors.support import RunMode
        from almanak.connectors.target_routing import connector_for_target
        from almanak.gateway.environments import origin_chain
        from almanak.operator_deployment import is_operator

        if operation.target.fund.chain != self.reader.chain:
            raise ValueError("operation belongs to a different execution route")
        mode = RunMode.PRODUCTION if self.config.production else RunMode.FORK
        source = origin_chain(self.config, self.reader.chain)
        profile = self.config.accounts.binding(operation.account).profile.kind
        connector = connector_for_target(operation.target)
        action = state.decision.intent.kind
        connector.require_target(source.chain_id, operation.target, mode, action, profile)
        if is_operator(state):
            connector.support_for(source.chain_id).require_direct(action)
        self.connector.execution.validate_operation(self, state, operation, before)

    def prepare(self, state, intent, before, attempt_id):
        self.require_preparation(state, intent, before)
        operation = plan_operation(self, state, intent, before)
        return self.prepare_operation(state, operation, before, attempt_id)

    def require_preparation(self, state, intent, before):
        self.audit_history(state)
        self.require_fresh(before.block)
        if self.reader.pin_block(before.block.number) != before.block:
            raise DataError("reorganization", "preparation block is no longer canonical")
        if state.decision is None or state.decision.intent != intent:
            raise ValueError("preparation requires the durably retained decision")

    def prepare_operation(self, state, operation, before, attempt_id):
        """The transaction plan of one planned operation: authority, simulation and fees."""
        adapter = self.connector.execution
        self.validate_operation(state, operation, before)
        self.verify_authority(state.permissions, operation.account)
        call = adapter.call(self.config, operation)
        evidence = adapter.plan_evidence(self, operation, call, before)
        execution = prepare_call(self.config.accounts, operation.account, call)
        result = simulated(
            self.reader,
            execution,
            "eth_call",
            [
                protocol_simulation(execution),
                {"blockHash": before.block.hash, "requireCanonical": True},
            ],
        )
        adapter.validate_simulation(operation, protocol_result(execution, result), before)
        nonce, fees = self.estimate_execution(state, operation, before, execution, attempt_id)
        return self.connector.codec.plan(
            attempt_id=attempt_id,
            decision=state.decision,
            operation=operation,
            before=before,
            call=call,
            execution=execution,
            nonce=nonce,
            **evidence,
            **fees,
        )

    def estimate_execution(self, state, operation, before, execution, attempt_id=None):
        tx = {
            "from": execution.submitter.address,
            "to": execution.to,
            "value": hex(execution.value),
            "data": execution.data,
        }
        if isinstance(execution.binding.profile, SafeRoles):
            simulated(self.reader, execution, "eth_call", [tx, "latest"])
        submitter = execution.submitter.address
        nonce = int(self.reader._rpc("eth_getTransactionCount", [submitter, "latest"]), 16)
        if nonce != int(self.reader._rpc("eth_getTransactionCount", [submitter, "pending"]), 16):
            raise DataError("nonce_conflict", "submitter has an unknown pending transaction")
        for released in getattr(state, "released", ()):
            released_submitter, released_nonce = submission_slot(released)
            if released_submitter == submitter and released_nonce != nonce:
                # A released copy landed first: stop for reconciliation, never skip past it.
                raise DataError(
                    "nonce_conflict",
                    "the released attempt's nonce was consumed; reconcile it before continuing",
                )
        return nonce, self.prepare_fees(state, operation, before, tx, attempt_id)

    def prepare_fees(self, state, operation, before, tx, attempt_id=None):
        estimates = self.estimate_gas(operation, before, tx)
        estimate = estimates["gas_estimate"]
        admitted_gas = execution_gas(self.config)
        if admitted_gas is not None:
            policy = admitted_gas.policy(operation.account)
            key = (attempt_id, before.block.hash)
            fees = quoted_fees(
                self.reader,
                policy,
                before.block,
                tx,
                estimate=estimate,
                node_gas_price=self.node_reading(key),
            )
            self.remember_node_reading(key, fees["fee_quote"].node_gas_price)
            exposure = self.op_exposure(operation, before.block, tx["data"], fees["gas"])
            fees["fee_quote"] = fees["fee_quote"].model_copy(
                update={"additional_fee_exposure": exposure}
            )
            validate_quoted_budget(self, state, operation, before, fees, exposure)
            return {**estimates, **fees}
        return {
            **estimates,
            "gas": (estimate * 5 + 3) // 4,
            "gas_price": pinned_gas_price(self.reader, before.block),
        }

    def node_reading(self, key):
        with self.node_readings_lock:
            return self.node_readings.get(key)

    def remember_node_reading(self, key, node_gas_price):
        if node_gas_price is None or key[0] is None:
            return
        with self.node_readings_lock:
            self.node_readings.pop(key, None)
            self.node_readings[key] = node_gas_price
            while len(self.node_readings) > NODE_READINGS:
                self.node_readings.pop(next(iter(self.node_readings)))

    def op_exposure(self, operation, block, data, gas):
        """Pinned quote-time bound for OP L1 data and operator fees; zero elsewhere."""
        from almanak.chains import evm_network
        from almanak.gateway.op_fees import exposure_bound

        admitted_gas = execution_gas(self.config)
        if admitted_gas is None:
            return 0
        policy = admitted_gas.policy(operation.account)
        if evm_network(policy.asset.chain).family != "optimism":
            return 0
        length = (len(data) - 2) // 2 if isinstance(data, str) else len(data)
        return exposure_bound(self.reader, block, gas, length, policy.base_fee_multiplier_bps)

    def retained_fee_exposure(self, plan):
        quote = plan.fee_quote
        if quote is None:
            raise ValueError("bounded gas execution requires its pinned fee quote")
        exposure = self.op_exposure(plan.operation, quote.block, plan.execution.data, plan.gas)
        if exposure != quote.additional_fee_exposure:
            raise DataError(
                "inconsistent", "additional fee exposure differs from its retained quote"
            )
        return exposure

    def validate_fee_budget(self, state, plan, balance):
        validate_budget(
            state,
            plan.operation,
            balance,
            plan.gas,
            plan.gas_price,
            self.retained_fee_exposure(plan),
        )

    def estimate_gas(self, operation, before, tx):
        from almanak.connectors.target_routing import connector_for_target

        execution = connector_for_target(operation.target).execution
        if estimator := getattr(execution, "estimate_gas", None):
            return estimator(self.reader, operation, before, tx)
        # Revalidation must use the retained block, not a moving pending timestamp.
        selector = {"blockHash": before.block.hash, "requireCanonical": True}
        return {"gas_estimate": int(self.reader._rpc("eth_estimateGas", [tx, selector]), 16)}

    def sign(self, state):
        authorized = self.authorize_signing(state)
        with self.reader.retained_pin():
            self.require_deadline_margin(state.plan, self.reader.pin_block("latest"))
        return self.signed_pending(state, self._sign(*authorized))

    def require_deadline_margin(self, plan, block):
        """Refuse to sign an operation whose protocol deadline window is mostly spent.

        The deadline is fixed when the decision is observed, and a preceding approval can
        consume most of it, so bytes signed with seconds left mine after it and revert.
        The refusal discards the unsigned plan; the next decision plans afresh with a
        new deadline, bounded by the coordinator's re-plan limit.
        """
        if not self.enforces_deadline(plan.operation):
            return
        window = getattr(
            self.config.allocation(plan.operation.allocation), "deadline_seconds", None
        )
        if not isinstance(window, int) or window <= 0:
            return
        remaining = plan.operation.deadline - block.timestamp
        if remaining * DEADLINE_MARGIN_DIVISOR < window:
            raise DataError(
                "deadline_margin",
                f"only {remaining}s of the {window}s protocol deadline window remain at"
                f" signing (block {block.number}); planning afresh with a new deadline",
            )

    def verify_signed_admission(self, state, pending):
        """Whether `pending` is what `sign(state)` authorizes, without another signature.

        The recorded envelope must carry exactly the authorized fields and recover to the
        bound submitter, whose key only this gateway's signer holds.
        """
        self.authorize_signing(state)
        retained = SimpleNamespace(
            raw_transaction=bytes.fromhex(pending.raw_transaction.removeprefix("0x")),
            hash=bytes.fromhex(pending.tx_hash.removeprefix("0x")),
        )
        expected = self.signed_pending(state, retained)
        verify_signed(expected, expected.execution, self.reader.transport_chain)
        return expected == pending

    def authorize_signing(self, state):
        plan = state.plan
        validate_signing_state(state)
        assert plan is not None
        self.audit_history(state)
        self.require_fresh(plan.before.block)
        self.validate_plan_budget(state, plan)
        self.validate_operation(state, plan.operation, plan.before)
        self.verify_authority(state.permissions, plan.operation.account)
        if plan.call != self.connector.execution.call(self.config, plan.operation):
            raise ValueError("recorded plan does not match the authorized protocol call")
        self.connector.execution.current_checks(self, plan)
        execution = prepare_call(self.config.accounts, plan.operation.account, plan.call)
        if plan.execution != execution:
            raise ValueError("recorded outer transaction differs from its account binding")
        current = self.validate_current(plan)
        if execution_gas(self.config) is not None:
            self.validate_fee_budget(state, plan, current.native_balance)
        return plan.operation, execution, plan.nonce, plan.gas, plan.gas_price

    def signed_pending(self, state, signed):
        plan = state.plan
        pending = self.connector.codec.pending(
            **plan.model_dump(),
            raw_transaction="0x" + signed.raw_transaction.hex(),
            tx_hash="0x" + signed.hash.hex(),
        )

        admitted_gas = execution_gas(self.config)
        if admitted_gas is not None and admitted_gas.policy(plan.operation.account).replacement:
            self.connector.execution.validate_replacement(self.config, plan.operation)
        pending = start_family(self.config, pending)
        if pending.family is None and "unsent" in type(pending).model_fields:
            # Proof the bytes never left until submission durably clears it before sending.
            pending = pending.model_copy(update={"unsent": True})
        return pending

    def require_fresh(self, block):
        if self.config.production is None:
            return
        age = getattr(self, "clock", time.time)() - block.timestamp
        if (
            not -15
            <= age
            <= self.config.production.network(block.chain).max_observation_age_seconds
        ):
            raise StaleObservation(
                "stale_plan", "execution observation expired; prepare a fresh unsigned plan"
            )

    def validate_plan_budget(self, state, plan):
        if execution_gas(self.config) is None:
            return
        if plan.fee_quote is None or plan.fee_quote.block != plan.before.block:
            raise ValueError("bounded gas execution requires its pinned fee quote")
        self.validate_fee_budget(state, plan, plan.before.native_balance)

    def validate_current(self, plan):
        self.guard()
        block = self.reader.pin_block("latest")
        self.require_fresh(block)
        current = self.current_snapshot(plan, block)
        self.connector.execution.validate_current(self, plan, current)
        exposure = self.retained_fee_exposure(plan) if execution_gas(self.config) is not None else 0
        if current.native_balance.amount < plan.gas * plan.gas_price + exposure:
            raise DataError(
                "precondition_changed", "gas payer no longer covers the prepared transaction"
            )
        self.connector.execution.current_checks(self, plan, block)
        self.simulate_current(plan, current)
        return current

    def simulate_current(self, plan, current):
        execution = prepare_call(self.config.accounts, plan.operation.account, plan.call)
        tx = protocol_simulation(execution)
        if not isinstance(execution.binding.profile, SafeRoles):
            tx.update(gas=hex(plan.gas), gasPrice=hex(plan.gas_price))
        selector = {"blockHash": current.block.hash, "requireCanonical": True}
        result = simulated(self.reader, execution, "eth_call", [tx, selector])
        self.connector.execution.validate_simulation(
            plan.operation, protocol_result(execution, result), current
        )
        if isinstance(execution.binding.profile, SafeRoles):
            outer = {
                "from": execution.submitter.address,
                "to": execution.to,
                "value": hex(execution.value),
                "data": execution.data,
                "gas": hex(plan.gas),
                "gasPrice": hex(plan.gas_price),
            }
            simulated(self.reader, execution, "eth_call", [outer, selector])

    def validate_pending(self, pending):
        fund = self.config.accounts.binding(pending.operation.account).profile.fund
        if fund.chain != self.reader.chain:
            raise ValueError("pending transaction belongs to another chain route")
        if (
            pending.call != self.connector.execution.call(self.config, pending.operation)
            or pending.before.target.fund != fund
        ):
            raise DataError(
                "inconsistent", "pending operation differs from its authorized allocation"
            )
        execution = prepare_call(self.config.accounts, pending.operation.account, pending.call)
        validate_outer_identity(pending, execution)
        from almanak.gateway.replacement import validate_family

        validate_family(self.config, pending)
        for candidate in signed_candidates(pending):
            verify_signed(candidate, execution, self.reader.transport_chain)
        self.validate_pending_block(pending)

    def validate_pending_block(self, pending):
        before = pending.before.block
        if self.reader.pin_block(before.number).hash != before.hash:
            raise DataError(
                "reorganization", "pending transaction preconditions are no longer canonical"
            )

    def receipt(self, pending):
        self.guard()
        self.validate_pending(pending)
        receipts = []
        for candidate in signed_candidates(pending):
            receipt = self.reader._rpc(
                "eth_getTransactionReceipt", [candidate.tx_hash], allow_none=True
            )
            if receipt is not None:
                validate_receipt_identity(candidate, receipt)
                number = int(receipt["blockNumber"], 16)
                if self.reader.pin_block(number).hash == receipt["blockHash"]:
                    receipts.append(receipt)
        if len(receipts) > 1:
            raise DataError("inconsistent", "multiple canonical winners for one transaction nonce")
        return receipts[0] if receipts else None

    def prepare_submission(self, state):
        """Run a first broadcast's slow checks before the journal lock is taken.

        A broadcast later in the same request reuses the authority and current-state
        checks and repeats the envelope, unknown-hash and nonce checks before it sends.
        """
        pending = state.pending
        if pending.family is not None:
            return
        self.guard()
        self.validate_pending(pending)
        if not self.submitted(pending):
            self.check_submission(pending, state.permissions)

    def submitted(self, pending) -> bool:
        return (
            self.reader._rpc("eth_getTransactionByHash", [pending.tx_hash], allow_none=True)
            is not None
        )

    def check_submission(self, pending, permissions):
        key = ("submission", self.reader.chain, pending.tx_hash)
        once(
            (*key, "authority"),
            lambda: self.verify_authority(permissions, pending.operation.account) or True,
        )
        self.require_nonce_unused(pending)
        if self.expired_at_head(pending.operation):
            raise DataError(
                "transaction_expired",
                "the signed transaction passed its protocol deadline and can only revert",
            )
        try:
            once((*key, "current"), lambda: self.validate_current(pending) or True)
        except DataError as exc:
            if unsendable_refusal(pending, exc) and not self.enforces_deadline(pending.operation):
                raise unsendable(self.config, pending, exc) from exc
            raise

    def require_nonce_unused(self, pending):
        submitter = self.config.accounts.binding(pending.operation.account).profile.submitter
        counts = [
            int(self.reader._rpc("eth_getTransactionCount", [submitter.address, tag]), 16)
            for tag in ("latest", "pending")
        ]
        if counts != [pending.nonce, pending.nonce]:
            raise DataError(
                "nonce_conflict",
                "unknown execution occupies the recorded nonce; reconcile before continuing",
            )

    def enforces_deadline(self, operation) -> bool:
        """Whether the operation's calldata carries a deadline the protocol enforces."""
        expired = getattr(self.connector.execution, "expired", None)
        # Past every encodable deadline, only an operation that carries one has expired.
        return expired is not None and expired(operation, SimpleNamespace(timestamp=2**256))

    def expired_at_head(self, operation) -> bool:
        """Whether the operation's protocol deadline has passed at a fresh canonical head."""
        expired = getattr(self.connector.execution, "expired", None)
        if expired is None:
            return False
        with self.reader.retained_pin():
            return expired(operation, self.reader.pin_block("latest"))

    def require_releasable(self, pending):
        """Refuse to release a signed attempt unless it can never take protocol effect.

        Bytes this gateway retains as never sent cannot land. Past its protocol deadline a
        transaction can only revert. With no receipt, the provider holding no copy and its
        nonce unused, a fresh plan for that nonce excludes it, so releasing it cannot
        duplicate an economic effect. A copy that still lands can only revert, and the new
        attempt then stops at the occupied nonce.
        """
        if pending.family is not None:
            raise ValueError("a transaction family retains every signed candidate")
        self.guard()
        if self.receipt(pending) is not None:
            raise ValueError("a mined transaction is resolved only by its recorded outcome")
        if self.submitted(pending):
            raise ValueError("a transaction the provider still holds cannot be released")
        self.require_nonce_unused(pending)
        if not is_unsent(pending) and not self.expired_at_head(pending.operation):
            raise ValueError(
                "only a transaction never sent or past its protocol deadline can be released"
            )

    def superseded(self, pending):
        """Evidence that a final transaction took the attempt's nonce, or a refusal.

        Unlike `require_releasable` this needs no deadline and no unused nonce: the
        attempt can never be mined at all. See `almanak.gateway.consumed_nonce`.
        """
        from almanak.gateway.consumed_nonce import prove

        return prove(self, pending)

    def broadcast(self, pending, permissions=None, *, state=None):
        if pending.family is not None:
            from almanak.gateway.replacement import broadcast

            if state is None or state.pending != pending:
                raise ValueError("family broadcast requires the durable deployment state")
            return broadcast(self, state)
        self.guard()
        self.validate_pending(pending)
        if is_unsent(pending):
            raise ValueError("signed bytes are sent only after the send is durably recorded")
        if self.submitted(pending):
            return False
        self.check_submission(pending, permissions)
        returned = self.reader._rpc("eth_sendRawTransaction", [pending.raw_transaction])
        if returned != pending.tx_hash:
            raise DataError("inconsistent", "RPC returned a different transaction hash")
        return True

    def verify_authority(self, manifest, account):
        profile = self.config.accounts.binding(account).profile
        if isinstance(profile, Eoa):
            require_plain_eoa(self.reader._rpc, profile.fund.address)
        if isinstance(profile, SafeRoles):
            from almanak.gateway.permissions import verify

            if manifest is None:
                raise ValueError("Safe dispatch requires its retained permission manifest")
            # Verification pins a fresh head. Plan evidence that follows must still
            # read at the observation block its caller pinned, not that head.
            with self.reader.retained_pin():
                self.guard()
                head = self.reader.pin_block("latest")
                key = (manifest.digest, account)
                if self.verified_authority.get(key) == head:
                    return
                verify(
                    self.config,
                    manifest,
                    self,
                    connector=self.connector.permissions,
                    account=account,
                    head=head,
                    reads=self.authority_reads.setdefault(key, ReadPlan()),
                )
                self.verified_authority[key] = head

    def reconcile(self, pending, receipt):
        self.guard()
        original = pending
        pending = receipt_candidate(pending, receipt)
        validate_receipt_cost(self.config, pending, receipt)
        validate_receipt_identity(pending, receipt)
        block = self.reader.pin_block(int(receipt["blockNumber"], 16))
        if block.hash != receipt["blockHash"] or receipt["status"] not in ("0x0", "0x1"):
            raise DataError("inconsistent", "receipt is not canonical or has an invalid status")
        outcome = self.connector.execution.reconcile(self, pending, receipt, block)
        if receipt["status"] == "0x0" and "revert_reason" in type(outcome).model_fields:
            reason = self.deadline_revert(pending.operation, block) or mined_revert(
                self.reader, pending.execution, receipt
            )
            if reason is not None:
                outcome = outcome.model_copy(update={"revert_reason": reason})
        if original.family is not None:
            outcome = outcome.model_copy(
                update={"family": original.family, "transaction_plan": original_plan(original)}
            )
        return outcome

    def deadline_revert(self, operation, block):
        """Report that a revert was mined past the operation's protocol deadline.

        Replaying at the parent block runs at the parent's timestamp, where the deadline
        still holds, so a deadline revert would otherwise read as not reproduced. The
        mined timestamp proves only that the deadline had passed, so the reason states
        that timing fact rather than naming the deadline as the confirmed cause.
        """
        expired = getattr(self.connector.execution, "expired", None)
        if expired is None or not expired(operation, block):
            return None
        return (
            f"mined past protocol deadline: block timestamp {block.timestamp} is"
            f" {block.timestamp - operation.deadline}s after deadline {operation.deadline}"
        )


class StaleObservation(DataError):
    """The gateway's own head is too old to authorize execution; it says nothing of the plan."""


class Unsendable(DataError):
    """A maybe-sent attempt that current validation refuses and no deadline can retire."""

    def __init__(self, refusal_kind: str, message: str, *, code: int | None = None):
        super().__init__("signed_attempt_unsendable", message, code=code)
        # The underlying refusal, so its decoded revert reason is logged like the refusal's.
        self.refusal_kind = refusal_kind


def unsendable_refusal(pending, refusal) -> bool:
    """Whether a current-state refusal leaves a maybe-sent attempt stuck for good.

    Only connectors that record the never-sent proof can tell a maybe-sent attempt from
    one that never left; the others keep their own refusal. A lagging provider head or a
    transient read says nothing about the attempt and is retried.
    """
    return (
        "unsent" in type(pending).model_fields
        and not is_unsent(pending)
        and refusal.kind in CURRENT_REFUSALS
        and not refusal.transient
        and not isinstance(refusal, StaleObservation)
    )


def unsendable(config, pending, refusal):
    profile = config.accounts.binding(pending.operation.account).profile
    return Unsendable(
        refusal.kind,
        f"the signed attempt at nonce {pending.nonce} from submitter"
        f" {profile.submitter.address} no longer passes current validation"
        f" ({refusal.kind}: {refusal}). It may already have been"
        " sent and its calldata enforces no deadline, so no deadline releases it. Restore"
        " what validation requires (for example fund the gas payer, or wait for the market)"
        " and resume. Cancelling the nonce instead retires a strategy deployment: it has no"
        " reconciliation of a foreign transaction at a recorded nonce, so every resume"
        f" then stops at the occupied nonce. {cancelled_record(profile.fund.chain.chain_id)}"
        " A cancel must replace any copy a provider holds, so it needs a higher fee than"
        f" the signed attempt's at nonce {pending.nonce}",
        code=refusal.code,
    )


def cancelled_record(chain) -> str:
    """What cancelling the nonce does to a direct action's record on this network."""
    from almanak.chains import finality_margin

    if finality_margin(chain) is None:
        return (
            f"On {chain} a used nonce releases no direct action's record either, so"
            " cancelling there frees nothing and leaves the record unresolvable."
        )
    return (
        "A direct action's record is released by `ax recover` once the cancelling"
        " transaction is final."
    )


def validate_quoted_budget(backend, state, operation, before, fees, exposure):
    """Admit a fresh quote, telling a fee ceiling waiting can pass from one it cannot.

    `gas_fee_quote` holds the coordinator until the market falls. When the fixed OP
    Stack operator fees alone exceed `max_transaction_fee`, no market change admits the
    transaction, so the refusal is `gas_budget`, which stops.
    """
    gas, price = fees["gas"], fees["gas_price"]
    try:
        validate_budget(state, operation, before.native_balance, gas, price, exposure)
    except DataError as exc:
        if exc.kind != "gas_fee_quote" or not exposure:
            raise
        from almanak.gateway.op_fees import fixed_exposure

        admitted = execution_gas(backend.config)
        assert admitted is not None, "an OP fee exposure requires admitted execution gas"
        policy = admitted.policy(operation.account)
        fixed = fixed_exposure(backend.reader, before.block, gas, policy.base_fee_multiplier_bps)
        if fixed <= policy.max_transaction_fee.amount:
            raise
        raise DataError(
            "gas_budget",
            f"fixed operator fees exceed the transaction fee ceiling: {gas} gas costs at "
            f"least {fixed} in OP Stack operator fees, max_transaction_fee is "
            f"{policy.max_transaction_fee.amount} {policy.asset} base units",
        ) from exc


def submission_slot(pending):
    """The submitter and nonce a signed transaction occupies; None for work that occupies no
    submitter nonce, such as a venue action signed by an agent (blueprint 30, 13.3)."""
    if getattr(pending, "nonce", None) is None:
        return None
    execution = pending.execution
    submitter = execution.submitter if execution else pending.call.fund
    return submitter.address, pending.nonce


def receipt_candidate(pending, receipt):
    matches = [c for c in signed_candidates(pending) if c.tx_hash == receipt["transactionHash"]]
    if len(matches) != 1:
        raise DataError("inconsistent", "receipt is not a retained family candidate")
    return matches[0]


def validate_receipt_identity(pending, receipt):
    execution = pending.execution
    submitter = execution.submitter if execution else pending.call.fund
    target = execution.to if execution else pending.call.to
    if (
        receipt["transactionHash"] != pending.tx_hash
        or receipt["from"].lower() != submitter.address
        or receipt["to"].lower() != target
    ):
        raise DataError(
            "inconsistent", "receipt transaction identity does not match the pending call"
        )


def validate_signing_state(state):
    if state.plan is None:
        raise ValueError("signing requires one durably reserved transaction plan")
    if not state.plan.attempt_id or state.pending is not None:
        raise ValueError("signing requires one durably reserved transaction plan")


def validate_outer_identity(pending, execution):
    if pending.execution == execution:
        return
    if pending.execution is not None or isinstance(execution.binding.profile, SafeRoles):
        raise ValueError("pending outer transaction differs from its account binding")


def simulated(reader, execution, method, params):
    """One simulation read; an EVM revert is refused as `simulation_reverted` with its reason.

    Nothing is signed when a simulation reverts, so the coordinator may discard the unsigned
    plan and prepare afresh. The reason is decoded from revert data, never provider text.
    """
    try:
        return reader._rpc(method, params)
    except Reverted as exc:
        data = exc.data
        if data == MODULE_TRANSACTION_FAILED:
            data = diagnosed_revert(reader, execution, params[1]) or data
        raise DataError(
            "simulation_reverted", f"{method}: simulation reverted: {describe(data)}"
        ) from None


def diagnosed_revert(reader, execution, selector):
    """The inner revert data behind a Roles ModuleTransactionFailed(), when one is readable."""
    tx = revert_diagnosis(execution)
    if tx is None:
        return None
    try:
        return inner_revert(reader._rpc("eth_call", [tx, selector]))
    except DataError:
        # Diagnosis is best effort; the refusal keeps the outer reason.
        return None


def mined_revert(reader, execution, receipt):
    """Why a mined Roles transaction failed, from its inner call replayed at the parent block.

    The receipt keeps only ModuleTransactionFailed(). Replaying with shouldRevert cleared
    returns the inner revert data. Transactions earlier in the same block are not
    replayed, so a failure that depended on them is reported as not reproduced.
    """
    tx = None if execution is None else revert_diagnosis(execution)
    if tx is None:
        return None
    parent = {"blockNumber": hex(int(receipt["blockNumber"], 16) - 1)}
    try:
        data = inner_revert(reader._rpc("eth_call", [tx, parent]))
    except DataError:
        return None
    if data is None:
        return "inner revert not reproduced at the parent block"
    return describe(data)
