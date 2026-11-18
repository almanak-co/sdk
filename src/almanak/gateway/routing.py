"""Explicit trusted adapter composition inside the deployment's single gateway."""

import threading
from collections.abc import Callable, Mapping

from almanak.chains import ChainAdapter, configured_chains
from almanak.models import Chain


class ChainRouter:
    def __init__(self, config, factories: Mapping[Chain, Callable[[], ChainAdapter]]):
        if set(factories) != set(configured_chains(config)):
            raise ValueError("adapter routes must cover exactly the deployment chains")
        self.config = config
        self.factories = dict(factories)
        self.adapters: dict[Chain, ChainAdapter] = {}
        # Requests are served concurrently; one adapter per chain keeps its prepared
        # node gas-price readings visible to the revalidation of the same attempt.
        self.adapters_lock = threading.Lock()

    def for_chain(self, chain):
        if chain not in self.factories:
            raise ValueError("requested chain/environment has no authorized adapter route")
        with self.adapters_lock:
            if chain not in self.adapters:
                self.adapters[chain] = self.factories[chain]()
            return self.adapters[chain]

    def close(self):
        with self.adapters_lock:
            for adapter in self.adapters.values():
                adapter.close()
            self.adapters.clear()


def backend_for(backend, chain):
    if isinstance(backend, ChainRouter):
        return backend.for_chain(chain)
    if getattr(backend.reader, "chain", chain) != chain:
        raise ValueError("backend belongs to another chain/environment")
    return backend


class ScopedGateway:
    """Local runtime view; HTTP updates still compare and commit the whole checkpoint."""

    def __init__(self, parent, chain):
        self.parent, self.chain = parent, chain

    def close(self):
        self.parent.close()

    def load(self):
        from almanak.work import project

        return project(self.parent.load(), self.chain)

    def update(self, change):
        from almanak.work import merge, project

        result = self.parent.update(
            lambda state: merge(state, self.chain, change(project(state, self.chain))),
            chain=self.chain,
        )
        return project(result, self.chain)

    def call(self, method, model=None, **fields):
        from almanak.gateway.client import _decode

        result = self.parent.scoped(method, chain=self.chain, **fields)
        return _decode(model, result) if model is not None else result

    def observe(self):
        from almanak.gateway.client import _decode

        return tuple(_decode(self.parent.connector.codec.snapshot, i) for i in self.call("observe"))

    def prices(self, bindings, cutoff):
        # A price series names no chain; the deployment's gateway serves it once.
        return self.parent.prices(bindings, cutoff)

    def audit_history(self, state):
        self.call("audit")

    def prepare(self, state, intent):
        from uuid import uuid4

        return self.call(
            "plan", self.parent.connector.codec.plan, intent=intent, attempt_id=uuid4().hex
        )

    def sign(self):
        return self.call("sign", self.parent.connector.codec.pending)

    def receipt(self, pending):
        return self.call("receipt")

    def broadcast(self, pending):
        return self.call("broadcast")

    def family(self, pending, action, index):
        from almanak.work import project

        state = self.call(
            "family",
            self.parent.connector.codec.checkpoint,
            family_id=pending.family.identity,
            candidate_index=index,
            action=action,
        )
        return project(state, self.chain)

    def reconcile(self, pending, receipt):
        return self.call("reconcile", self.parent.connector.codec.outcome)

    def request_reconcile(self):
        from almanak.gateway.client import _decode

        result = self.call("request-reconcile")
        return None if result is None else _decode(self.parent.connector.codec.outcome, result)

    def external_reconcile(self):
        from almanak.gateway.client import _decode

        binding = self.parent.connector.external_effects
        if binding is None:
            raise ValueError("connector has no external-effect binding")
        result = self.call("external-reconcile")
        return None if result is None else _decode(binding.page_type, result)

    def request_cancel_plan(self):
        from uuid import uuid4

        from almanak.gateway.client import _decode

        result = self.call("request-cancel-plan", attempt_id=uuid4().hex)
        return None if result is None else _decode(self.parent.connector.codec.plan, result)
