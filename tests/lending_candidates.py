"""Private lending candidate declarations bound only inside qualification tests."""

from dataclasses import replace

from almanak.caip import ChainId
from almanak.connectors import composition
from almanak.sandbox import worker_command


def sandbox_available():
    try:
        worker_command()
    except RuntimeError:
        return False
    return True


def private_declaration(monkeypatch, connector_module, support_module, chain):
    """Extend a connector's candidate declaration in this process only.

    The public declaration in `support_module` stays unchanged; fork and composed
    validation construct bindings from the wire discriminator, so the name consumed by
    `connector_module.connector()` is patched, and `composition.connector_for` returns
    the extended binding for the connector's own configurations.
    """
    chain = chain if isinstance(chain, ChainId) else ChainId(chain)
    public = support_module.candidate_support()
    assert chain not in {entry.chain for entry in public}
    extended = (*public, public[0].model_copy(update={"chain": chain}))
    monkeypatch.setattr(connector_module, "candidate_support", lambda: extended)
    original = composition.connector_for

    def connector_for(config):
        binding = original(config)
        if binding.codec.config is connector_module.Config:
            return replace(binding, chain_support=extended)
        return binding

    monkeypatch.setattr(composition, "connector_for", connector_for)
    return extended
