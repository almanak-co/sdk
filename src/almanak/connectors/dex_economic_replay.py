"""Replay archival EVM reads from immutable evidence without creating an HTTP client."""

from contextvars import ContextVar

from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.gateway.ethereum import EthereumReader


class RetainedReader(EthereumReader):
    def __init__(self, chain, evidence):
        self.chain = chain
        self._verified = True
        self._pinned_context = ContextVar("retained_exchange_block", default=None)
        self.records: dict[str, ReadEvidence] = {}
        for raw in evidence:
            record = ReadEvidence.model_validate_json(raw.body)
            if record.request.chain != chain:
                raise ValueError("prepared RPC evidence belongs to another chain")
            old = self.records.setdefault(record.request.key, record)
            if old.result != record.result:
                raise ValueError("prepared RPC evidence contains conflicting results")

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        if request.key not in self.records:
            raise ValueError("prepared pool omits required archival RPC evidence")
        return self.records[request.key].result
