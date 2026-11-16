"""Re-decode prepared GMX inputs exclusively from their retained raw RPC responses."""

from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.connectors.gmx_v2.deployments import verify_deployments
from almanak.connectors.gmx_v2.economic_collection import RISK_READERS, read_opportunities
from almanak.connectors.gmx_v2.economic_data import frame_market
from almanak.gateway.ethereum import EthereumReader


class ReplayReader(EthereumReader):
    def __init__(self, chain, dataset, digests):
        self.responses: dict[str, ReadEvidence] = {}
        for digest in digests:
            evidence = ReadEvidence.model_validate_json(dataset.evidence(digest).body)
            if evidence.request.chain != chain:
                raise ValueError("GMX retained response belongs to another historical chain")
            previous = self.responses.setdefault(evidence.request.key, evidence)
            if previous != evidence:
                raise ValueError("GMX retained response has conflicting request evidence")
        super().__init__(chain, "http://unused.invalid")
        self.cache = self.responses
        self.used: set[str] = set()

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        evidence = self.responses.get(request.key)
        if evidence is None or evidence.request != request:
            raise ValueError("GMX prepared re-decoding requires a missing retained RPC response")
        self.used.add(request.key)
        return evidence.result


def validate_frame(reader, frame, source, candidates):
    family = type(frame.risk[0])
    market = frame_market(frame.target, family)
    opportunities = []
    for risk in frame.risk:
        block = reader.pin_block(risk.block.number)
        if block != risk.block:
            raise ValueError("GMX prepared risk block differs from its retained canonical header")
        verify_deployments(reader, market, block)
        if RISK_READERS[family](reader, market, block) != risk:
            raise ValueError("GMX prepared risk differs from its retained DataStore responses")
        selected = {
            tx: identity for tx, identity in candidates.items() if identity[0] == block.number
        }
        opportunities.extend(
            read_opportunities(reader, market, source, selected, risk, frame.target)
        )
    if tuple(opportunities) != frame.opportunities:
        raise ValueError("GMX prepared opportunities differ from their complete publication scan")
