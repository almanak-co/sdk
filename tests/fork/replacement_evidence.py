"""Dynamic evidence for the pinned, exercised protocol and wrapper call paths."""


def gas_price_traces(node, state):
    result = []
    for outcome in state.outcomes:
        trace = node.rpc(
            "debug_traceTransaction",
            [
                outcome.tx_hash,
                {"disableStorage": True, "disableStack": True, "enableMemory": False},
            ],
        )
        uses = sum(item["op"] == "GASPRICE" for item in trace["structLogs"])
        assert uses == 0
        result.append(
            {
                "operation": outcome.operation.kind,
                "transaction": outcome.tx_hash,
                "gasprice_opcodes": uses,
            }
        )
    return result
