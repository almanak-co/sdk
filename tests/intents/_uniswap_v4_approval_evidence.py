"""Independent allowance witnesses for the existing four-layer V4 fork proofs."""

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.uniswap_v4.sdk import PERMIT2_ADDRESS
from almanak.connectors.uniswap_v4.venue_verifier import address_ref
from almanak.framework.venues import VenueTargetRole


def assert_approval_evidence(bundle, observations, gateway):
    artifact = bundle.metadata["v4_operation"]
    if bundle.intent_type == "SWAP":
        budgets = [(artifact["token_in"], int(artifact["amount_in"]))]
    elif bundle.intent_type == "LP_OPEN":
        key = artifact["pool_key"]
        budgets = [(key[f"currency{i}"], int(bundle.metadata[f"amount{i}_desired"])) for i in (0, 1)]
    else:
        budgets = []
    budgets = [(token.lower(), amount) for token, amount in budgets if int(token, 16)]
    compiled = bundle.metadata.get("v4_approval_checks", [])
    executed = observations["approval_checks"]
    assert len(compiled) == len(executed) == len(budgets)
    for (token, required), planned, checked in zip(budgets, compiled, executed, strict=True):
        writes = [
            decode(["address", "uint256"], bytes.fromhex(tx["data"][10:]))
            for tx in bundle.transactions[:-1]
            if tx["to"].lower() == token and tx["data"][:10] == "0x095ea7b3"
        ]
        assert all(spender == PERMIT2_ADDRESS.lower() for spender, _ in writes)
        amounts = [str(amount) for _, amount in writes]
        assert planned["approval_amounts_raw"] == checked["approval_amounts_raw"] == amounts
        for evidence in (planned, checked):
            assert evidence["token"] == token
            assert evidence["owner"] == artifact["wallet"]
            assert evidence["chain"] == artifact["chain"]
            assert evidence["spender"] == PERMIT2_ADDRESS.lower()
            assert evidence["required_raw"] == str(required)
            if evidence["measured"]:
                payload = keccak(text="allowance(address,address)")[:4] + encode(
                    ["address", "address"], [artifact["wallet"], PERMIT2_ADDRESS]
                )
                assert type(evidence["block_number"]) is int
                raw = gateway.read(
                    chain=artifact["chain"],
                    target=address_ref(VenueTargetRole.PERMISSION_TARGET, token),
                    payload=payload,
                    block_number=evidence["block_number"],
                )
                assert evidence["call_data"] == "0x" + payload.hex()
                assert evidence["return_data"] == "0x" + raw.hex()
                assert evidence["current_raw"] == str(decode(["uint256"], raw)[0])
            else:
                assert all(
                    evidence[field] is None for field in ("current_raw", "return_data", "call_data", "block_number")
                )
        assert planned["measured"] is True
        assert checked["measured"] is (not amounts)
        current = int(planned["current_raw"])
        expected = [] if current >= required else ["0", str(required)] if current else [str(required)]
        assert amounts == expected
        if not amounts:
            assert int(checked["current_raw"]) >= required
