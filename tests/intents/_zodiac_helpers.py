"""Safe + Zodiac Roles v2 deployment and target-application helpers for intent tests.

These helpers are only used by the on-chain permission-authorisation tests
(``tests/intents/<chain>/test_zodiac_permission_correctness.py`` and the
parametrized harness in ``_permission_onchain_harness.py``). They build
everything on top of the canonical CREATE2 factories that already live on any
Anvil fork of a real EVM chain (Arbitrum, Base, Optimism, …):

- ``SafeProxyFactory`` v1.4.1 → deploys a new Safe proxy
- ``ModuleProxyFactory`` → clones a fresh Zodiac Roles Modifier proxy
- ``Roles`` v2 singleton → the master copy the proxy delegates to
- ``MultiSend`` v1.4.1 → batches target-application calls atomically

Safe owner + role member collapse to the same EOA (``owner_eoa``) for test
simplicity. Every Safe-authorised call (``enableModule``, ``assignRoles``,
``allowTarget``, …) routes through ``Safe.execTransaction`` with a
v=1 pre-validated signature — no EIP-712 signing needed because
``msg.sender == owner`` satisfies Safe's signature check.

Key entry points:

    deploy_test_safe(web3, owner_eoa, owner_private_key) -> str
    deploy_test_zodiac_roles(web3, safe, owner_eoa, owner_private_key) -> str
    assign_role_to_member(web3, roles, safe, role_key, member_eoa, owner_private_key)
    apply_manifest_targets(web3, roles, safe, role_key, targets, owner_private_key)

Reference: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

import secrets

from eth_abi import encode as abi_encode
from eth_account import Account
from web3 import Web3

from almanak.framework.execution.signer.safe.constants import (
    MODULE_PROXY_FACTORY,
    MODULE_PROXY_FACTORY_DEPLOY_MODULE_ABI,
    MULTISEND_ADDRESSES,
    ROLES_ALLOW_FUNCTION_ABI,
    ROLES_ALLOW_TARGET_ABI,
    ROLES_ASSIGN_ROLES_ABI,
    ROLES_MODIFIER_SINGLETON,
    ROLES_REVOKE_TARGET_ABI,
    ROLES_SCOPE_TARGET_ABI,
    ROLES_SET_DEFAULT_ROLE_ABI,
    ROLES_SET_UP_ABI,
    SAFE_ENABLE_MODULE_ABI,
    SAFE_EXEC_TRANSACTION_ABI,
    SAFE_L2_SINGLETON_V1_4_1,
    SAFE_PROXY_FACTORY_CREATE_PROXY_WITH_NONCE_ABI,
    SAFE_PROXY_FACTORY_V1_4_1,
    SAFE_SETUP_ABI,
    ZERO_ADDRESS,
    SafeOperation,
)
from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder

# =============================================================================
# Constants
# =============================================================================

# SafeProxyFactory emits ProxyCreation(address indexed proxy, address singleton)
_PROXY_CREATION_TOPIC = Web3.keccak(text="ProxyCreation(address,address)").hex()

# ModuleProxyFactory emits ModuleProxyCreation(address indexed proxy, address indexed masterCopy)
_MODULE_PROXY_CREATION_TOPIC = Web3.keccak(text="ModuleProxyCreation(address,address)").hex()


# =============================================================================
# Safe transaction primitives
# =============================================================================


def _prevalidated_signature(owner: str) -> bytes:
    """Build a Safe ``checkSignatures`` v=1 pre-validated signature for ``owner``.

    Format: ``r = left-padded owner address`` (32 bytes), ``s = 0`` (32 bytes),
    ``v = 0x01`` (1 byte). Total 65 bytes.

    Safe's ``checkSignatures`` accepts v=1 iff the supplied ``r`` address equals
    ``msg.sender`` (or has pre-approved the tx hash via ``approveHash``). We
    always send Safe transactions from ``owner``, so v=1 is sufficient for
    single-owner threshold-1 Safes used in test fixtures.
    """
    addr = Web3.to_checksum_address(owner)
    return bytes(12) + bytes.fromhex(addr[2:]) + bytes(32) + bytes([0x01])


def _exec_safe_tx(
    web3: Web3,
    safe: str,
    to: str,
    data: bytes,
    operation: SafeOperation,
    owner_eoa: str,
    owner_private_key: str,
    *,
    value: int = 0,
) -> dict:
    """Build + submit + confirm one ``Safe.execTransaction``.

    Uses the v=1 pre-validated signature pattern so the owner EOA doesn't have to
    produce an EIP-712 signature. Suitable for test fixtures where the Safe has a
    single owner and threshold 1.

    Returns the transaction receipt (raises on revert or status != 1).
    """
    safe_c = web3.eth.contract(address=Web3.to_checksum_address(safe), abi=SAFE_EXEC_TRANSACTION_ABI)

    tx = safe_c.functions.execTransaction(
        Web3.to_checksum_address(to),
        value,
        data,
        int(operation),
        0,  # safeTxGas
        0,  # baseGas
        0,  # gasPrice
        ZERO_ADDRESS,  # gasToken
        ZERO_ADDRESS,  # refundReceiver
        _prevalidated_signature(owner_eoa),
    ).build_transaction(
        {
            "from": Web3.to_checksum_address(owner_eoa),
            "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(owner_eoa)),
        }
    )
    signed = Account.sign_transaction(tx, owner_private_key)
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    if receipt["status"] != 1:
        raise RuntimeError(f"Safe.execTransaction reverted: to={to}, tx={tx_hash.hex()}")
    return receipt


def _send_eoa_tx(
    web3: Web3,
    to: str,
    data: bytes,
    owner_eoa: str,
    owner_private_key: str,
    *,
    value: int = 0,
) -> dict:
    """Build + submit + confirm a plain EOA → contract call (no Safe wrapping)."""
    tx = {
        "from": Web3.to_checksum_address(owner_eoa),
        "to": Web3.to_checksum_address(to),
        "value": value,
        "data": data,
        "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(owner_eoa)),
        "chainId": web3.eth.chain_id,
    }
    # Let the node estimate gas + fee params.
    tx["gas"] = int(web3.eth.estimate_gas(tx) * 1.3)
    tx["maxFeePerGas"] = web3.eth.gas_price * 2
    tx["maxPriorityFeePerGas"] = web3.eth.max_priority_fee
    signed = Account.sign_transaction(tx, owner_private_key)
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    if receipt["status"] != 1:
        raise RuntimeError(f"EOA tx reverted: to={to}, tx={tx_hash.hex()}")
    return receipt


def _extract_proxy_from_receipt(receipt: dict, topic: str) -> str:
    """Pull a proxy address out of a factory-emitted *Creation event log."""
    topic_bytes = topic if topic.startswith("0x") else "0x" + topic
    for log in receipt["logs"]:
        log_topic = log["topics"][0].hex() if hasattr(log["topics"][0], "hex") else log["topics"][0]
        if not log_topic.startswith("0x"):
            log_topic = "0x" + log_topic
        if log_topic.lower() == topic_bytes.lower():
            proxy_topic = log["topics"][1]
            proxy_hex = proxy_topic.hex() if hasattr(proxy_topic, "hex") else proxy_topic
            if not proxy_hex.startswith("0x"):
                proxy_hex = "0x" + proxy_hex
            return Web3.to_checksum_address("0x" + proxy_hex[-40:])
    raise RuntimeError(f"Factory event topic {topic_bytes} not found in receipt {receipt.get('transactionHash')}")


# =============================================================================
# Safe deployment
# =============================================================================


def deploy_test_safe(web3: Web3, owner_eoa: str, owner_private_key: str) -> str:
    """Deploy a fresh Safe v1.4.1 with ``owner_eoa`` as the sole owner, threshold=1.

    Uses the canonical ``SafeProxyFactory`` already present on the forked chain.
    Returns the new Safe proxy address (EIP-55 checksummed).
    """
    factory = web3.eth.contract(
        address=Web3.to_checksum_address(SAFE_PROXY_FACTORY_V1_4_1),
        abi=SAFE_PROXY_FACTORY_CREATE_PROXY_WITH_NONCE_ABI,
    )
    safe_c = web3.eth.contract(abi=SAFE_SETUP_ABI)

    setup_calldata = safe_c.encode_abi(
        "setup",
        args=[
            [Web3.to_checksum_address(owner_eoa)],
            1,  # threshold
            ZERO_ADDRESS,  # to (no delegatecall during setup)
            b"",  # data
            ZERO_ADDRESS,  # fallbackHandler (leave unset for test fixtures)
            ZERO_ADDRESS,  # paymentToken
            0,  # payment
            ZERO_ADDRESS,  # paymentReceiver
        ],
    )

    deploy_calldata = factory.encode_abi(
        "createProxyWithNonce",
        args=[
            Web3.to_checksum_address(SAFE_L2_SINGLETON_V1_4_1),
            bytes.fromhex(setup_calldata[2:]) if isinstance(setup_calldata, str) else setup_calldata,
            secrets.randbits(256),  # random salt so parallel tests don't collide
        ],
    )

    receipt = _send_eoa_tx(
        web3,
        SAFE_PROXY_FACTORY_V1_4_1,
        bytes.fromhex(deploy_calldata[2:]) if isinstance(deploy_calldata, str) else deploy_calldata,
        owner_eoa,
        owner_private_key,
    )
    return _extract_proxy_from_receipt(receipt, _PROXY_CREATION_TOPIC)


# =============================================================================
# Zodiac Roles Modifier deployment + wiring
# =============================================================================


def deploy_test_zodiac_roles(
    web3: Web3,
    safe: str,
    owner_eoa: str,
    owner_private_key: str,
) -> str:
    """Clone a Roles v2 module with ``owner == avatar == target == safe``, then
    call ``Safe.enableModule(roles)``.

    Returns the new Roles Modifier proxy address.
    """
    # 1. Build the Roles setUp(bytes) initializer: abi.encode(owner, avatar, target).
    init_params = abi_encode(
        ["address", "address", "address"],
        [
            Web3.to_checksum_address(safe),
            Web3.to_checksum_address(safe),
            Web3.to_checksum_address(safe),
        ],
    )
    roles_c = web3.eth.contract(abi=ROLES_SET_UP_ABI)
    setup_calldata_hex = roles_c.encode_abi("setUp", args=[init_params])
    setup_calldata = (
        bytes.fromhex(setup_calldata_hex[2:]) if isinstance(setup_calldata_hex, str) else setup_calldata_hex
    )

    # 2. Deploy the proxy via ModuleProxyFactory (public, any EOA can call).
    factory = web3.eth.contract(
        address=Web3.to_checksum_address(MODULE_PROXY_FACTORY),
        abi=MODULE_PROXY_FACTORY_DEPLOY_MODULE_ABI,
    )
    deploy_calldata_hex = factory.encode_abi(
        "deployModule",
        args=[
            Web3.to_checksum_address(ROLES_MODIFIER_SINGLETON),
            setup_calldata,
            secrets.randbits(256),
        ],
    )
    deploy_calldata = (
        bytes.fromhex(deploy_calldata_hex[2:]) if isinstance(deploy_calldata_hex, str) else deploy_calldata_hex
    )
    receipt = _send_eoa_tx(web3, MODULE_PROXY_FACTORY, deploy_calldata, owner_eoa, owner_private_key)
    roles_address = _extract_proxy_from_receipt(receipt, _MODULE_PROXY_CREATION_TOPIC)

    # 3. Enable the module on the Safe (authorized → must go through Safe.execTransaction).
    safe_c = web3.eth.contract(abi=SAFE_ENABLE_MODULE_ABI)
    enable_calldata_hex = safe_c.encode_abi("enableModule", args=[Web3.to_checksum_address(roles_address)])
    enable_calldata = (
        bytes.fromhex(enable_calldata_hex[2:]) if isinstance(enable_calldata_hex, str) else enable_calldata_hex
    )
    _exec_safe_tx(web3, safe, safe, enable_calldata, SafeOperation.CALL, owner_eoa, owner_private_key)

    return roles_address


def assign_role_to_member(
    web3: Web3,
    roles: str,
    safe: str,
    role_key: bytes,
    member_eoa: str,
    owner_eoa: str,
    owner_private_key: str,
) -> None:
    """Grant ``member_eoa`` membership of ``role_key`` on ``roles`` and set it as default.

    Both calls are ``onlyOwner`` on the Roles contract (owner == safe), so they
    route through ``Safe.execTransaction``. We batch them via MultiSend so only
    one Safe tx is needed.
    """
    assert len(role_key) == 32, f"role_key must be bytes32, got {len(role_key)} bytes"

    roles_c_assign = web3.eth.contract(abi=ROLES_ASSIGN_ROLES_ABI)
    assign_calldata = roles_c_assign.encode_abi(
        "assignRoles",
        args=[Web3.to_checksum_address(member_eoa), [role_key], [True]],
    )

    roles_c_default = web3.eth.contract(abi=ROLES_SET_DEFAULT_ROLE_ABI)
    set_default_calldata = roles_c_default.encode_abi(
        "setDefaultRole",
        args=[Web3.to_checksum_address(member_eoa), role_key],
    )

    batch = [
        {"to": roles, "value": 0, "data": assign_calldata},
        {"to": roles, "value": 0, "data": set_default_calldata},
    ]
    multisend_calldata = MultiSendEncoder.encode_from_dicts(batch, web3)
    multisend_addr = _multisend_for_chain_id(web3.eth.chain_id)
    _exec_safe_tx(
        web3,
        safe,
        multisend_addr,
        multisend_calldata,
        SafeOperation.DELEGATE_CALL,
        owner_eoa,
        owner_private_key,
    )


# =============================================================================
# Manifest target application
# =============================================================================


def apply_manifest_targets(
    web3: Web3,
    roles: str,
    safe: str,
    role_key: bytes,
    targets: list[dict],
    owner_eoa: str,
    owner_private_key: str,
) -> None:
    """Apply ``PermissionManifest.to_zodiac_targets()`` output on-chain under ``role_key``.

    Walks the ``targets`` list and emits one or more Roles v2 calls per entry:

    - ``clearance == 1`` (whole-contract wildcard) → ``allowTarget(role_key, addr, options)``
    - ``clearance == 2`` (function-scoped) → ``scopeTarget(role_key, addr)`` then one
      ``allowFunction(role_key, addr, selector, options)`` per entry in ``target['functions']``

    All calls are batched into a single MultiSend payload and submitted as one
    Safe tx (DELEGATECALL to MultiSend).

    ``role_key`` must be the same 32-byte key that will be used by
    ``execTransactionWithRole``.

    Raises if ``targets`` is empty or if an unsupported clearance is encountered.
    """
    assert len(role_key) == 32, f"role_key must be bytes32, got {len(role_key)} bytes"
    if not targets:
        raise ValueError("apply_manifest_targets received empty targets list")

    roles_allow_target = web3.eth.contract(abi=ROLES_ALLOW_TARGET_ABI)
    roles_scope_target = web3.eth.contract(abi=ROLES_SCOPE_TARGET_ABI)
    roles_allow_function = web3.eth.contract(abi=ROLES_ALLOW_FUNCTION_ABI)

    batch: list[dict] = []

    for target in targets:
        addr = Web3.to_checksum_address(target["address"])
        clearance = int(target["clearance"])
        exec_options = int(target["executionOptions"])

        if clearance == 1:
            # Whole target wildcarded — one allowTarget call.
            data = roles_allow_target.encode_abi("allowTarget", args=[role_key, addr, exec_options])
            batch.append({"to": roles, "value": 0, "data": data})
        elif clearance == 2:
            # Function-scoped — scopeTarget then one allowFunction per selector.
            functions = target.get("functions", []) or []
            if not functions:
                raise ValueError(
                    f"Target {addr} has clearance=2 (function-scoped) but no functions[] — "
                    f"manifest.to_zodiac_targets() should have produced at least one function"
                )
            data = roles_scope_target.encode_abi("scopeTarget", args=[role_key, addr])
            batch.append({"to": roles, "value": 0, "data": data})
            for fn in functions:
                selector_hex = fn["selector"]
                # bytes4: accept both "0x..." and raw hex
                selector_bytes = (
                    bytes.fromhex(selector_hex[2:]) if selector_hex.startswith("0x") else bytes.fromhex(selector_hex)
                )
                if len(selector_bytes) != 4:
                    raise ValueError(f"selector must be 4 bytes, got {len(selector_bytes)}: {selector_hex}")
                data = roles_allow_function.encode_abi(
                    "allowFunction",
                    args=[role_key, addr, selector_bytes, exec_options],
                )
                batch.append({"to": roles, "value": 0, "data": data})
        else:
            raise ValueError(f"Unsupported clearance {clearance} on target {addr} — expected 1 or 2")

    multisend_calldata = MultiSendEncoder.encode_from_dicts(batch, web3)
    multisend_addr = _multisend_for_chain_id(web3.eth.chain_id)
    _exec_safe_tx(
        web3,
        safe,
        multisend_addr,
        multisend_calldata,
        SafeOperation.DELEGATE_CALL,
        owner_eoa,
        owner_private_key,
    )


def revoke_target(
    web3: Web3,
    roles: str,
    safe: str,
    role_key: bytes,
    target_address: str,
    owner_eoa: str,
    owner_private_key: str,
) -> None:
    """Call ``Roles.revokeTarget(role_key, target_address)`` via ``Safe.execTransaction``.

    Useful for negative tests: apply the full manifest first, then revoke one
    target and re-execute the same intent to assert the Roles Modifier now
    blocks it.
    """
    assert len(role_key) == 32, f"role_key must be bytes32, got {len(role_key)} bytes"
    roles_c = web3.eth.contract(abi=ROLES_REVOKE_TARGET_ABI)
    data_hex = roles_c.encode_abi(
        "revokeTarget",
        args=[role_key, Web3.to_checksum_address(target_address)],
    )
    data = bytes.fromhex(data_hex[2:]) if isinstance(data_hex, str) else data_hex
    _exec_safe_tx(web3, safe, roles, data, SafeOperation.CALL, owner_eoa, owner_private_key)


# =============================================================================
# Chain plumbing
# =============================================================================


_CHAIN_ID_TO_MULTISEND_KEY: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    56: "bsc",
    100: "gnosis",
    137: "polygon",
    196: "xlayer",
    5000: "mantle",
    8453: "base",
    42161: "arbitrum",
    43114: "avalanche",
}


def _multisend_for_chain_id(chain_id: int) -> str:
    key = _CHAIN_ID_TO_MULTISEND_KEY.get(chain_id)
    if key is None:
        raise ValueError(f"No MultiSend mapping for chain_id={chain_id}")
    return MULTISEND_ADDRESSES[key]
