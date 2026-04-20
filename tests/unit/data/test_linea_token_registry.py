"""Guard test: verify Linea token addresses and decimals in the static registry.

VIB-2725: Linea bridged USDC was flagged as a potential 18-decimal surprise.
On-chain verification confirmed 6 decimals. This test guards against
accidental changes to Linea token metadata.

VIB-2724: Linea token storage slots were all zero (unverified placeholders).
Verified on-chain and corrected: USDC=9, WETH=3, USDT=51.
"""

import pytest

from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE


# On-chain verified addresses (Linea mainnet, verified 2026-04-12)
LINEA_USDC = "0x176211869cA2b568f2A7D4EE941E073a821EE1ff"
LINEA_USDT = "0xA219439258ca9da29E9Cc4cE5596924745e12B93"
LINEA_WETH = "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f"


def test_linea_in_wrapped_native():
    """Linea must have a WRAPPED_NATIVE entry (WETH)."""
    assert "linea" in WRAPPED_NATIVE, "Linea missing from WRAPPED_NATIVE"
    assert WRAPPED_NATIVE["linea"].lower() == LINEA_WETH.lower()


def test_linea_usdc_is_6_decimals():
    """Linea bridged USDC must be registered with 6 decimals.

    On-chain verification: cast call 0x176211869cA2b568f2A7D4EE941E073a821EE1ff
    "decimals()(uint8)" --rpc-url linea -> 6
    """
    from almanak.framework.data.tokens.defaults import USDC

    assert "linea" in USDC.addresses, "Linea missing from USDC.addresses"
    assert USDC.addresses["linea"].lower() == LINEA_USDC.lower()
    # USDC default decimals is 6; Linea has no chain_override, so it inherits 6
    assert USDC.decimals == 6
    assert "linea" not in getattr(USDC, "chain_overrides", {}), (
        "Linea USDC should NOT have a chain_override (it uses standard 6 decimals)"
    )


def test_linea_usdt_is_6_decimals():
    """Linea bridged USDT must be registered with 6 decimals.

    On-chain verification: cast call 0xA219439258ca9da29E9Cc4cE5596924745e12B93
    "decimals()(uint8)" --rpc-url linea -> 6
    """
    from almanak.framework.data.tokens.defaults import USDT

    assert "linea" in USDT.addresses, "Linea missing from USDT.addresses"
    assert USDT.addresses["linea"].lower() == LINEA_USDT.lower()
    assert USDT.decimals == 6
    assert "linea" not in getattr(USDT, "chain_overrides", {}), (
        "Linea USDT should NOT have a chain_override (it uses standard 6 decimals)"
    )


def test_linea_weth_is_18_decimals():
    """Linea WETH must be registered with 18 decimals."""
    from almanak.framework.data.tokens.defaults import WETH

    assert "linea" in WETH.addresses, "Linea missing from WETH.addresses"
    assert WETH.addresses["linea"].lower() == LINEA_WETH.lower()
    assert WETH.decimals == 18


@pytest.mark.parametrize(
    "token_symbol,expected_slot",
    [("USDC", 9), ("WETH", 3), ("USDT", 51)],
    ids=["USDC-slot-9", "WETH-slot-3", "USDT-slot-51"],
)
def test_linea_storage_slots_are_verified(token_symbol: str, expected_slot: int):
    """Linea storage slots must match on-chain verified values.

    Verified 2026-04-12 using cast index + cast storage on Linea mainnet.
    Wrong slots cause silent Anvil funding failures (balanceOf returns 0).
    """
    from almanak.framework.anvil.fork_manager import KNOWN_BALANCE_SLOTS

    assert "linea" in KNOWN_BALANCE_SLOTS, "Linea missing from KNOWN_BALANCE_SLOTS"
    assert token_symbol in KNOWN_BALANCE_SLOTS["linea"], (
        f"{token_symbol} missing from KNOWN_BALANCE_SLOTS['linea']"
    )
    assert KNOWN_BALANCE_SLOTS["linea"][token_symbol] == expected_slot, (
        f"KNOWN_BALANCE_SLOTS['linea']['{token_symbol}'] is "
        f"{KNOWN_BALANCE_SLOTS['linea'][token_symbol]} but on-chain verified "
        f"value is {expected_slot}. Wrong slots cause silent Anvil funding failures."
    )
