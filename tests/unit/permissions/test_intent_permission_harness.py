"""Address-only funding joins used by the on-chain intent harness."""

import pytest

from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from tests.intents._permission_onchain_harness import _funding_key_for_token_ref


@pytest.mark.parametrize(
    ("chain", "label", "expected_address"),
    [
        ("base", "sUSDai", "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"),
        ("ethereum", "sUSDe", "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"),
        ("ethereum", "YT-sUSDe-13AUG2026", "0x45a699a11a4a17fe0931ef3cea4bfc3235e659f2"),
        ("arbitrum", "sUSDai", "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"),
        ("arbitrum", "PT-SUSDAI-15OCT2026", "0xb459db106f645d698e74027eef6019a26a0675cc"),
        ("polygon", "frxUSD", "0x80eede496655fb9047dd39d9f418d5483ed600df"),
        ("polygon", "WBTC", "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6"),
    ],
)
def test_long_tail_and_synthetic_labels_join_to_exact_addresses(
    chain: str,
    label: str,
    expected_address: str,
) -> None:
    assert _funding_key_for_token_ref(label, chain) == expected_address.lower()


@pytest.mark.parametrize(
    ("chain", "label"),
    [
        ("arbitrum", "ETH"),
        ("base", "ETH"),
        ("avalanche", "AVAX"),
        ("bnb", "BNB"),
        ("polygon", "MATIC"),
        ("polygon", "POL"),
    ],
)
def test_native_labels_join_to_shared_sentinel(chain: str, label: str) -> None:
    assert _funding_key_for_token_ref(label, chain) == NATIVE_SENTINEL
