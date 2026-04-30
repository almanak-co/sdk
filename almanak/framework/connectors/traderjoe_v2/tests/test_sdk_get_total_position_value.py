"""Tests for ``TraderJoeV2SDK.get_total_position_value`` strict mode + sanity floor.

VIB-3757: investigation discovered the historical "best-effort" silent
skip was masking a real ABI selector mismatch (``getBin(uint256)`` vs
``getBin(uint24)``). With the ABI fixed, well-behaved RPCs do not revert.
This test suite pins the new defensive behaviour:

- ``strict=True`` raises on the first per-bin read error.
- ``strict=False`` (default) tolerates failures up to ``sanity_floor``,
  raises beyond that.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ..sdk import TraderJoeV2SDK


@pytest.fixture
def sdk_with_mock_pair() -> tuple[TraderJoeV2SDK, MagicMock]:
    """Return an SDK and a MagicMock LBPair contract wired into it."""
    sdk = TraderJoeV2SDK.__new__(TraderJoeV2SDK)
    pair = MagicMock()
    sdk.get_pair_contract = MagicMock(return_value=pair)  # type: ignore[method-assign]
    return sdk, pair


def _wire_bins(
    pair: MagicMock,
    *,
    bin_data: dict[int, tuple[int, int, int]],
    revert_bins: set[int] | None = None,
) -> None:
    """Wire pair.functions.getBin / totalSupply with per-bin behaviour.

    ``bin_data`` maps bin_id -> (reserve_x, reserve_y, total_supply).
    ``revert_bins`` is a set of bin_ids whose getBin/totalSupply call
    should raise (simulating an RPC revert).
    """
    revert = revert_bins or set()

    def get_bin(bin_id: int) -> MagicMock:
        m = MagicMock()
        if bin_id in revert:
            m.call.side_effect = RuntimeError("execution reverted")
        else:
            m.call.return_value = (bin_data[bin_id][0], bin_data[bin_id][1])
        return m

    def total_supply(bin_id: int) -> MagicMock:
        m = MagicMock()
        if bin_id in revert:
            m.call.side_effect = RuntimeError("execution reverted")
        else:
            m.call.return_value = bin_data[bin_id][2]
        return m

    pair.functions.getBin.side_effect = get_bin
    pair.functions.totalSupply.side_effect = total_supply


class TestStrictMode:
    """``strict=True`` must fail loudly on any per-bin revert."""

    def test_strict_raises_on_single_bin_revert(self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]) -> None:
        sdk, pair = sdk_with_mock_pair
        _wire_bins(
            pair,
            bin_data={
                100: (1000, 2000, 10),
                101: (0, 0, 0),
                102: (3000, 4000, 30),
            },
            revert_bins={101},
        )
        balances = {100: 5, 101: 5, 102: 5}

        with pytest.raises(RuntimeError, match="bin_id=101"):
            sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances=balances, strict=True)

    def test_strict_returns_correct_value_when_all_succeed(
        self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]
    ) -> None:
        sdk, pair = sdk_with_mock_pair
        # bin 100: balance 5, supply 10 → share = 50%; reserves (1000, 2000) → 500/1000
        _wire_bins(pair, bin_data={100: (1000, 2000, 10)})

        x, y = sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances={100: 5}, strict=True)
        assert (x, y) == (500, 1000)


class TestSanityFloor:
    """``strict=False`` keeps best-effort but raises below ``sanity_floor``."""

    def test_default_floor_passes_when_all_bins_succeed(
        self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]
    ) -> None:
        sdk, pair = sdk_with_mock_pair
        _wire_bins(
            pair,
            bin_data=dict.fromkeys(range(10), (1000, 2000, 10)),
        )
        balances = dict.fromkeys(range(10), 5)

        x, y = sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances=balances)
        # 10 bins, each contributing balance/supply * reserves = 5/10 * (1000, 2000) = (500, 1000)
        assert (x, y) == (5000, 10000)

    def test_default_floor_passes_when_one_bin_fails_in_ten(
        self,
        sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        sdk, pair = sdk_with_mock_pair
        _wire_bins(
            pair,
            bin_data=dict.fromkeys(range(10), (1000, 2000, 10)),
            revert_bins={3},
        )
        balances = dict.fromkeys(range(10), 5)

        # 9/10 = 90% success — exactly at the default floor of 0.9, must pass.
        x, y = sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances=balances)
        # 9 succeeding bins, each 500/1000 → totals 4500/9000.
        assert (x, y) == (4500, 9000)
        # And a warning surfaces for operators.
        assert any("1/10 bins failed" in r.getMessage() for r in caplog.records)

    def test_default_floor_raises_when_too_many_bins_fail(
        self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]
    ) -> None:
        sdk, pair = sdk_with_mock_pair
        _wire_bins(
            pair,
            bin_data=dict.fromkeys(range(10), (1000, 2000, 10)),
            revert_bins={1, 2, 3, 4},  # 4/10 fail → 60% success
        )
        balances = dict.fromkeys(range(10), 5)

        with pytest.raises(RuntimeError, match=r"60\.0%"):
            sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances=balances)

    def test_explicit_zero_floor_restores_silent_degradation(
        self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]
    ) -> None:
        # ``sanity_floor=0`` opts out of the guard entirely — this is the
        # legacy behaviour, available as an explicit escape hatch but not
        # the default.
        sdk, pair = sdk_with_mock_pair
        _wire_bins(
            pair,
            bin_data=dict.fromkeys(range(10), (1000, 2000, 10)),
            revert_bins={1, 2, 3, 4, 5, 6, 7, 8, 9},  # 9/10 fail
        )
        balances = dict.fromkeys(range(10), 5)

        x, y = sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances=balances, sanity_floor=0.0)
        # Only bin 0 succeeded → 500/1000 contribution.
        assert (x, y) == (500, 1000)


class TestEmptyAndPrecomputed:
    def test_returns_zero_for_empty_balances(self, sdk_with_mock_pair: tuple[TraderJoeV2SDK, MagicMock]) -> None:
        sdk, _ = sdk_with_mock_pair
        x, y = sdk.get_total_position_value("0xpool", "0xwallet", precomputed_balances={})
        assert (x, y) == (0, 0)


class TestLBPairABISelector:
    """Pin the ``getBin`` ABI selector against the deployed contract.

    VIB-3757 audit: the unit tests above mock ``pair.functions.getBin`` and
    bypass web3.py's contract dispatch entirely, so they cannot regression-
    protect the actual fix this PR makes (changing the ``getBin`` parameter
    from ``uint256`` to ``uint24`` to match the deployed contract). A
    future PR could revert ``LBPair.json`` and the mocked tests would still
    pass.

    These tests inspect the raw ABI JSON and assert the selector-affecting
    pieces, which DO catch a regression of the form "someone reverted the
    ABI signature".
    """

    def _load_lbpair_abi(self) -> list[dict]:
        from importlib.resources import files

        abi_path = files("almanak.framework.connectors.traderjoe_v2.abis").joinpath("LBPair.json")
        import json

        return json.loads(abi_path.read_text())

    def test_get_bin_uses_uint24_not_uint256(self) -> None:
        # The deployed LBPair on Avalanche / Arbitrum / etc. exposes
        # ``getBin(uint24)``. ``getBin(uint256)`` has a different selector
        # and reverts when called against the real contract — see
        # docs/internal/reports/vib-3757-tj-v2-investigation-2026-04-30.md
        # for the cast-call evidence. Pin the parameter type so we do not
        # silently regress.
        abi = self._load_lbpair_abi()
        get_bin = next((f for f in abi if f.get("name") == "getBin"), None)
        assert get_bin is not None, "getBin missing from LBPair.json"
        inputs = get_bin.get("inputs", [])
        assert len(inputs) == 1
        assert inputs[0]["type"] == "uint24", (
            f"getBin parameter must be uint24 (deployed selector 0x0abe9688), "
            f"got {inputs[0]['type']!r}. See VIB-3757 investigation report."
        )

    def test_get_active_id_returns_uint24(self) -> None:
        # ``getActiveId`` returns the active bin id as ``uint24``; this
        # value flows into ``getBin(uint24)``. If the return type ever
        # widens, callers may pass values that overflow uint24 and revert.
        abi = self._load_lbpair_abi()
        get_active = next((f for f in abi if f.get("name") == "getActiveId"), None)
        assert get_active is not None, "getActiveId missing from LBPair.json"
        outputs = get_active.get("outputs", [])
        assert len(outputs) == 1
        assert outputs[0]["type"] == "uint24"
