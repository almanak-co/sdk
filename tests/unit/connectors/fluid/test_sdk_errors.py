"""Unit tests for Fluid module-aware error decoding (VIB-5031, D3.F2).

Fluid wraps every revert in one generic per-module custom error carrying a
uint256 errorId, and every module numbers its ids independently — so a name
table is only meaningful TOGETHER with its module selector. Phase 3 adds the
VaultT1 31xxx table (``contracts/protocols/vault/errorTypes.sol``) alongside
the existing DexT1 table and makes ``decode_fluid_revert`` dispatch the name
lookup on the MODULE, never on the bare number.
"""

from __future__ import annotations

from almanak.connectors.fluid.sdk import (
    DEX_T1_ERROR_IDS,
    VAULT_T1_ERROR_IDS,
    decode_fluid_revert,
    fluid_error_id,
    fluid_error_module,
)


def _module_revert(selector: str, error_id: int) -> str:
    return "0x" + selector + f"{error_id:064x}"


class TestVaultErrorTable:
    """The VaultT1 31xxx table ships alongside the DexT1 table."""

    def test_31015_named_excess_debt_payback_in_table(self):
        # The over-repay revert characterized on-chain in Phase-0 V3.5 —
        # the Morpho-class repay-full trap's protocol error.
        assert VAULT_T1_ERROR_IDS[31015] == "Vault__ExcessDebtPayback"

    def test_vault_error_table_present_alongside_dex_table(self):
        # Both tables coexist; neither replaced the other.
        assert DEX_T1_ERROR_IDS[51049] == "DexT1__LimitingAmountsSwapAndNonPerfectActions"
        assert VAULT_T1_ERROR_IDS[31014] == "Vault__ExcessCollateralWithdrawal"
        assert VAULT_T1_ERROR_IDS[31006] == "Vault__PositionAboveCF"

    def test_vault_error_ids_all_31xxx(self):
        # The table is the Vault (T1 core) section only — other vault-side
        # modules (factory 30xxx, ERC721 32xxx, admin 33xxx) are not T1
        # operate() errors and render numerically.
        assert all(31000 < error_id < 32000 for error_id in VAULT_T1_ERROR_IDS)


class TestVaultErrorDecoding:
    """D3.F2 — a 31015 revert decodes BY NAME through the existing path."""

    def test_31015_excess_debt_decodes_by_name(self):
        raw = _module_revert("60121cca", 31015)
        decoded = decode_fluid_revert(raw)
        assert "FluidVaultError" in decoded
        assert "31015" in decoded
        assert "Vault__ExcessDebtPayback" in decoded, f"31015 must decode by name, got: {decoded}"

    def test_vault_error_id_and_module_extraction(self):
        raw = _module_revert("60121cca", 31015)
        assert fluid_error_id(raw) == 31015
        assert fluid_error_module(raw) == "FluidVaultError"

    def test_unknown_vault_error_id_renders_numerically(self):
        raw = _module_revert("60121cca", 31999)
        decoded = decode_fluid_revert(raw)
        assert "FluidVaultError" in decoded
        assert "errorId=31999" in decoded


class TestModuleAwareDispatch:
    """Names never cross modules — ids are only meaningful with their selector."""

    def test_vault_error_never_gets_dex_name(self):
        # FluidVaultError wrapping a numerically-DexT1 id must NOT be labeled
        # with the DexT1 name.
        raw = _module_revert("60121cca", 51049)
        decoded = decode_fluid_revert(raw)
        assert "FluidVaultError" in decoded
        assert "DexT1__" not in decoded
        assert "errorId=51049" in decoded

    def test_liquidity_error_never_gets_vault_error_name(self):
        # FluidLiquidityError has no name table — even a numerically-vault id
        # renders numerically.
        raw = _module_revert("dcab82e2", 31015)
        decoded = decode_fluid_revert(raw)
        assert "FluidLiquidityError" in decoded
        assert "Vault__" not in decoded
        assert "errorId=31015" in decoded

    def test_dex_error_names_unchanged(self):
        # Phase-1/2 regression guard: DexT1 decoding is untouched.
        raw = _module_revert("2fee3e0e", 51049)
        decoded = decode_fluid_revert(raw)
        assert "FluidDexError" in decoded
        assert "DexT1__LimitingAmountsSwapAndNonPerfectActions" in decoded
