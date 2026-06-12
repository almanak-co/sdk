"""Registry/manifest integration tests for the fluid_vault connector (VIB-5031).

Per-module behaviour is covered in ``tests/unit/connectors/fluid/``; this
file pins what the rest of the framework sees (UAT card D2.M4): the
manifest surface, the lending-read registry slot, the capabilities table,
the support matrix, the synthetic-intent (Zodiac) matrix, and — critically
— that the shipped Phase-2 fToken surface is provably unperturbed.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors.fluid_vault.connector import CONNECTOR


class TestFluidVaultManifest:
    def test_lending_intents_exactly_four(self):
        # DELEVERAGE compiles as a REPAY (BaseLendingCompiler dispatch) and
        # is deliberately NOT a declared intent row.
        assert CONNECTOR.strategy_intents == ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")

    def test_chains_exactly_arbitrum_base_no_cross_product(self):
        assert CONNECTOR.strategy_chains == ("arbitrum", "base")
        entries = {e.category: e.chains for e in CONNECTOR.strategy_matrix_entries}
        assert entries == {"lending": frozenset(("arbitrum", "base"))}

    def test_kind_is_lending(self):
        assert CONNECTOR.kind is ProtocolKind.LENDING

    def test_no_aliases_fluid_lending_stays_on_fluid(self):
        # ADR r2 Q0: nothing aliases to fluid_vault; the platform-spec
        # ``fluid_lending`` keeps resolving to the fToken connector.
        assert CONNECTOR.aliases == ()
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        assert normalize_protocol("arbitrum", "fluid_lending") == "fluid"

    def test_metadata_amounts_declared_wei(self):
        assert CONNECTOR.metadata_amount_encoding is not None
        assert CONNECTOR.metadata_amount_encoding.lending == "wei"

    def test_vault_universe_chain_sets_in_sync(self):
        # The chain universe lives in three hand-maintained places — the
        # manifest, the compiler gate, and the pinned vault/market table.
        # Drift would compile positions valuation cannot mark (the Phase-2
        # fluid connector pins the same invariant for its surfaces).
        from almanak.connectors.fluid.addresses import FLUID_VAULT, FLUID_VAULT_MARKETS
        from almanak.connectors.fluid.vault_compiler import FluidVaultCompiler

        manifest_chains = frozenset(CONNECTOR.strategy_chains)
        assert FluidVaultCompiler.VAULT_CHAINS == manifest_chains
        assert frozenset(FLUID_VAULT_MARKETS.keys()) == manifest_chains
        assert frozenset(FLUID_VAULT.keys()) == manifest_chains


class TestFluidVaultRegistries:
    def test_capabilities_registered_requires_market_id(self):
        from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

        capabilities = get_protocol_capabilities("fluid_vault")
        assert capabilities.get("requires_market_id") is True
        # The fToken key must NOT have gained a capabilities entry.
        assert get_protocol_capabilities("fluid") == {}

    def test_compiler_registered(self):
        from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

        compiler = CompilerRegistry.get("fluid_vault")
        assert compiler is not None
        assert type(compiler).__name__ == "FluidVaultCompiler"

    def test_lending_read_registry_has_fluid_vault_slot(self):
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        assert LendingReadRegistry.supports_account_state("fluid_vault")
        assert LendingReadRegistry.publishes_market_table("fluid_vault")
        assert LendingReadRegistry.declares_valuation_roles("fluid_vault")
        # The Phase-2 fToken slot is intact and DISTINCT.
        assert LendingReadRegistry.supports_account_state("fluid")
        assert LendingReadRegistry.normalize_protocol("fluid_vault") == "fluid_vault"
        assert LendingReadRegistry.normalize_protocol("fluid_lending") == "fluid"

    def test_market_params_resolve_pinned_vaults(self):
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        params = LendingReadRegistry.market_params(
            "fluid_vault", "arbitrum", "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C"
        )
        assert params is not None
        assert params["collateral_token"] == "ETH"
        assert params["loan_token"] == "USDC"
        base_params = LendingReadRegistry.market_params(
            "fluid_vault", "base", "0x01F0D07fdE184614216e76782c6b7dF663F5375e"
        )
        assert base_params is not None
        assert base_params["collateral_token"] == "sUSDai"

    def test_vault_resolver_address_resolves(self):
        from almanak.connectors._strategy_base.address_registry import AddressRegistry

        for chain in ("arbitrum", "base"):
            address = AddressRegistry.resolve_contract_address("fluid_vault", chain, ("vault_resolver",))
            assert address
            assert address.lower() == "0xa5c3e16523eeeddcc34706b0e6be88b4c6ea95cc"

    def test_receipt_registry_routes_fluid_vault_to_vault_parser(self):
        from almanak.connectors.fluid.receipt_parser import FluidReceiptParser, FluidVaultReceiptParser
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        registry = ReceiptParserRegistry()
        assert isinstance(registry.get("fluid_vault"), FluidVaultReceiptParser)
        # The DEX/fToken keys keep their Phase-1/2 parser.
        assert isinstance(registry.get("fluid"), FluidReceiptParser)
        assert isinstance(registry.get("fluid_lending"), FluidReceiptParser)

    def test_support_matrix_has_fluid_vault_lending_row(self):
        from almanak.framework.cli.support_matrix import _build_matrix

        matrix = _build_matrix()
        entries = [p for p in matrix["protocols"] if p["name"] == "fluid_vault"]
        assert len(entries) == 1, "fluid_vault must appear exactly once in the support matrix"
        assert entries[0]["category"] == "lending"
        assert set(entries[0]["chains"]) == {"arbitrum", "base"}

    def test_generic_pre_state_capture_enabled(self):
        # Sibling-connector convention (silo_v2 / euler_v2 / benqi / fluid):
        # confidence=HIGH only with explicit membership on the live read path.
        from almanak.framework.accounting.lending_accounting import _GENERIC_PRE_STATE_PROTOCOLS

        assert "fluid_vault" in _GENERIC_PRE_STATE_PROTOCOLS
        assert "fluid" in _GENERIC_PRE_STATE_PROTOCOLS  # Phase 2 untouched


class TestFluidVaultZodiacMatrix:
    def test_synthetic_matrix_covers_exactly_four_lending_intents(self):
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.permissions.synthetic_intents import get_protocol_intent_matrix

        matrix = get_protocol_intent_matrix()
        assert matrix.get("fluid_vault") == frozenset(
            {IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW}
        )

    def test_discovery_vectors_cover_pinned_vaults_no_erc721_surface(self):
        from almanak.framework.permissions.hints import (
            DiscoveryContext,
            get_discovery_vectors_override,
            get_permission_hints,
        )

        hints = get_permission_hints("fluid_vault")
        assert hints.synthetic_discovery_intents == frozenset({"SUPPLY", "BORROW", "REPAY", "WITHDRAW"})
        assert "0x032d2276" in hints.selector_labels  # the vault operate() selector
        # No ERC-721-ONLY selectors (setApprovalForAll / transferFrom /
        # safeTransferFrom) anywhere in the declared surface. NOTE:
        # approve(address,uint256) (0x095ea7b3) is deliberately NOT forbidden
        # — the selector is shared between ERC-20 and ERC-721, and the ERC-20
        # approve on the collateral/debt legs is REQUIRED. The target-side
        # invariant (approves hit token addresses, never the VaultFactory) is
        # pinned in test_no_erc721_vectors_and_approves_target_erc20_tokens_only.
        erc721_vectors = ("0xa22cb465", "0x23b872dd", "0x42842e0e", "0xb88d4fde")
        assert not any(s in hints.selector_labels for s in erc721_vectors)

        vectors_fn = get_discovery_vectors_override("fluid_vault")
        assert vectors_fn is not None
        ctx = DiscoveryContext(usdc="0x" + "1" * 40, weth="0x" + "2" * 40)
        for intent_type in ("SUPPLY", "BORROW", "REPAY", "WITHDRAW"):
            for chain, vault in (
                ("arbitrum", "0xeabbfca72f8a8bf14c4ac59e69ecb2eb69f0811c"),
                ("base", "0x01f0d07fde184614216e76782c6b7df663f5375e"),
            ):
                vectors = vectors_fn("fluid_vault", intent_type, chain, ctx)
                assert vectors, f"no {intent_type} discovery vector on {chain}"
                assert [v.market_id for v in vectors] == [vault]
        # Unsupported chains emit NO doomed synthetics.
        assert vectors_fn("fluid_vault", "SUPPLY", "ethereum", ctx) == []

    def test_no_erc721_vectors_and_approves_target_erc20_tokens_only(self):
        """Compile every discovery vector and pin the REAL no-ERC-721 invariant.

        ``approve(address,uint256)`` (``0x095ea7b3``) shares its selector
        between ERC-20 and ERC-721 — forbidding the selector outright would
        forbid the required ERC-20 approve on the collateral/debt legs. The
        true invariant: no setApprovalForAll / transferFrom /
        safeTransferFrom vector is ever compiled, and every compiled approve
        targets a TOKEN address (collateral or debt), never the VaultFactory
        (the ERC-721 home of the position NFTs).
        """
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
        from almanak.connectors.fluid.addresses import FLUID_VAULT, FLUID_VAULT_MARKETS
        from almanak.connectors.fluid.vault_compiler import FluidVaultCompiler
        from almanak.framework.permissions.hints import DiscoveryContext, get_discovery_vectors_override

        erc721_vectors = ("0xa22cb465", "0x23b872dd", "0x42842e0e", "0xb88d4fde")
        token_decimals = {"ETH": 18, "sUSDai": 18, "USDC": 6}
        vectors_fn = get_discovery_vectors_override("fluid_vault")
        assert vectors_fn is not None
        discovery_ctx = DiscoveryContext(usdc="0x" + "1" * 40, weth="0x" + "2" * 40)
        wallet = "0x2222222222222222222222222222222222222222"

        for chain, markets in FLUID_VAULT_MARKETS.items():
            factory = FLUID_VAULT[chain]["vault_factory"].lower()
            tokens: dict[str, SimpleNamespace] = {}
            token_addresses: set[str] = set()
            for entry in markets.values():
                legs = (
                    (entry["collateral_token"], entry["collateral_address"], bool(entry["native_collateral"])),
                    (entry["loan_token"], entry["loan_address"], bool(entry["native_debt"])),
                )
                for symbol, address, is_native in legs:
                    tokens[symbol] = SimpleNamespace(
                        symbol=symbol,
                        address=address,
                        decimals=token_decimals[symbol],
                        is_native=is_native,
                        to_dict=lambda s=symbol, a=address: {
                            "symbol": s,
                            "address": a,
                            "decimals": token_decimals[s],
                        },
                    )
                    token_addresses.add(address.lower())
            services = MagicMock()
            services.resolve_token.side_effect = tokens.get
            services.format_amount.side_effect = lambda amount, decimals: str(amount)
            ctx = BaseCompilerContext(
                chain=chain,
                wallet_address=wallet,
                rpc_url="http://localhost:9",
                rpc_timeout=10.0,
                permission_discovery=True,  # calldata shape only — no on-chain reads
                allow_placeholder_prices=True,
                token_resolver=None,
                gateway_client=None,
                price_oracle={},
                cache={},
                services=services,
            )
            for intent_type in ("SUPPLY", "BORROW", "REPAY", "WITHDRAW"):
                for intent in vectors_fn("fluid_vault", intent_type, chain, discovery_ctx):
                    result = FluidVaultCompiler().compile(ctx, intent)
                    assert result.status.value == "SUCCESS", f"{chain}/{intent_type}: {result.error}"
                    for tx in result.transactions:
                        selector = tx.data[:10].lower()
                        assert selector not in erc721_vectors, f"{chain}/{intent_type}: ERC-721 vector {selector}"
                        if selector == "0x095ea7b3":
                            target = tx.to.lower()
                            assert target != factory, f"{chain}/{intent_type}: approve targets the VaultFactory"
                            assert target in token_addresses, f"{chain}/{intent_type}: approve targets non-token {target}"

    def test_discovery_borrow_amount_is_concrete_decimal(self):
        from almanak.framework.permissions.hints import DiscoveryContext, get_discovery_vectors_override

        vectors_fn = get_discovery_vectors_override("fluid_vault")
        ctx = DiscoveryContext(usdc="0x" + "1" * 40, weth="0x" + "2" * 40)
        (borrow,) = vectors_fn("fluid_vault", "BORROW", "arbitrum", ctx)
        assert isinstance(borrow.collateral_amount, Decimal)
        assert isinstance(borrow.borrow_amount, Decimal)
        assert borrow.borrow_amount > 0
