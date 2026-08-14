"""Contract tests for the report-only effective capability matrix."""

from dataclasses import replace
from datetime import date

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector_descriptor import (
    Connector,
    DexVolumeDecl,
    FungibleLpCloseDecl,
    ImportRef,
    LendingReadDecl,
    LifecycleObligationDecl,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ExactTargetFeature,
    IntentSemantics,
    NotApplicable,
    NotApplicableRuleId,
    ObligationDeclaration,
    ObligationId,
    Satisfied,
    SupportClaim,
    Unsupported,
)
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities import effective_matrix as effective_matrix_module
from almanak.framework.capabilities.effective_matrix import (
    CapabilityCellKey,
    EffectiveDeclarationRule,
    MatrixUniverse,
    ProviderSource,
    ResolvedDeclaration,
    SourceKind,
    SourceRole,
    UniverseKind,
    build_effective_capability_matrix,
    render_markdown,
)
from almanak.framework.capabilities.obligation_profiles import InvalidNotApplicableRule, ReportedObligationState
from almanak.framework.primitives.types import Primitive


def _ref(attribute: str) -> ImportRef:
    return ImportRef("almanak.example.providers", attribute)


def _swap_connector(
    name: str = "test_swap",
    *,
    alias_on_base: bool = False,
) -> Connector:
    return Connector(
        name=name,
        kind=ProtocolKind.SWAP,
        aliases=(f"{name}_alias",) if alias_on_base else (),
        compiler=_ref("Compiler"),
        receipt_parser_connector=_ref("ReceiptParser"),
        swap_quote_connector=_ref("QuoteProvider"),
        gas_estimate_connector=_ref("GasEstimator"),
        supported_chains=SupportedChainsSpec(
            chains=(ETHEREUM,),
            protocol_overrides={f"{name}_alias": (BASE,)} if alias_on_base else {},
        ),
        strategy_intents=(IntentType.SWAP,),
    )


def _lp_connector(*, teardown_hook: bool) -> Connector:
    return Connector(
        name="test_lp",
        kind=ProtocolKind.LP,
        compiler=_ref("LpCompiler"),
        receipt_parser_connector=_ref("LpReceiptParser"),
        teardown_post_condition=_ref("check_closed") if teardown_hook else None,
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
        strategy_intents=(IntentType.LP_OPEN,),
    )


def _satisfied() -> Satisfied:
    return Satisfied(
        provider_ref="almanak.example.providers:Provider",
        contract_version="provider.v1",
        test_evidence=(EvidenceRef(EvidenceKind.CONTRACT_TEST, "tests/example.py::test_provider"),),
    )


def _source(kind: SourceKind, rule_id: str) -> ProviderSource:
    return ProviderSource(
        kind=kind,
        role=SourceRole.DECLARATION,
        rule_id=rule_id,
        ref=f"test:{rule_id}",
        detail="Explicit test declaration with durable evidence.",
    )


def _complete_resolver(_connector: object, _key: CapabilityCellKey, profile: object):
    for row in profile.obligations:
        obligation = row.obligation
        if obligation is ObligationId.PERMISSION_PLAN:
            yield ResolvedDeclaration(
                ObligationDeclaration(
                    obligation,
                    NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED),
                ),
                _source(SourceKind.CORE_RULE, "core.permission_plan_not_required.v1"),
            )
        else:
            kind = SourceKind.FRAMEWORK_DEFAULT
            if obligation is ObligationId.COMPILER:
                kind = SourceKind.CONNECTOR_MANIFEST
            elif obligation is ObligationId.AMOUNT_PROTECTION:
                kind = SourceKind.APPROVED_ALTERNATIVE
            yield ResolvedDeclaration(
                ObligationDeclaration(obligation, _satisfied()),
                _source(kind, f"test.{obligation.value}.v1"),
            )


def _cell(matrix: object, claim: SupportClaim):
    return next(item for item in matrix.cells if item.key.claim is claim)


def test_registered_strategy_universe_is_explicit_and_protocol_overrides_are_exact() -> None:
    matrix = build_effective_capability_matrix((_swap_connector(alias_on_base=True),))
    assert matrix.universe.kind is UniverseKind.REGISTERED_STRATEGY_SUPPORT
    assert {(cell.key.protocol, cell.key.chain) for cell in matrix.cells} == {
        ("test_swap", "ethereum"),
        ("test_swap_alias", "base"),
    }
    assert {cell.key.claim for cell in matrix.cells} == {
        SupportClaim.CORE_EXECUTION,
        SupportClaim.EXACT_TARGET_DATA,
        SupportClaim.MANAGED_ANVIL_TESTABLE,
    }


def test_manifest_provider_presence_is_candidate_provenance_not_satisfaction() -> None:
    matrix = build_effective_capability_matrix((_swap_connector(),))
    cell = _cell(matrix, SupportClaim.CORE_EXECUTION)
    compiler = next(item for item in cell.obligations if item.audited.obligation is ObligationId.COMPILER)
    assert compiler.state is ReportedObligationState.UNDECLARED
    assert [source.role for source in compiler.sources] == [SourceRole.REQUIREMENT, SourceRole.CANDIDATE]
    assert compiler.sources[1].ref.endswith("almanak.example.providers:Compiler")
    assert not cell.claim_satisfied


def test_default_generator_consumes_connector_owned_evidence_backed_declarations() -> None:
    declaration = LifecycleObligationDecl(
        protocol="test_swap",
        chain=ETHEREUM,
        intent=IntentType.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
        declaration=ObligationDeclaration(ObligationId.COMPILER, _satisfied()),
        rule_id="connector.compiler.v1",
        source_ref="Connector.lifecycle_declarations[test_swap.compiler]",
        source_detail="Evidence-backed connector compiler declaration.",
    )
    connector = Connector(
        name="test_swap",
        kind=ProtocolKind.SWAP,
        compiler=_ref("Compiler"),
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
        strategy_intents=(IntentType.SWAP,),
        lifecycle_declarations=(declaration,),
    )
    matrix = build_effective_capability_matrix((connector,))
    cell = _cell(matrix, SupportClaim.CORE_EXECUTION)
    compiler = next(item for item in cell.obligations if item.audited.obligation is ObligationId.COMPILER)
    assert compiler.state is ReportedObligationState.SATISFIED


def test_connector_declaration_intent_fails_loud_before_primitive_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _swap_connector()
    declaration = LifecycleObligationDecl(
        protocol="test_swap",
        chain=ETHEREUM,
        intent=IntentType.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
        declaration=ObligationDeclaration(ObligationId.COMPILER, _satisfied()),
        rule_id="connector.compiler.v1",
        source_ref="Connector.lifecycle_declarations[test_swap.compiler]",
        source_detail="Evidence-backed connector compiler declaration.",
    )
    connector = replace(connector, lifecycle_declarations=(declaration,))

    monkeypatch.setattr(effective_matrix_module, "record_for", lambda _intent: (_ for _ in ()).throw(ValueError("unknown intent")))
    monkeypatch.setattr(effective_matrix_module, "primitive_for", lambda *_args: pytest.fail("fallback must not run"))
    with pytest.raises(ValueError, match="unknown intent"):
        effective_matrix_module._connector_declaration_rules((connector,))


def test_default_full_generation_rejects_unmatched_canonical_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    key = CapabilityCellKey(
        protocol="unowned_typo",
        chain="ethereum",
        intent=IntentType.SWAP,
        semantics=IntentSemantics.ATOMIC_EXECUTION,
        primitive=Primitive.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
    )
    rule = EffectiveDeclarationRule(
        key,
        ResolvedDeclaration(
            ObligationDeclaration(ObligationId.COMPILER, _satisfied()),
            _source(SourceKind.FRAMEWORK_DEFAULT, "unmatched.compiler.v1"),
        ),
    )
    monkeypatch.setattr(effective_matrix_module, "EFFECTIVE_DECLARATIONS", (rule,))
    with pytest.raises(ValueError, match="unmatched cell keys: unowned_typo/ethereum/SWAP"):
        build_effective_capability_matrix()
    subset = build_effective_capability_matrix((_swap_connector(),))
    assert subset.cells


@pytest.mark.parametrize("teardown_hook", [False, True])
def test_teardown_hook_absence_or_presence_never_fabricates_certification(teardown_hook: bool) -> None:
    matrix = build_effective_capability_matrix((_lp_connector(teardown_hook=teardown_hook),))
    cell = _cell(matrix, SupportClaim.FULL_LIFECYCLE_CERTIFICATION)
    certification = next(
        item for item in cell.obligations if item.audited.obligation is ObligationId.CERTIFICATION_PATH
    )
    assert certification.state is ReportedObligationState.UNDECLARED
    candidates = [source for source in certification.sources if source.role is SourceRole.CANDIDATE]
    assert bool(candidates) is teardown_hook


def test_explicit_sources_resolve_connector_default_alternative_and_typed_na() -> None:
    matrix = build_effective_capability_matrix(
        (_swap_connector(),),
        declaration_resolver=_complete_resolver,
    )
    cell = _cell(matrix, SupportClaim.CORE_EXECUTION)
    assert cell.claim_satisfied
    assert {item.state for item in cell.obligations} == {
        ReportedObligationState.SATISFIED,
        ReportedObligationState.NOT_APPLICABLE,
    }
    kinds = {
        source.kind for item in cell.obligations for source in item.sources if source.role is SourceRole.DECLARATION
    }
    assert kinds == {
        SourceKind.CONNECTOR_MANIFEST,
        SourceKind.FRAMEWORK_DEFAULT,
        SourceKind.APPROVED_ALTERNATIVE,
        SourceKind.CORE_RULE,
    }


def test_invalid_typed_not_applicable_rule_still_fails_core_policy() -> None:
    def invalid_resolver(_connector: object, _key: CapabilityCellKey, _profile: object):
        return (
            ResolvedDeclaration(
                ObligationDeclaration(
                    ObligationId.COMPILER,
                    NotApplicable(NotApplicableRuleId.PERMISSION_PLAN_NOT_REQUIRED),
                ),
                _source(SourceKind.CORE_RULE, "core.invalid.v1"),
            ),
        )

    with pytest.raises(InvalidNotApplicableRule, match="cannot justify compiler"):
        build_effective_capability_matrix((_swap_connector(),), declaration_resolver=invalid_resolver)


def test_unsupported_metadata_is_preserved_and_claim_remains_unsatisfied() -> None:
    def unsupported_resolver(_connector: object, key: CapabilityCellKey, _profile: object):
        if key.claim is not SupportClaim.CORE_EXECUTION:
            return ()
        return (
            ResolvedDeclaration(
                ObligationDeclaration(
                    ObligationId.COMPILER,
                    Unsupported("compiler gap", "VIB-6650", "sdk-connectors", date(2026, 9, 1)),
                ),
                _source(SourceKind.CONNECTOR_MANIFEST, "connector.compiler_gap.v1"),
            ),
        )

    matrix = build_effective_capability_matrix((_swap_connector(),), declaration_resolver=unsupported_resolver)
    cell = _cell(matrix, SupportClaim.CORE_EXECUTION)
    compiler = next(item for item in cell.obligations if item.audited.obligation is ObligationId.COMPILER)
    assert compiler.to_dict()["owner"] == "sdk-connectors"
    assert compiler.to_dict()["tracking_ref"] == "VIB-6650"
    assert not cell.claim_satisfied


def test_duplicate_resolved_declarations_fail_loudly() -> None:
    def duplicate_resolver(_connector: object, _key: CapabilityCellKey, _profile: object):
        declaration = ObligationDeclaration(ObligationId.COMPILER, _satisfied())
        return (
            ResolvedDeclaration(declaration, _source(SourceKind.CONNECTOR_MANIFEST, "one.v1")),
            ResolvedDeclaration(declaration, _source(SourceKind.FRAMEWORK_DEFAULT, "two.v1")),
        )

    with pytest.raises(ValueError, match="conflicting resolved declarations"):
        build_effective_capability_matrix((_swap_connector(),), declaration_resolver=duplicate_resolver)


def test_unexpected_resolved_obligation_never_disappears_from_output() -> None:
    def unexpected_resolver(_connector: object, key: CapabilityCellKey, _profile: object):
        if key.claim is not SupportClaim.CORE_EXECUTION:
            return ()
        return (
            ResolvedDeclaration(
                ObligationDeclaration(ObligationId.POSITION_ENUMERATION, _satisfied()),
                _source(SourceKind.CONNECTOR_MANIFEST, "unexpected.v1"),
            ),
        )

    with pytest.raises(ValueError, match="outside the canonical profile: position_enumeration"):
        build_effective_capability_matrix((_swap_connector(),), declaration_resolver=unexpected_resolver)


def test_injected_claim_universe_requires_provenance_and_manifest_compatibility() -> None:
    connector = _swap_connector()
    key = CapabilityCellKey(
        protocol="test_swap",
        chain="ethereum",
        intent=IntentType.SWAP,
        semantics=IntentSemantics.ATOMIC_EXECUTION,
        primitive=Primitive.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
    )
    with pytest.raises(ValueError, match="universe_source_ref"):
        build_effective_capability_matrix((connector,), claim_cells=(key,))
    matrix = build_effective_capability_matrix(
        (connector,),
        claim_cells=(key,),
        universe_source_ref="docs/internal/reports/vib-6647:candidate-claims.csv@sha256:test",
    )
    assert matrix.universe.kind is UniverseKind.INJECTED_CLAIM_CELLS
    assert len(matrix.cells) == 1

    wrong_chain = CapabilityCellKey(
        protocol="test_swap",
        chain="base",
        intent=IntentType.SWAP,
        semantics=IntentSemantics.ATOMIC_EXECUTION,
        primitive=Primitive.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
    )
    with pytest.raises(ValueError, match="outside manifest support"):
        build_effective_capability_matrix(
            (connector,),
            claim_cells=(wrong_chain,),
            universe_source_ref="test:wrong-chain",
        )


def test_display_only_strategy_matrix_name_never_becomes_an_executable_protocol_cell() -> None:
    connector = Connector(
        name="owned_swap",
        kind=ProtocolKind.SWAP,
        compiler=_ref("Compiler"),
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
        strategy_intents=(IntentType.SWAP,),
        strategy_matrix_entries=(StrategyMatrixEntry("display_swap", "swap", (IntentType.SWAP,)),),
    )
    matrix = build_effective_capability_matrix((connector,))
    assert {cell.key.protocol for cell in matrix.cells} == {"owned_swap"}


def test_candidates_honor_protocol_and_chain_specific_declaration_scope() -> None:
    connector = Connector(
        name="scoped_lp",
        kind=ProtocolKind.LP,
        aliases=("scoped_lp_alias",),
        compiler=_ref("Compiler"),
        compiler_protocols=("scoped_lp",),
        receipt_parser_connector=_ref("ReceiptParser"),
        fungible_lp_close=FungibleLpCloseDecl(
            units="raw",
            decimals=18,
            clamp=True,
            identity=_ref("canonical_pool_key"),
        ),
        dex_volume=DexVolumeDecl(chains=("ethereum",), amm_family="v3_concentrated"),
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM, BASE)),
        strategy_intents=(IntentType.LP_OPEN,),
    )
    matrix = build_effective_capability_matrix((connector,))

    alias_core = next(
        cell
        for cell in matrix.cells
        if cell.key.protocol == "scoped_lp_alias"
        and cell.key.chain == "ethereum"
        and cell.key.claim is SupportClaim.CORE_EXECUTION
    )
    alias_compiler = next(item for item in alias_core.obligations if item.audited.obligation is ObligationId.COMPILER)
    assert all(source.role is not SourceRole.CANDIDATE for source in alias_compiler.sources)

    alias_open = next(
        cell
        for cell in matrix.cells
        if cell.key.protocol == "scoped_lp_alias"
        and cell.key.chain == "ethereum"
        and cell.key.claim is SupportClaim.POSITION_OPEN
    )
    identity = next(
        item for item in alias_open.obligations if item.audited.obligation is ObligationId.POSITION_IDENTITY
    )
    assert all("fungible_lp_close" not in source.ref for source in identity.sources)

    exact_cells = {
        (cell.key.protocol, cell.key.chain, cell.key.exact_target_feature)
        for cell in matrix.cells
        if cell.key.claim is SupportClaim.EXACT_TARGET_DATA
    }
    assert ("scoped_lp", "ethereum", ExactTargetFeature.OHLCV) in exact_cells
    assert ("scoped_lp", "base", ExactTargetFeature.OHLCV) not in exact_cells
    assert all(protocol != "scoped_lp_alias" for protocol, _chain, _feature in exact_cells)


def test_domain_specific_reader_never_leaks_into_unrelated_intent_cells() -> None:
    connector = Connector(
        name="mixed_protocol",
        kind=ProtocolKind.SWAP,
        compiler=_ref("Compiler"),
        lending_read=LendingReadDecl(spec=_ref("LendingRead")),
        supported_chains=SupportedChainsSpec(
            chains=(ETHEREUM,),
            intent_overrides={IntentType.SUPPLY: (BASE,)},
        ),
        strategy_intents=(IntentType.SWAP, IntentType.SUPPLY),
    )
    matrix = build_effective_capability_matrix((connector,))
    swap_anvil = next(
        cell
        for cell in matrix.cells
        if cell.key.intent is IntentType.SWAP and cell.key.claim is SupportClaim.MANAGED_ANVIL_TESTABLE
    )
    fork_read = next(item for item in swap_anvil.obligations if item.audited.obligation is ObligationId.ANVIL_FORK_READ)
    assert all("lending_read" not in source.ref for source in fork_read.sources)


def test_all_advertised_exact_target_feature_profiles_are_reachable() -> None:
    connector = Connector(
        name="data_swap",
        kind=ProtocolKind.SWAP,
        compiler=_ref("Compiler"),
        swap_quote_connector=_ref("Quote"),
        pool_reader=_ref("PoolReader"),
        dex_volume=DexVolumeDecl(chains=("ethereum",), amm_family="v3_concentrated"),
        principal_token_market_reader=_ref("ReferencePrice"),
        supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
        strategy_intents=(IntentType.SWAP,),
    )
    matrix = build_effective_capability_matrix((connector,))
    assert {
        cell.key.exact_target_feature for cell in matrix.cells if cell.key.claim is SupportClaim.EXACT_TARGET_DATA
    } == set(ExactTargetFeature)


def test_json_and_markdown_are_byte_stable_and_have_no_volatile_provenance() -> None:
    first = build_effective_capability_matrix((_swap_connector("z_swap"), _swap_connector("a_swap")))
    second = build_effective_capability_matrix((_swap_connector("a_swap"), _swap_connector("z_swap")))
    assert first.to_json() == second.to_json()
    assert render_markdown(first) == render_markdown(second)
    assert "generatedAt" not in first.to_json()
    assert "sourceCommit" not in first.to_json()
    assert first.to_json().endswith("\n")
    assert render_markdown(first).endswith("\n")
    assert "## Obligation provenance" in render_markdown(first)
    assert "requirement:core_profile:core.profile_requirement.v1" in render_markdown(first)


def test_markdown_escapes_dynamic_table_cell_content() -> None:
    def resolver(_connector: object, key: CapabilityCellKey, _profile: object):
        if key.claim is not SupportClaim.CORE_EXECUTION:
            return ()
        return (
            ResolvedDeclaration(
                ObligationDeclaration(
                    ObligationId.COMPILER,
                    Unsupported("gap|with\nnewline", "VIB-6650", "sdk-connectors", date(2026, 9, 1)),
                ),
                _source(SourceKind.CONNECTOR_MANIFEST, "markdown.v1"),
            ),
        )

    rendered = render_markdown(build_effective_capability_matrix((_swap_connector(),), declaration_resolver=resolver))
    assert "reason=gap\\|with<br>newline" in rendered
    assert "reason=gap|with" not in rendered


def test_effective_artifact_rejects_checkout_specific_absolute_references() -> None:
    with pytest.raises(ValueError, match="durable non-absolute"):
        MatrixUniverse(UniverseKind.INJECTED_CLAIM_CELLS, "/private/tmp/claims.csv")
    with pytest.raises(ValueError, match="durable non-absolute"):
        ProviderSource(
            SourceKind.CONNECTOR_MANIFEST,
            SourceRole.CANDIDATE,
            "absolute.v1",
            "/Users/example/provider.py",
            "invalid checkout-specific source",
        )

    def absolute_evidence(_connector: object, key: CapabilityCellKey, _profile: object):
        if key.claim is not SupportClaim.CORE_EXECUTION:
            return ()
        disposition = Satisfied(
            provider_ref="almanak.example:Compiler",
            contract_version="compiler.v1",
            test_evidence=(EvidenceRef(EvidenceKind.CONTRACT_TEST, "/tmp/test_compiler.py"),),
        )
        return (
            ResolvedDeclaration(
                ObligationDeclaration(ObligationId.COMPILER, disposition),
                _source(SourceKind.CONNECTOR_MANIFEST, "absolute-evidence.v1"),
            ),
        )

    with pytest.raises(ValueError, match="EvidenceRef.ref must be a durable non-absolute reference"):
        build_effective_capability_matrix((_swap_connector(),), declaration_resolver=absolute_evidence)


def test_generation_never_loads_provider_import_refs(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    def fail_load(_self: ImportRef):
        raise AssertionError("provider ImportRef.load() must not run during generation")

    def fail_socket(*_args: object, **_kwargs: object):
        raise AssertionError("network access must not run during generation")

    monkeypatch.setattr(ImportRef, "load", fail_load)
    monkeypatch.setattr(socket, "socket", fail_socket)
    matrix = build_effective_capability_matrix((_swap_connector(),))
    assert matrix.cells


def test_default_registry_generation_is_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    def fail_load(_self: ImportRef):
        raise AssertionError("provider ImportRef.load() must not run during default generation")

    def fail_socket(*_args: object, **_kwargs: object):
        raise AssertionError("network access must not run during default generation")

    monkeypatch.setattr(ImportRef, "load", fail_load)
    monkeypatch.setattr(socket, "socket", fail_socket)
    matrix = build_effective_capability_matrix()
    assert matrix.universe.kind is UniverseKind.REGISTERED_STRATEGY_SUPPORT
    assert matrix.cells


def test_every_obligation_including_undeclared_has_requirement_provenance() -> None:
    matrix = build_effective_capability_matrix((_swap_connector(),))
    for cell in matrix.cells:
        for obligation in cell.obligations:
            assert obligation.sources[0].kind is SourceKind.CORE_PROFILE
            assert obligation.sources[0].role is SourceRole.REQUIREMENT
            assert obligation.sources[0].rule_id == "core.profile_requirement.v1"
