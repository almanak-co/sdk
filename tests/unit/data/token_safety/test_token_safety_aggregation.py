import pytest

from almanak.framework.data.token_safety.client import TokenSafetyClient
from almanak.framework.data.token_safety.models import GoPlusResult, RiskFlag, RiskLevel, RugCheckResult


def _aggregate(
    *,
    rugcheck: RugCheckResult | None = None,
    goplus: GoPlusResult | None = None,
):
    client = object.__new__(TokenSafetyClient)
    return client._aggregate_results("Mint111111111111111111111111111111111111111", rugcheck, goplus)


def test_no_sources_returns_unknown_middle_score() -> None:
    result = _aggregate()

    assert result.risk_level == RiskLevel.UNKNOWN
    assert result.risk_score == 0.5
    assert result.flags == []
    assert result.sources == []


def test_rugcheck_rugged_preserves_existing_risks_and_adds_critical() -> None:
    existing = RiskFlag(
        name="mutable_metadata",
        description="Metadata can be changed",
        level=RiskLevel.MEDIUM,
        source="rugcheck",
    )
    rugcheck = RugCheckResult(score=950, risks=[existing], rugged=True)

    result = _aggregate(rugcheck=rugcheck)

    assert result.sources == ["rugcheck"]
    assert [flag.name for flag in result.flags] == ["mutable_metadata", "already_rugged"]
    assert result.flags[1] == RiskFlag(
        name="already_rugged",
        description="Token has been confirmed as a rug pull",
        level=RiskLevel.CRITICAL,
        source="rugcheck",
    )
    assert result.risk_level == RiskLevel.CRITICAL


def test_goplus_boolean_flags_exact_names_order_and_levels() -> None:
    goplus = GoPlusResult(
        mintable=True,
        freezable=True,
        closable=True,
        balance_mutable=True,
        has_transfer_fee=True,
        transfer_hook=True,
        non_transferable=True,
        default_account_state_frozen=True,
        holder_count=100,
        top_holder_pct=20.0,
    )

    result = _aggregate(goplus=goplus)

    assert result.sources == ["goplus"]
    assert [(flag.name, flag.description, flag.level, flag.source) for flag in result.flags] == [
        (
            "mint_authority_enabled",
            "Token supply can be increased by mint authority",
            RiskLevel.HIGH,
            "goplus",
        ),
        (
            "freeze_authority_enabled",
            "Token accounts can be frozen by authority",
            RiskLevel.HIGH,
            "goplus",
        ),
        (
            "close_authority_enabled",
            "Token program can be closed (destroying all tokens)",
            RiskLevel.CRITICAL,
            "goplus",
        ),
        (
            "balance_mutable",
            "Authority can modify token balances directly",
            RiskLevel.CRITICAL,
            "goplus",
        ),
        (
            "transfer_fee",
            "Token has a non-zero transfer fee (potential sell tax)",
            RiskLevel.HIGH,
            "goplus",
        ),
        (
            "transfer_hook_active",
            "External transfer hook attached (can block/modify transfers)",
            RiskLevel.HIGH,
            "goplus",
        ),
        (
            "non_transferable",
            "Token is soulbound / non-transferable",
            RiskLevel.CRITICAL,
            "goplus",
        ),
        (
            "default_account_frozen",
            "New token accounts start in frozen state",
            RiskLevel.HIGH,
            "goplus",
        ),
    ]
    assert result.risk_level == RiskLevel.CRITICAL


@pytest.mark.parametrize(
    ("goplus", "expected"),
    [
        (
            GoPlusResult(holder_count=99),
            [("low_holder_count", "Very few holders (99)", RiskLevel.MEDIUM)],
        ),
        (GoPlusResult(holder_count=100, top_holder_pct=20.0), []),
        (
            GoPlusResult(holder_count=100, top_holder_pct=20.1),
            [("high_holder_concentration", "Top holder owns 20.1% of supply", RiskLevel.MEDIUM)],
        ),
        (
            GoPlusResult(holder_count=100, top_holder_pct=50.1),
            [("concentrated_holdings", "Top holder owns 50.1% of supply", RiskLevel.HIGH)],
        ),
    ],
)
def test_goplus_holder_thresholds(
    goplus: GoPlusResult,
    expected: list[tuple[str, str, RiskLevel]],
) -> None:
    result = _aggregate(goplus=goplus)

    assert [(flag.name, flag.description, flag.level) for flag in result.flags] == expected


def test_trusted_token_caps_overall_to_medium_but_keeps_flags() -> None:
    goplus = GoPlusResult(closable=True, trusted_token=True, holder_count=100)

    result = _aggregate(goplus=goplus)

    assert result.risk_level == RiskLevel.MEDIUM
    assert [flag.name for flag in result.flags] == ["close_authority_enabled"]


def test_untrusted_critical_remains_critical() -> None:
    goplus = GoPlusResult(closable=True, trusted_token=False, holder_count=100)

    result = _aggregate(goplus=goplus)

    assert result.risk_level == RiskLevel.CRITICAL
    assert [flag.name for flag in result.flags] == ["close_authority_enabled"]


def test_sources_order_with_both_providers() -> None:
    rugcheck = RugCheckResult(score=50)
    goplus = GoPlusResult(holder_count=100)

    result = _aggregate(rugcheck=rugcheck, goplus=goplus)

    assert result.sources == ["rugcheck", "goplus"]
