"""`ax dex-pools` table rendering (VIB-6599).

The renderer carries the safety-critical distinction: THREE different empties
that mean different things, and must never print the same line. Venues the
caller's own floor excluded, an absence the provider can vouch for, and a view
too truncated to vouch for anything.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus
from almanak.framework.cli.ax import _format_usd_cell, _render_dex_pools_table


def _response(pools: list[dict], **data) -> ToolResponse:
    payload = {
        "pools": pools,
        "count": len(pools),
        "unfiltered_count": len(pools),
        "complete": True,
        "product_distinct_dex_id": True,
        "source": "coingecko_onchain",
    }
    payload.update(data)
    return ToolResponse(status=ToolResponseStatus.SUCCESS, data=payload)


def _render(response: ToolResponse) -> str:
    """Render inside a Click context so styling/echo behave as in the CLI."""
    runner = CliRunner()

    @click.command()
    def _cmd():
        _render_dex_pools_table(response, token="BTT", chain="ethereum")

    return runner.invoke(_cmd, color=False).output


_A_POOL = {
    "pool_address": "0x2d0ba902badaa82592f0e1c04c71d66cea21d921",
    "dex_id": "uniswap_v2",
    "name": "BTT / WETH",
    "reserve_usd": "211219.19",
    "volume_24h_usd": "84.35",
}


def test_renders_venue_rows_with_both_money_columns():
    out = _render(_response([_A_POOL]))

    assert "uniswap_v2" in out
    assert "BTT / WETH" in out
    assert "$211,219" in out
    assert "$84" in out  # the column that distinguishes a live venue from a dead one
    assert "0x2d0ba902badaa82592f0e1c04c71d66cea21d921" in out


def test_empty_and_complete_reports_absence():
    out = _render(_response([], unfiltered_count=0))
    assert "no DEX venues found" in out
    assert "COULD NOT VERIFY" not in out


def test_empty_because_of_the_floor_says_so_and_never_claims_absence():
    """Venues exist; the caller's --min-liquidity excluded them. Printing the
    absence line here is how an operator concludes a tradeable token is
    untradeable — and the machine-readable half of this same bug classified it
    as authoritative NOT_FOUND."""
    out = _render(_response([], unfiltered_count=6, min_liquidity_usd="1000000"))

    assert "6 venue(s) found" in out
    assert "--min-liquidity" in out
    assert "no DEX venues found" not in out


def test_empty_and_truncated_says_could_not_verify():
    out = _render(_response([], unfiltered_count=0, complete=False))

    assert "COULD NOT VERIFY" in out
    assert "could not verify" in out  # the hint, spelled for a human report
    assert "no DEX venues found" not in out


def test_the_three_empties_never_render_the_same_line():
    """Guard against a future edit collapsing them back together."""
    absent = _render(_response([], unfiltered_count=0))
    filtered = _render(_response([], unfiltered_count=6, min_liquidity_usd="1000000"))
    unverified = _render(_response([], unfiltered_count=0, complete=False))

    assert len({absent, filtered, unverified}) == 3


def test_truncated_window_with_venues_warns_about_depth_not_existence():
    out = _render(_response([_A_POOL], complete=False))

    assert "truncated" in out
    assert "deepest venue SHOWN" in out
    # Existence IS answered — the rows are right there, so no verify warning.
    assert "COULD NOT VERIFY" not in out


def test_non_product_distinct_provider_warns_before_pinning_a_protocol():
    out = _render(_response([_A_POOL], product_distinct_dex_id=False))
    assert "not product-distinct" in out
    assert "aerodrome" in out


@pytest.mark.parametrize(
    ("value", "expected"),
    [("211219.19", "$211,219"), ("0", "$0"), ("", "?"), ("not-a-number", "?")],
)
def test_unmeasured_money_renders_as_question_mark_never_zero(value: str, expected: str):
    """Empty != Zero at the last mile: `""` is unmeasured and must not print as
    `$0`, which would read as a measured-empty venue."""
    assert _format_usd_cell(value) == expected


_SUPPORTED = {**_A_POOL, "execution_support": "supported", "protocols": ["uniswap_v3"]}
_UNSUPPORTED = {**_A_POOL, "dex_id": "someswap", "execution_support": "unsupported", "protocols": []}
_UNKNOWN = {**_A_POOL, "dex_id": "thena-fusion", "execution_support": "unknown", "protocols": []}


def test_each_support_state_renders_a_distinct_mark():
    """'?' must never read as 'no': an undeclared connector is not an absent one."""
    out = _render(
        _response(
            [_SUPPORTED, _UNSUPPORTED, _UNKNOWN],
            venue_support_complete=False,
            supported_pool_protocols=["uniswap_v3"],
        )
    )
    marks = [line.split()[-2] for line in out.splitlines() if "0x2d0ba902" in line]
    assert marks == ["yes", "no", "?"]
    assert "venue support is incomplete on this chain" in out


def test_a_complete_chain_does_not_warn_about_incompleteness():
    out = _render(_response([_SUPPORTED], venue_support_complete=True, supported_pool_protocols=["uniswap_v3"]))
    assert "venue support is incomplete" not in out
    assert "executable venue (uniswap_v3)" in out
    # A supported VENUE is not a verified POOL — the next step must stay named.
    assert "almanak ax -c ethereum pool <pool-address>" in out


def test_no_matched_venue_says_the_sdk_cannot_target_any_of_them():
    out = _render(
        _response([_UNSUPPORTED], venue_support_complete=True, supported_pool_protocols=["uniswap_v3", "curve"])
    )
    assert "the SDK cannot target any of these pools" in out
    assert "uniswap_v3, curve" in out


def test_an_undetermined_row_never_becomes_a_definitive_rejection():
    """The footer is where a '?' row would be laundered into a 'no'.

    On an incomplete chain nothing was ruled out, so the only honest footer is
    an inconclusive one — the same distinction the column itself draws.
    """
    out = _render(
        _response(
            [_UNSUPPORTED, _UNKNOWN],
            venue_support_complete=False,
            supported_pool_protocols=["uniswap_v3"],
        )
    )
    assert "the SDK cannot target any of these pools" not in out
    assert "INCONCLUSIVE" in out
    assert "undetermined for 1 of 2" in out


def test_a_non_product_distinct_response_is_inconclusive_even_when_complete():
    """Every row unknown with the chain complete: the ids, not the manifests, are the gap."""
    out = _render(_response([_UNKNOWN], venue_support_complete=True, supported_pool_protocols=["uniswap_v3"]))
    assert "the SDK cannot target any of these pools" not in out
    assert "INCONCLUSIVE" in out


def test_the_legend_only_claims_the_marks_the_table_actually_shows():
    """'no = no connector owns it' is false for a '?' row, so it must not appear alone."""
    out = _render(
        _response(
            [_SUPPORTED, _UNKNOWN],
            venue_support_complete=False,
            supported_pool_protocols=["uniswap_v3"],
        )
    )
    assert "no = no connector owns it" not in out
    assert "? = undetermined for 1 venue(s)" in out
