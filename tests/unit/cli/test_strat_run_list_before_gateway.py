"""`--list` must not require a gateway (VIB-5846 review, codex P2).

VIB-5846 exposed `--list` on the `almanak strat run` wrapper (it was previously
unreachable and hardcoded off). `--list` only reads the strategy registry, so
`run()` now handles it BEFORE `_setup_gateway` — otherwise `strat run --list`
would pay for (and, in a gateway-less environment, fail at) gateway startup just
to print strategy names. This pins that ordering.
"""

from __future__ import annotations

from click.testing import CliRunner


def test_run_list_exits_before_gateway_setup(monkeypatch) -> None:
    from almanak.framework.cli import run_helpers
    from almanak.framework.cli.run import run as framework_run

    def _boom_gateway(**_kwargs):
        raise AssertionError("_setup_gateway must not run for --list")

    # run() imports these from the run_helpers facade at call time, so patching
    # the facade attribute is what the in-function import resolves.
    monkeypatch.setattr(run_helpers, "_setup_gateway", _boom_gateway)

    result = CliRunner().invoke(framework_run, ["--list"])

    assert result.exit_code == 0, result.output
    # The list branch ran (registry header or the empty-registry message), and
    # the gateway assertion never fired.
    assert "strategies" in result.output.lower()
