"""Tests for ``almanak ax`` subcommand flag propagation and OR-merge semantics.

Verifies that --yes, --dry-run, and --json flags work when placed either
at the group level (before subcommand) or at the subcommand level (after).
"""

from __future__ import annotations

import click

from almanak.framework.cli.ax import _action_options, _merge_flags


class TestMergeFlags:
    """Test _merge_flags OR-merge semantics."""

    def _make_ctx(self, yes=False, dry_run=False, json_output=False):
        ctx = click.Context(click.Command("test"))
        ctx.obj = {"yes": yes, "dry_run": dry_run, "json_output": json_output}
        return ctx

    def test_all_false_by_default(self):
        ctx = self._make_ctx()
        yes, dry_run, json_output = _merge_flags(ctx)
        assert yes is False
        assert dry_run is False
        assert json_output is False

    def test_group_yes_propagates(self):
        ctx = self._make_ctx(yes=True)
        yes, dry_run, json_output = _merge_flags(ctx)
        assert yes is True
        assert dry_run is False
        assert json_output is False

    def test_sub_yes_propagates(self):
        ctx = self._make_ctx()
        yes, dry_run, json_output = _merge_flags(ctx, sub_yes=True)
        assert yes is True

    def test_both_yes_is_true(self):
        ctx = self._make_ctx(yes=True)
        yes, _, _ = _merge_flags(ctx, sub_yes=True)
        assert yes is True

    def test_group_dry_run_propagates(self):
        ctx = self._make_ctx(dry_run=True)
        _, dry_run, _ = _merge_flags(ctx)
        assert dry_run is True

    def test_sub_dry_run_propagates(self):
        ctx = self._make_ctx()
        _, dry_run, _ = _merge_flags(ctx, sub_dry_run=True)
        assert dry_run is True

    def test_group_json_propagates(self):
        ctx = self._make_ctx(json_output=True)
        _, _, json_output = _merge_flags(ctx)
        assert json_output is True

    def test_sub_json_propagates(self):
        ctx = self._make_ctx()
        _, _, json_output = _merge_flags(ctx, sub_json_output=True)
        assert json_output is True

    def test_mixed_flags(self):
        """Group --yes + sub --dry-run -> both True."""
        ctx = self._make_ctx(yes=True)
        yes, dry_run, json_output = _merge_flags(ctx, sub_dry_run=True)
        assert yes is True
        assert dry_run is True
        assert json_output is False

    def test_all_flags_from_sub(self):
        ctx = self._make_ctx()
        yes, dry_run, json_output = _merge_flags(ctx, sub_yes=True, sub_dry_run=True, sub_json_output=True)
        assert yes is True
        assert dry_run is True
        assert json_output is True


class TestActionOptionsDecorator:
    """Test _action_options adds hidden parameters."""

    def test_adds_three_hidden_options(self):
        @_action_options
        @click.command()
        @click.pass_context
        def dummy(ctx, sub_yes, sub_dry_run, sub_json_output):
            pass

        param_names = {p.name for p in dummy.params if isinstance(p, click.Option)}
        assert "sub_yes" in param_names
        assert "sub_dry_run" in param_names
        assert "sub_json_output" in param_names

        # All should be hidden
        for p in dummy.params:
            if isinstance(p, click.Option) and p.name in ("sub_yes", "sub_dry_run", "sub_json_output"):
                assert p.hidden is True, f"{p.name} should be hidden"

    def test_options_default_to_false(self):
        @_action_options
        @click.command()
        @click.pass_context
        def dummy(ctx, sub_yes, sub_dry_run, sub_json_output):
            pass

        for p in dummy.params:
            if isinstance(p, click.Option) and p.name in ("sub_yes", "sub_dry_run", "sub_json_output"):
                assert p.default is False, f"{p.name} should default to False"
