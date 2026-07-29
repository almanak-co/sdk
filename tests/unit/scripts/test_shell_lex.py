"""Tests for ``scripts/ci/_shell_lex.py``.

This module exists because both CI gates got quote-awareness wrong in the same
way, in opposite directions — one produced a false alarm on correct code, the
other a vacuous pass on a genuine orphan. Since it is now the single
implementation, a defect here breaks both gates at once, so the cases below are
the specific shapes that broke them.
"""

from __future__ import annotations

import pytest

from scripts.ci._shell_lex import (
    drop_heredoc_bodies,
    segment_before,
    segments_with_spans,
    split_segments,
    strip_comments,
)


class TestSplitSegments:
    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("a && b", 2),
            ("a || b", 2),
            ("a ; b", 2),
            ("a | b", 2),
            ("a && b && c", 3),
            ("just one command", 1),
        ],
    )
    def test_unquoted_separators_split(self, line, expected):
        assert len(split_segments(line)) == expected

    @pytest.mark.parametrize(
        "line",
        [
            # The orphan gate's vacuous pass: a quoted `|` split the line and
            # the right-hand fragment escaped the executing-command guard.
            "grep -E 'foo|scripts/ci/orphan.sh' f.txt",
            'grep -E "foo|bar" f.txt',
            "grep 'a;b' f.txt",
            "sed -e 's|a|b|' f.txt",
            # The provenance gate's false alarm: `$(` cut the pinned-runner
            # lookback, so `uv run --directory "$(pwd)" python x.py` reported
            # the CORRECT form as a violation.
            'uv run --directory "$(pwd)" python x.py',
            "uv run --directory $(pwd) python x.py",
            "uv run --with 'pkg;extra' python x.py",
            "uv run --directory `pwd` python x.py",
        ],
    )
    def test_separators_inside_quotes_or_substitution_do_not_split(self, line):
        assert len(split_segments(line)) == 1, line

    def test_nested_command_substitution_stays_opaque(self):
        line = 'uv run --project "$(git rev-parse --show-toplevel)" python x.py'
        assert len(split_segments(line)) == 1

    def test_a_real_separator_after_a_quoted_one_still_splits(self):
        """The quoting must not swallow genuine separators that follow it."""
        assert len(split_segments("grep 'a|b' f.txt && bash x.sh")) == 2


class TestSegmentBefore:
    def test_returns_only_the_current_command(self):
        line = "uv run pytest && python3 x.py"
        pos = line.index("python3")
        assert "uv run" not in segment_before(line, pos)

    def test_keeps_a_runner_in_the_same_command(self):
        line = 'uv run --directory "$(pwd)" python x.py'
        pos = line.index("python x.py")
        assert "uv run" in segment_before(line, pos)


class TestStripComments:
    def test_drops_a_trailing_comment(self):
        assert strip_comments("bash x.sh  # do the thing").strip() == "bash x.sh"

    def test_keeps_a_hash_inside_quotes(self):
        line = "python3 -c \"print('#not-a-comment')\""
        assert "#not-a-comment" in strip_comments(line)

    def test_drops_a_whole_comment_line(self):
        assert strip_comments("# just a comment").strip() == ""

    def test_preserves_line_count(self):
        """Callers map violations back to line numbers, so lines must not shift."""
        text = "a\n# comment\nb  # trailing\nc"
        assert len(strip_comments(text).splitlines()) == 4


def test_spans_cover_their_own_text():
    """A span's recorded offsets must actually index the text it reports."""
    line = "bash a.sh && python b.py ; echo done"
    for start, end, text in segments_with_spans(line):
        assert line[start:end] == text


class TestFailureModes:
    """The shapes that produced silent drops before round 3."""

    def test_unbalanced_quote_falls_back_to_loud_splitting(self):
        """A stray apostrophe must not swallow the rest of the line.

        Quote state ran to end-of-line with no recovery, so `echo don't && bash
        x.sh` collapsed to ONE segment led by `echo` — and the real invocation
        was dropped entirely. The fallback splits on every separator: at worst
        that over-segments, which means more scrutiny, never less.
        """
        segments = split_segments("echo don't && bash scripts/ci/x.sh")
        assert len(segments) == 2
        assert any("bash scripts/ci/x.sh" in s for s in segments)

    def test_lone_ampersand_separates_commands(self):
        """`a & b` backgrounds `a`; `b` is a new command.

        Only `&&` was handled, so a pinned runner vouched across a lone `&`.
        """
        assert len(split_segments("uv run python a.py & python3 b.py")) == 2

    @pytest.mark.parametrize("line", ["cmd >&2", "cmd 2>&1", "cmd &> out.log"])
    def test_redirections_are_not_separators(self, line):
        assert len(split_segments(line)) == 1

    @pytest.mark.parametrize(
        "line",
        [
            "bash a.sh && python b.py ; echo done & bash c.sh",
            "grep -E 'a|b' f.txt && bash x.sh",
            'uv run --directory "$(pwd)" python x.py | tee log',
            "a && b || c ; d & e",
        ],
    )
    def test_every_non_separator_character_belongs_to_exactly_one_span(self, line):
        """Structural invariant: spans do not overlap and leave no gaps.

        Two rounds of honesty about what this does and does not prove.

        The first version asserted `text in line` — trivially true of any slice
        — and `covered > 0`, which one span satisfies. It verified nothing.

        This version verifies something real, but **it still does not catch the
        silent-drop regression**, and the earlier docstring claiming otherwise
        was wrong. Verified by mutation: deleting the loud fallback leaves the
        whole line as ONE span, so coverage and non-overlap both still hold and
        this test passes. What actually kills that mutant is
        `test_unbalanced_quote_falls_back_to_loud_splitting`, which asserts the
        expected segment COUNT.

        Detecting a missed split from structure alone is circular — deciding
        which separators are real is the thing under test. So this stays as a
        structural invariant (no overlaps, no dropped regions) and the
        drop-detection lives in the explicit count assertions above.
        """
        spans = segments_with_spans(line)

        for i, (start, end, _) in enumerate(spans):
            for other_start, other_end, _ in spans[i + 1 :]:
                assert end <= other_start or other_end <= start, "spans overlap"

        covered = set()
        for start, end, _ in spans:
            covered.update(range(start, end))

        separators = set()
        for idx, ch in enumerate(line):
            if ch in ";|&":
                separators.add(idx)

        missing = [
            (idx, ch)
            for idx, ch in enumerate(line)
            if idx not in covered and idx not in separators and not ch.isspace()
        ]
        assert not missing, f"characters belong to no span: {missing}"


class TestHeredocBodies:
    def test_body_is_blanked_but_opener_survives(self):
        text = "cat <<'EOF'\nrun scripts/ci/orphan.sh\nEOF\nbash scripts/ci/real.sh\n"
        out = drop_heredoc_bodies(text)
        assert "orphan.sh" not in out
        assert "cat <<'EOF'" in out
        assert "bash scripts/ci/real.sh" in out

    def test_line_count_is_preserved(self):
        text = "cat <<'EOF'\na\nb\nEOF\nlast\n"
        assert len(drop_heredoc_bodies(text).splitlines()) == len(text.splitlines())

    def test_shell_arithmetic_left_shift_is_not_a_heredoc(self):
        """`$((1<<2))` matched the opener regex and blanked the rest of the file.

        The terminator `2` never appeared on its own line, so every subsequent
        line — including real invocations — was silently removed.
        """
        text = "MASK=$((1<<2))\nbash scripts/ci/real.sh\n"
        assert "bash scripts/ci/real.sh" in drop_heredoc_bodies(text)

    def test_unterminated_heredoc_blanks_nothing(self):
        """Blanking to EOF on a malformed opener has far too large a blast radius."""
        text = "cat <<'EOF'\nsome text\nbash scripts/ci/real.sh\n"
        assert "bash scripts/ci/real.sh" in drop_heredoc_bodies(text)


class TestUnbalancedOpeners:
    """Every unbalanced opener must fall back loudly, not just an unbalanced quote.

    The quote case was guarded and `$(` / backtick were not — the same defect
    one branch along. Each of these swallowed all following separators, so a
    real invocation after the opener vanished from the reference graph.
    """

    @pytest.mark.parametrize(
        "line",
        [
            "echo don't && bash scripts/ci/x.sh",  # unbalanced quote (was guarded)
            "echo $(broken && bash scripts/ci/x.sh",  # unclosed $( (was not)
            "echo `broken && bash scripts/ci/x.sh",  # unterminated backtick (was not)
        ],
    )
    def test_unbalanced_opener_still_splits(self, line):
        segments = split_segments(line)
        assert len(segments) == 2, line
        assert any("bash scripts/ci/x.sh" in s for s in segments), line

    def test_balanced_forms_are_unaffected(self):
        """The fallback must not fire on correct input, or it over-segments."""
        assert len(split_segments('uv run --directory "$(pwd)" python x.py')) == 1
        assert len(split_segments("uv run --directory `pwd` python x.py")) == 1


class TestMultipleHeredocsOnOneLine:
    def test_every_body_on_the_line_is_blanked(self):
        """`search` found only the first opener, so the second body survived.

        A script named in that second body still certified as wired.
        """
        text = (
            "cat <<'EOF' > a && cat <<'EOF2' > b\n"
            "first body\n"
            "EOF\n"
            "named scripts/ci/orphan.sh here\n"
            "EOF2\n"
            "bash scripts/ci/real.sh\n"
        )
        out = drop_heredoc_bodies(text)
        assert "orphan.sh" not in out, "second heredoc body must be blanked too"
        assert "bash scripts/ci/real.sh" in out, "code after the heredocs must survive"

    def test_line_count_preserved_with_two_heredocs(self):
        text = "cat <<'A' && cat <<'B'\n1\nA\n2\nB\nlast\n"
        assert len(drop_heredoc_bodies(text).splitlines()) == len(text.splitlines())
