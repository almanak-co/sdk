import pytest

from almanak.dashboard import build as b
from almanak.dashboard.author_errors import explain
from almanak.dashboard.models import Dashboard


def raise_in(source: str):
    namespace = {}
    exec(compile(source, "dashboard.py", "exec"), namespace)
    try:
        namespace["dashboard"](None)
    except Exception as exc:
        return explain(exc)
    raise AssertionError("no error")


def test_validation_error_names_author_line_field_and_input():
    message = raise_in(
        "from almanak.dashboard import build as b\n"
        "def dashboard(ctx):\n"
        "    return b.steps('s', 'How', [('a', 'A'), ('c', 'C')], [('a', 'c', 'go', 'loud')])\n"
    )
    assert message.startswith("dashboard.py line 3 (dashboard): ")
    assert "tone" in message and "'loud'" in message
    assert "Traceback" not in message
    assert "dashboard.py (`almanak support --references" in message


def test_author_key_typo_stays_a_key_error():
    message = raise_in("def dashboard(ctx):\n    return {'loops': 2}['lops']\n")
    assert message.startswith("dashboard.py line 2 (dashboard): KeyError: 'lops'")


def test_repeated_block_ids_are_named():
    text = b.callout("dup", "T", "x")
    with pytest.raises(ValueError, match="repeated: dup"):
        Dashboard(title="t", blocks=(text, b.callout("dup", "U", "y")))


def test_steps_accept_info():
    steps = b.steps("s", "How", [("a", "A"), ("c", "C")], [("a", "c", "note", "info")])
    assert steps.transitions[0].tone == "info"


def test_syntax_error_names_the_author_line():
    try:
        compile("def dashboard(ctx):\n    return (\n", "dashboard.py", "exec")
    except SyntaxError as exc:
        assert explain(exc).startswith("dashboard.py line ")
