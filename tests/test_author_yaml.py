import pytest

from almanak.configuration.values import instant, quantity, seconds
from almanak.configuration.yaml import ConfigurationError, parse
from scripts.legacy_configuration.author import normalize


@pytest.mark.parametrize(
    "body",
    [
        "a: 1\na: 2",
        "a: &a 1\nb: *a",
        "a: {<<: {b: 1}}",
        "a: !!python/object/apply:os.system ['false']",
        "a: .nan",
        "a: .inf",
        "a: [1\n",
    ],
)
def test_reject_ambiguous_or_unsafe_yaml(body):
    with pytest.raises(ConfigurationError):
        parse(body, file="config.yaml")


def test_exact_yaml_scalars_and_locations():
    doc = parse('address: 0x123\namount: "0.01"\nflag: true\nname: on\ntime: 2026-09-06T00:00:00Z')
    assert doc.value == {
        "address": "0x123",
        "amount": "0.01",
        "flag": True,
        "name": "on",
        "time": "2026-09-06T00:00:00Z",
    }
    assert doc.locations["amount"] == 2


def test_exact_amounts_and_time():
    assert quantity("0.010000000000000001", 18) == 10000000000000001
    assert quantity({"base_units": 10**30}, 6) == 10**30
    assert seconds("5m") == 300
    assert instant("2026-09-06T08:00:00+08:00") == instant("2026-09-06T00:00:00Z")
    for value in ("1.0000001", "1e6", 1.2, -1):
        with pytest.raises(ConfigurationError):
            quantity(value, 6)
    with pytest.raises(ConfigurationError):
        instant("2026-09-06T00:00:00")


def test_duplicate_location():
    with pytest.raises(ConfigurationError) as error:
        parse("strategy:\n  name: a\n  name: b", file="config.yaml")
    assert error.value.diagnostic()["line"] == 3
    assert error.value.field == "strategy.name"


def test_unknown_author_sections():
    with pytest.raises(ConfigurationError, match="unknown fields"):
        normalize({"unexpected": 1})
