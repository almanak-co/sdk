"""Historical component inputs stay separate from current public author examples."""

from pathlib import Path


def retained_author(source, filename):
    return Path(__file__).parents[1] / "tests/fixtures/author" / source.parent.name / filename
