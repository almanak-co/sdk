"""Bind SDK core metadata requirements to the reviewed project table."""

from collections import Counter
from email.message import Message

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name


def _requirements(values: list[str]) -> Counter[Requirement]:
    try:
        return Counter(Requirement(value) for value in values)
    except InvalidRequirement as exc:
        raise ValueError("distribution contains an invalid dependency requirement") from exc


def _expected_requirements(project: dict) -> Counter[Requirement]:
    values = list(project.get("dependencies", ()))
    for extra, requirements in project.get("optional-dependencies", {}).items():
        for value in requirements:
            requirement = Requirement(value)
            if requirement.marker is not None:
                raise ValueError("project optional dependencies must not carry nested markers")
            values.append(f"{value}; extra == '{canonicalize_name(extra)}'")
    return _requirements(values)


def verify_project_metadata(metadata: Message, project: dict) -> None:
    """Require exact Python and dependency metadata, including optional extras."""
    python = metadata.get_all("Requires-Python", [])
    if len(python) != 1 or SpecifierSet(python[0]) != SpecifierSet(project["requires-python"]):
        raise ValueError("distribution Requires-Python differs from the project")

    provided = metadata.get_all("Provides-Extra", [])
    expected_extras = [canonicalize_name(name) for name in project.get("optional-dependencies", {})]
    normalized = [canonicalize_name(name) for name in provided]
    if Counter(normalized) != Counter(expected_extras):
        raise ValueError("distribution optional extras differ from the project")

    actual = _requirements(metadata.get_all("Requires-Dist", []))
    expected = _expected_requirements(project)
    if actual != expected:
        raise ValueError("distribution dependencies differ from the project")
