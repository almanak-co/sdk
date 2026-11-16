"""Built dependency metadata must remain exactly tied to the project table."""

from email.parser import BytesParser

import pytest

from scripts.ci.distribution_metadata import verify_project_metadata

PROJECT = {
    "requires-python": ">=3.12",
    "dependencies": ["eth-abi>=6,<7", "eth-hash[pycryptodome]>=0.8,<0.9"],
    "optional-dependencies": {"code": ["pyright>=1.1.390"]},
}


def metadata(*headers: str):
    body = [
        "Metadata-Version: 2.5",
        "Name: almanak",
        "Version: 3.0.0rc2",
        *headers,
        "",
        "",
    ]
    return BytesParser().parsebytes("\n".join(body).encode())


def valid():
    return metadata(
        "Requires-Python: >=3.12",
        "Requires-Dist: eth-abi<7,>=6",
        "Requires-Dist: eth-hash[pycryptodome]<0.9,>=0.8",
        "Provides-Extra: code",
        'Requires-Dist: pyright>=1.1.390; extra == "code"',
    )


def test_semantically_equivalent_metadata_is_accepted():
    verify_project_metadata(valid(), PROJECT)


@pytest.mark.parametrize(
    "headers",
    [
        ("Requires-Dist: eth-abi>=6",),
        (),
        ("Requires-Dist: unrelated>=1",),
        ("Requires-Dist: eth-abi<7,>=6", "Requires-Dist: eth-abi<7,>=6"),
    ],
    ids=("modified", "missing", "added", "duplicate"),
)
def test_changed_runtime_dependencies_are_refused(headers):
    document = valid()
    del document["Requires-Dist"]
    for value in headers:
        document["Requires-Dist"] = value.removeprefix("Requires-Dist: ")
    with pytest.raises(ValueError, match="dependencies"):
        verify_project_metadata(document, PROJECT)


@pytest.mark.parametrize("values", [(), (">=3.11",), (">=3.12", ">=3.12")])
def test_missing_changed_or_duplicate_python_requirement_is_refused(values):
    document = valid()
    del document["Requires-Python"]
    for value in values:
        document["Requires-Python"] = value
    with pytest.raises(ValueError, match="Requires-Python"):
        verify_project_metadata(document, PROJECT)


@pytest.mark.parametrize(
    ("provided", "requirement"),
    [
        ((), ()),
        (("other",), ('pyright>=1.1.390; extra == "other"',)),
        (("code", "code"), ('pyright>=1.1.390; extra == "code"',)),
        (("code",), ()),
    ],
)
def test_missing_changed_or_duplicate_extra_metadata_is_refused(provided, requirement):
    document = valid()
    del document["Provides-Extra"]
    for value in provided:
        document["Provides-Extra"] = value
    requirements = [
        value for value in document.get_all("Requires-Dist", []) if "extra ==" not in value
    ]
    del document["Requires-Dist"]
    for value in (*requirements, *requirement):
        document["Requires-Dist"] = value
    with pytest.raises(ValueError, match="extras|dependencies"):
        verify_project_metadata(document, PROJECT)
