"""Shard junit merging into .test_durations for pytest-split balancing."""

import json

import pytest

from scripts.ci.merge_test_durations import junit_files, main, merge

JUNIT = """<testsuite>
  <testcase classname="{classname}" name="{name}" time="{time}"/>
</testsuite>
"""

JUNIT_WITH_FILE = """<testsuite>
  <testcase classname="{classname}" name="{name}" file="{file}" time="{time}"/>
</testsuite>
"""


def write_junit(path, classname, name, time):
    path.write_text(JUNIT.format(classname=classname, name=name, time=time))


def test_merge_converts_classnames_and_keeps_the_longest_time(tmp_path):
    first = tmp_path / "test-results-1.xml"
    second = tmp_path / "test-results-2.xml"
    write_junit(first, "tests.test_example", "test_one[a]", "1.5")
    write_junit(second, "tests.test_example", "test_one[a]", "0.25")
    write_junit(second, "tests.sub.test_other", "test_two", "3")
    durations = merge([first, second])
    assert durations == {
        "tests/test_example.py::test_one[a]": 1.5,
        "tests/sub/test_other.py::test_two": 3.0,
    }


def test_file_attribute_rebuilds_class_method_node_ids(tmp_path):
    # xunit1 output keeps `file`; the classname alone cannot be split into
    # module and class segments without it.
    path = tmp_path / "test-results-1.xml"
    path.write_text(
        JUNIT_WITH_FILE.format(
            classname="tests.test_example.TestSuite",
            name="test_one[a]",
            file="tests/test_example.py",
            time="2.5",
        )
    )
    assert merge([path]) == {"tests/test_example.py::TestSuite::test_one[a]": 2.5}


def test_missing_name_or_classname_is_an_error(tmp_path):
    path = tmp_path / "test-results-1.xml"
    path.write_text('<testsuite><testcase classname="tests.test_example" time="1"/></testsuite>')
    with pytest.raises(ValueError, match="name or classname"):
        merge([path])


def test_directories_are_searched_for_shard_results(tmp_path):
    nested = tmp_path / "shard"
    nested.mkdir()
    write_junit(nested / "test-results-3.xml", "tests.test_example", "test_one", "2")
    (nested / "other.xml").write_text(JUNIT.format(classname="x", name="y", time="9"))
    files = junit_files([str(tmp_path)])
    assert files == [nested / "test-results-3.xml"]


def test_missing_input_is_an_error(tmp_path):
    with pytest.raises(ValueError, match="does not exist"):
        junit_files([str(tmp_path / "absent")])
    with pytest.raises(ValueError, match="no test-results"):
        junit_files([str(tmp_path)])


def test_main_writes_the_durations_file(tmp_path):
    write_junit(tmp_path / "test-results-1.xml", "tests.test_example", "test_one", "0.5")
    output = tmp_path / ".test_durations"
    assert main([str(output), str(tmp_path)]) == 0
    assert json.loads(output.read_text()) == {"tests/test_example.py::test_one": 0.5}
