"""Root pytest configuration for all tests.

This file is automatically loaded by pytest and provides shared configuration
and plugins for the entire test suite.
"""

import importlib.util

import pytest

# Load gateway fixtures for integration tests
# This makes gateway_server, gateway_client, and gateway_web3_* fixtures
# available to all test files that need them.
#
# The stripped strategy container image (deploy/docker/Dockerfile.strategy)
# deletes almanak/gateway/server.py, which tests.conftest_gateway imports at
# module level — registering the plugin there would abort collection of every
# test, including the network-isolation suite that image exists to run
# (deploy/docker/docker-compose.test.yml). find_spec resolves without
# executing the module, so a normal checkout always registers the plugin.
pytest_plugins = ["tests.conftest_gateway"] if importlib.util.find_spec("almanak.gateway.server") is not None else []


@pytest.fixture(autouse=True)
def _fail_on_token_resolver_defects(request: pytest.FixtureRequest):
    """Fail any test in which the token-resolution seam saw a DEFECT (VIB-6100).

    This is where VIB-6100 is actually closed, and it is worth being explicit
    about why it lives here rather than in production code.

    The original defect was a **test double** whose ``resolve()`` signature did
    not match the real one. The seam swallowed the resulting ``TypeError`` and
    reported it as an ordinary "this token did not resolve", so a test believing
    it exercised the *resolved* path silently exercised the *fallback* path and
    passed. That is a test-quality defect; it was never present in production.

    The first attempt to fix it made the production seam **raise** on those
    exception types. Seven rounds of fault injection then found seven ways an
    *environmental* fault reaches that seam wearing the same types — each one
    turning an accounting write into a halt with the trade already on-chain. The
    cure's blast radius exceeded the disease's, and the disease was never in
    production. VIB-6167 records the full evidence.

    So the seam degrades (it never raises) and reports defect-shaped failures to
    an observer instead. In production no observer is installed and the code path
    is unchanged. Here, the observer turns that report into a **test failure** —
    which closes the original defect exactly where it lives, at zero production
    risk.

    A test that deliberately drives a defect shape (the seam's own suite) opts
    out with ``@pytest.mark.expects_resolver_defect``.
    """
    try:
        from almanak.framework.data.tokens.best_effort import set_resolver_defect_observer
    except ModuleNotFoundError as exc:
        # Only tolerate the seam module itself being absent (stripped strategy-
        # container layouts). A nested ImportError/ModuleNotFoundError from a
        # dependency of best_effort must NOT disable this gate — that would
        # recreate "green because nothing ran" for the whole suite
        # (Codex/CodeRabbit #3694).
        if exc.name != "almanak.framework.data.tokens.best_effort":
            raise
        yield
        return

    collected: list = []
    previous = set_resolver_defect_observer(collected.append)
    try:
        yield
    finally:
        set_resolver_defect_observer(previous)

    if not collected or request.node.get_closest_marker("expects_resolver_defect"):
        return

    lines = [
        f"  - {d.exc_type} at {d.context!r} (token={d.token!r}, chain={d.chain!r})"
        + ("  <- raised AT the call boundary: a SIGNATURE mismatch" if d.at_call_boundary else "")
        for d in collected
    ]
    pytest.fail(
        "VIB-6100: the token-resolution seam reported "
        f"{len(collected)} defect-shaped failure(s) during this test:\n"
        + "\n".join(lines)
        + "\n\nEach one was degraded to 'this token did not resolve', so any assertion "
        "above may have passed while exercising the FALLBACK branch rather than the "
        "one it names. Fix the double (use tests.support.token_resolver.FakeTokenResolver, "
        "which cannot mismatch) or the call site. If the defect is the point of the test, "
        "mark it @pytest.mark.expects_resolver_defect."
    )
