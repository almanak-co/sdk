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


@pytest.fixture(scope="session", autouse=True)
def _explicit_curve_pool_fixtures():
    """Inject test-only Curve catalogs after production removed CURVE_TEST_POOLS.

    Legacy unit/integration tests intentionally exercise named representative
    pools without network access. Their addresses now live under ``tests/`` and
    enter connector instances through the same deployment-local override seam
    used by bound permission compilation; production defaults remain empty.
    """
    from almanak.connectors.curve import compiler as curve_compiler
    from almanak.connectors.curve import permission_hints as curve_permission_hints
    from almanak.connectors.curve import pool_resolver as curve_pool_resolver
    from almanak.connectors.curve.adapter import CurveAdapter
    from almanak.connectors.curve.pool_binding import CurvePoolPermissionBinding
    from almanak.connectors.curve.receipt_parser import CurveReceiptParser
    from tests.support.curve_pool_catalog import CURVE_TEST_POOLS, curve_test_meta_lookup, curve_test_metadata

    monkeypatch = pytest.MonkeyPatch()
    original_adapter_init = CurveAdapter.__init__

    def adapter_init(self, config, *args, **kwargs):
        fixtures = {}
        for name, pool in CURVE_TEST_POOLS.get(config.chain, {}).items():
            metadata = curve_test_metadata(config.chain, str(pool["address"]))
            fixtures[name] = {
                **pool,
                "coin_decimals": list(metadata.coin_decimals) if metadata is not None else [],
            }
        overrides = {**fixtures, **config.permission_pool_overrides}
        previous = config.permission_pool_overrides
        config.permission_pool_overrides = overrides
        try:
            return original_adapter_init(self, config, *args, **kwargs)
        finally:
            config.permission_pool_overrides = previous

    monkeypatch.setattr(CurveAdapter, "__init__", adapter_init)
    monkeypatch.setattr(curve_compiler, "_deployment_pool_catalog", lambda: CURVE_TEST_POOLS)

    original_resolve_pool_metadata = curve_pool_resolver.resolve_pool_metadata

    def resolve_pool_metadata(chain, pool_address, **kwargs):
        fixture = curve_test_metadata(chain, pool_address)
        # Explicit gateway doubles belong to the resolver's own tests and must
        # never be shadowed. The local fixture substitutes only for RPC-backed
        # intent admission, where CI fork providers may lack archival reads.
        if fixture is not None and kwargs.get("gateway_client") is None and kwargs.get("rpc_url"):
            return fixture
        return original_resolve_pool_metadata(chain, pool_address, **kwargs)

    # Permission admission in intent tests must still consume an exact config
    # binding, but it cannot rely on archival MetaRegistry storage being
    # available through every CI fork provider. Resolve only explicitly listed
    # test fixtures locally; unknown pools continue through the real resolver.
    monkeypatch.setattr(curve_pool_resolver, "resolve_pool_metadata", resolve_pool_metadata)

    original_discovery_pools = curve_permission_hints._discovery_pools

    def discovery_pools(chain, ctx):
        if ctx.strategy_config:
            return original_discovery_pools(chain, ctx)
        pools = []
        for name, data in CURVE_TEST_POOLS.get(chain, {}).items():
            metadata = curve_test_metadata(chain, str(data["address"]))
            assert metadata is not None
            binding = CurvePoolPermissionBinding.from_metadata(chain, metadata)
            pools.append((name, binding.pool_data(), binding))
        return pools

    monkeypatch.setattr(curve_permission_hints, "_discovery_pools", discovery_pools)

    original_parser_init = CurveReceiptParser.__init__

    def parser_init(self, chain="ethereum", pool_meta_lookup=None, **kwargs):
        return original_parser_init(
            self,
            chain,
            pool_meta_lookup=pool_meta_lookup or curve_test_meta_lookup,
            **kwargs,
        )

    monkeypatch.setattr(CurveReceiptParser, "__init__", parser_init)
    yield
    monkeypatch.undo()


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
