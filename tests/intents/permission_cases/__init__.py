"""Per-protocol on-chain permission-authorisation test cases.

Each ``<protocol>.py`` in this package exports ``CASES: list[PermissionTestCase]``,
declaring one or more ``(chain, intent_type, config)`` tuples the on-chain
harness should exercise against a deployed Zodiac Roles Modifier. A
``<protocol>.permissions_onchain_exempt`` sentinel opts the whole protocol
out instead.

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
Gate: ``tests/unit/permissions/test_onchain_case_coverage.py``.
Harness: ``tests/intents/_permission_onchain_harness.py``.
"""
