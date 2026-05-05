"""Data QA test definitions.

The "tests" in this package's class names (`CEXSpotPriceTest`, `RSITest`,
etc.) refer to *data quality tests* in the QA domain -- they are production
library modules consumed by `almanak.framework.data.qa.runner` and the QA
reporting layer, NOT pytest tests. The package was renamed from `qa.tests/`
to `qa.test_definitions/` to remove that overlap.
"""
