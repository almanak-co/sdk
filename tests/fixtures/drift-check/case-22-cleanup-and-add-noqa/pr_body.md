# refactor(connector): drop xfail and add noqa back

## Summary
Adversarial cleanup-verb bypass — commit subject contains BOTH a cleanup verb
(`drop xfail`) AND a quick-patch marker addition (`add noqa back`). Filter
must NOT suppress this; should override to QUICK_PATCH_DETECTED.
