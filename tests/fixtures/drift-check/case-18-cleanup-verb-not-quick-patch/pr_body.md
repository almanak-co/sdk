# refactor: remove dead noqa markers from connector

## Summary
Positive refactor that REMOVES quick-patch markers — must NOT trigger
QUICK_PATCH_DETECTED. The cleanup-verb filter must catch `remove`, `delete`,
`replace`, etc. before the quick-patch regex sees the marker word.
