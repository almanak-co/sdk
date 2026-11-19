"""Opt-in live QA: free public-endpoint reads and gated real-funds write checks.

Nothing here runs in the default test suite or in hosted CI. The read level calls
`almanak ax` against the named public endpoints as a user without a provider key
would. The write level spends real gas and refuses to start unless every gate in
`scripts.live_qa.writes` is satisfied. See docs/qa.md.
"""
