# Accounting admission regression evidence

`lp-anvil.sqlite` is the closed managed-Anvil EOA database from the September 6, 2026 QA Lab audit of PR #3931 at `57dd1395adaa4fe2d135710ed29ee65d1ee623ad`, matrix row `lp-uniswap_v3-arbitrum`.

It contains four primary ledger transactions and twelve persisted submitted hashes. The canonical G14 predicate is XFAIL. Admission tests change only the supplied verdict or claimed recipe, preserving the database, to exercise the two reproduced false-green controls. This is historical test evidence, not an executable run or a current SDK certification. No keys or RPC credentials are included.

`observed-lp/` preserves the complete independent observation from the harness recheck at `30bd345905`: ten mined submissions (four primary ledger rows), eight balance checks, 1,254,459 gas units, and two stable post-teardown observations. Its wallet is an isolated test signer. Compiler logs are excerpts; database, Accountant output, RPC receipts/transactions/balances/census retain their captured bytes. The regression test revalidates this positive chain proof while retaining UNMEASURED Accountant and position-closure claims.

`observed-provisioning/` captures the Aave Base run at `6e314a39b0`: one pre-strategy WETH provisioning deposit, fourteen strategy transactions and the actual pre-dispatch block boundary. The complete census, persisted transaction set and relevant log lines reproduce both correct setup exclusion and recovery of an omitted first execution approval. These are captured facts from the synthetic-wallet run, not a constructed boundary.
