# Production operation

Use the current [production operating guide](../../docs/OPERATING.md#production)
with this strategy's [production author configuration](config.production.yaml)
and an operator file shaped like
[the production example](../../examples/operators/aave_v4_weth_supply.production.yaml).
The author file keeps the economic gas policy; the operator file owns providers,
the funded account binding, independent payer ceilings and the encrypted
submitter keystore references. Production requires read-only admission before
execution. Generated Safe owner transactions require owner installation and
subsequent verification.

Running a fork is not production authorization.
