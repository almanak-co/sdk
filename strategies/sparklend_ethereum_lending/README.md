# SparkLend Ethereum lending

> SparkLend supply-and-borrow lifecycle on Ethereum: supply WETH, check collateral controls, borrow USDC, hold, partially repay and withdraw, then close fully.

Lifecycle for the configured SparkLend revision-4 Pool and revision-1
reserve tokens. It supplies WETH, checks collateral controls, borrows USDC, holds
the proceeds, then performs partial repayment and withdrawal before full cleanup.
The Safe owns collateral, debt and proceeds; the named operator pays gas.

`strategy.py` and `config.yaml` are the author-maintained inputs. The configuration
identifies the Pool, assets and oracle feeds. Runtime state retains owned shares
independently of strategy memory.

The generated Safe/Roles and EOA fork lifecycles passed, including gateway and
runtime recovery. Prepared economic execution, reproduction and process recovery
also passed. `almanak support --connector sparklend --chain eip155:1` states the
declaration; fork proofs use simulated funds.

The loan allocation accepts an optional
[loan-asset reserve](../../docs/authoring.md#a-loan-asset-reserve-for-interest) for the
interest the teardown's full repay needs.
