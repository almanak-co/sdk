"""Intent-level tests for validating the full execution pipeline.

These tests execute Intents on Anvil forks and verify on-chain state changes.
They validate the complete flow: Intent -> IntentCompiler -> ActionBundle -> Execution -> Receipt Parsing.

Test Organization:
- test_{protocol}_swap.py - SwapIntent tests
- test_{protocol}_lp.py - LPOpenIntent + LPCloseIntent tests
- test_{protocol}_lending.py - Supply/Borrow/Repay/Withdraw sequence tests
- test_{protocol}_perps.py - PerpOpenIntent + PerpCloseIntent tests
"""
