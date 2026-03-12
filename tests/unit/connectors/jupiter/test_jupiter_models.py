"""Tests for Jupiter data models."""

import pytest

from almanak.framework.connectors.jupiter.models import (
    JupiterQuote,
    JupiterRoutePlan,
    JupiterSwapTransaction,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_QUOTE_RESPONSE = {
    "inputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "outputMint": "So11111111111111111111111111111111111111112",
    "inAmount": "1000000000",
    "outAmount": "6666666",
    "otherAmountThreshold": "6633333",
    "priceImpactPct": "0.12",
    "slippageBps": 50,
    "routePlan": [
        {
            "swapInfo": {
                "ammKey": "pool123",
                "label": "Raydium",
                "inputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "outputMint": "So11111111111111111111111111111111111111112",
                "inAmount": "1000000000",
                "outAmount": "6666666",
                "feeAmount": "3000",
                "feeMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            },
            "percent": 100.0,
        }
    ],
}

SAMPLE_SWAP_RESPONSE = {
    "swapTransaction": "AQAAAA...base64encodedtx",
    "lastValidBlockHeight": 280000000,
    "prioritizationFeeLamports": 5000,
}


# ---------------------------------------------------------------------------
# JupiterQuote tests
# ---------------------------------------------------------------------------


class TestJupiterQuote:
    def test_from_api_response(self):
        quote = JupiterQuote.from_api_response(SAMPLE_QUOTE_RESPONSE)

        assert quote.input_mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        assert quote.output_mint == "So11111111111111111111111111111111111111112"
        assert quote.in_amount == "1000000000"
        assert quote.out_amount == "6666666"
        assert quote.other_amount_threshold == "6633333"
        assert quote.price_impact_pct == "0.12"
        assert quote.slippage_bps == 50
        assert len(quote.route_plan) == 1
        assert quote.raw_response == SAMPLE_QUOTE_RESPONSE

    def test_get_price_impact_float(self):
        quote = JupiterQuote.from_api_response(SAMPLE_QUOTE_RESPONSE)
        assert quote.get_price_impact_float() == pytest.approx(0.12)

    def test_get_price_impact_float_invalid(self):
        quote = JupiterQuote(
            input_mint="A",
            output_mint="B",
            in_amount="100",
            out_amount="200",
            price_impact_pct="invalid",
        )
        assert quote.get_price_impact_float() == 0.0

    def test_get_amounts_int(self):
        quote = JupiterQuote.from_api_response(SAMPLE_QUOTE_RESPONSE)
        assert quote.get_in_amount_int() == 1000000000
        assert quote.get_out_amount_int() == 6666666

    def test_from_api_response_minimal(self):
        """Test with minimal/empty response."""
        quote = JupiterQuote.from_api_response({})
        assert quote.input_mint == ""
        assert quote.output_mint == ""
        assert quote.in_amount == "0"
        assert quote.out_amount == "0"
        assert quote.route_plan == []

    def test_from_api_response_multi_hop(self):
        """Test with multi-hop route."""
        data = {
            **SAMPLE_QUOTE_RESPONSE,
            "routePlan": [
                {
                    "swapInfo": {
                        "ammKey": "pool1",
                        "label": "Raydium",
                        "inputMint": "USDC_MINT",
                        "outputMint": "MID_MINT",
                        "inAmount": "1000000",
                        "outAmount": "500000",
                    },
                    "percent": 100.0,
                },
                {
                    "swapInfo": {
                        "ammKey": "pool2",
                        "label": "Orca",
                        "inputMint": "MID_MINT",
                        "outputMint": "SOL_MINT",
                        "inAmount": "500000",
                        "outAmount": "6666",
                    },
                    "percent": 100.0,
                },
            ],
        }
        quote = JupiterQuote.from_api_response(data)
        assert len(quote.route_plan) == 2
        assert quote.route_plan[0].label == "Raydium"
        assert quote.route_plan[1].label == "Orca"


# ---------------------------------------------------------------------------
# JupiterRoutePlan tests
# ---------------------------------------------------------------------------


class TestJupiterRoutePlan:
    def test_from_api_response(self):
        data = {
            "swapInfo": {
                "ammKey": "pool_abc",
                "label": "Meteora",
                "inputMint": "A",
                "outputMint": "B",
                "inAmount": "100",
                "outAmount": "200",
                "feeAmount": "1",
                "feeMint": "A",
            },
            "percent": 50.0,
        }
        plan = JupiterRoutePlan.from_api_response(data)
        assert plan.amm_key == "pool_abc"
        assert plan.label == "Meteora"
        assert plan.input_mint == "A"
        assert plan.output_mint == "B"
        assert plan.in_amount == "100"
        assert plan.out_amount == "200"
        assert plan.fee_amount == "1"
        assert plan.fee_mint == "A"
        assert plan.percent == 50.0

    def test_from_api_response_defaults(self):
        plan = JupiterRoutePlan.from_api_response({})
        assert plan.amm_key == ""
        assert plan.label == ""
        assert plan.percent == 100.0


# ---------------------------------------------------------------------------
# JupiterSwapTransaction tests
# ---------------------------------------------------------------------------


class TestJupiterSwapTransaction:
    def test_from_api_response(self):
        quote = JupiterQuote.from_api_response(SAMPLE_QUOTE_RESPONSE)
        swap_tx = JupiterSwapTransaction.from_api_response(SAMPLE_SWAP_RESPONSE, quote=quote)

        assert swap_tx.swap_transaction == "AQAAAA...base64encodedtx"
        assert swap_tx.last_valid_block_height == 280000000
        assert swap_tx.priority_fee_lamports == 5000
        assert swap_tx.quote is quote

    def test_from_api_response_without_quote(self):
        swap_tx = JupiterSwapTransaction.from_api_response(SAMPLE_SWAP_RESPONSE)
        assert swap_tx.quote is None
        assert swap_tx.swap_transaction == "AQAAAA...base64encodedtx"

    def test_from_api_response_minimal(self):
        swap_tx = JupiterSwapTransaction.from_api_response({})
        assert swap_tx.swap_transaction == ""
        assert swap_tx.last_valid_block_height == 0
        assert swap_tx.priority_fee_lamports == 0
