"""Fee model endpoints: list and detail."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from almanak.services.backtest.models import FeeModelDetail, FeeModelListResponse
from almanak.services.backtest.services.fee_model_exporter import (
    get_fee_model_detail,
    list_fee_models,
)

router = APIRouter(prefix="/api/v1", tags=["fee-models"])


@router.get("/fee-models")
async def list_all_fee_models() -> FeeModelListResponse:
    """List all supported protocols with fee model summaries."""
    return FeeModelListResponse(protocols=list_fee_models())


@router.get("/fee-models/{protocol}")
async def get_fee_model(protocol: str) -> FeeModelDetail:
    """Get detailed fee model information for a specific protocol."""
    detail = get_fee_model_detail(protocol)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Fee model for '{protocol}' not found")
    return detail
