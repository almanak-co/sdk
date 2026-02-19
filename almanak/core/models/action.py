import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator

from almanak.core.enums import ActionType, Protocol
from almanak.core.models.params import Params
from almanak.core.models.receipt import Receipt
from almanak.core.models.transaction import Transaction


def default_list():
    return []


class Action(BaseModel):
    type: ActionType
    params: Any
    protocol: Protocol
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    execution_details: Any | None = None
    transactions: list[Transaction] = Field(default_factory=default_list)
    transaction_hashes: list[str] = Field(default_factory=default_list)
    bundle_id: uuid.UUID | None = None

    @field_validator("params", mode="before")
    @classmethod
    def validate_params(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return Params.from_dict(v)
        return v

    @field_validator("execution_details", mode="before")
    @classmethod
    def validate_execution_details(cls, v: Any) -> Any:
        if isinstance(v, dict):
            return Receipt.from_dict(v)
        return v

    model_config = {
        "arbitrary_types_allowed": True,
    }

    def get_id(self) -> uuid.UUID:
        return self.id

    def get_type(self) -> ActionType:
        return self.type

    def get_params(self) -> Params:
        return self.params

    def get_protocol(self) -> Protocol:
        return self.protocol

    def get_execution_details(self) -> Receipt | None:
        return self.execution_details if self.execution_details else None

    def __str__(self):
        # fmt: off
        return (
            f"{self.type} id={self.id},\n"
            f"  Protocol: {self.protocol}\n"
            f"  Params: {self.params}"
        )
        # fmt: on

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        d["type"] = self.type.value
        d["protocol"] = self.protocol.value
        d["id"] = str(self.id)
        d["params"] = self.params.model_dump()
        d["execution_details"] = self.execution_details.model_dump() if self.execution_details else None
        d["transactions"] = [transaction.model_dump() for transaction in self.transactions]
        d["bundle_id"] = str(self.bundle_id) if self.bundle_id else None
        return d

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> Any:
        if isinstance(obj, dict):
            obj["type"] = ActionType(obj["type"])
            obj["protocol"] = Protocol(obj["protocol"])
            obj["id"] = uuid.UUID(obj["id"])
            obj["params"] = Params.from_dict(obj["params"])
            obj["execution_details"] = Receipt.from_dict(obj["execution_details"]) if obj["execution_details"] else None
            obj["transactions"] = [Transaction.model_validate(transaction) for transaction in obj["transactions"]]
            obj["bundle_id"] = uuid.UUID(obj["bundle_id"]) if obj.get("bundle_id") else None
        return super().model_validate(obj)
