import json
import uuid
from datetime import datetime
from typing import Any

import pytz
from pydantic import BaseModel, Field

from almanak import Chain, Network
from almanak.core.enums import ExecutionStatus
from almanak.core.models.action import Action
from almanak.core.models.transaction import Transaction, deserialize_timestamp, serialize_timestamp


def default_list():
    return []


class ActionBundle(BaseModel):
    actions: list[Action]
    network: Network
    chain: Chain
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    transactions: list[Transaction] = Field(default_factory=default_list)
    signed_transactions: list[dict] = Field(default_factory=default_list, exclude=True)
    raw_transactions: list[str] = Field(default_factory=default_list)
    transaction_hashes: list[str] = Field(default_factory=default_list)
    cached_receipts: dict[str, dict] = Field(default_factory=dict, exclude=True)
    deadline: float | None = None
    created_at: float = Field(default_factory=lambda: datetime.now(pytz.utc).timestamp())
    executed_at: float | None = None
    status: ExecutionStatus = ExecutionStatus.CREATED
    strategy_id: str
    config: Any
    persistent_state: Any

    model_config = {
        "arbitrary_types_allowed": True,
    }

    def __init__(self, **data):
        super().__init__(**data)  # Call the Pydantic BaseModel constructor
        for action in self.actions:
            action.bundle_id = self.id

    def get_actions(self) -> list[Action]:
        return self.actions

    def get_network(self) -> Network:
        return self.network

    def get_chain(self) -> Chain:
        return self.chain

    def get_transactions(self) -> list[Transaction] | None:
        return self.transactions if self.transactions else None

    def get_signed_transactions(self) -> list[dict] | None:
        return self.signed_transactions if self.signed_transactions else None

    def get_raw_transactions(self) -> list[str] | None:
        return self.raw_transactions if self.raw_transactions else None

    def get_transaction_hashes(self) -> list[str] | None:
        return self.transaction_hashes if self.transaction_hashes else None

    def get_deadline(self) -> float | None:
        return self.deadline

    def get_created_at(self) -> float:
        return self.created_at

    def get_executed_at(self) -> float | None:
        return self.executed_at

    def get_status(self) -> ExecutionStatus | None:
        return self.status

    def get_action_types(self):
        return [action.get_type().value for action in self.actions]

    def add_transaction(self, transaction: Transaction):
        self.transactions.append(transaction)

    def add_transactions(self, transactions: list[Transaction]):
        self.transactions.extend(transactions)

    def add_signed_transaction(self, signed_transaction: Any):
        self.signed_transactions.append(signed_transaction)
        self.raw_transactions.append(signed_transaction.rawTransaction.hex())
        self.transaction_hashes.append(signed_transaction.hash.hex())

    def __str__(self):
        return (
            f"ActionBundle(\n"
            f"  id={self.id},\n"
            f"  actions=\n{self.actions},\n"
            f"  network={self.network.value},\n"
            f"  chain={self.chain.value},\n"
            f"  created_at={datetime.fromtimestamp(self.created_at).strftime('%Y-%m-%d %H:%M:%S.%f')},\n"
            f"  transactions={self.transactions if self.transactions else None},\n"
            f"  signed_transactions={self.signed_transactions if self.signed_transactions else None},\n"
            f"  transaction_hashes={self.transaction_hashes if self.transaction_hashes else None},\n"
            f"  cached_receipts={self.cached_receipts if self.cached_receipts else None},\n"
            f"  deadline={datetime.fromtimestamp(self.deadline).strftime('%Y-%m-%d %H:%M:%S.%f') if self.deadline else None},\n"
            f"  executed_at={datetime.fromtimestamp(self.executed_at).strftime('%Y-%m-%d %H:%M:%S.%f') if self.executed_at else None},\n"
            f"  status={self.status.value}\n"
            f")"
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        d["network"] = self.network.value
        d["chain"] = self.chain.value
        d["status"] = self.status.value if self.status else None
        d["id"] = str(self.id)
        d["actions"] = [action.model_dump() for action in self.actions]
        d["transactions"] = [transaction.model_dump() for transaction in self.transactions]
        d["created_at"] = serialize_timestamp(self.created_at)
        d["executed_at"] = serialize_timestamp(self.executed_at) if self.executed_at else None
        d["deadline"] = serialize_timestamp(self.deadline) if self.deadline else None
        if isinstance(self.config, dict):
            d["config"] = self.config
        else:
            d["config"] = self.config.model_dump() if self.config else None
        if isinstance(self.persistent_state, dict):
            d["persistent_state"] = self.persistent_state
        else:
            d["persistent_state"] = self.persistent_state.model_dump() if self.persistent_state else None
        return d

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> Any:
        if isinstance(obj, dict):
            obj["network"] = Network(obj["network"])
            obj["chain"] = Chain(obj["chain"])
            obj["id"] = uuid.UUID(obj["id"])
            obj["actions"] = [Action.model_validate(action) for action in obj["actions"]]
            obj["transactions"] = [Transaction.model_validate(transaction) for transaction in obj["transactions"]]
            obj["created_at"] = deserialize_timestamp(obj["created_at"])
            obj["executed_at"] = deserialize_timestamp(obj["executed_at"]) if obj["executed_at"] else None
            obj["deadline"] = deserialize_timestamp(obj["deadline"]) if obj["deadline"] else None
        return super().model_validate(obj)

    @classmethod
    def from_json(cls, json_str: str):
        obj = json.loads(json_str)
        return cls.model_validate(obj)

    def summary(self):
        result = (
            f"ActionBundle(\n"
            f"  id={self.id},\n"
            f"  network={self.network.value},\n"
            f"  chain={self.chain.value},\n"
            f"  status={self.status.value}\n"
            f"  strategy_id={self.strategy_id}\n"
        )
        result += f"  Actions= {self.get_action_types()}\n"
        if self.transactions:
            result += f"  Transactions Hash= {self.transaction_hashes}\n"

        result += ")"
        return result

    def to_json(self) -> str:
        data = {
            "id": self.id,
            "network": self.network.value,
            "chain": self.chain.value,
            "status": self.status.value,
            "strategy_id": self.strategy_id,
            "actions": self.get_action_types(),
        }
        if self.transactions:
            data["transactions_hash"] = self.transaction_hashes

        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)
