import json
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from almanak.core.enums import ExecutionStatus, TransactionType


def serialize_timestamp(timestamp: float) -> str:
    """Convert a Unix timestamp to a human-readable string."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")


def deserialize_timestamp(timestamp_str: str) -> float:
    """Convert a human-readable string to a Unix timestamp."""
    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f").timestamp()


class Transaction(BaseModel):
    """
    Represents a blockchain transaction.

    Attributes:
        type (TransactionType): The type of the transaction.
        dict (Dict[str, Any]): The transaction details in dictionary format.
        action_id (uuid.UUID): The UUID of the associated action.
        id (uuid.UUID): A unique identifier for the transaction, generated at creation.
        tx_hash (Optional[str]): The transaction hash, which uniquely identifies the transaction
                                 on the blockchain. This is set after the transaction is signed and
                                 broadcasted.
        from_address (Optional[str]): The address from which the transaction originates.
        created_at (float): The timestamp when the transaction was created.

    Methods:
        get_type() -> TransactionType:
            Returns the type of the transaction.

        get_dict() -> Dict[str, Any]:
            Returns the transaction details as a dictionary.

        get_action_id() -> uuid.UUID:
            Returns the UUID of the associated action.

        get_id() -> str:
            Returns the unique identifier for the transaction. If the transaction hash (`tx_hash`) is available,
            it is returned as the ID. Otherwise, the UUID (`id`) generated at creation is returned.

        get_from_address() -> Optional[str]:
            Returns the address from which the transaction originates.

        get_created_at() -> float:
            Returns the timestamp when the transaction was created.

        __str__() -> str:
            Returns a string representation of the transaction, including its ID, type, action ID, creation time,
            and other relevant details.

        model_dump(*args, **kwargs) -> dict:
            Returns a dictionary representation of the transaction suitable for serialization. This includes converting
            types to their appropriate serialized forms and ensuring the correct ID (either `tx_hash` or `id`) is used.

        model_validate(cls, obj) -> 'Transaction':
            Validates and converts a dictionary representation of a transaction into a `Transaction` object, ensuring
            correct types and deserialization of fields.

    """

    type: TransactionType
    tx_dict: dict[str, Any]
    action_id: uuid.UUID
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    from_address: str | None = Field(default=None)
    tx_hash: str | None = Field(default=None)
    tx_status: ExecutionStatus | None = Field(default=None)
    created_at: float = Field(default_factory=lambda: datetime.now().timestamp())

    model_config = {
        "arbitrary_types_allowed": True,
    }

    def get_type(self) -> TransactionType:
        return self.type

    def get_dict(self) -> dict[str, Any]:
        return self.tx_dict

    def get_action_id(self) -> uuid.UUID:
        return self.action_id

    def get_id(self) -> str:
        return self.tx_hash if self.tx_hash else str(self.id)

    def get_from_address(self) -> str | None:
        return self.from_address

    def get_created_at(self) -> float:
        return self.created_at

    def __str__(self):
        result = (
            f"Transaction(\n"
            f"  id={self.id},\n"
            f"  type={self.type},\n"
            f"  action_id={self.action_id},\n"
            f"  created_at={datetime.fromtimestamp(self.created_at).strftime('%Y-%m-%d %H:%M:%S.%f')},\n"
            f"  tx_dict={json.dumps(self.tx_dict, indent=2)},\n"
        )

        if self.from_address is not None:
            result += f"  from_address={self.from_address},\n"

        if self.tx_hash is not None:
            result += f"  tx_hash={self.tx_hash},\n"

        result += ")"

        return result

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        d["type"] = self.type.value
        d["tx_status"] = self.tx_status.value if self.tx_status else None
        d["id"] = str(self.id)
        d["action_id"] = str(self.action_id)
        d["created_at"] = serialize_timestamp(self.created_at)
        return d

    @classmethod
    def model_validate(cls, obj: Any, **kwargs: Any) -> Any:
        if isinstance(obj, dict):
            obj["type"] = TransactionType(obj["type"])
            obj["id"] = uuid.UUID(obj["id"])
            obj["action_id"] = uuid.UUID(obj["action_id"])
            obj["created_at"] = deserialize_timestamp(obj["created_at"])
        return super().model_validate(obj)
