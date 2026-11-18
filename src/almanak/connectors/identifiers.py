"""Connector and binding labels are separate from CAIP chain and asset identity."""

from typing import Annotated, ClassVar

from pydantic import Field

from almanak.caip import Identifier

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class ConnectorId(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = r"[a-z][a-z0-9_]*"


class ProtocolVariant(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = r"[a-z][a-z0-9_]*"
