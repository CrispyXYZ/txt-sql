from decimal import Decimal
from enum import unique, StrEnum


# ---------------------------------------------------------------------------
# Atomic value types
# ---------------------------------------------------------------------------

type LiteralValue = str | Decimal | bytes | None | bool
type NumberValue = Decimal
type StringValue = str
type BinaryValue = bytes
type DataValue = NumberValue | StringValue | BinaryValue | None


# ---------------------------------------------------------------------------
# Composite types
# ---------------------------------------------------------------------------

type RowDict = dict[str, DataValue]


# ---------------------------------------------------------------------------
# Type enumeration
# ---------------------------------------------------------------------------

@unique
class Types(StrEnum):
    NUMBER = 'NUMBER'
    STRING = 'STRING'
    BINARY = 'BINARY'
