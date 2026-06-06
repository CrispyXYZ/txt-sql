from decimal import Decimal
from enum import unique, StrEnum


# ---------------------------------------------------------------------------
# Atomic value types
# ---------------------------------------------------------------------------

type LiteralValue = str | Decimal | None | bool
type NumberValue = Decimal
type StringValue = str
type DataValue = NumberValue | StringValue | None


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
