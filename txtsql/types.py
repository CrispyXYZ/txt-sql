from decimal import Decimal
from enum import unique, StrEnum

type NumberValue = Decimal | str | int
type StringValue = str
type BinaryValue = bytes
type DataValue = NumberValue | StringValue | BinaryValue

@unique
class Types(StrEnum):
    NUMBER = 'NUMBER'
    STRING = 'STRING'
    BINARY = 'BINARY'
