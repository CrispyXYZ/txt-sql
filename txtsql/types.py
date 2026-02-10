from enum import Enum, unique


@unique
class Types(Enum):
    NUMBER = 1
    STRING = 2
    BINARY = 3
