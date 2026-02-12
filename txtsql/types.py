from enum import unique, StrEnum


@unique
class Types(StrEnum):
    NUMBER = 'NUMBER'
    STRING = 'STRING'
    BINARY = 'BINARY'
