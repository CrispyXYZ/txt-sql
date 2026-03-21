from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from .exceptions import SqlSyntaxError


class TokenType(StrEnum):
    CREATE = 'CREATE'
    DROP = 'DROP'
    TABLE = 'TABLE'

    SEMICOLON = 'SEMICOLON'

    EOF = 'EOF'

    STRING = 'STRING'
    NUMBER = 'NUMBER'
    IDENTIFIER = 'IDENTIFIER'


@dataclass(frozen=True, slots=True)
class Token:
    type: TokenType
    value: Any
    line: int
    column: int


class Lexer:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1

    def current_char(self) -> str | None:
        if self.pos < len(self.text):
            return self.text[self.pos]
        return None

    def advance(self) -> None:
        if self.current_char() == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        self.pos += 1

    def skip_whitespace(self) -> None:
        while self.current_char() is not None and self.current_char().isspace():
            self.advance()

    def read_number(self) -> Token:
        num_str = ''
        while self.current_char() is not None and self.current_char().isdigit():
            num_str += self.current_char()
            self.advance()
        return Token(TokenType.NUMBER, Decimal(num_str), self.line, self.column)

    def read_string(self) -> Token:
        self.advance()  # to skip quote
        string_str = ''
        while self.current_char() is not None and self.current_char() != "'":
            string_str += self.current_char()
            self.advance()
        if self.current_char() == '"':
            self.advance()
        return Token(TokenType.STRING, string_str, self.line, self.column)

    def read_identifier_or_keyword(self) -> Token:
        identifier = ''
        token_type = TokenType.IDENTIFIER
        while self.current_char() is not None and (self.current_char().isalnum() or self.current_char() == '_'):
            identifier += self.current_char()
            self.advance()
        match identifier.upper():
            case 'CREATE':
                token_type = TokenType.CREATE
            case 'DROP':
                token_type = TokenType.DROP
            case 'TABLE':
                token_type = TokenType.TABLE
            case _:
                token_type = TokenType.IDENTIFIER
        return Token(token_type, identifier, self.line, self.column)

    def get_next_token(self) -> Token:
        self.skip_whitespace()
        ch = self.current_char()
        if ch is None:
            return Token(TokenType.EOF, None, self.line, self.column)

        match ch:
            case ';':
                self.advance()
                return Token(TokenType.SEMICOLON, ';', self.line, self.column - 1)
            case _:
                if ch.isdigit():
                    return self.read_number()
                if ch == "'":
                    return self.read_string()
                if ch.isalpha() or ch == '_':
                    return self.read_identifier_or_keyword()
                raise SqlSyntaxError(f'Unexpected character {ch} at line {self.line} column {self.column}')

    def tokenize(self) -> list[Token]:
        tokens = []
        token = self.get_next_token()
        while token.type != TokenType.EOF:
            tokens.append(token)
            token = self.get_next_token()
        tokens.append(token)
        return tokens
