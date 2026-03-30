from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from .exceptions import SqlSyntaxError


class TokenType(StrEnum):
    CREATE = 'CREATE'
    DROP = 'DROP'
    TABLE = 'TABLE'
    INSERT = 'INSERT'
    INTO = 'INTO'
    VALUES = 'VALUES'
    TYPE_STRING = 'STRING'
    TYPE_NUMBER = 'NUMBER'
    TYPE_BINARY = 'BINARY'

    DELETE = 'DELETE'
    FROM = 'FROM'
    WHERE = 'WHERE'
    AND = 'AND'
    OR = 'OR'
    IS = 'IS'
    TRUE = 'TRUE'
    FALSE = 'FALSE'
    NULL = 'NULL'

    EQ = '='
    NE = '<>'
    GT = '>'
    LT = '<'
    GE = '>='
    LE = '<='

    SEMICOLON = 'SEMICOLON'
    COMMA = 'COMMA'
    RPAREN = 'RPAREN'
    LPAREN = 'LPAREN'

    EOF = 'EOF'

    STRING = 'STRING'
    NUMBER = 'NUMBER'
    BINARY = 'BINARY'
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

    def peek(self) -> str | None:
        pos = self.pos + 1
        if pos < len(self.text):
            return self.text[pos]
        return None

    def skip_whitespace(self) -> None:
        while self.current_char() is not None and self.current_char().isspace():
            self.advance()

    def read_number(self) -> Token:
        start_col = self.column
        num_str = ''
        while self.current_char() is not None and self.current_char().isdigit():
            num_str += self.current_char()
            self.advance()
        return Token(TokenType.NUMBER, Decimal(num_str), self.line, start_col)

    def read_string(self) -> Token:
        start_col = self.column
        self.advance()  # to skip quote
        string_str = ''
        while self.current_char() is not None and self.current_char() != "'":
            string_str += self.current_char()
            self.advance()
        if self.current_char() == "'":
            self.advance()
        return Token(TokenType.STRING, string_str, self.line, start_col)

    def read_binary(self) -> Token:
        start_col = self.column

        self.advance()
        self.advance()

        hex_chars = ''
        while self.current_char() is not None and self.current_char() in '0123456789abcdefABCDEF':
            hex_chars += self.current_char()
            self.advance()

        data = bytes.fromhex(hex_chars)

        return Token(TokenType.BINARY, data, self.line, start_col)

    def read_identifier_or_keyword(self) -> Token:
        start_col = self.column
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
            case 'INSERT':
                token_type = TokenType.INSERT
            case 'INTO':
                token_type = TokenType.INTO
            case 'VALUES':
                token_type = TokenType.VALUES
            case 'DELETE':
                token_type = TokenType.DELETE
            case 'FROM':
                token_type = TokenType.FROM
            case 'WHERE':
                token_type = TokenType.WHERE
            case 'AND':
                token_type = TokenType.AND
            case 'OR':
                token_type = TokenType.OR
            case 'IS':
                token_type = TokenType.IS
            case 'TRUE':
                token_type = TokenType.TRUE
            case 'FALSE':
                token_type = TokenType.FALSE
            case 'NULL':
                token_type = TokenType.NULL
            case 'STRING' | 'VARCHAR':
                token_type = TokenType.STRING
                identifier = 'STRING'
            case 'NUMBER' | 'DECIMAL':
                token_type = TokenType.NUMBER
                identifier = 'NUMBER'
            case 'BINARY':
                token_type = TokenType.BINARY
            case _:
                token_type = TokenType.IDENTIFIER
        return Token(token_type, identifier, self.line, start_col)

    def get_next_token(self) -> Token:
        self.skip_whitespace()
        ch = self.current_char()
        if ch is None:
            return Token(TokenType.EOF, None, self.line, self.column)

        match ch:
            case ';':
                self.advance()
                return Token(TokenType.SEMICOLON, ';', self.line, self.column - 1)
            case ',':
                self.advance()
                return Token(TokenType.COMMA, ',', self.line, self.column - 1)
            case '(':
                self.advance()
                return Token(TokenType.LPAREN, '(', self.line, self.column - 1)
            case ')':
                self.advance()
                return Token(TokenType.RPAREN, ')', self.line, self.column - 1)
            case '=':
                self.advance()
                return Token(TokenType.EQ, '=', self.line, self.column - 1)
            case '<':
                next_char = self.peek()
                if next_char == '>':  # <>
                    self.advance()
                    self.advance()
                    return Token(TokenType.NE, '<>', self.line, self.column - 2)
                elif next_char == '=':  # <=
                    self.advance()
                    self.advance()
                    return Token(TokenType.LE, '<=', self.line, self.column - 2)
                else:  # <
                    self.advance()
                    return Token(TokenType.LT, '<', self.line, self.column - 1)
            case '>':
                next_char = self.peek()
                if next_char == '=':  # >=
                    self.advance()
                    self.advance()
                    return Token(TokenType.GE, '>=', self.line, self.column - 2)
                else:  # >
                    self.advance()
                    return Token(TokenType.GT, '>', self.line, self.column - 1)
            case _:
                if ch.isdigit():
                    if ch == '0' and self.peek() in ('x', 'X'):
                        return self.read_binary()
                    else:
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
