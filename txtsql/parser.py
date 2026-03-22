from dataclasses import dataclass
from typing import Any

from .exceptions import SqlSyntaxError
from .lexer import Token, TokenType


@dataclass(slots=True, frozen=True)
class DropTable:
    table_name: str


@dataclass(slots=True, frozen=True)
class CreateTable:
    table_name: str
    columns: list[tuple[str, str]]


class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    def current_token(self) -> Token:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(TokenType.EOF, None, -1, -1)

    def eat(self, token_type: TokenType) -> Token:
        token = self.current_token()
        if token.type != token_type:
            raise SqlSyntaxError(
                f'Expected {token_type} but got {token.type} at line {token.line}, column {token.column}')
        self.pos += 1
        return token

    def peek(self) -> TokenType:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos].type
        return TokenType.EOF

    def parse(self) -> Any:
        token = self.current_token()
        match token.type:
            case TokenType.CREATE:
                return self.create_table()
            case TokenType.DROP:
                return self.drop_table()
            case _:
                raise SqlSyntaxError(f'Unexpected statement: {token.type}')

    def create_table(self) -> CreateTable:
        self.eat(TokenType.CREATE)
        self.eat(TokenType.TABLE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.LPAREN)
        columns = []
        col_name = self.eat(TokenType.IDENTIFIER).value
        col_type = self._parse_type().value
        columns.append((col_name, col_type))
        while self.peek() == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            col_name = self.eat(TokenType.IDENTIFIER).value
            col_type = self._parse_type().type
            columns.append((col_name, col_type))
        self.eat(TokenType.RPAREN)
        if self.peek() == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return CreateTable(table_name, columns)

    def drop_table(self) -> DropTable:
        self.eat(TokenType.DROP)
        self.eat(TokenType.TABLE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        if self.peek() == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return DropTable(table_name)

    def _parse_type(self) -> Token:
        token = self.current_token()
        if token.type not in (TokenType.TYPE_STRING, TokenType.TYPE_NUMBER, TokenType.TYPE_BINARY):
            raise SyntaxError(f"Expected type STRING, NUMBER or BINARY, but got {token.type}")
        self.pos += 1
        return token
