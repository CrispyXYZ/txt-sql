from dataclasses import dataclass
from typing import Any

from .exceptions import SqlSyntaxError
from .lexer import Token, TokenType


@dataclass(slots=True, frozen=True)
class DropTable:
    table_name: str


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
            case TokenType.DROP:
                return self.drop_table()
            case _:
                raise SqlSyntaxError(f'Unexpected statement: {token.type}')

    def drop_table(self) -> DropTable:
        self.eat(TokenType.DROP)
        self.eat(TokenType.TABLE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        if self.peek() == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return DropTable(table_name)
