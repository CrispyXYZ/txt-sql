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


@dataclass(slots=True, frozen=True)
class InsertValues:
    table_name: str
    columns: list[str] | None
    values: list[list[Any]]


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
            case TokenType.INSERT:
                return self.insert_values()
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
            raise SqlSyntaxError(f'Expected type STRING, NUMBER or BINARY, but got {token.type}')
        self.pos += 1
        return token

    def insert_values(self) -> InsertValues:
        self.eat(TokenType.INSERT)
        self.eat(TokenType.INTO)
        table_name = self.eat(TokenType.IDENTIFIER).value

        # Optional column list
        columns = None
        if self.peek() == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            columns = []
            col_name = self.eat(TokenType.IDENTIFIER).value
            columns.append(col_name)
            while self.peek() == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                col_name = self.eat(TokenType.IDENTIFIER).value
                columns.append(col_name)
            self.eat(TokenType.RPAREN)

        self.eat(TokenType.VALUES)

        # Parse multiple VALUES clauses
        all_values = []
        while True:
            self.eat(TokenType.LPAREN)
            values = []
            first_value = self.current_token()
            if first_value.type not in (TokenType.STRING, TokenType.NUMBER, TokenType.BINARY):
                raise SqlSyntaxError(f'Expected value but got {first_value.type}')
            values.append(first_value.value)
            self.pos += 1

            while self.peek() == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                value_token = self.current_token()
                if value_token.type not in (TokenType.STRING, TokenType.NUMBER, TokenType.BINARY):
                    raise SqlSyntaxError(f'Expected value but got {value_token.type}')
                values.append(value_token.value)
                self.pos += 1

            self.eat(TokenType.RPAREN)
            all_values.append(values)

            # Check if there are more VALUES clauses
            if self.peek() != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)

        if self.peek() == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return InsertValues(table_name, columns, all_values)
