from dataclasses import dataclass
from enum import Enum
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


class LogicalOp(Enum):
    AND = 'and'
    OR = 'or'


class ComparisonOp(Enum):
    EQ = '='
    NE = '<>'
    GT = '>'
    LT = '<'
    GE = '>='
    LE = '<='


class Expression:
    """Base class: expression"""
    pass


@dataclass
class LiteralExpression(Expression):
    """Literal expression: 1, 'hello', NULL, TRUE, FALSE"""
    value: Any


@dataclass
class ColumnExpression(Expression):
    """Column name expression: age, name"""
    column_name: str


@dataclass
class NullCheckExpression(Expression):
    """NULL check: column IS [NOT] NULL"""
    column: ColumnExpression
    is_null: bool  # True for IS NULL, False for IS NOT NULL


@dataclass
class ConditionExpression(Expression):
    """Condition expression: column op literal (e.g., age > 18)"""
    column: ColumnExpression
    op: ComparisonOp
    literal: LiteralExpression


@dataclass
class LogicalExpression(Expression):
    """Logical expression: left AND/OR right"""
    left: Expression
    op: LogicalOp
    right: Expression


@dataclass
class WhereClause:
    """WHERE clause wrapper"""
    expression: Expression


@dataclass(slots=True, frozen=True)
class DeleteStatement:
    table_name: str
    where_clause: WhereClause | None


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
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1].type
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
            case TokenType.DELETE:
                return self.delete_statement()
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
        # Check if current token is COMMA
        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            col_name = self.eat(TokenType.IDENTIFIER).value
            col_type = self._parse_type().value
            columns.append((col_name, col_type))
        self.eat(TokenType.RPAREN)
        # Check if current token is SEMICOLON
        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return CreateTable(table_name, columns)

    def drop_table(self) -> DropTable:
        self.eat(TokenType.DROP)
        self.eat(TokenType.TABLE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        # Check if current token is SEMICOLON
        if self.current_token().type == TokenType.SEMICOLON:
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
        if self.current_token().type == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            columns = []
            col_name = self.eat(TokenType.IDENTIFIER).value
            columns.append(col_name)
            while self.current_token().type == TokenType.COMMA:
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

            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                value_token = self.current_token()
                if value_token.type not in (TokenType.STRING, TokenType.NUMBER, TokenType.BINARY):
                    raise SqlSyntaxError(f'Expected value but got {value_token.type}')
                values.append(value_token.value)
                self.pos += 1

            self.eat(TokenType.RPAREN)
            all_values.append(values)

            # Check if there are more VALUES clauses
            if self.current_token().type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return InsertValues(table_name, columns, all_values)

    # Expression parsing methods
    def parse_expression(self) -> Expression:
        """Parse expression (entry point)"""
        return self.parse_or()

    def parse_or(self) -> Expression:
        """Parse OR expression (AND has higher precedence than OR)"""
        left = self.parse_and()
        while self.current_token().type == TokenType.OR:
            op = LogicalOp.OR
            self.eat(TokenType.OR)
            right = self.parse_and()
            left = LogicalExpression(left, op, right)
        return left

    def parse_and(self) -> Expression:
        """Parse AND expression"""
        left = self.parse_condition()
        while self.current_token().type == TokenType.AND:
            op = LogicalOp.AND
            self.eat(TokenType.AND)
            right = self.parse_condition()
            left = LogicalExpression(left, op, right)
        return left

    def parse_condition(self) -> Expression:
        """Parse condition (comparison expression or parenthesized expression)"""
        token = self.current_token()

        # Parenthesized expression
        if token.type == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            expr = self.parse_expression()
            self.eat(TokenType.RPAREN)
            return expr

        # NULL check: column IS [NOT] NULL
        if token.type == TokenType.IDENTIFIER and self.peek() == TokenType.IS:
            return self._parse_null_check()

        # Comparison expression: column_name op literal
        if token.type == TokenType.IDENTIFIER and self._is_comparison_op(self.peek()):
            return self._parse_comparison()

        raise SqlSyntaxError(f"Unexpected token: {token.type} at line {token.line}, column {token.column}")

    def _parse_null_check(self) -> Expression:
        """Parse column IS [NOT] NULL"""
        column = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.IS)

        # NOT NULL
        is_not = False
        if self.current_token().type == TokenType.NOT:
            self.eat(TokenType.NOT)
            is_not = True

        self.eat(TokenType.NULL)

        return NullCheckExpression(
            ColumnExpression(column),
            is_null=not is_not  # True means IS NULL, False means IS NOT NULL
        )

    def _parse_comparison(self) -> Expression:
        """Parse column_name op literal"""
        column_token = self.eat(TokenType.IDENTIFIER)
        column_name = column_token.value

        op_token = self.current_token()
        match op_token.type:
            case TokenType.EQ:
                self.eat(TokenType.EQ)
                op = ComparisonOp.EQ
            case TokenType.NE:
                self.eat(TokenType.NE)
                op = ComparisonOp.NE
            case TokenType.GT:
                self.eat(TokenType.GT)
                op = ComparisonOp.GT
            case TokenType.LT:
                self.eat(TokenType.LT)
                op = ComparisonOp.LT
            case TokenType.GE:
                self.eat(TokenType.GE)
                op = ComparisonOp.GE
            case TokenType.LE:
                self.eat(TokenType.LE)
                op = ComparisonOp.LE
            case _:
                raise SqlSyntaxError(f"Invalid comparison operator: {op_token.type}")

        # Parse literal value
        literal_token = self.current_token()
        if literal_token.type in (TokenType.STRING, TokenType.NUMBER, TokenType.BINARY):
            self.pos += 1
            literal = LiteralExpression(literal_token.value)
        elif literal_token.type == TokenType.NULL:
            self.eat(TokenType.NULL)
            literal = LiteralExpression(None)
        elif literal_token.type == TokenType.TRUE:
            self.eat(TokenType.TRUE)
            literal = LiteralExpression(True)
        elif literal_token.type == TokenType.FALSE:
            self.eat(TokenType.FALSE)
            literal = LiteralExpression(False)
        else:
            raise SqlSyntaxError(f"Expected literal value but got {literal_token.type}")

        return ConditionExpression(
            ColumnExpression(column_name),
            op,
            literal
        )

    def _is_comparison_op(self, token_type: TokenType) -> bool:
        """Determine if token is a comparison operator"""
        return token_type in [
            TokenType.EQ, TokenType.NE, TokenType.GT, TokenType.LT,
            TokenType.GE, TokenType.LE
        ]

    def delete_statement(self) -> DeleteStatement:
        """Parse DELETE FROM table_name WHERE condition;"""
        self.eat(TokenType.DELETE)
        self.eat(TokenType.FROM)

        # Table name
        table_name = self.eat(TokenType.IDENTIFIER).value

        # WHERE clause (optional)
        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            expression = self.parse_expression()
            where_clause = WhereClause(expression)

        # Semicolon optional
        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)

        return DeleteStatement(table_name, where_clause)
