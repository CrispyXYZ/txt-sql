from .ast import (
    LogicalOp, AggFunc, ComparisonOp, LiteralValue,
    Expression, LiteralExpression, ColumnExpression,
    NullCheckExpression, ConditionExpression, LogicalExpression,
    WhereClause, AggregateColumn,
    CreateTable, DropTable, InsertValues,
    DeleteStatement, SelectStatement, UpdateStatement,
)
from .exceptions import SqlSyntaxError
from .lexer import Token, TokenType


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

    def parse(self) -> CreateTable | DropTable | InsertValues | DeleteStatement | SelectStatement | UpdateStatement:
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
            case TokenType.SELECT:
                return self.select_statement()
            case TokenType.UPDATE:
                return self.update_statement()
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
            values.append(self._parse_literal_value())

            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                values.append(self._parse_literal_value())

            self.eat(TokenType.RPAREN)
            all_values.append(values)

            # Check if there are more VALUES clauses
            if self.current_token().type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        return InsertValues(table_name, columns, all_values)

    # ------------------------------------------------------------------
    # Expression parsing
    # ------------------------------------------------------------------

    def parse_expression(self) -> Expression:
        """Parse expression (entry point)."""
        return self.parse_or()

    def parse_or(self) -> Expression:
        """Parse OR expression (AND has higher precedence)."""
        left = self.parse_and()
        while self.current_token().type == TokenType.OR:
            op = LogicalOp.OR
            self.eat(TokenType.OR)
            right = self.parse_and()
            left = LogicalExpression(left, op, right)
        return left

    def parse_and(self) -> Expression:
        """Parse AND expression."""
        left = self.parse_condition()
        while self.current_token().type == TokenType.AND:
            op = LogicalOp.AND
            self.eat(TokenType.AND)
            right = self.parse_condition()
            left = LogicalExpression(left, op, right)
        return left

    def parse_condition(self) -> Expression:
        """Parse condition (comparison, NULL check, or parenthesized expression)."""
        token = self.current_token()

        if token.type == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            expr = self.parse_expression()
            self.eat(TokenType.RPAREN)
            return expr

        if token.type == TokenType.IDENTIFIER and self.peek() == TokenType.IS:
            return self._parse_null_check()

        if token.type == TokenType.IDENTIFIER and self._is_comparison_op(self.peek()):
            return self._parse_comparison()

        raise SqlSyntaxError(f"Unexpected token: {token.type} at line {token.line}, column {token.column}")

    def _parse_null_check(self) -> Expression:
        """Parse column IS [NOT] NULL."""
        column = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.IS)

        is_not = False
        if self.current_token().type == TokenType.NOT:
            self.eat(TokenType.NOT)
            is_not = True

        self.eat(TokenType.NULL)

        return NullCheckExpression(
            ColumnExpression(column),
            is_null=not is_not,
        )

    def _parse_comparison(self) -> Expression:
        """Parse column_name op literal."""
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

        return ConditionExpression(ColumnExpression(column_name), op, literal)

    @staticmethod
    def _is_comparison_op(token_type: TokenType) -> bool:
        """Determine if token is a comparison operator."""
        return token_type in [
            TokenType.EQ, TokenType.NE, TokenType.GT, TokenType.LT,
            TokenType.GE, TokenType.LE,
        ]

    # ------------------------------------------------------------------
    # Statement parsing
    # ------------------------------------------------------------------

    def delete_statement(self) -> DeleteStatement:
        """Parse DELETE FROM table_name WHERE condition;"""
        self.eat(TokenType.DELETE)
        self.eat(TokenType.FROM)

        table_name = self.eat(TokenType.IDENTIFIER).value

        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            expression = self.parse_expression()
            where_clause = WhereClause(expression)

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)

        return DeleteStatement(table_name, where_clause)

    def select_statement(self) -> SelectStatement:
        """Parse SELECT [DISTINCT] col1, agg(col) AS alias, * FROM table
           [WHERE expr] [GROUP BY col, ...] [HAVING expr]
           [ORDER BY col [ASC|DESC], ...] [LIMIT n] [OFFSET n];"""
        self.eat(TokenType.SELECT)

        distinct = False
        if self.current_token().type == TokenType.DISTINCT:
            self.eat(TokenType.DISTINCT)
            distinct = True

        columns: list[str] | None
        aggregates: list[AggregateColumn] = []
        if self.current_token().type == TokenType.STAR:
            self.eat(TokenType.STAR)
            columns = None
        else:
            columns = []
            self._parse_select_item(columns, aggregates)
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                self._parse_select_item(columns, aggregates)

        self.eat(TokenType.FROM)
        table_name = self.eat(TokenType.IDENTIFIER).value

        # Optional WHERE
        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where_clause = WhereClause(self.parse_expression())

        # Optional GROUP BY
        group_by: list[str] | None = None
        if self.current_token().type == TokenType.GROUP:
            self.eat(TokenType.GROUP)
            self.eat(TokenType.BY)
            group_by = [self.eat(TokenType.IDENTIFIER).value]
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                group_by.append(self.eat(TokenType.IDENTIFIER).value)

        # Optional HAVING
        having: WhereClause | None = None
        if self.current_token().type == TokenType.HAVING:
            self.eat(TokenType.HAVING)
            having = WhereClause(self.parse_expression())

        # Optional ORDER BY
        order_by: list[tuple[str, bool]] | None = None
        if self.current_token().type == TokenType.ORDER:
            self.eat(TokenType.ORDER)
            self.eat(TokenType.BY)
            order_by = []
            col = self.eat(TokenType.IDENTIFIER).value
            desc = False
            if self.current_token().type == TokenType.DESC:
                self.eat(TokenType.DESC)
                desc = True
            elif self.current_token().type == TokenType.ASC:
                self.eat(TokenType.ASC)
            order_by.append((col, desc))
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                col = self.eat(TokenType.IDENTIFIER).value
                desc = False
                if self.current_token().type == TokenType.DESC:
                    self.eat(TokenType.DESC)
                    desc = True
                elif self.current_token().type == TokenType.ASC:
                    self.eat(TokenType.ASC)
                order_by.append((col, desc))

        # Optional LIMIT
        limit: int | None = None
        if self.current_token().type == TokenType.LIMIT:
            self.eat(TokenType.LIMIT)
            limit = int(self.eat(TokenType.NUMBER).value)

        # Optional OFFSET
        offset = 0
        if self.current_token().type == TokenType.OFFSET:
            self.eat(TokenType.OFFSET)
            offset = int(self.eat(TokenType.NUMBER).value)

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)

        # Validation: aggregates + plain columns require GROUP BY
        if aggregates:
            if group_by is None:
                if columns:
                    raise SqlSyntaxError(
                        'SELECT with aggregate functions and plain columns requires GROUP BY'
                    )
            else:
                non_grouped = [c for c in columns if c not in group_by]
                if non_grouped:
                    raise SqlSyntaxError(
                        f'Column "{non_grouped[0]}" must appear in GROUP BY clause '
                        f'when used with aggregate functions'
                    )

        return SelectStatement(table_name, columns, aggregates, distinct,
                               where_clause, group_by, having, order_by, limit, offset)

    _AGG_FUNC_TOKEN_MAP = {
        TokenType.COUNT: AggFunc.COUNT,
        TokenType.SUM: AggFunc.SUM,
        TokenType.AVG: AggFunc.AVG,
        TokenType.MIN: AggFunc.MIN,
        TokenType.MAX: AggFunc.MAX,
    }

    def _parse_select_item(self, columns: list[str], aggregates: list[AggregateColumn]) -> None:
        """Parse a SELECT list item: plain column or aggregate call."""
        token = self.current_token()
        if token.type in self._AGG_FUNC_TOKEN_MAP:
            func = self._AGG_FUNC_TOKEN_MAP[token.type]
            self.pos += 1  # consume function name token
            self.eat(TokenType.LPAREN)
            if func == AggFunc.COUNT and self.current_token().type == TokenType.STAR:
                self.eat(TokenType.STAR)
                col: str | None = None
            else:
                col = self.eat(TokenType.IDENTIFIER).value
            self.eat(TokenType.RPAREN)
            self.eat(TokenType.AS)
            alias = self.eat(TokenType.IDENTIFIER).value
            aggregates.append(AggregateColumn(func, col, alias))
        else:
            columns.append(self.eat(TokenType.IDENTIFIER).value)

    def _parse_literal_value(self) -> LiteralValue:
        """Parse a literal value (STRING, NUMBER, BINARY, NULL, TRUE, FALSE)."""
        val_token = self.current_token()
        if val_token.type in (TokenType.STRING, TokenType.NUMBER, TokenType.BINARY):
            self.pos += 1
            return val_token.value
        elif val_token.type == TokenType.NULL:
            self.eat(TokenType.NULL)
            return None
        elif val_token.type == TokenType.TRUE:
            self.eat(TokenType.TRUE)
            return True
        elif val_token.type == TokenType.FALSE:
            self.eat(TokenType.FALSE)
            return False
        else:
            raise SqlSyntaxError(f'Expected value but got {val_token.type}')

    def update_statement(self) -> UpdateStatement:
        """Parse UPDATE table SET col1=val1, col2=val2 [WHERE expr];"""
        self.eat(TokenType.UPDATE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.SET)

        set_clauses: list[tuple[str, LiteralValue]] = []
        col = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.EQ)
        set_clauses.append((col, self._parse_literal_value()))

        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            col = self.eat(TokenType.IDENTIFIER).value
            self.eat(TokenType.EQ)
            set_clauses.append((col, self._parse_literal_value()))

        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where_clause = WhereClause(self.parse_expression())

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)

        return UpdateStatement(table_name, set_clauses, where_clause)
