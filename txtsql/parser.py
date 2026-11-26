from decimal import Decimal

from .ast import (
    LogicalOp, AggFunc, ComparisonOp, ArithmeticOp, JoinType, JoinClause, LiteralValue,
    Expression, LiteralExpression, ColumnExpression, BinaryExpression,
    NullCheckExpression, ConditionExpression, LogicalExpression, NotExpression,
    AggregateExpression, InExpression, InListExpression, AnyAllExpression,
    ExistsExpression, BetweenExpression, LikeExpression, ScalarSubqueryExpression,
    StarExpression,
    WhereClause, SelectExpression, TableSource,
    CreateTable, DropTable, InsertValues, InsertSelect,
    DeleteStatement, SelectStatement, UnionStatement, UpdateStatement, ImportStatement,
    ShowTables, ShowIndexes, DescribeTable, CreateIndex, DropIndex, ExplainStatement,
    CheckTable, BeginTransaction, CommitTransaction, RollbackTransaction,
    contains_aggregate, collect_non_agg_columns,
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

    def peek(self, offset: int = 1) -> TokenType:
        if self.pos + offset < len(self.tokens):
            return self.tokens[self.pos + offset].type
        return TokenType.EOF

    # ------------------------------------------------------------------
    # Top-level dispatch
    # ------------------------------------------------------------------

    def parse(self) -> CreateTable | DropTable | CreateIndex | DropIndex | InsertValues | InsertSelect | DeleteStatement | SelectStatement | UnionStatement | UpdateStatement | ImportStatement | ShowTables | ShowIndexes | DescribeTable | ExplainStatement | CheckTable | BeginTransaction | CommitTransaction | RollbackTransaction:
        token = self.current_token()
        match token.type:
            case TokenType.CREATE:   stmt = self.create_statement()
            case TokenType.DROP:     stmt = self.drop_statement()
            case TokenType.INSERT:   stmt = self.insert_values()
            case TokenType.DELETE:   stmt = self.delete_statement()
            case TokenType.SELECT:   stmt = self.select_or_union(allow_outfile=True)
            case TokenType.UPDATE:   stmt = self.update_statement()
            case TokenType.IMPORT:   stmt = self.import_statement()
            case TokenType.SHOW:     stmt = self.show_statement()
            case TokenType.DESCRIBE: stmt = self.describe()
            case TokenType.EXPLAIN:  stmt = self.explain_statement()
            case TokenType.CHECK:    stmt = self.check_table_statement()
            case TokenType.BEGIN:    stmt = self.begin_transaction()
            case TokenType.COMMIT:   stmt = self.commit_transaction()
            case TokenType.ROLLBACK: stmt = self.rollback_transaction()
            case _: raise SqlSyntaxError(f'Unexpected statement: {token.type}')

        if self.current_token().type == TokenType.SEMICOLON:
            self.eat(TokenType.SEMICOLON)
        if self.current_token().type != TokenType.EOF:
            extra = self.current_token()
            raise SqlSyntaxError(
                f'Unexpected token {extra.type} at line {extra.line}, column {extra.column}')
        return stmt

    # ------------------------------------------------------------------
    # Expression parsing (OR < AND < NOT < compare/IS/IN/BETWEEN/LIKE < +/- < */ < term)
    # ------------------------------------------------------------------

    def parse_expression(self) -> Expression:
        return self.parse_or()

    def parse_or(self) -> Expression:
        left = self.parse_and()
        while self.current_token().type == TokenType.OR:
            self.eat(TokenType.OR)
            left = LogicalExpression(left, LogicalOp.OR, self.parse_and())
        return left

    def parse_and(self) -> Expression:
        left = self.parse_not()
        while self.current_token().type == TokenType.AND:
            self.eat(TokenType.AND)
            left = LogicalExpression(left, LogicalOp.AND, self.parse_not())
        return left

    def parse_not(self) -> Expression:
        if self.current_token().type == TokenType.NOT:
            if self.peek() == TokenType.EXISTS:
                self.eat(TokenType.NOT)
                self.eat(TokenType.EXISTS)
                return ExistsExpression(self._parse_subquery(), negated=True)
            self.eat(TokenType.NOT)
            return NotExpression(self.parse_not())
        return self.parse_comparison()

    def parse_comparison(self) -> Expression:
        left = self.parse_add()
        token_type = self.current_token().type

        # expr IS [NOT] NULL
        if token_type == TokenType.IS:
            self.eat(TokenType.IS)
            is_null = True
            if self.current_token().type == TokenType.NOT:
                self.eat(TokenType.NOT)
                is_null = False
            self.eat(TokenType.NULL)
            return NullCheckExpression(left, is_null)

        # [NOT] IN (subquery | item1, item2, ...)
        if token_type == TokenType.IN or (token_type == TokenType.NOT and self.peek() == TokenType.IN):
            not_in = False
            if token_type == TokenType.NOT:
                self.eat(TokenType.NOT)
                not_in = True
            self.eat(TokenType.IN)
            self.eat(TokenType.LPAREN)
            if self.current_token().type == TokenType.SELECT:
                sub = self.select_or_union(allow_outfile=False)
                self.eat(TokenType.RPAREN)
                return InExpression(left, sub, not_in)
            items = [self.parse_expression()]
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                items.append(self.parse_expression())
            self.eat(TokenType.RPAREN)
            return InListExpression(left, items, not_in)

        # [NOT] BETWEEN low AND high
        if token_type == TokenType.BETWEEN or (token_type == TokenType.NOT and self.peek() == TokenType.BETWEEN):
            negated = False
            if token_type == TokenType.NOT:
                self.eat(TokenType.NOT)
                negated = True
            self.eat(TokenType.BETWEEN)
            low = self.parse_add()
            self.eat(TokenType.AND)
            high = self.parse_add()
            return BetweenExpression(left, low, high, negated)

        # [NOT] LIKE pattern
        if token_type == TokenType.LIKE or (token_type == TokenType.NOT and self.peek() == TokenType.LIKE):
            negated = False
            if token_type == TokenType.NOT:
                self.eat(TokenType.NOT)
                negated = True
            self.eat(TokenType.LIKE)
            pattern = self.parse_add()
            return LikeExpression(left, pattern, negated)

        # Comparison op
        if self._is_comparison_op(token_type):
            op_token = self.current_token()
            match op_token.type:
                case TokenType.EQ:  op = ComparisonOp.EQ
                case TokenType.NE:  op = ComparisonOp.NE
                case TokenType.GT:  op = ComparisonOp.GT
                case TokenType.LT:  op = ComparisonOp.LT
                case TokenType.GE:  op = ComparisonOp.GE
                case TokenType.LE:  op = ComparisonOp.LE
                case _: raise SqlSyntaxError(f"Invalid comparison operator: {op_token.type}")
            self.pos += 1
            # Check for ANY/ALL quantifier
            if self.current_token().type in (TokenType.ANY, TokenType.ALL):
                quant = self.current_token().value.upper()
                self.pos += 1
                sub = self._parse_subquery()
                return AnyAllExpression(left, op, quant, sub)
            right = self.parse_add()
            return ConditionExpression(left, op, right)
        return left

    def parse_add(self) -> Expression:
        left = self.parse_mul()
        while self.current_token().type in (TokenType.PLUS, TokenType.MINUS):
            op = ArithmeticOp.ADD if self.current_token().type == TokenType.PLUS else ArithmeticOp.SUB
            self.pos += 1
            left = BinaryExpression(left, op, self.parse_mul())
        return left

    def parse_mul(self) -> Expression:
        left = self.parse_term()
        while self.current_token().type in (TokenType.STAR, TokenType.DIV):
            op = ArithmeticOp.MUL if self.current_token().type == TokenType.STAR else ArithmeticOp.DIV
            self.pos += 1
            left = BinaryExpression(left, op, self.parse_term())
        return left

    def parse_term(self) -> Expression:
        token = self.current_token()

        # Parenthesized expression or scalar subquery
        if token.type == TokenType.LPAREN:
            if self.peek() == TokenType.SELECT:
                return ScalarSubqueryExpression(self._parse_subquery())
            self.eat(TokenType.LPAREN)
            expr = self.parse_expression()
            self.eat(TokenType.RPAREN)
            return expr

        # EXISTS (subquery)
        if token.type == TokenType.EXISTS:
            self.eat(TokenType.EXISTS)
            return ExistsExpression(self._parse_subquery(), negated=False)

        # Arithmetic unary: -expr or +expr
        if token.type == TokenType.MINUS:
            self.eat(TokenType.MINUS)
            return BinaryExpression(LiteralExpression(Decimal('0')), ArithmeticOp.SUB, self.parse_term())
        if token.type == TokenType.PLUS:
            self.eat(TokenType.PLUS)
            return self.parse_term()

        primaries = (
            TokenType.IDENTIFIER, TokenType.NUMBER, TokenType.STRING,
            TokenType.NULL, TokenType.TRUE, TokenType.FALSE,
            TokenType.COUNT, TokenType.SUM, TokenType.AVG,
            TokenType.MIN, TokenType.MAX,
        )
        if token.type in primaries:
            return self._parse_primary()

        raise SqlSyntaxError(f"Unexpected token: {token.type} at line {token.line}, column {token.column}")

    def _parse_subquery(self) -> SelectStatement | UnionStatement:
        """Parse (SELECT ...) as a subquery."""
        self.eat(TokenType.LPAREN)
        sub = self.select_or_union(allow_outfile=False)
        self.eat(TokenType.RPAREN)
        return sub

    def _parse_primary(self) -> Expression:
        """Parse literal, aggregate call, or column reference."""
        token = self.current_token()

        if token.type == TokenType.NUMBER:
            self.pos += 1
            return LiteralExpression(Decimal(token.value))
        if token.type == TokenType.STRING:
            self.pos += 1
            return LiteralExpression(str(token.value))
        if token.type == TokenType.NULL:
            self.eat(TokenType.NULL)
            return LiteralExpression(None)
        if token.type == TokenType.TRUE:
            self.eat(TokenType.TRUE)
            return LiteralExpression(Decimal('1'))
        if token.type == TokenType.FALSE:
            self.eat(TokenType.FALSE)
            return LiteralExpression(Decimal('0'))

        if token.type in (TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MIN, TokenType.MAX):
            return self._parse_aggregate_call()

        if token.type == TokenType.IDENTIFIER:
            return self._parse_column_ref()

        raise SqlSyntaxError(f"Unexpected token in expression: {token.type}")

    def _parse_column_ref(self) -> ColumnExpression:
        """Parse col or table.col."""
        ident = self.eat(TokenType.IDENTIFIER).value
        if self.current_token().type == TokenType.DOT:
            self.eat(TokenType.DOT)
            col = self.eat(TokenType.IDENTIFIER).value
            return ColumnExpression(col, table=ident)
        return ColumnExpression(ident)

    _AGG_FUNC_TOKEN_MAP = {
        'COUNT': AggFunc.COUNT, 'SUM': AggFunc.SUM, 'AVG': AggFunc.AVG,
        'MIN': AggFunc.MIN, 'MAX': AggFunc.MAX,
    }

    def _parse_aggregate_call(self) -> AggregateExpression:
        """Parse COUNT(*), COUNT(DISTINCT col), SUM(col), etc."""
        func_token = self.current_token()
        self.pos += 1
        func_name = func_token.value.upper()
        func = _AGG_FUNC_TOKEN_MAP[func_name]
        self.eat(TokenType.LPAREN)
        distinct = False
        if self.current_token().type == TokenType.DISTINCT:
            self.eat(TokenType.DISTINCT)
            distinct = True
        if func == AggFunc.COUNT and self.current_token().type == TokenType.STAR:
            if distinct:
                raise SqlSyntaxError('COUNT(DISTINCT *) is not supported')
            self.eat(TokenType.STAR)
            col_expr: ColumnExpression | None = None
        else:
            col_expr = self._parse_column_ref()
        self.eat(TokenType.RPAREN)
        return AggregateExpression(func, col_expr, distinct=distinct)

    @staticmethod
    def _is_comparison_op(token_type: TokenType) -> bool:
        return token_type in (TokenType.EQ, TokenType.NE, TokenType.GT, TokenType.LT, TokenType.GE, TokenType.LE)

    # ------------------------------------------------------------------
    # Literal & validation helpers
    # ------------------------------------------------------------------

    def _parse_literal_value(self) -> LiteralValue:
        val_token = self.current_token()
        if val_token.type == TokenType.MINUS:
            self.eat(TokenType.MINUS)
            num_tok = self.eat(TokenType.NUMBER)
            return -Decimal(num_tok.value)
        if val_token.type == TokenType.PLUS:
            self.eat(TokenType.PLUS)
            num_tok = self.eat(TokenType.NUMBER)
            return Decimal(num_tok.value)
        if val_token.type == TokenType.NUMBER:
            self.pos += 1
            return Decimal(val_token.value)
        if val_token.type == TokenType.STRING:
            self.pos += 1
            return str(val_token.value)
        if val_token.type == TokenType.NULL:
            self.eat(TokenType.NULL)
            return None
        if val_token.type == TokenType.TRUE:
            self.eat(TokenType.TRUE)
            return Decimal('1')
        if val_token.type == TokenType.FALSE:
            self.eat(TokenType.FALSE)
            return Decimal('0')
        raise SqlSyntaxError(f'Expected value but got {val_token.type}')

    def _parse_type(self) -> Token:
        token = self.current_token()
        if token.type not in (TokenType.TYPE_STRING, TokenType.TYPE_NUMBER):
            raise SqlSyntaxError(f'Expected type STRING or NUMBER, but got {token.type}')
        self.pos += 1
        return token

    def _parse_non_negative_int(self, clause_name: str) -> int:
        token = self.eat(TokenType.NUMBER)
        val = token.value
        if not isinstance(val, Decimal) or val != int(val) or val < 0:
            raise SqlSyntaxError(f'{clause_name} requires a non-negative integer, got {val}')
        return int(val)

    @staticmethod
    def _ensure_unique_names(names: list[str], context: str) -> None:
        seen: set[str] = set()
        for name in names:
            if name in seen:
                raise SqlSyntaxError(f'Duplicate column name "{name}" in {context}')
            seen.add(name)

    # ------------------------------------------------------------------
    # DDL / DML parsing
    # ------------------------------------------------------------------

    def create_statement(self) -> CreateTable | CreateIndex:
        if self.peek() == TokenType.INDEX:
            return self.create_index()
        return self.create_table()

    def create_index(self) -> CreateIndex:
        self.eat(TokenType.CREATE)
        self.eat(TokenType.INDEX)
        if_not_exists = False
        if self.current_token().type == TokenType.IF:
            self.eat(TokenType.IF)
            self.eat(TokenType.NOT)
            self.eat(TokenType.EXISTS)
            if_not_exists = True
        index_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.ON)
        table_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.LPAREN)
        column_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.RPAREN)
        return CreateIndex(index_name, table_name, column_name, if_not_exists=if_not_exists)

    def create_table(self) -> CreateTable:
        self.eat(TokenType.CREATE)
        self.eat(TokenType.TABLE)
        if_not_exists = False
        if self.current_token().type == TokenType.IF:
            self.eat(TokenType.IF)
            self.eat(TokenType.NOT)
            self.eat(TokenType.EXISTS)
            if_not_exists = True
        table_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.LPAREN)
        columns = []
        col_name = self.eat(TokenType.IDENTIFIER).value
        col_type = self._parse_type().value
        columns.append((col_name, col_type))
        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            col_name = self.eat(TokenType.IDENTIFIER).value
            col_type = self._parse_type().value
            columns.append((col_name, col_type))
        self.eat(TokenType.RPAREN)
        self._ensure_unique_names([name for name, _ in columns], 'CREATE TABLE')
        return CreateTable(table_name, columns, if_not_exists=if_not_exists)

    def drop_statement(self) -> DropTable | DropIndex:
        if self.peek() == TokenType.INDEX:
            return self.drop_index()
        return self.drop_table()

    def drop_index(self) -> DropIndex:
        self.eat(TokenType.DROP)
        self.eat(TokenType.INDEX)
        if_exists = False
        if self.current_token().type == TokenType.IF:
            self.eat(TokenType.IF)
            self.eat(TokenType.EXISTS)
            if_exists = True
        index_name = self.eat(TokenType.IDENTIFIER).value
        return DropIndex(index_name, if_exists=if_exists)

    def drop_table(self) -> DropTable:
        self.eat(TokenType.DROP)
        self.eat(TokenType.TABLE)
        if_exists = False
        if self.current_token().type == TokenType.IF:
            self.eat(TokenType.IF)
            self.eat(TokenType.EXISTS)
            if_exists = True
        table_name = self.eat(TokenType.IDENTIFIER).value
        return DropTable(table_name, if_exists=if_exists)

    def insert_values(self) -> InsertValues | InsertSelect:
        self.eat(TokenType.INSERT)
        self.eat(TokenType.INTO)
        table_name = self.eat(TokenType.IDENTIFIER).value
        columns = None
        if self.current_token().type == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            columns = [self.eat(TokenType.IDENTIFIER).value]
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                columns.append(self.eat(TokenType.IDENTIFIER).value)
            self.eat(TokenType.RPAREN)
            self._ensure_unique_names(columns, 'INSERT')
        if self.current_token().type == TokenType.SELECT:
            query = self.select_or_union(allow_outfile=False)
            return InsertSelect(table_name, columns, query)
        self.eat(TokenType.VALUES)
        all_values = []
        while True:
            self.eat(TokenType.LPAREN)
            values = [self._parse_literal_value()]
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                values.append(self._parse_literal_value())
            self.eat(TokenType.RPAREN)
            all_values.append(values)
            if self.current_token().type != TokenType.COMMA:
                break
            self.eat(TokenType.COMMA)
        return InsertValues(table_name, columns, all_values)

    # ------------------------------------------------------------------
    # SELECT / UNION
    # ------------------------------------------------------------------

    def select_statement(self) -> SelectStatement:
        """Parse a single SELECT statement including trailing clauses."""
        core = self._parse_select_core()
        order_by, limit, offset, output_file = self._parse_select_tail(allow_outfile=True)
        return SelectStatement(
            sources=core.sources,
            joins=core.joins,
            columns=core.columns,
            aggregates=core.aggregates,
            distinct=core.distinct,
            where_clause=core.where_clause,
            group_by=core.group_by,
            having=core.having,
            order_by=order_by,
            limit=limit,
            offset=offset,
            output_file=output_file,
        )

    def select_or_union(self, allow_outfile: bool = False) -> SelectStatement | UnionStatement:
        first = self._parse_select_core()
        if self.current_token().type != TokenType.UNION:
            order_by, limit, offset, output_file = self._parse_select_tail(allow_outfile=allow_outfile)
            return SelectStatement(
                sources=first.sources,
                joins=first.joins,
                columns=first.columns,
                aggregates=first.aggregates,
                distinct=first.distinct,
                where_clause=first.where_clause,
                group_by=first.group_by,
                having=first.having,
                order_by=order_by,
                limit=limit,
                offset=offset,
                output_file=output_file,
            )

        branches: list[tuple[SelectStatement, bool]] = [(first, False)]
        while self.current_token().type == TokenType.UNION:
            self.eat(TokenType.UNION)
            is_all = False
            if self.current_token().type == TokenType.ALL:
                self.eat(TokenType.ALL)
                is_all = True
            next_core = self._parse_select_core()
            branches.append((next_core, is_all))

        order_by, limit, offset, output_file = self._parse_select_tail(allow_outfile=allow_outfile)
        return UnionStatement(
            branches=branches,
            order_by=order_by,
            limit=limit,
            offset=offset,
            output_file=output_file,
        )

    def _parse_select_core(self) -> SelectStatement:
        self.eat(TokenType.SELECT)

        distinct = False
        if self.current_token().type == TokenType.DISTINCT:
            self.eat(TokenType.DISTINCT)
            distinct = True

        # SELECT list
        select_exprs: list[SelectExpression] | None = []
        self._parse_select_expression(select_exprs)
        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            self._parse_select_expression(select_exprs)
        if len(select_exprs) == 1 and isinstance(select_exprs[0].expr, StarExpression) and select_exprs[0].expr.table is None:
            select_exprs = None

        # FROM clause
        self.eat(TokenType.FROM)
        sources = [self._parse_table_source()]
        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            sources.append(self._parse_table_source())

        # JOIN clauses
        joins: list[JoinClause] = []
        _JOIN_TOKENS = {
            TokenType.INNER, TokenType.LEFT, TokenType.RIGHT, TokenType.FULL,
            TokenType.CROSS, TokenType.NATURAL, TokenType.JOIN,
        }
        while self.current_token().type in _JOIN_TOKENS:
            natural = False
            join_type = JoinType.INNER

            if self.current_token().type == TokenType.NATURAL:
                self.eat(TokenType.NATURAL)
                natural = True

            if self.current_token().type == TokenType.CROSS:
                self.eat(TokenType.CROSS)
                join_type = JoinType.CROSS
            elif self.current_token().type == TokenType.INNER:
                self.eat(TokenType.INNER)
            elif self.current_token().type == TokenType.LEFT:
                self.eat(TokenType.LEFT)
                join_type = JoinType.LEFT
                if self.current_token().type == TokenType.OUTER:
                    self.eat(TokenType.OUTER)
            elif self.current_token().type == TokenType.RIGHT:
                self.eat(TokenType.RIGHT)
                join_type = JoinType.RIGHT
                if self.current_token().type == TokenType.OUTER:
                    self.eat(TokenType.OUTER)
            elif self.current_token().type == TokenType.FULL:
                self.eat(TokenType.FULL)
                join_type = JoinType.FULL
                if self.current_token().type == TokenType.OUTER:
                    self.eat(TokenType.OUTER)

            if natural and join_type == JoinType.CROSS:
                raise SqlSyntaxError('NATURAL cannot be combined with CROSS JOIN')

            self.eat(TokenType.JOIN)
            table = self._parse_table_source()

            if join_type == JoinType.CROSS:
                if self.current_token().type in (TokenType.ON, TokenType.USING):
                    raise SqlSyntaxError('CROSS JOIN cannot have ON or USING')
                joins.append(JoinClause(join_type, table))
            elif natural:
                if self.current_token().type in (TokenType.ON, TokenType.USING):
                    raise SqlSyntaxError('NATURAL JOIN cannot have ON or USING')
                joins.append(JoinClause(join_type, table, natural=True))
            elif self.current_token().type == TokenType.ON:
                self.eat(TokenType.ON)
                condition = self.parse_expression()
                joins.append(JoinClause(join_type, table, condition=condition))
            elif self.current_token().type == TokenType.USING:
                self.eat(TokenType.USING)
                self.eat(TokenType.LPAREN)
                using_columns = [self.eat(TokenType.IDENTIFIER).value]
                while self.current_token().type == TokenType.COMMA:
                    self.eat(TokenType.COMMA)
                    using_columns.append(self.eat(TokenType.IDENTIFIER).value)
                self.eat(TokenType.RPAREN)
                joins.append(JoinClause(join_type, table, using_columns=tuple(using_columns)))
            else:
                raise SqlSyntaxError(
                    f'Expected ON or USING after JOIN at line {self.current_token().line}, '
                    f'column {self.current_token().column}')

        # Optional WHERE
        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where_expr = self.parse_expression()
            if contains_aggregate(where_expr):
                raise SqlSyntaxError('Aggregate functions are not allowed in WHERE clause')
            where_clause = WhereClause(where_expr)

        # Optional GROUP BY
        group_by: list[ColumnExpression] | None = None
        if self.current_token().type == TokenType.GROUP:
            self.eat(TokenType.GROUP)
            self.eat(TokenType.BY)
            group_by = [self._parse_column_ref()]
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                group_by.append(self._parse_column_ref())

        # Optional HAVING
        having = None
        if self.current_token().type == TokenType.HAVING:
            self.eat(TokenType.HAVING)
            having = WhereClause(self.parse_expression())

        # Semantic validation for GROUP BY / Aggregates
        if select_exprs is None and group_by is not None:
            raise SqlSyntaxError('SELECT * cannot be used with GROUP BY')

        has_agg_in_select = any(contains_aggregate(se.expr) for se in (select_exprs or []))
        has_wildcard = select_exprs is not None and any(isinstance(se.expr, StarExpression) for se in select_exprs)
        if has_wildcard and (group_by is not None or has_agg_in_select):
            raise SqlSyntaxError('Wildcard (*) cannot be used with GROUP BY or aggregate functions')
        if select_exprs is not None:
            non_agg_cols = [
                col
                for se in select_exprs
                for col in collect_non_agg_columns(se.expr)
            ]
            if has_agg_in_select and group_by is None:
                if non_agg_cols:
                    raise SqlSyntaxError('SELECT with aggregate functions and plain columns requires GROUP BY')
            elif group_by is not None:
                for col in non_agg_cols:
                    if not any(_col_matches_group_by(col, gb) for gb in group_by):
                        raise SqlSyntaxError(
                            f'Column "{col.column_name}" must appear in GROUP BY clause')

        return SelectStatement(
            sources=sources,
            joins=joins,
            columns=select_exprs,
            aggregates=[],
            distinct=distinct,
            where_clause=where_clause,
            group_by=group_by,
            having=having,
            order_by=None,
            limit=None,
            offset=0,
            output_file=None,
        )

    def _parse_select_tail(
        self,
        allow_outfile: bool,
    ) -> tuple[list[tuple[ColumnExpression, bool]] | None, int | None, int, str | None]:
        order_by: list[tuple[ColumnExpression, bool]] | None = None
        if self.current_token().type == TokenType.ORDER:
            self.eat(TokenType.ORDER)
            self.eat(TokenType.BY)
            order_by = []
            col = self._parse_column_ref()
            desc = False
            if self.current_token().type in (TokenType.DESC, TokenType.ASC):
                desc = self.current_token().type == TokenType.DESC
                self.pos += 1
            order_by.append((col, desc))
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                col = self._parse_column_ref()
                desc = False
                if self.current_token().type in (TokenType.DESC, TokenType.ASC):
                    desc = self.current_token().type == TokenType.DESC
                    self.pos += 1
                order_by.append((col, desc))

        limit: int | None = None
        if self.current_token().type == TokenType.LIMIT:
            self.eat(TokenType.LIMIT)
            limit = self._parse_non_negative_int('LIMIT')

        offset = 0
        if self.current_token().type == TokenType.OFFSET:
            self.eat(TokenType.OFFSET)
            offset = self._parse_non_negative_int('OFFSET')

        output_file: str | None = None
        if allow_outfile and self.current_token().type == TokenType.INTO:
            self.eat(TokenType.INTO)
            self.eat(TokenType.OUTFILE)
            output_file = self.eat(TokenType.STRING).value

        return order_by, limit, offset, output_file

    def _parse_table_source(self) -> TableSource:
        name = self.eat(TokenType.IDENTIFIER).value
        alias = None
        if self.current_token().type == TokenType.AS:
            self.eat(TokenType.AS)
            alias = self.eat(TokenType.IDENTIFIER).value
        elif self.current_token().type == TokenType.IDENTIFIER:
            # t1 x — alias without AS keyword
            alias = self.eat(TokenType.IDENTIFIER).value
        return TableSource(name, alias)

    def _parse_select_expression(self, select_exprs: list[SelectExpression]) -> None:
        """Parse one SELECT list item: *, table.*, or expression [AS alias]."""
        if self.current_token().type == TokenType.STAR:
            self.eat(TokenType.STAR)
            select_exprs.append(SelectExpression(StarExpression(None), None))
            return
        if (
            self.current_token().type == TokenType.IDENTIFIER
            and self.peek(1) == TokenType.DOT
            and self.peek(2) == TokenType.STAR
        ):
            table = self.eat(TokenType.IDENTIFIER).value
            self.eat(TokenType.DOT)
            self.eat(TokenType.STAR)
            select_exprs.append(SelectExpression(StarExpression(table), None))
            return
        expr = self.parse_expression()
        alias = None
        if self.current_token().type == TokenType.AS:
            self.eat(TokenType.AS)
            alias = self.eat(TokenType.IDENTIFIER).value
        select_exprs.append(SelectExpression(expr, alias))

    # ------------------------------------------------------------------
    # Other statements
    # ------------------------------------------------------------------

    def delete_statement(self) -> DeleteStatement:
        self.eat(TokenType.DELETE)
        self.eat(TokenType.FROM)
        table_name = self.eat(TokenType.IDENTIFIER).value
        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where_expr = self.parse_expression()
            if contains_aggregate(where_expr):
                raise SqlSyntaxError('Aggregate functions are not allowed in WHERE clause')
            where_clause = WhereClause(where_expr)
        return DeleteStatement(table_name, where_clause)

    def update_statement(self) -> UpdateStatement:
        self.eat(TokenType.UPDATE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.SET)
        set_clauses: list[tuple[str, Expression]] = []
        col = self.eat(TokenType.IDENTIFIER).value
        self.eat(TokenType.EQ)
        val_expr = self.parse_expression()
        if contains_aggregate(val_expr):
            raise SqlSyntaxError('Aggregate functions are not allowed in UPDATE SET clause')
        set_clauses.append((col, val_expr))
        while self.current_token().type == TokenType.COMMA:
            self.eat(TokenType.COMMA)
            col = self.eat(TokenType.IDENTIFIER).value
            self.eat(TokenType.EQ)
            val_expr = self.parse_expression()
            if contains_aggregate(val_expr):
                raise SqlSyntaxError('Aggregate functions are not allowed in UPDATE SET clause')
            set_clauses.append((col, val_expr))
        where_clause = None
        if self.current_token().type == TokenType.WHERE:
            self.eat(TokenType.WHERE)
            where_expr = self.parse_expression()
            if contains_aggregate(where_expr):
                raise SqlSyntaxError('Aggregate functions are not allowed in WHERE clause')
            where_clause = WhereClause(where_expr)
        return UpdateStatement(table_name, set_clauses, where_clause)

    def import_statement(self) -> ImportStatement:
        self.eat(TokenType.IMPORT)
        table_name = self.eat(TokenType.IDENTIFIER).value
        columns = None
        if self.current_token().type == TokenType.LPAREN:
            self.eat(TokenType.LPAREN)
            columns = []
            col_name = self.eat(TokenType.IDENTIFIER).value
            col_type = self._parse_type().value
            columns.append((col_name, col_type))
            while self.current_token().type == TokenType.COMMA:
                self.eat(TokenType.COMMA)
                col_name = self.eat(TokenType.IDENTIFIER).value
                col_type = self._parse_type().value
                columns.append((col_name, col_type))
            self.eat(TokenType.RPAREN)
            self._ensure_unique_names([name for name, _ in columns], 'IMPORT')
        self.eat(TokenType.FROM)
        file_path = self.eat(TokenType.STRING).value
        return ImportStatement(table_name, file_path, columns)

    def show_statement(self) -> ShowTables | ShowIndexes:
        if self.peek() == TokenType.INDEXES:
            return self.show_indexes()
        return self.show_tables()

    def show_indexes(self) -> ShowIndexes:
        self.eat(TokenType.SHOW)
        self.eat(TokenType.INDEXES)
        table_name = None
        if self.current_token().type == TokenType.FROM:
            self.eat(TokenType.FROM)
            table_name = self.eat(TokenType.IDENTIFIER).value
        return ShowIndexes(table_name)

    def show_tables(self) -> ShowTables:
        self.eat(TokenType.SHOW)
        self.eat(TokenType.TABLES)
        return ShowTables()

    def describe(self) -> DescribeTable:
        self.eat(TokenType.DESCRIBE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        return DescribeTable(table_name)

    def explain_statement(self) -> ExplainStatement:
        self.eat(TokenType.EXPLAIN)
        analyze = False
        if self.current_token().type == TokenType.ANALYZE:
            self.eat(TokenType.ANALYZE)
            analyze = True
        if self.current_token().type != TokenType.SELECT:
            raise SqlSyntaxError('EXPLAIN only supports SELECT and UNION statements')
        query = self.select_or_union(allow_outfile=False)
        return ExplainStatement(query=query, analyze=analyze)

    def check_table_statement(self) -> CheckTable:
        self.eat(TokenType.CHECK)
        self.eat(TokenType.TABLE)
        table_name = self.eat(TokenType.IDENTIFIER).value
        return CheckTable(table_name=table_name)

    def begin_transaction(self) -> BeginTransaction:
        self.eat(TokenType.BEGIN)
        if self.current_token().type == TokenType.TRANSACTION:
            self.eat(TokenType.TRANSACTION)
        return BeginTransaction()

    def commit_transaction(self) -> CommitTransaction:
        self.eat(TokenType.COMMIT)
        if self.current_token().type == TokenType.TRANSACTION:
            self.eat(TokenType.TRANSACTION)
        return CommitTransaction()

    def rollback_transaction(self) -> RollbackTransaction:
        self.eat(TokenType.ROLLBACK)
        if self.current_token().type == TokenType.TRANSACTION:
            self.eat(TokenType.TRANSACTION)
        return RollbackTransaction()


# ----------------------------------------------------------------------
# Semantic validation helpers
# ----------------------------------------------------------------------

_AGG_FUNC_TOKEN_MAP = Parser._AGG_FUNC_TOKEN_MAP


def _col_matches_group_by(col: ColumnExpression, gb: ColumnExpression) -> bool:
    if col.column_name != gb.column_name:
        return False
    if col.table is not None and gb.table is not None:
        return col.table == gb.table
    return True
