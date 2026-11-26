from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from .exceptions import SqlSyntaxError


class TokenType(StrEnum):
    CREATE = 'CREATE'
    DROP = 'DROP'
    TABLE = 'TABLE'
    INSERT = 'INSERT'
    INTO = 'INTO'
    VALUES = 'VALUES'
    TYPE_STRING = 'TYPE_STRING'
    TYPE_NUMBER = 'TYPE_NUMBER'

    SELECT = 'SELECT'
    DISTINCT = 'DISTINCT'
    STAR = 'STAR'
    UPDATE = 'UPDATE'
    SET = 'SET'
    ORDER = 'ORDER'
    GROUP = 'GROUP'
    BY = 'BY'
    HAVING = 'HAVING'
    ASC = 'ASC'
    DESC = 'DESC'
    LIMIT = 'LIMIT'
    OFFSET = 'OFFSET'
    AS = 'AS'
    COUNT = 'COUNT'
    SUM = 'SUM'
    AVG = 'AVG'
    MIN = 'MIN'
    MAX = 'MAX'

    DELETE = 'DELETE'
    FROM = 'FROM'
    IMPORT = 'IMPORT'
    OUTFILE = 'OUTFILE'
    SHOW = 'SHOW'
    TABLES = 'TABLES'
    DESCRIBE = 'DESCRIBE'
    LEFT = 'LEFT'
    RIGHT = 'RIGHT'
    INNER = 'INNER'
    FULL = 'FULL'
    OUTER = 'OUTER'
    CROSS = 'CROSS'
    NATURAL = 'NATURAL'
    JOIN = 'JOIN'
    ON = 'ON'
    USING = 'USING'
    IN = 'IN'
    BETWEEN = 'BETWEEN'
    LIKE = 'LIKE'
    EXISTS = 'EXISTS'
    IF = 'IF'
    ANY = 'ANY'
    ALL = 'ALL'
    UNION = 'UNION'
    WHERE = 'WHERE'
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'
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

    PLUS = '+'
    MINUS = '-'
    DIV = '/'
    DOT = '.'

    SEMICOLON = 'SEMICOLON'
    COMMA = 'COMMA'
    RPAREN = 'RPAREN'
    LPAREN = 'LPAREN'

    EOF = 'EOF'

    STRING = 'STRING'
    NUMBER = 'NUMBER'
    IDENTIFIER = 'IDENTIFIER'


type TokenValue = str | Decimal | None


@dataclass(frozen=True, slots=True)
class Token:
    type: TokenType
    value: TokenValue
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
        while True:
            while self.current_char() is not None and self.current_char().isspace():
                self.advance()
            if self.current_char() == '-' and self.peek() == '-':
                while self.current_char() is not None and self.current_char() != '\n':
                    self.advance()
            else:
                break

    def read_number(self) -> Token:
        start_line = self.line
        start_col = self.column
        num_str = ''
        while self.current_char() is not None and self.current_char().isdigit():
            num_str += self.current_char()
            self.advance()
        # Support decimal point (e.g. 3.14) - require at least one digit after the point
        if self.current_char() == '.' and self.peek() is not None and self.peek().isdigit():
            num_str += '.'
            self.advance()
            while self.current_char() is not None and self.current_char().isdigit():
                num_str += self.current_char()
                self.advance()
        return Token(TokenType.NUMBER, Decimal(num_str), start_line, start_col)

    def read_string(self) -> Token:
        start_line = self.line
        start_col = self.column
        self.advance()  # skip opening quote
        chars: list[str] = []
        while self.current_char() is not None:
            ch = self.current_char()
            if ch == "'":
                if self.peek() == "'":
                    chars.append("'")
                    self.advance()
                    self.advance()
                    continue
                self.advance()  # skip closing quote
                return Token(TokenType.STRING, ''.join(chars), start_line, start_col)
            chars.append(ch)
            self.advance()
        raise SqlSyntaxError(f'Unterminated string literal at line {start_line} column {start_col}')

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
            case 'SELECT':
                token_type = TokenType.SELECT
            case 'DISTINCT':
                token_type = TokenType.DISTINCT
            case 'UPDATE':
                token_type = TokenType.UPDATE
            case 'SET':
                token_type = TokenType.SET
            case 'ORDER':
                token_type = TokenType.ORDER
            case 'GROUP':
                token_type = TokenType.GROUP
            case 'BY':
                token_type = TokenType.BY
            case 'HAVING':
                token_type = TokenType.HAVING
            case 'AS':
                token_type = TokenType.AS
            case 'COUNT':
                token_type = TokenType.COUNT
            case 'SUM':
                token_type = TokenType.SUM
            case 'AVG':
                token_type = TokenType.AVG
            case 'MIN':
                token_type = TokenType.MIN
            case 'MAX':
                token_type = TokenType.MAX
            case 'ASC':
                token_type = TokenType.ASC
            case 'DESC':
                token_type = TokenType.DESC
            case 'LIMIT':
                token_type = TokenType.LIMIT
            case 'OFFSET':
                token_type = TokenType.OFFSET
            case 'DELETE':
                token_type = TokenType.DELETE
            case 'IMPORT':
                token_type = TokenType.IMPORT
            case 'OUTFILE':
                token_type = TokenType.OUTFILE
            case 'SHOW':
                token_type = TokenType.SHOW
            case 'TABLES':
                token_type = TokenType.TABLES
            case 'DESCRIBE':
                token_type = TokenType.DESCRIBE
            case 'LEFT':
                token_type = TokenType.LEFT
            case 'RIGHT':
                token_type = TokenType.RIGHT
            case 'INNER':
                token_type = TokenType.INNER
            case 'FULL':
                token_type = TokenType.FULL
            case 'OUTER':
                token_type = TokenType.OUTER
            case 'CROSS':
                token_type = TokenType.CROSS
            case 'NATURAL':
                token_type = TokenType.NATURAL
            case 'JOIN':
                token_type = TokenType.JOIN
            case 'ON':
                token_type = TokenType.ON
            case 'USING':
                token_type = TokenType.USING
            case 'IN':
                token_type = TokenType.IN
            case 'BETWEEN':
                token_type = TokenType.BETWEEN
            case 'LIKE':
                token_type = TokenType.LIKE
            case 'EXISTS':
                token_type = TokenType.EXISTS
            case 'IF':
                token_type = TokenType.IF
            case 'ANY':
                token_type = TokenType.ANY
            case 'ALL':
                token_type = TokenType.ALL
            case 'UNION':
                token_type = TokenType.UNION
            case 'FROM':
                token_type = TokenType.FROM
            case 'WHERE':
                token_type = TokenType.WHERE
            case 'AND':
                token_type = TokenType.AND
            case 'OR':
                token_type = TokenType.OR
            case 'NOT':
                token_type = TokenType.NOT
            case 'IS':
                token_type = TokenType.IS
            case 'TRUE':
                token_type = TokenType.TRUE
            case 'FALSE':
                token_type = TokenType.FALSE
            case 'NULL':
                token_type = TokenType.NULL
            case 'STRING' | 'VARCHAR':
                token_type = TokenType.TYPE_STRING
                identifier = 'STRING'
            case 'NUMBER' | 'DECIMAL' | 'INT':
                token_type = TokenType.TYPE_NUMBER
                identifier = 'NUMBER'
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
            case '*':
                self.advance()
                return Token(TokenType.STAR, '*', self.line, self.column - 1)
            case '+':
                self.advance()
                return Token(TokenType.PLUS, '+', self.line, self.column - 1)
            case '-':
                self.advance()
                return Token(TokenType.MINUS, '-', self.line, self.column - 1)
            case '/':
                self.advance()
                return Token(TokenType.DIV, '/', self.line, self.column - 1)
            case '.':
                self.advance()
                return Token(TokenType.DOT, '.', self.line, self.column - 1)
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
            case '!':
                if self.peek() == '=':
                    start_col = self.column
                    self.advance()
                    self.advance()
                    return Token(TokenType.NE, '!=', self.line, start_col)
                raise SqlSyntaxError(f'Unexpected character {ch} at line {self.line} column {self.column}')
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
