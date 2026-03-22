from typing import Any

from .exceptions import EngineError
from .executor import execute_drop, execute_create
from .lexer import Lexer
from .parser import Parser, DropTable, CreateTable


def execute_sql(sql: str) -> Any:
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    statement = parser.parse()

    match statement:
        case CreateTable():
            execute_create(statement)
            return None
        case DropTable():
            execute_drop(statement)
            return None
        case _:
            raise EngineError(f'Unsupported statement type: {type(statement)}')
