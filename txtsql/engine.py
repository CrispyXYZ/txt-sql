from typing import Any

from .exceptions import EngineError
from .executor import execute_drop, execute_create, execute_insert, execute_delete
from .lexer import Lexer
from .parser import Parser, DropTable, CreateTable, InsertValues, DeleteStatement


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
        case InsertValues():
            execute_insert(statement)
            return None
        case DeleteStatement():
            return execute_delete(statement)
        case _:
            raise EngineError(f'Unsupported statement type: {type(statement)}')
