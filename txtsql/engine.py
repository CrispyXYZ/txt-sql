from .ast import CreateTable, DeleteStatement, DropTable, InsertValues, ImportStatement, SelectStatement, UpdateStatement
from .exceptions import EngineError
from .executor import execute_drop, execute_create, execute_insert, execute_delete, execute_import, execute_select, execute_update
from .lexer import Lexer
from .parser import Parser
from .types import RowDict


def execute_sql(sql: str) -> list[RowDict] | int | None:
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
        case SelectStatement():
            return execute_select(statement)
        case UpdateStatement():
            return execute_update(statement)
        case ImportStatement():
            return execute_import(statement)
        case _:
            raise EngineError(f'Unsupported statement type: {type(statement)}')
