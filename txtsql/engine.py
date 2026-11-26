from .ast import (
    CreateTable, DeleteStatement, DescribeTable, DropTable, InsertValues,
    ImportStatement, SelectStatement, UnionStatement, ShowTables, UpdateStatement,
)
from .exceptions import EngineError
from .executor import (
    execute_drop, execute_create, execute_insert, execute_delete,
    execute_describe, execute_import, execute_select, execute_union,
    execute_show_tables, execute_update,
)
from .lexer import Lexer
from .parser import Parser
from .types import RowDict


def execute_sql(sql: str) -> list[RowDict] | int | None:
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    statement = parser.parse()

    match statement:
        case CreateTable():     return execute_create(statement)
        case DropTable():       execute_drop(statement); return None
        case InsertValues():    execute_insert(statement); return None
        case DeleteStatement(): return execute_delete(statement)
        case SelectStatement(): return execute_select(statement)
        case UnionStatement():  return execute_union(statement)
        case UpdateStatement(): return execute_update(statement)
        case ImportStatement(): return execute_import(statement)
        case ShowTables():      return execute_show_tables(statement)
        case DescribeTable():   return execute_describe(statement)
        case _: raise EngineError(f'Unsupported statement type: {type(statement)}')
