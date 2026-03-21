from typing import Any

from .exceptions import EngineError
from .executor import execute_drop
from .lexer import Lexer
from .parser import Parser, DropTable


def execute_sql(sql: str) -> Any:
    lexer = Lexer(sql)
    tokens = lexer.tokenize()
    parser = Parser(tokens)
    statement = parser.parse()

    match statement:
        case DropTable():
            execute_drop(statement)
            return None
        case _:
            raise EngineError(f'Unsupported statement type: {type(statement)}')
