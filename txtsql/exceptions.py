class TxtSqlError(Exception):
    pass


class TableAlreadyExistsError(TxtSqlError):
    pass


class SqlSyntaxError(TxtSqlError):
    pass


class EngineError(TxtSqlError):
    pass
