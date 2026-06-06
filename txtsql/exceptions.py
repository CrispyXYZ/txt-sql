class TxtSqlError(Exception):
    """Base exception for all TxtSQL errors."""
    pass


class TableAlreadyExistsError(TxtSqlError):
    """CREATE TABLE on an existing table."""
    pass


class TableNotFoundError(TxtSqlError):
    """Table does not exist."""
    pass


class ColumnNotFoundError(TxtSqlError):
    """Column does not exist in the table."""
    pass


class TypeMismatchError(TxtSqlError):
    """Type mismatch in comparison or assignment."""
    pass


class SqlSyntaxError(TxtSqlError):
    """SQL syntax is invalid."""
    pass


class EngineError(TxtSqlError):
    """Internal engine error."""
    pass
