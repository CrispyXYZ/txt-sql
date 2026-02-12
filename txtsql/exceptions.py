class TxtSqlError(Exception):
    pass


class TableAlreadyExistsError(TxtSqlError):
    pass
