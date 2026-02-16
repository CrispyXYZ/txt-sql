import logging

import txtsql.storage as storage
from txtsql.types import Types


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)

    if storage.get_table('users') is None:
        storage.create_table('users', {'name': Types.STRING, 'age': Types.NUMBER})
    if storage.get_table('foo') is None:
        storage.create_table('foo', {'name': Types.STRING, 'age': Types.NUMBER})
    users = storage.get_table('users')
    # users.insert_values({'name': 'John', 'age': 24})
    users.update({'age': 22}, lambda row: row['name'] == 'CAFE')
    users.delete(lambda row: row['age'] < 20)


if __name__ == '__main__':
    main()
