import logging

from txtsql.storage import create_table
from txtsql.types import Types


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    create_table('users', {'name': Types.STRING, 'age': Types.NUMBER})


if __name__ == '__main__':
    main()
