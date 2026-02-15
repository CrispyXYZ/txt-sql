import csv
import logging
import os
from collections.abc import Callable

from txtsql.exceptions import TableAlreadyExistsError
from txtsql.types import Types

logger = logging.getLogger(__name__)
metadata_filename = 'metadata.txt'


def create_table(name: str, defs: dict[str, Types]) -> Table:
    logger.debug(f'Creating table: {name}')
    try:
        with open(metadata_filename, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            if any(row[0] == name for row in reader):
                raise TableAlreadyExistsError(f'Table already exists: {name}')
    except FileNotFoundError:
        pass

    with open(metadata_filename, 'a', encoding='utf-8') as metadata:
        row = [name, str(len(defs))] + [str(x) for pair in defs.items() for x in pair]
        logger.debug(f'Writing: {row}')
        metadata.write('\t'.join(row) + '\n')
        table: Table = Table(name, defs)
        table.ensure_file()
        return table


def get_table(name: str) -> Table | None:
    logger.debug(f'Getting table: {name}')
    try:
        with open(metadata_filename, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            row = next((row for row in reader if row[0] == name), None)
            if row is None:
                return None
            count = int(row[1])
            return Table(name, {row[i]: Types(row[i + 1]) for i in range(2, 2 + count * 2, 2)})
    except FileNotFoundError:
        logger.warning('metadata.txt file not found. Returning None.')
        return None


class Table:
    name: str
    filename: str
    defs: dict[str, Types]

    def __init__(self, name: str, defs: dict[str, Types]) -> None:
        self.name = name
        self.filename = f'{self.name}.txt'
        self.defs = defs

    def ensure_file(self) -> None:
        if not os.path.exists(self.filename):
            logger.debug(f'Creating file: {self.filename}')
            open(self.filename, 'w', encoding='utf-8').close()

    def insert_values(self, values: dict[str, str]) -> None:
        """ This method does not accept None in values parameter. Use empty string instead. """
        logger.debug(f'Inserting values: {values}')
        with open(self.filename, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            row = [values.get(key) for key in self.defs]
            logger.debug(f'Writing: {row}')
            writer.writerow(row)

    def update(self, values: dict[str, str], where: Callable[[dict[str, str]], bool] | None = None) -> None:
        """ This method does not accept None in values parameter. Use empty string instead.
            :param values: Dictionary of values to update
            :param where: Function which takes a row dictionary as parameter
        """
        logger.debug(f'Updating values: {values}')

        def_keys: list[str] = list(self.defs.keys())
        def_count: int = len(def_keys)
        updated_values: list[str | None] = [values.get(col) for col in def_keys] # get method returns None if not found
        logger.debug(f'Generated values: {updated_values}')

        if all(value is None for value in updated_values):
            logger.warning(f'No values to update: {values}')
            return

        table_values: list[list[str]] = []
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                if where is None or where(dict(zip(def_keys, row))):
                    new_row = [row[i] if updated_values[i] is None else updated_values[i] for i in range(def_count)]
                else:
                    new_row = row
                table_values.append(new_row)

        logger.debug(f'Updated values: {table_values}')

        with open(self.filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(table_values)
