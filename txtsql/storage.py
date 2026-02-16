import csv
import logging
import os
from collections.abc import Callable
from decimal import Decimal

from txtsql.exceptions import TableAlreadyExistsError
from txtsql.types import Types, DataValue

logger = logging.getLogger(__name__)
metadata_filename = 'metadata.txt'

type RowValue = dict[str, DataValue]

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

def _string_to_number(string: str) -> Decimal:
    return Decimal(string)

def _string_to_binary(string: str) -> bytes:
    return bytes.fromhex(string)

def _number_to_string(number: Decimal) -> str:
    return str(number)

def _binary_to_string(binary: bytes) -> str:
    return binary.hex()

def _data_to_string(value: DataValue, type_def: Types) -> str | None:
    match type_def:
        case Types.NUMBER:
            return _number_to_string(Decimal(value))
        case Types.BINARY:
            return _binary_to_string(value)
        case Types.STRING:
            return value
    return None

def _string_to_data(string: str, type_def: Types) -> str | Decimal | bytes | None:
    match type_def:
        case Types.NUMBER:
            return _string_to_number(string)
        case Types.BINARY:
            return _string_to_binary(string)
        case Types.STRING:
            return string
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

    def insert_values(self, values: RowValue) -> None:
        """ This method does not accept None in values parameter. Use empty string instead. """
        logger.debug(f'Inserting values: {values}')
        with open(self.filename, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            row = [_data_to_string(values.get(key), value) for key, value in self.defs.items()]
            logger.debug(f'Writing: {row}')
            writer.writerow(row)

    def update(self, values: RowValue, where: Callable[[RowValue], bool] | None = None) -> None:
        """
        This method does not accept None in values parameter. Use empty string instead.
        :param values: Dictionary of values to update
        :param where: Function which takes a row dictionary as parameter, returns bool
        """
        logger.debug(f'Updating values: {values}')

        def_keys: list[str] = list(self.defs.keys())
        def_types: list[Types] = list(self.defs.values())
        def_count: int = len(self.defs)
        updated_values: list[str | None] = [_data_to_string(values.get(key), value) for key, value in self.defs.items()] # get method returns None if not found
        logger.debug(f'Generated values: {updated_values}')

        if all(value is None for value in updated_values):
            logger.warning(f'No values to update: {values}')
            return

        table_values: list[list[str]] = []
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                if where is None or where({key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}):
                    new_row = [row[i] if updated_values[i] is None else updated_values[i] for i in range(def_count)]
                else:
                    new_row = row
                table_values.append(new_row)

        logger.debug(f'Updated values: {table_values}')

        with open(self.filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(table_values)

    def delete(self, where: Callable[[RowValue], bool] | None = None) -> None:
        """
        :param where: Function which takes a row dictionary as parameter, returns bool
        """
        logger.debug(f'Deleting values')

        table_values: list[list[str]] = []
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                if where({key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}):
                    logger.debug(f'Deleting value: {row}')
                else:
                    table_values.append(row)

        with open(self.filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(table_values)