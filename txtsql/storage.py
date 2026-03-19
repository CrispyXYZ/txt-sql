import csv
import logging
import os
from collections.abc import Callable
from decimal import Decimal

from txtsql.exceptions import TableAlreadyExistsError
from txtsql.types import Types, DataValue

_log = logging.getLogger(__name__)
metadata_filename = 'metadata.txt'

type RowDict = dict[str, DataValue]

def create_table(name: str, defs: dict[str, Types]) -> Table:
    """ Create Table and return it. """
    _log.debug(f'Creating table: {name}')

    # Check if metadata file exists and whether the table name is already taken
    try:
        with open(metadata_filename, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            # If any row has the same table name, raise an exception
            if any(row[0] == name for row in reader):
                raise TableAlreadyExistsError(f'Table already exists: {name}')
    except FileNotFoundError:
        pass

    # Process metadata file
    with open(metadata_filename, 'a', encoding='utf-8') as metadata:
        # [table_name, column_count, col1_name, col1_type, col2_name, col2_type]
        row = [name, str(len(defs))] + [str(x) for pair in defs.items() for x in pair]
        _log.debug(f'Writing: {row}')
        metadata.write('\t'.join(row) + '\n')
        table: Table = Table(name, defs)
        # Create empty table file.
        table.create_if_not_exists()
        return table


def get_table(name: str) -> Table | None:
    """ Return Table object or None if table doesn't exist. """
    _log.debug(f'Getting table: {name}')
    try:
        with open(metadata_filename, 'r', encoding='utf-8') as metadata:
            reader = csv.reader(metadata, delimiter='\t')
            # Find the first row whose first column matches the table name
            row = next((row for row in reader if row[0] == name), None)
            if row is None:
                return None
            count = int(row[1])
            return Table(name, {row[i]: Types(row[i + 1]) for i in range(2, 2 + count * 2, 2)})
    except FileNotFoundError:
        _log.warning('metadata.txt file not found. Returning None.')
        return None

def drop_table(name: str) -> None:
    """ Drop table. Also erase metadata. """
    _log.debug(f'Dropping table: {name}')
    data_filename = f'{name}.txt'
    os.remove(data_filename)

    # Read metadata except target name
    with open(metadata_filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        rows = [row for row in reader if row[0] != name]

    # Rewrite metadata
    with open(metadata_filename, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(rows)


def _string_to_number(string: str) -> Decimal:
    return Decimal(string)


def _string_to_binary(string: str) -> bytes:
    return bytes.fromhex(string)


def _number_to_string(number: Decimal) -> str:
    return str(number)


def _binary_to_string(binary: bytes) -> str:
    return binary.hex()


def _data_to_string(value: DataValue, type_def: Types) -> str | None:
    """ Convert data type to string, usually for writing. """
    match type_def:
        case Types.NUMBER:
            return _number_to_string(Decimal(value))
        case Types.BINARY:
            return _binary_to_string(value)
        case Types.STRING:
            return value
    return None


def _string_to_data(string: str, type_def: Types) -> str | Decimal | bytes | None:
    """ Convert a raw string (usually read from file) to data type. """
    match type_def:
        case Types.NUMBER:
            return _string_to_number(string)
        case Types.BINARY:
            return _string_to_binary(string)
        case Types.STRING:
            return string
    return None


class Table:
    """ A class that describes a table. """
    name: str
    """ Example: students """
    filename: str
    """ Example: students.txt """
    defs: dict[str, Types]
    """ Example: {name: Types.STRING, id: Types.NUMBER} """

    def __init__(self, name: str, defs: dict[str, Types]) -> None:
        self.name = name
        self.filename = f'{self.name}.txt'
        self.defs = defs

    def create_if_not_exists(self) -> None:
        if not os.path.exists(self.filename):
            _log.debug(f'Creating file: {self.filename}')
            with open(self.filename, 'w', encoding='utf-8'):
                pass

    def insert_values(self, values: RowDict) -> None:
        """ This method does not accept None in values parameter. Use empty string instead. TODO use better placeholder """
        # TODO insert multiple lines
        _log.debug(f'Inserting values into {self.name}: {values}')
        with open(self.filename, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            row = [_data_to_string(values.get(key), value) for key, value in self.defs.items()]
            _log.debug(f'Writing: {row}')
            writer.writerow(row)

    def update(self, values: RowDict, where: Callable[[RowDict], bool] | None = None) -> None:
        """
        This method does not accept None in values parameter. Use empty string instead. TODO use better placeholder
        :param values: Dictionary of values to update
        :param where: Function which takes a row dictionary as parameter, returns bool
        """
        _log.debug(f'Updating values of {self.name}: {values}')

        def_count: int = len(self.defs)  # number of columns in the table, also the count of the table defs

        # build a list of new values for each column in the order of self.defs. None represents no change
        updated_values: list[str | None] = [_data_to_string(values.get(key), value) for key, value in
                                            self.defs.items()]  # get method returns None if not found
        _log.debug(f'Generated values: {updated_values}')

        # check if every column stayed unchanged
        if all(value is None for value in updated_values):
            _log.warning(f'No values to update: {values}')
            return

        table_values: list[list[str]] = []  # will hold the entire updated table
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                # determine whether this row should be updated
                if where is None or where(
                        {key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}):
                    # keep original value where updated_values[i] is None, otherwise update
                    new_row = [row[i] if updated_values[i] is None else updated_values[i] for i in range(def_count)]
                else:
                    new_row = row
                table_values.append(new_row)

        _log.debug(f'Updated values: {table_values}')

        with open(self.filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(table_values)

    def delete(self, where: Callable[[RowDict], bool] | None = None) -> None:
        """
        :param where: Function which takes a row dictionary as parameter, returns bool. None to truncate all. (reserve metadata)
        """
        _log.debug(f'Deleting values from {self.name}')

        # If no condition, truncate all
        if where is None:
            with open(self.filename, 'w', encoding='utf-8'):
                return

        table_values: list[list[str]] = []
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                # Check each row
                if where({key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}):
                    _log.debug(f'Deleting value: {row}')
                else:
                    table_values.append(row)

        # Write back
        with open(self.filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerows(table_values)

    def select(
            self,
            columns: list[str] | None = None,
            aggregations: dict[str, Callable[[list[RowDict]], DataValue]] | None = None,
            where: Callable[[RowDict], bool] | None = None,
            group_by: list[str] | None = None,
            having: Callable[[RowDict], bool] | None = None,
            order_by: list[tuple[str, bool]] | None = None,
            distinct: bool = False,
            limit: int | None = None,
            offset: int = 0
    ) -> list[RowDict]:
        """
        :param columns: Raw column names
        :param aggregations: Dictionary like {alias: function}, where function is an aggregation function taking a group (subset of entire table) as input and returning aggregated value, i.e. lambda group: sum(row["salary"] for row in group) / len(group)
        :param where: Function which takes a row dictionary as parameter, returns bool
        :param group_by: Raw column names
        :param having: Function which takes a row dictionary as parameter, returns bool
        :param order_by: Column list for sorting. i.e. [('dept', false), ('gender', true)]. The bool value represents desc if True, asc if False
        :param distinct: Remove duplicates
        :param limit: Maximum number of rows to return
        :param offset: Skip row number
        :return: A list of row values dict
        """
        # Read all rows and convert
        rows: list[RowDict] = []
        with open(self.filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                row_dict = {key: _string_to_data(val, typ) for val, (key, typ) in zip(row, self.defs.items())}
                rows.append(row_dict)

        # Apply WHERE
        if where is not None:
            rows = [row for row in rows if where(row)]

        # Handle aggregation
        if aggregations is not None:
            # Group rows by given columns
            groups: dict[tuple, list[RowDict]] = {}
            if group_by:
                # i.e. group_by=['dept', 'gender'] ==> key = (${dept}, ${gender})
                for row in rows:
                    key = tuple(row[col] for col in group_by)
                    # Get key or insert [] then append
                    groups.setdefault(key, []).append(row)
            else:
                groups = {(): rows} # treat entire set as one group

            # Calculate aggregations on each group
            result_rows: list[RowDict] = []
            for key, group_rows in groups.items():
                row_result: RowDict = {}
                if group_by:
                    # i.e. row_result = {dept: 'Sales', gender: 'Male'}
                    for col, val in zip(group_by, key):
                        row_result[col] = val
                # i.e. row_result = {dept: 'Sales', gender: 'Male', avg_salary: '3600'}
                for alias, agg_func in aggregations.items():
                    row_result[alias] = agg_func(group_rows)
                result_rows.append(row_result)

            # Apply HAVING
            if having is not None:
                result_rows = [r for r in result_rows if having(r)]

        else:
            # Handle non-aggregation

            # Handle columns selection
            if columns is not None:
                selected_rows = []
                for r in rows:
                    new_r = {col: r[col] for col in columns if col in r}
                    selected_rows.append(new_r)
                rows = selected_rows

            # Handle distinction
            if distinct:
                seen = set()
                unique_rows = []
                for r in rows:
                    key = tuple(sorted(r.items())) # to ensure the order of elements is the same
                    if key not in seen:
                        seen.add(key)
                        unique_rows.append(r)
                rows = unique_rows

            result_rows = rows

        # Handle ORDER BY
        if order_by:
            for item in reversed(order_by):
                col, desc = item
                result_rows.sort(key=lambda r: r[col], reverse=desc)

        if offset:
            result_rows = result_rows[offset:]
        if limit is not None:
            result_rows = result_rows[:limit]

        return result_rows