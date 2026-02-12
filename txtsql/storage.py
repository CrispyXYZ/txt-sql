import csv
import logging
import os
from pathlib import Path

from txtsql.exceptions import TableAlreadyExistsError
from txtsql.types import Types

logger = logging.getLogger(__name__)
metadata_filename = 'metadata.txt'


def create_table(name: str, defs: dict[str, Types]) -> Table:
    logger.debug(f'Creating table: {name}')

    if not os.path.exists(metadata_filename): \
            open(metadata_filename, 'w', encoding='utf-8').close()

    with open(metadata_filename, 'r', encoding='utf-8') as metadata:
        reader = csv.reader(metadata, delimiter='\t')
        for line in reader:
            if line[0] == name:
                raise TableAlreadyExistsError(f'Table already exists: {name}')
    with open(metadata_filename, 'a') as metadata:
        row: list[str] = [name, str(len(defs))]
        row += [str(item) for pair in defs.items() for item in pair]
        logger.debug(f'Writing: {row}')
        metadata.write('\t'.join(row))
    return Table(name, defs)


class Table:
    def __init__(self, name: str, defs: dict[str, Types]) -> None:
        self.name = name
        self.path = Path(name)
        self.defs = defs
