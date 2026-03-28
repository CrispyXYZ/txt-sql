import logging
import sys
from decimal import Decimal

from . import storage, engine
from .types import Types


def main() -> None:
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

    print('========== BEGIN STORAGE TEST ==========')

    if storage.get_table("test_table") is not None:
        storage.drop_table("test_table")
    test = storage.create_table("test_table", {'dept': Types.STRING, 'name': Types.STRING, 'salary': Types.NUMBER})
    test.insert_values({'dept': 'Sales', 'name': 'Alice', 'salary': 3000})
    test.insert_values({'dept': 'Sales', 'name': 'Bob', 'salary': 4000})
    test.insert_values({'dept': 'Advertising', 'name': 'Charlie', 'salary': 5000})
    test.insert_values({'dept': 'Advertising', 'name': 'David', 'salary': 6000})

    print(test.select(columns=['dept'], distinct=True))
    print(test.select(columns=['name'], where=lambda row: row['salary'] > 4000))
    print(test.select(aggregations={'avg_salary': lambda rows: Decimal(sum(row['salary'] for row in rows) / len(rows))},
                      group_by=['dept']))
    print(test.select(order_by=[('dept', True), ('salary', True)]))

    print('========== END STORAGE TEST ==========')

    print('========== BEGIN SQL ENGINE TEST ==========')
    # Drop table if it already exists
    try:
        engine.execute_sql('CREATE TABLE test ( name VARCHAR, age DECIMAL );')
    except Exception:
        engine.execute_sql('DROP TABLE test;')
        engine.execute_sql('CREATE TABLE test ( name VARCHAR, age DECIMAL );')

    engine.execute_sql("INSERT INTO test VALUES ('Alice', 25);")
    engine.execute_sql("INSERT INTO test (age, name) VALUES (30, 'Bob');")
    engine.execute_sql("INSERT INTO test (name) VALUES ('Charlie'), ('David');")
    engine.execute_sql('DROP TABLE test;')
    print('========== END SQL ENGINE TEST ==========')


if __name__ == '__main__':
    main()
