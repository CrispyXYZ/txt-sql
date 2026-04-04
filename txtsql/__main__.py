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

    print('========== BEGIN DELETE TEST ==========')
    # Recreate test table for DELETE test
    engine.execute_sql('DROP TABLE test;')
    engine.execute_sql('CREATE TABLE test ( name VARCHAR, age DECIMAL );')

    # Insert test data
    engine.execute_sql("INSERT INTO test VALUES ('Alice', 25);")
    engine.execute_sql("INSERT INTO test VALUES ('Bob', 30);")
    engine.execute_sql("INSERT INTO test VALUES ('Charlie', 35);")
    engine.execute_sql("INSERT INTO test VALUES ('David', 20);")

    # ===== Test 1: DELETE FROM ... WHERE ... ====
    print("Test 1: DELETE FROM test WHERE age > 25")
    deleted = engine.execute_sql("DELETE FROM test WHERE age > 25")
        print(f"Deleted rows: {deleted}")  # Should delete Bob(30) and Charlie(35) = 2 rows

    # Verify deletion result
    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 2
    names = {row['name'] for row in result}
    assert names == {'Alice', 'David'}

    # ===== Test 2: DELETE FROM ... WHERE ... AND ... ====
    print("\nTest 2: DELETE FROM test WHERE age >= 20 AND age <= 25")
    deleted = engine.execute_sql("DELETE FROM test WHERE age >= 20 AND age <= 25")
    print(f"Deleted rows: {deleted}")  # Should delete Alice(25) and David(20) = 2 rows

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 0

    # ===== Test 3: DELETE FROM ... WHERE ... OR ... ====
    print("\nTest 3: DELETE FROM test (re-insert data)")
    engine.execute_sql("INSERT INTO test VALUES ('Eve', 18);")
    engine.execute_sql("INSERT INTO test VALUES ('Frank', 22);")
    engine.execute_sql("INSERT INTO test VALUES ('Grace', 40);")

    deleted = engine.execute_sql("DELETE FROM test WHERE age < 20 OR age > 30")
        print(f"Deleted rows: {deleted}")  # Should delete Eve(18) and Grace(40) = 2 rows

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 1
    assert result[0]['name'] == 'Frank'

    # ===== Test 4: DELETE FROM ... (without WHERE) ====
    print("\nTest 4: DELETE FROM test (delete all)")
    deleted = engine.execute_sql("DELETE FROM test")
        print(f"Deleted rows: {deleted}")  # Should delete all rows

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 0

    # ===== Test 5: WHERE using "empty value" check (using empty string to simulate NULL) ====
    print("\nTest 5: Testing handling of empty optional_field (simulated NULL)")
    # Recreate table and insert data containing "empty values" (using empty string instead of NULL)
    engine.execute_sql('DROP TABLE test;')
    engine.execute_sql('CREATE TABLE test ( name VARCHAR, optional_field VARCHAR );')
    engine.execute_sql("INSERT INTO test VALUES ('Item1', 'value');")
    engine.execute_sql("INSERT INTO test VALUES ('Item2', '');")

    deleted = engine.execute_sql("DELETE FROM test WHERE optional_field = ''")
        print(f"Deleted rows with empty optional_field: {deleted}")  # Should delete 1 row

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 1
    assert result[0]['name'] == 'Item1'

    engine.execute_sql('DROP TABLE test;')
    print('========== END DELETE TEST ==========')
    print('========== END SQL ENGINE TEST ==========')


if __name__ == '__main__':
    main()
