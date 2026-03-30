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
    # 重新创建测试表用于 DELETE 测试
    engine.execute_sql('DROP TABLE test;')
    engine.execute_sql('CREATE TABLE test ( name VARCHAR, age DECIMAL );')

    # 插入测试数据
    engine.execute_sql("INSERT INTO test VALUES ('Alice', 25);")
    engine.execute_sql("INSERT INTO test VALUES ('Bob', 30);")
    engine.execute_sql("INSERT INTO test VALUES ('Charlie', 35);")
    engine.execute_sql("INSERT INTO test VALUES ('David', 20);")

    # ===== 测试 1: DELETE FROM ... WHERE ... ====
    print("Test 1: DELETE FROM test WHERE age > 25")
    deleted = engine.execute_sql("DELETE FROM test WHERE age > 25")
    print(f"Deleted rows: {deleted}")  # 应该删除 Bob(30) 和 Charlie(35) = 2 行

    # 验证删除结果
    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 2
    names = {row['name'] for row in result}
    assert names == {'Alice', 'David'}

    # ===== 测试 2: DELETE FROM ... WHERE ... AND ... ====
    print("\nTest 2: DELETE FROM test WHERE age >= 20 AND age <= 25")
    deleted = engine.execute_sql("DELETE FROM test WHERE age >= 20 AND age <= 25")
    print(f"Deleted rows: {deleted}")  # 应该删除 Alice(25) 和 David(20) = 2 行

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 0

    # ===== 测试 3: DELETE FROM ... WHERE ... OR ... ====
    print("\nTest 3: DELETE FROM test (重新插入数据)")
    engine.execute_sql("INSERT INTO test VALUES ('Eve', 18);")
    engine.execute_sql("INSERT INTO test VALUES ('Frank', 22);")
    engine.execute_sql("INSERT INTO test VALUES ('Grace', 40);")

    deleted = engine.execute_sql("DELETE FROM test WHERE age < 20 OR age > 30")
    print(f"Deleted rows: {deleted}")  # 应该删除 Eve(18) 和 Grace(40) = 2 行

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 1
    assert result[0]['name'] == 'Frank'

    # ===== 测试 4: DELETE FROM ... (不带 WHERE) ====
    print("\nTest 4: DELETE FROM test (删除所有)")
    deleted = engine.execute_sql("DELETE FROM test")
    print(f"Deleted rows: {deleted}")  # 应该删除所有行

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 0

    # ===== 测试 5: WHERE 使用"空值"判断（使用空字符串模拟 NULL） ====
    print("\nTest 5: Testing handling of empty optional_field (simulated NULL)")
    # 重新创建表并插入包含"空值"的数据（使用空字符串代替 NULL）
    engine.execute_sql('DROP TABLE test;')
    engine.execute_sql('CREATE TABLE test ( name VARCHAR, optional_field VARCHAR );')
    engine.execute_sql("INSERT INTO test VALUES ('Item1', 'value');")
    engine.execute_sql("INSERT INTO test VALUES ('Item2', '');")

    deleted = engine.execute_sql("DELETE FROM test WHERE optional_field = ''")
    print(f"Deleted rows with empty optional_field: {deleted}")  # 应该删除 1 行

    result = storage.get_table('test').select()
    print("Remaining records:", result)
    assert len(result) == 1
    assert result[0]['name'] == 'Item1'

    engine.execute_sql('DROP TABLE test;')
    print('========== END DELETE TEST ==========')
    print('========== END SQL ENGINE TEST ==========')


if __name__ == '__main__':
    main()
