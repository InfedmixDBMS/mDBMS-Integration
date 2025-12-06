import sys
import os
import json
import unittest

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))
sys.path.append(os.getcwd())

from StorageManager.classes.API import StorageEngine
from StorageManager.classes.DataModels import Schema, DataWrite, DataRetrieval, DataDeletion, Condition, Operation
from StorageManager.classes.Types import IntType, VarCharType, FloatType
from StorageManager.classes.globals import CATALOG_FILE, STATS_BASE_PATH

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

TEST_TABLE_NAME = "test_crud_students_v2"
TEST_TABLE_SCHEMA = Schema(
    id=IntType(),
    name=VarCharType(50),
    gpa=FloatType()
)
TEST_DATA = [
    [101, 'Alice', 3.8],
    [102, 'Bob', 3.5],
    [103, 'Charlie', 3.9],
]
USER_COLUMNS = ['id', 'name', 'gpa']

def cleanup_test_files():
    """Removes any artifacts from previous test runs."""
    print(f"{Colors.OKCYAN}[CLEANUP]{Colors.ENDC} Removing old test artifacts...")
    
    try:
        StorageEngine.drop_table(TEST_TABLE_NAME)
        print(f"  - Dropped table '{TEST_TABLE_NAME}' successfully.")
    except Exception:
        pass

    dat_file_path = f"storage/data/{TEST_TABLE_NAME}.dat"
    if os.path.exists(dat_file_path):
        os.remove(dat_file_path)
        print(f"  - Removed '{dat_file_path}'.")

    stats_file_path = f"{STATS_BASE_PATH}{TEST_TABLE_NAME}_stats.json"
    if os.path.exists(stats_file_path):
        os.remove(stats_file_path)
        print(f"  - Removed '{stats_file_path}'.")
    print(f"{Colors.OKCYAN}[CLEANUP]{Colors.ENDC} Cleanup complete.")

def print_read_data(header, result_rows):
    print(f"\n  {Colors.BOLD}{header}:{Colors.ENDC}")
    if result_rows.data:
        col_names = ", ".join(result_rows.columns)
        print(f"  {Colors.OKBLUE}Columns: ({col_names}){Colors.ENDC}")
        for row in result_rows.data:
            print(f"  {Colors.OKCYAN}- {row}{Colors.ENDC}")
    else:
        print(f"  {Colors.WARNING}(No rows){Colors.ENDC}")


def test_storage_crud_operations():
    tc = unittest.TestCase()
    print("==================================================")
    print("       STORAGE ENGINE CRUD TEST (User Perspective)")
    print("==================================================")

    cleanup_test_files()
    print(60*"=" + "\n")

    print(f"{Colors.HEADER}[TEST - CREATE]{Colors.ENDC} Testing table creation...")
    user_schema_for_creation = Schema(id=IntType(), name=VarCharType(50), gpa=FloatType())
    success = StorageEngine.create_table(TEST_TABLE_NAME, user_schema_for_creation)
    tc.assertTrue(success, "StorageEngine.create_table should return True.")
    
    with open(CATALOG_FILE, "r") as f:
        catalog_data = json.load(f)
    tc.assertIn(TEST_TABLE_NAME, catalog_data, "Test table should exist in catalog.json.")
    print(f"{Colors.OKGREEN}Table '{TEST_TABLE_NAME}' created successfully.{Colors.ENDC}\n")
    print(60*"=" + "\n")

    print(f"{Colors.HEADER}[TEST - WRITE]{Colors.ENDC} Testing data writing...")
    data_write = DataWrite(
        table=TEST_TABLE_NAME,
        column=USER_COLUMNS, 
        conditions=[],
        new_value=TEST_DATA
    )
    rows_written = StorageEngine.write_block(data_write)
    tc.assertEqual(rows_written, len(TEST_DATA), f"Should have written {len(TEST_DATA)} rows.")
    print(f"{Colors.OKGREEN}Wrote {rows_written} rows successfully.{Colors.ENDC}")

    read_after_write_retrieval = DataRetrieval(table=TEST_TABLE_NAME, column=USER_COLUMNS, conditions=[])
    result_after_write = StorageEngine.read_block(read_after_write_retrieval)
    print_read_data("Data after initial write", result_after_write)
    
    print(60*"=" + "\n")

    print(f"{Colors.HEADER}[TEST - READ]{Colors.ENDC} Testing data reading...")
    
    def assert_rows_almost_equal(list1, list2, msg=None, delta=1e-7):
        tc.assertEqual(len(list1), len(list2), msg)
        for i, (row1, row2) in enumerate(zip(list1, list2)):
            tc.assertEqual(len(row1), len(row2), f"Row {i} length mismatch: {msg}")
            for j, (val1, val2) in enumerate(zip(row1, row2)):
                if isinstance(val1, float) and isinstance(val2, float):
                    tc.assertAlmostEqual(val1, val2, delta=delta, msg=f"Row {i}, Col {j} float mismatch: {msg}")
                else:
                    tc.assertEqual(val1, val2, f"Row {i}, Col {j} mismatch: {msg}")

    print("  - Performing full table scan...")
    read_all_retrieval = DataRetrieval(table=TEST_TABLE_NAME, column=USER_COLUMNS, conditions=[])
    result_all = StorageEngine.read_block(read_all_retrieval)
    tc.assertEqual(result_all.row_count, len(TEST_DATA), "Full scan should return all rows.")
    assert_rows_almost_equal(result_all.data, TEST_DATA, "Full scan data should match original data.")
    print(f"  {Colors.OKGREEN}Full scan successful. Found {result_all.row_count} rows.{Colors.ENDC}")

    print("  - Performing scan with condition (gpa > 3.6)... ")
    read_cond_retrieval = DataRetrieval(
        table=TEST_TABLE_NAME,
        column=USER_COLUMNS, 
        conditions=[Condition(column='gpa', operation=Operation.GT, operand=3.6)]
    )
    result_cond = StorageEngine.read_block(read_cond_retrieval)
    expected_data_cond = [row for row in TEST_DATA if row[2] > 3.6]
    tc.assertEqual(result_cond.row_count, len(expected_data_cond), "Conditional scan should return correct number of rows.")
    assert_rows_almost_equal(result_cond.data, expected_data_cond, "Conditional scan data should match expected data.")
    print(f"  {Colors.OKGREEN}Conditional scan successful. Found {result_cond.row_count} rows.{Colors.ENDC}\n")
    print(60*"=" + "\n")

    print(f"{Colors.HEADER}[TEST - DELETE]{Colors.ENDC} Testing data deletion (id = 102)...")
    delete_condition = Condition(column='id', operation=Operation.EQ, operand=102)
    data_deletion = DataDeletion(table=TEST_TABLE_NAME, conditions=[delete_condition])
    rows_deleted = StorageEngine.delete_block(data_deletion)
    tc.assertEqual(rows_deleted, 1, "Should have deleted 1 row.")
    print(f"{Colors.OKGREEN}Deleted {rows_deleted} row successfully.{Colors.ENDC}")

    result_after_delete_print = StorageEngine.read_block(read_all_retrieval) 
    print_read_data("Data after deletion", result_after_delete_print)
    
    print("\n  - Verifying deletion by assertion...")
    result_after_delete = StorageEngine.read_block(read_all_retrieval)
    remaining_data = [row for row in TEST_DATA if row[0] != 102]
    tc.assertEqual(result_after_delete.row_count, len(remaining_data), "Row count should be reduced by 1 after deletion.")
    assert_rows_almost_equal(result_after_delete.data, remaining_data, "Data after deletion should not contain the deleted row.")
    print(f"  {Colors.OKGREEN}Verification successful. {result_after_delete.row_count} rows remain.{Colors.ENDC}\n")
    print(60*"=" + "\n")
    
    cleanup_test_files()
    
    print("==================================================")
    print("       TEST SUITE COMPLETED SUCCESSFULLY")
    print("==================================================")


if __name__ == "__main__":
    test_storage_crud_operations()