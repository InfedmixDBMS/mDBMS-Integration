"""
Integration test for Timestamp-Based Concurrency Control
Tests the complete flow from QueryProcessor through IntegratedConcurrencyManager to TimestampBasedCCM
"""

import sys
import os
sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from QueryProcessor.query_processor_core import QueryProcessor
from ConcurrencyControl.src.timestamp_based_concurrency_control_manager import TimestampBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine


def print_test_header(test_num, description):
    print("\n" + "="*70)
    print(f"TEST {test_num}: {description}")
    print("="*70)


def test_1_basic_timestamp_ordering():
    """Test basic timestamp ordering with single transaction"""
    print_test_header(1, "Basic Timestamp Ordering - Single Transaction")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Begin transaction")
    tid = processor.begin_transaction()
    print(f"   Transaction ID (timestamp): {tid}")
    
    print("\n2. Request read access")
    result = concurrency_manager.request_lock(tid, "users", "READ")
    print(f"   Lock result: granted={result.granted}, status={result.status}")
    assert result.granted, "Read access should be granted"
    
    print("\n3. Request write access")
    result = concurrency_manager.request_lock(tid, "users", "WRITE")
    print(f"   Lock result: granted={result.granted}, status={result.status}")
    assert result.granted, "Write access should be granted"
    
    print("\n4. Commit transaction")
    commit_result = processor.commit_transaction(tid)
    print(f"   Commit result: success={commit_result.success}")
    assert commit_result.success, "Commit should succeed"
    
    print("\n✅ Test 1 PASSED: Basic timestamp ordering works correctly")


def test_2_read_write_conflict():
    """Test that younger transaction cannot read data written by older transaction that hasn't committed"""
    print_test_header(2, "Read-Write Conflict Detection")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Begin two transactions (T1 has older timestamp)")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   T1 (older) ID: {tid1}")
    print(f"   T2 (younger) ID: {tid2}")
    
    print("\n2. T2 (younger) writes to users table")
    result = concurrency_manager.request_lock(tid2, "users", "WRITE")
    print(f"   T2 write result: granted={result.granted}, status={result.status}")
    assert result.granted, "T2 write should be granted"
    
    print("\n3. T1 (older) tries to read users table")
    print("   This should FAIL because T1 has older timestamp than T2's write")
    result = concurrency_manager.request_lock(tid1, "users", "READ")
    print(f"   T1 read result: granted={result.granted}, status={result.status}")
    assert not result.granted, "T1 read should be denied (older timestamp)"
    assert result.status == "FAILED", "Status should be FAILED"
    
    print("\n4. T1 is automatically aborted by the protocol")
    status = concurrency_manager.get_transaction_status(tid1)
    print(f"   T1 status: {status}")
    # Transaction should be aborted
    
    print("\n✅ Test 2 PASSED: Read-write conflict detected correctly")


def test_3_write_read_conflict():
    """Test that younger transaction cannot write data read by older transaction"""
    print_test_header(3, "Write-Read Conflict Detection")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Begin two transactions")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   T1 (older) ID: {tid1}")
    print(f"   T2 (younger) ID: {tid2}")
    
    print("\n2. T2 (younger) reads from users table")
    result = concurrency_manager.request_lock(tid2, "users", "READ")
    print(f"   T2 read result: granted={result.granted}, status={result.status}")
    assert result.granted, "T2 read should be granted"
    
    print("\n3. T1 (older) tries to write to users table")
    print("   This should FAIL because T1 has older timestamp than T2's read")
    result = concurrency_manager.request_lock(tid1, "users", "WRITE")
    print(f"   T1 write result: granted={result.granted}, status={result.status}")
    assert not result.granted, "T1 write should be denied (older timestamp)"
    assert result.status == "FAILED", "Status should be FAILED"
    
    print("\n✅ Test 3 PASSED: Write-read conflict detected correctly")


def test_4_thomas_write_rule():
    """Test Thomas Write Rule - obsolete writes are ignored"""
    print_test_header(4, "Thomas Write Rule")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Begin two transactions")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   T1 (older) ID: {tid1}")
    print(f"   T2 (younger) ID: {tid2}")
    
    print("\n2. T2 (younger) writes to users table")
    result = concurrency_manager.request_lock(tid2, "users", "WRITE")
    print(f"   T2 write result: granted={result.granted}, status={result.status}")
    assert result.granted, "T2 write should be granted"
    
    print("\n3. T1 (older) tries to write to same table")
    print("   Thomas Write Rule: Obsolete write is IGNORED but transaction continues")
    result = concurrency_manager.request_lock(tid1, "users", "WRITE")
    print(f"   T1 write result: granted={result.granted}, status={result.status}")
    assert result.granted, "T1 write should be granted (ignored by Thomas Write Rule)"
    
    print("\n✅ Test 4 PASSED: Thomas Write Rule works correctly")


def test_5_restart_after_abort():
    """Test that after timestamp conflict abort, new transaction can succeed"""
    print_test_header(5, "Transaction Restart After Abort")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Begin two transactions")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   T1 (older) ID: {tid1}")
    print(f"   T2 (younger) ID: {tid2}")
    
    print("\n2. T2 writes to table")
    concurrency_manager.request_lock(tid2, "products", "WRITE")
    
    print("\n3. T1 tries to read - should fail and abort")
    result = concurrency_manager.request_lock(tid1, "products", "READ")
    print(f"   T1 read result: granted={result.granted}, status={result.status}")
    assert not result.granted, "T1 should be denied"
    
    print("\n4. Start NEW transaction (T3) with fresh timestamp")
    tid3 = processor.begin_transaction()
    print(f"   T3 (newest) ID: {tid3}")
    
    print("\n5. T3 reads from table (should succeed - newest timestamp)")
    result = concurrency_manager.request_lock(tid3, "products", "READ")
    print(f"   T3 read result: granted={result.granted}, status={result.status}")
    assert result.granted, "T3 should be able to read (newest timestamp)"
    
    print("\n6. T3 commits successfully")
    commit_result = processor.commit_transaction(tid3)
    print(f"   T3 commit result: success={commit_result.success}")
    assert commit_result.success, "T3 should commit successfully"
    
    print("\n✅ Test 5 PASSED: Transaction restart after abort works correctly")


def test_6_query_execution_with_timestamp():
    """Test full query execution with timestamp-based protocol"""
    print_test_header(6, "Full Query Execution with Timestamp Protocol")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = TimestampBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    concurrency_manager.setVerbose(True)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    # Execute
    print("\n1. Create table")
    result = processor.execute_query("CREATE TABLE orders (id INT, amount INT)", None)
    print(f"   Create table: {result.success}")
    assert result.success, "Table creation should succeed"
    
    print("\n2. Begin transaction T1")
    tid1 = processor.begin_transaction()
    print(f"   T1 ID: {tid1}")
    
    print("\n3. T1 inserts data")
    result = processor.execute_query("INSERT INTO orders VALUES (1, 100)", tid1)
    print(f"   T1 insert: {result.success}")
    assert result.success, "Insert should succeed"
    
    print("\n4. T1 commits")
    result = processor.commit_transaction(tid1)
    print(f"   T1 commit: {result.success}")
    assert result.success, "Commit should succeed"
    
    print("\n5. Begin transaction T2")
    tid2 = processor.begin_transaction()
    print(f"   T2 ID: {tid2}")
    
    print("\n6. T2 reads data")
    result = processor.execute_query("SELECT * FROM orders", tid2)
    print(f"   T2 select: {result.success}")
    assert result.success, "Select should succeed"
    
    print("\n7. T2 commits")
    result = processor.commit_transaction(tid2)
    print(f"   T2 commit: {result.success}")
    assert result.success, "Commit should succeed"
    
    print("\n✅ Test 6 PASSED: Full query execution works correctly")


def main():
    print("\n" + "="*70)
    print("TIMESTAMP-BASED CONCURRENCY CONTROL INTEGRATION TESTS")
    print("="*70)
    
    try:
        test_1_basic_timestamp_ordering()
        test_2_read_write_conflict()
        test_3_write_read_conflict()
        test_4_thomas_write_rule()
        test_5_restart_after_abort()
        test_6_query_execution_with_timestamp()
        
        print("\n" + "="*70)
        print("ALL TESTS PASSED ✅")
        print("="*70)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
