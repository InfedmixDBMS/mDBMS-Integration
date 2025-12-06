"""
Integration test for Validation-Based Concurrency Control
Tests the complete flow from QueryProcessor through IntegratedConcurrencyManager to ValidationBasedCCM
"""

import sys
import os
sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from QueryProcessor.query_processor_core import QueryProcessor
from ConcurrencyControl.src.validation_based_concurrency_control_manager import ValidationBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine


def print_test_header(test_num, description):
    print("\n" + "="*70)
    print(f"TEST {test_num}: {description}")
    print("="*70)


def test_1_basic_commit():
    """Test basic transaction commit without conflicts"""
    print_test_header(1, "Basic Single Transaction Commit")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = ValidationBasedConcurrencyControlManager()
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
    print(f"   Transaction ID: {tid}")
    
    print("\n2. Request read access")
    result = concurrency_manager.request_lock(tid, "users", "READ")
    print(f"   Lock result: {result}")
    assert result.granted, "Read access should be granted"
    
    print("\n3. Request write access")
    result = concurrency_manager.request_lock(tid, "users", "WRITE")
    print(f"   Lock result: {result}")
    assert result.granted, "Write access should be granted"
    
    print("\n4. Commit transaction")
    commit_result = processor.commit_transaction(tid)
    print(f"   Commit result: {commit_result}")
    assert commit_result.success, "Commit should succeed without conflicts"
    
    print("\n✅ Test 1 PASSED: Basic commit works correctly")


def test_2_validation_conflict():
    """Test validation failure when transactions conflict"""
    print_test_header(2, "Validation Conflict Detection")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = ValidationBasedConcurrencyControlManager()
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
    print("\n1. Begin two concurrent transactions")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   Transaction 1 ID: {tid1}")
    print(f"   Transaction 2 ID: {tid2}")
    
    print("\n2. T1 reads users table")
    result = concurrency_manager.request_lock(tid1, "users", "READ")
    print(f"   T1 read result: {result}")
    assert result.granted, "T1 read should be granted"
    
    print("\n3. T2 writes to users table")
    result = concurrency_manager.request_lock(tid2, "users", "WRITE")
    print(f"   T2 write result: {result}")
    assert result.granted, "T2 write should be granted (no blocking in OCC)"
    
    print("\n4. T2 commits first (validation should pass)")
    commit_result = processor.commit_transaction(tid2)
    print(f"   T2 commit result: {commit_result}")
    assert commit_result.success, "T2 commit should succeed"
    
    print("\n5. T1 tries to commit (validation should fail)")
    commit_result = processor.commit_transaction(tid1)
    print(f"   T1 commit result: {commit_result}")
    assert not commit_result.success, "T1 commit should fail due to validation conflict"
    assert "validation failure" in commit_result.error.lower() or "protocol conflict" in commit_result.error.lower(), \
        "Error message should mention validation failure or protocol conflict"
    
    print("\n✅ Test 2 PASSED: Validation conflict detected correctly")


def test_3_restart_after_abort():
    """Test that transaction can restart after validation failure"""
    print_test_header(3, "Transaction Restart After Validation Failure")
    
    # Setup
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = ValidationBasedConcurrencyControlManager()
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
    print("\n1. Begin two concurrent transactions")
    tid1 = processor.begin_transaction()
    tid2 = processor.begin_transaction()
    print(f"   Transaction 1 ID: {tid1}")
    print(f"   Transaction 2 ID: {tid2}")
    
    print("\n2. Both transactions access same table")
    concurrency_manager.request_lock(tid1, "users", "READ")
    concurrency_manager.request_lock(tid2, "users", "WRITE")
    
    print("\n3. T2 commits first")
    processor.commit_transaction(tid2)
    
    print("\n4. T1 fails to commit")
    result1 = processor.commit_transaction(tid1)
    assert not result1.success, "T1 should fail validation"
    
    print("\n5. Start NEW transaction (T3) and try again")
    tid3 = processor.begin_transaction()
    print(f"   New transaction ID: {tid3}")
    
    print("\n6. T3 accesses the table")
    result = concurrency_manager.request_lock(tid3, "users", "READ")
    assert result.granted, "T3 should be able to access table"
    
    print("\n7. T3 commits successfully")
    result3 = processor.commit_transaction(tid3)
    print(f"   T3 commit result: {result3}")
    assert result3.success, "T3 should commit successfully"
    
    print("\n✅ Test 3 PASSED: Transaction restart after abort works correctly")


def main():
    print("\n" + "="*70)
    print("VALIDATION-BASED CONCURRENCY CONTROL INTEGRATION TESTS")
    print("="*70)
    
    try:
        test_1_basic_commit()
        test_2_validation_conflict()
        test_3_restart_after_abort()
        
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