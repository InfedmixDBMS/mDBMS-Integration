"""
Test Event-Driven Wake-up Mechanism for Concurrency Control
Tests that waiting transactions use events instead of polling
"""
import sys
import os
import threading
import time

sys.path.append(os.path.join(os.getcwd(), "ConcurrencyControl", "src"))

from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from ConcurrencyControl.src.row_action import TableAction
from ConcurrencyControl.src.concurrency_response import LockStatus

def test_event_driven_waiting():
    """Test that events are created and signaled properly"""
    print("="*70)
    print("Testing Event-Driven Wake-up Mechanism")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Test 1: Verify events are created for transactions
    print("\n1. Testing event creation...")
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    
    if t1 in ccm.waiting_events and t2 in ccm.waiting_events:
        print("   ✅ PASSED: Events created for transactions")
    else:
        print("   ❌ FAILED: Events not created")
        return False
    
    # Test 2: Test event signaling when lock is released
    print("\n2. Testing event signaling on lock release...")
    
    # T2 acquires write lock on table X (younger transaction gets lock first)
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    print(f"   T{t2}: Write(X) -> {r2.status.value}")
    
    # T1 tries to write X - should wait (T1 is older in Wait-Die protocol)
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"   T{t1}: Write(X) -> {r1.status.value}")
    
    if r1.status != LockStatus.WAITING:
        print(f"   ❌ FAILED: Expected WAITING, got {r1.status}")
        return False
    
    # Verify T1 is registered as waiting
    if 'X' in ccm.resource_waiters and t1 in ccm.resource_waiters['X']:
        print("   ✅ PASSED: T1 registered as waiting for resource X")
    else:
        print("   ❌ FAILED: T1 not registered in resource_waiters")
        return False
    
    # Get T1's event
    t1_event = ccm.get_wait_event(t1)
    
    # Verify event is cleared (not set)
    if not t1_event.is_set():
        print("   ✅ PASSED: T1's event is cleared (waiting)")
    else:
        print("   ❌ FAILED: T1's event should be cleared")
        return False
    
    # Test 3: Test event-driven waiting in a thread
    print("\n3. Testing event-driven waiting...")
    
    wait_result = {'completed': False, 'wait_time': None}
    
    def waiter_thread():
        start = time.time()
        # Wait on event with timeout
        signaled = t1_event.wait(timeout=5.0)
        wait_time = time.time() - start
        wait_result['completed'] = signaled
        wait_result['wait_time'] = wait_time
        print(f"   Waiter thread: Event signaled={signaled}, wait_time={wait_time:.3f}s")
    
    waiter = threading.Thread(target=waiter_thread, daemon=True)
    waiter.start()
    
    # Give the waiter thread time to start waiting
    time.sleep(0.5)
    
    # Now commit T2 to release the lock
    print(f"   Committing T{t2} to release locks...")
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    # Wait for waiter thread to complete
    waiter.join(timeout=2.0)
    
    if wait_result['completed']:
        print(f"   ✅ PASSED: Event was signaled (wait time: {wait_result['wait_time']:.3f}s)")
        if wait_result['wait_time'] < 2.0:
            print("   ✅ PASSED: Wake-up was immediate (< 2s), not polling-based")
        else:
            print("   ⚠️  WARNING: Wake-up took longer than expected")
    else:
        print("   ❌ FAILED: Event was not signaled")
        return False
    
    # Test 4: Verify event is signaled
    print("\n4. Verifying event state after signal...")
    if t1_event.is_set():
        print("   ✅ PASSED: T1's event is now set (signaled)")
    else:
        print("   ❌ FAILED: T1's event should be set after T2 commits")
        return False
    
    # Test 5: Now T1 should be able to acquire the lock
    print("\n5. Testing lock acquisition after wake-up...")
    r1_retry = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"   T{t1}: Write(X) retry -> {r1_retry.status.value}")
    
    if r1_retry.status == LockStatus.GRANTED:
        print("   ✅ PASSED: T1 acquired lock after T2 released it")
    else:
        print(f"   ❌ FAILED: T1 should have acquired lock, got {r1_retry.status}")
        return False
    
    # Test 6: Cleanup verification
    print("\n6. Testing cleanup on transaction end...")
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    ccm.transaction_end(t1)
    ccm.transaction_end(t2)
    
    if t1 not in ccm.waiting_events and t2 not in ccm.waiting_events:
        print("   ✅ PASSED: Events cleaned up on transaction end")
    else:
        print("   ❌ FAILED: Events not properly cleaned up")
        return False
    
    # Test 7: Multiple waiters scenario
    print("\n7. Testing multiple waiters on same resource...")
    t3 = ccm.transaction_begin()
    t4 = ccm.transaction_begin()
    t5 = ccm.transaction_begin()
    
    # T5 gets the lock (youngest transaction gets lock first)
    ccm.transaction_query(t5, TableAction.WRITE, 'Y')
    
    # T3 and T4 both wait (both are older than T5)
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 'Y')
    r4 = ccm.transaction_query(t4, TableAction.WRITE, 'Y')
    
    if r3.status == LockStatus.WAITING and r4.status == LockStatus.WAITING:
        print(f"   ✅ PASSED: Both T{t3} and T{t4} are waiting")
    else:
        print(f"   ❌ FAILED: Expected both to wait, got {r3.status}, {r4.status}")
        return False
    
    # Verify both are registered
    if 'Y' in ccm.resource_waiters:
        waiters = ccm.resource_waiters['Y']
        if t3 in waiters and t4 in waiters:
            print("   ✅ PASSED: Both waiters registered for resource Y")
        else:
            print("   ❌ FAILED: Not all waiters registered")
            return False
    
    # Get their events
    t3_event = ccm.get_wait_event(t3)
    t4_event = ccm.get_wait_event(t4)
    
    # Commit T5
    ccm.transaction_commit(t5)
    ccm.transaction_commit_flushed(t5)
    
    # Give time for signals to propagate
    time.sleep(0.1)
    
    # Both events should be signaled
    if t3_event.is_set() and t4_event.is_set():
        print(f"   ✅ PASSED: Both T{t3} and T{t4} events were signaled")
    else:
        print(f"   ❌ FAILED: Not all events signaled (T{t3}:{t3_event.is_set()}, T{t4}:{t4_event.is_set()})")
        return False
    
    print("\n" + "="*70)
    print("ALL TESTS PASSED! ✅")
    print("Event-driven wake-up mechanism working correctly!")
    print("="*70)
    return True

if __name__ == "__main__":
    success = test_event_driven_waiting()
    sys.exit(0 if success else 1)
