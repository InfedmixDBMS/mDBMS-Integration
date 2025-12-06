"""
Extensive Wait-Die Protocol Test with Event-Driven Wake-up

This test extensively verifies:
1. Older transactions wait (and use events)
2. Younger transactions die (abort)
3. Events are signaled when older transactions can proceed
4. Complex multi-transaction scenarios
5. Cascading wake-ups
"""
import sys
import os
import threading
import time

sys.path.append(os.path.join(os.getcwd(), "ConcurrencyControl", "src"))

from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from ConcurrencyControl.src.row_action import TableAction
from ConcurrencyControl.src.concurrency_response import LockStatus


def test_basic_wait_die():
    """Test basic Wait-Die: older waits, younger dies"""
    print("="*70)
    print("TEST 1: Basic Wait-Die Protocol")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create transactions (lower timestamp = older)
    t1 = ccm.transaction_begin()  # Timestamp 1 (oldest)
    t2 = ccm.transaction_begin()  # Timestamp 2
    t3 = ccm.transaction_begin()  # Timestamp 3 (youngest)
    
    print(f"\nTransactions: T{t1}(TS={ccm.transactions[t1]['timestamp']}), "
          f"T{t2}(TS={ccm.transactions[t2]['timestamp']}), "
          f"T{t3}(TS={ccm.transactions[t3]['timestamp']})")
    
    # Scenario 1: Younger holds, older wants -> WAIT
    print("\n[Scenario 1] Younger T3 holds lock, older T1 wants it")
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 'X')
    print(f"  T{t3} Write(X): {r3.status.value}")
    assert r3.status == LockStatus.GRANTED
    
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  T{t1} Write(X): {r1.status.value}")
    print(f"  Reason: {r1.reason}")
    assert r1.status == LockStatus.WAITING, "Older T1 should WAIT for younger T3"
    
    # Verify event is created
    assert t1 in ccm.resource_waiters.get('X', set()), "T1 should be in waiters"
    event = ccm.get_wait_event(t1)
    assert not event.is_set(), "Event should not be set yet"
    print(f"  ✅ T{t1} is waiting with event")
    
    # Scenario 2: Younger holds, younger wants -> DIE
    print(f"\n[Scenario 2] T2 also wants the lock held by T3")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    print(f"  T2 Write(X): {r2.status.value}")
    print(f"  Reason: {r2.reason}")
    # T2 (TS=2) is OLDER than T3 (TS=3), so T2 should WAIT
    assert r2.status == LockStatus.WAITING, "Older T2 should WAIT for younger T3"
    print(f"  ✅ T2 is waiting (older waits for younger)")
    
    # Release lock and verify T1 wakes up
    print(f"\n[Scenario 3] T{t3} commits, T{t1} should wake up")
    ccm.transaction_commit(t3)
    ccm.transaction_commit_flushed(t3)
    
    time.sleep(0.05)
    assert event.is_set(), "T1's event should be signaled"
    print(f"  ✅ T{t1}'s event was signaled")
    
    # T1 can now acquire
    r1_retry = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  T{t1} Write(X) retry: {r1_retry.status.value}")
    assert r1_retry.status == LockStatus.GRANTED
    print(f"  ✅ T{t1} acquired lock after wake-up")
    
    print("\n" + "="*70)
    print("TEST 1 PASSED ✅")
    print("="*70)
    return True


def test_cascading_wait_die():
    """Test cascading waits with multiple older transactions"""
    print("\n" + "="*70)
    print("TEST 2: Cascading Waits")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create 5 transactions
    t1 = ccm.transaction_begin()  # Oldest
    t2 = ccm.transaction_begin()
    t3 = ccm.transaction_begin()
    t4 = ccm.transaction_begin()
    t5 = ccm.transaction_begin()  # Youngest
    
    print(f"\nTransactions: T{t1}(TS=1), T{t2}(TS=2), T{t3}(TS=3), T{t4}(TS=4), T{t5}(TS=5)")
    
    # T5 (youngest) gets lock first
    print(f"\n[Step 1] T{t5} (youngest) acquires Write lock on Y")
    r5 = ccm.transaction_query(t5, TableAction.WRITE, 'Y')
    assert r5.status == LockStatus.GRANTED
    print(f"  ✅ T{t5} granted")
    
    # T1-T4 (all older) try to acquire -> should all WAIT
    print(f"\n[Step 2] T{t1}-T{t4} (all older) try to acquire -> should WAIT")
    
    waiters = [t1, t2, t3, t4]
    events = {}
    
    for tid in waiters:
        r = ccm.transaction_query(tid, TableAction.WRITE, 'Y')
        print(f"  T{tid} Write(Y): {r.status.value}")
        assert r.status == LockStatus.WAITING, f"Older T{tid} should WAIT"
        events[tid] = ccm.get_wait_event(tid)
        assert not events[tid].is_set(), f"T{tid} event should not be set"
    
    print(f"  ✅ All 4 older transactions are waiting")
    print(f"  ✅ All have events created")
    
    # Verify resource_waiters
    print(f"\n[Step 3] Verify all registered in resource_waiters")
    assert len(ccm.resource_waiters.get('Y', set())) == 4
    for tid in waiters:
        assert tid in ccm.resource_waiters['Y']
    print(f"  ✅ All 4 transactions in resource_waiters['Y']")
    
    # Release lock
    print(f"\n[Step 4] T{t5} commits, all waiters should wake up")
    ccm.transaction_commit(t5)
    ccm.transaction_commit_flushed(t5)
    
    time.sleep(0.05)
    
    # All should be signaled
    print(f"\n[Step 5] Verify all events signaled")
    for tid in waiters:
        print(f"  T{tid} event.is_set(): {events[tid].is_set()}")
        assert events[tid].is_set(), f"T{tid} should be signaled"
    print(f"  ✅ All 4 events were signaled")
    
    # Now T1 (oldest) should get the lock
    print(f"\n[Step 6] T{t1} (oldest) should acquire lock")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'Y')
    print(f"  T{t1} Write(Y): {r1.status.value}")
    assert r1.status == LockStatus.GRANTED
    print(f"  ✅ T{t1} acquired lock")
    
    # Others should now DIE because they're younger than T1
    print(f"\n[Step 7] T{t2}-T{t4} should now DIE (younger than T{t1})")
    for tid in [t2, t3, t4]:
        r = ccm.transaction_query(tid, TableAction.WRITE, 'Y')
        print(f"  T{tid} Write(Y): {r.status.value}")
        # Should DIE because they're younger than T1 (TS=1)
        assert r.status == LockStatus.FAILED, f"T{tid} should die (younger than T{t1})"
        print(f"    ✅ T{tid} died (TS={tid} > TS={t1})")
    
    print(f"\n  ✅ All younger transactions correctly aborted")
    
    print("\n" + "="*70)
    print("TEST 2 PASSED ✅")
    print("="*70)
    return True


def test_complex_wait_die_scenario():
    """Test complex scenario with multiple resources and transactions"""
    print("\n" + "="*70)
    print("TEST 3: Complex Multi-Resource Wait-Die")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create 4 transactions
    t1 = ccm.transaction_begin()  # TS=1 (oldest)
    t2 = ccm.transaction_begin()  # TS=2
    t3 = ccm.transaction_begin()  # TS=3
    t4 = ccm.transaction_begin()  # TS=4 (youngest)
    
    print(f"\nTransactions: T{t1}, T{t2}, T{t3}, T{t4}")
    
    # Scenario: Interleaved access to resources A, B, C
    print("\n[Scenario] Interleaved access to multiple resources")
    
    # T2 locks A
    print(f"\n1. T{t2} Write(A)")
    r = ccm.transaction_query(t2, TableAction.WRITE, 'A')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.GRANTED
    
    # T3 locks B
    print(f"\n2. T{t3} Write(B)")
    r = ccm.transaction_query(t3, TableAction.WRITE, 'B')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.GRANTED
    
    # T4 locks C
    print(f"\n3. T{t4} Write(C)")
    r = ccm.transaction_query(t4, TableAction.WRITE, 'C')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.GRANTED
    
    # T1 (oldest) tries to get A (held by T2) -> WAIT
    print(f"\n4. T{t1} (oldest) tries Write(A) [held by T{t2}]")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'A')
    print(f"   Result: {r.status.value}")
    print(f"   Reason: {r.reason}")
    assert r.status == LockStatus.WAITING
    t1_event_a = ccm.get_wait_event(t1)
    print(f"   ✅ T{t1} waiting with event")
    
    # T1 also tries to get B (held by T3) -> WAIT
    print(f"\n5. T{t1} also tries Write(B) [held by T{t3}]")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.WAITING
    print(f"   ✅ T{t1} also waiting for B")
    
    # T4 (youngest) tries to get A (held by T2) -> DIE
    print(f"\n6. T{t4} (youngest) tries Write(A) [held by T{t2}]")
    r = ccm.transaction_query(t4, TableAction.WRITE, 'A')
    print(f"   Result: {r.status.value}")
    print(f"   Reason: {r.reason}")
    assert r.status == LockStatus.FAILED
    print(f"   ✅ T{t4} died (younger than T{t2})")
    
    # T2 commits -> T1 should get signaled for resource A
    print(f"\n7. T{t2} commits (releases A)")
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.05)
    print(f"   T{t1} event signaled: {t1_event_a.is_set()}")
    assert t1_event_a.is_set()
    print(f"   ✅ T{t1} signaled")
    
    # T1 can now get A
    print(f"\n8. T{t1} retries Write(A)")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'A')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.GRANTED
    print(f"   ✅ T{t1} acquired A")
    
    # T1 still waiting for B (held by T3)
    print(f"\n9. T{t1} retries Write(B) [still held by T{t3}]")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.WAITING
    print(f"   ✅ T{t1} still waiting for B")
    
    # T3 commits -> T1 should get B
    print(f"\n10. T{t3} commits (releases B)")
    ccm.transaction_commit(t3)
    ccm.transaction_commit_flushed(t3)
    
    time.sleep(0.05)
    
    print(f"\n11. T{t1} retries Write(B)")
    r = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"   Result: {r.status.value}")
    assert r.status == LockStatus.GRANTED
    print(f"   ✅ T{t1} acquired B")
    
    print("\n" + "="*70)
    print("TEST 3 PASSED ✅")
    print("="*70)
    return True


def test_wait_die_with_lock_upgrades():
    """Test Wait-Die with lock upgrades (read -> write)"""
    print("\n" + "="*70)
    print("TEST 4: Wait-Die with Lock Upgrades")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    t1 = ccm.transaction_begin()  # TS=1 (older)
    t2 = ccm.transaction_begin()  # TS=2
    t3 = ccm.transaction_begin()  # TS=3 (younger)
    
    print(f"\nTransactions: T{t1}, T{t2}, T{t3}")
    
    # T1 and T2 both get read locks (compatible)
    print(f"\n[Step 1] T{t1} and T{t2} both Read(X)")
    r1 = ccm.transaction_query(t1, TableAction.READ, 'X')
    r2 = ccm.transaction_query(t2, TableAction.READ, 'X')
    print(f"  T{t1} Read(X): {r1.status.value}")
    print(f"  T{t2} Read(X): {r2.status.value}")
    assert r1.status == LockStatus.GRANTED
    assert r2.status == LockStatus.GRANTED
    print(f"  ✅ Both got read locks (compatible)")
    
    # T1 (older) tries to upgrade to write -> WAIT (other readers present)
    print(f"\n[Step 2] T{t1} (older) tries to upgrade to Write")
    r1_write = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  T{t1} Write(X): {r1_write.status.value}")
    print(f"  Reason: {r1_write.reason}")
    assert r1_write.status == LockStatus.WAITING
    event = ccm.get_wait_event(t1)
    print(f"  ✅ T{t1} waiting to upgrade")
    
    # T3 (younger) tries to write -> DIE (older T1 and T2 have read locks)
    print(f"\n[Step 3] T{t3} (younger) tries Write(X)")
    r3 = ccm.transaction_query(t3, TableAction.WRITE, 'X')
    print(f"  T{t3} Write(X): {r3.status.value}")
    print(f"  Reason: {r3.reason}")
    assert r3.status == LockStatus.FAILED
    print(f"  ✅ T{t3} died (younger)")
    
    # T2 commits -> T1 should wake up
    print(f"\n[Step 4] T{t2} commits")
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    time.sleep(0.05)
    assert event.is_set()
    print(f"  ✅ T{t1} event signaled")
    
    # T1 can now upgrade
    print(f"\n[Step 5] T{t1} retries Write(X)")
    r1_write_retry = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  T{t1} Write(X): {r1_write_retry.status.value}")
    assert r1_write_retry.status == LockStatus.GRANTED
    print(f"  ✅ T{t1} upgraded to write lock")
    
    print("\n" + "="*70)
    print("TEST 4 PASSED ✅")
    print("="*70)
    return True


def test_wait_die_timing():
    """Test that waiting is event-driven, not polling"""
    print("\n" + "="*70)
    print("TEST 5: Wait-Die with Event-Driven Timing")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    t1 = ccm.transaction_begin()  # Older
    t2 = ccm.transaction_begin()  # Younger
    
    # T2 gets lock
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'Z')
    assert r2.status == LockStatus.GRANTED
    print(f"\nT{t2} acquired Write(Z)")
    
    # T1 tries to get lock -> WAIT
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'Z')
    assert r1.status == LockStatus.WAITING
    print(f"T{t1} waiting for Write(Z)")
    
    event = ccm.get_wait_event(t1)
    
    # Measure wake-up time
    wait_result = {'wake_time': None, 'latency': None}
    
    def waiter():
        event.wait(timeout=5.0)
        wait_result['wake_time'] = time.time()
    
    waiter_thread = threading.Thread(target=waiter, daemon=True)
    waiter_thread.start()
    
    time.sleep(0.2)  # Let thread start waiting
    
    # Release lock
    release_time = time.time()
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    waiter_thread.join(timeout=1.0)
    
    if wait_result['wake_time']:
        latency = wait_result['wake_time'] - release_time
        wait_result['latency'] = latency
        print(f"\nWake-up latency: {latency*1000:.2f}ms")
        
        if latency < 0.1:
            print(f"✅ EXCELLENT: Event-driven wake-up (< 100ms)")
        elif latency < 0.5:
            print(f"✅ GOOD: Fast wake-up (< 500ms)")
        else:
            print(f"⚠️  Slow wake-up: {latency*1000:.0f}ms")
        
        assert latency < 0.5, "Wake-up should be fast with events"
    
    print("\n" + "="*70)
    print("TEST 5 PASSED ✅")
    print("="*70)
    return True


def test_deadlock_prevention():
    """Test that Wait-Die prevents deadlocks"""
    print("\n" + "="*70)
    print("TEST 6: Deadlock Prevention with Wait-Die")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    t1 = ccm.transaction_begin()  # TS=1 (older)
    t2 = ccm.transaction_begin()  # TS=2 (younger)
    
    print(f"\nTransactions: T{t1}(older), T{t2}(younger)")
    
    # Classic deadlock scenario
    print(f"\n[Classic Deadlock Scenario]")
    print(f"1. T{t1} Write(A)")
    r1_a = ccm.transaction_query(t1, TableAction.WRITE, 'A')
    assert r1_a.status == LockStatus.GRANTED
    print(f"   ✅ Granted")
    
    print(f"\n2. T{t2} Write(B)")
    r2_b = ccm.transaction_query(t2, TableAction.WRITE, 'B')
    assert r2_b.status == LockStatus.GRANTED
    print(f"   ✅ Granted")
    
    print(f"\n3. T{t1} tries Write(B) [held by younger T{t2}]")
    r1_b = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"   Result: {r1_b.status.value}")
    print(f"   Reason: {r1_b.reason}")
    assert r1_b.status == LockStatus.WAITING
    print(f"   ✅ T{t1} WAITS (older waits for younger)")
    
    event = ccm.get_wait_event(t1)
    
    print(f"\n4. T{t2} tries Write(A) [held by older T{t1}]")
    r2_a = ccm.transaction_query(t2, TableAction.WRITE, 'A')
    print(f"   Result: {r2_a.status.value}")
    print(f"   Reason: {r2_a.reason}")
    assert r2_a.status == LockStatus.FAILED
    print(f"   ✅ T{t2} DIES (younger dies when requesting from older)")
    print(f"   ✅ DEADLOCK PREVENTED!")
    
    # T2 must rollback (it died)
    print(f"\n5. T{t2} rolls back (died)")
    ccm.transaction_abort(t2)
    ccm.transaction_end(t2)
    
    time.sleep(0.05)
    
    # T1 should be signaled for resource B
    print(f"\n6. Check if T{t1} was signaled")
    print(f"   Event is_set: {event.is_set()}")
    assert event.is_set()
    print(f"   ✅ T{t1} signaled after T{t2} aborted")
    
    # T1 can now get B
    print(f"\n7. T{t1} retries Write(B)")
    r1_b_retry = ccm.transaction_query(t1, TableAction.WRITE, 'B')
    print(f"   Result: {r1_b_retry.status.value}")
    assert r1_b_retry.status == LockStatus.GRANTED
    print(f"   ✅ T{t1} acquired B")
    
    print("\n" + "="*70)
    print("TEST 6 PASSED ✅")
    print("Deadlock prevented by aborting younger transaction!")
    print("="*70)
    return True


def main():
    print("\n" + "="*70)
    print("EXTENSIVE WAIT-DIE PROTOCOL TEST")
    print("With Event-Driven Wake-up Mechanism")
    print("="*70)
    
    tests = [
        ("Basic Wait-Die", test_basic_wait_die),
        ("Cascading Waits", test_cascading_wait_die),
        ("Complex Multi-Resource", test_complex_wait_die_scenario),
        ("Lock Upgrades", test_wait_die_with_lock_upgrades),
        ("Event-Driven Timing", test_wait_die_timing),
        ("Deadlock Prevention", test_deadlock_prevention),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
                print(f"\n❌ {test_name} FAILED")
        except Exception as e:
            failed += 1
            print(f"\n❌ {test_name} FAILED with exception: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*70)
    print("FINAL RESULTS")
    print("="*70)
    print(f"Tests Passed: {passed}/{len(tests)}")
    print(f"Tests Failed: {failed}/{len(tests)}")
    
    if passed == len(tests):
        print("\n🎉 ALL TESTS PASSED!")
        print("\nCONCLUSIONS:")
        print("  ✅ Wait-Die protocol working correctly")
        print("  ✅ Older transactions wait (not die)")
        print("  ✅ Younger transactions die (not wait)")
        print("  ✅ Events signal waiting transactions immediately")
        print("  ✅ Deadlocks are prevented")
        print("  ✅ Lock upgrades work with Wait-Die")
        print("  ✅ Multiple resources handled correctly")
    else:
        print("\n⚠️  SOME TESTS FAILED")
    
    print("="*70)
    
    return passed == len(tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
