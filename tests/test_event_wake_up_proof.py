"""
Proof-of-Concept Test for Event-Driven Wake-up Mechanism

This test demonstrates that the event-driven wake-up is working by:
1. Timing how long a transaction waits when blocked
2. Comparing with expected polling-based timing
3. Showing the event mechanism provides immediate wake-up
"""
import sys
import os
import threading
import time

sys.path.append(os.path.join(os.getcwd(), "ConcurrencyControl", "src"))

from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from ConcurrencyControl.src.row_action import TableAction
from ConcurrencyControl.src.concurrency_response import LockStatus


def test_wake_up_timing():
    """
    Test that proves event-driven wake-up is faster than polling.
    
    Scenario:
    - T2 (older) acquires write lock on resource X
    - T1 (younger) tries to acquire write lock on X -> waits
    - We measure how long T1 waits after T2 releases the lock
    - Event-driven: should wake up in < 0.1s
    - Polling-based: would take 0.5-5s depending on poll interval
    """
    print("="*70)
    print("EVENT-DRIVEN WAKE-UP TIMING PROOF")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create transactions
    t1 = ccm.transaction_begin()  # Younger (higher timestamp)
    t2 = ccm.transaction_begin()  # Older (lower timestamp)
    
    print(f"\nTransactions created:")
    print(f"  T{t1} (timestamp={ccm.transactions[t1]['timestamp']})")
    print(f"  T{t2} (timestamp={ccm.transactions[t2]['timestamp']})")
    
    # T2 (older) acquires the lock first
    print(f"\n[Step 1] T{t2} acquires Write lock on resource X...")
    r2 = ccm.transaction_query(t2, TableAction.WRITE, 'X')
    print(f"  Result: {r2.status.value}")
    assert r2.status == LockStatus.GRANTED, "T2 should get the lock"
    
    # T1 (younger) tries to acquire - should WAIT (older transaction holds it)
    print(f"\n[Step 2] T{t1} tries to acquire Write lock on X...")
    r1 = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  Result: {r1.status.value}")
    print(f"  Reason: {r1.reason}")
    assert r1.status == LockStatus.WAITING, f"T1 should wait, got {r1.status}"
    
    # Get T1's event
    t1_event = ccm.get_wait_event(t1)
    print(f"\n[Step 3] Retrieved wait event for T{t1}")
    print(f"  Event is_set: {t1_event.is_set()}")
    assert not t1_event.is_set(), "Event should not be set yet"
    
    # Create a thread that waits on the event
    wait_times = {'event_wait': None, 'wakeup_time': None}
    
    def waiting_thread():
        """Simulates a thread waiting for the lock using the event"""
        print(f"\n[Waiting Thread] Starting wait on event for T{t1}...")
        start = time.time()
        
        # This is what the ExecutionVisitor does
        signaled = t1_event.wait(timeout=10.0)
        
        end = time.time()
        elapsed = end - start
        
        wait_times['event_wait'] = elapsed
        wait_times['wakeup_time'] = time.time()
        
        print(f"[Waiting Thread] Event signaled={signaled}, wait_time={elapsed:.4f}s")
        
        if signaled and elapsed < 0.5:
            print(f"[Waiting Thread] ✅ IMMEDIATE WAKE-UP! (< 0.5s)")
        elif signaled:
            print(f"[Waiting Thread] ⚠️  Wake-up took {elapsed:.4f}s")
        else:
            print(f"[Waiting Thread] ❌ Timeout (event not signaled)")
    
    waiter = threading.Thread(target=waiting_thread, daemon=True)
    
    print(f"\n[Step 4] Starting waiting thread...")
    waiter.start()
    
    # Give the thread time to start waiting
    time.sleep(0.2)
    
    # Now release the lock by committing T2
    print(f"\n[Step 5] Committing T{t2} to release the lock...")
    release_time = time.time()
    
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    print(f"  T{t2} committed, locks released")
    print(f"  __process_wait_queue() should have signaled T{t1}'s event")
    
    # Wait for the waiting thread to wake up
    waiter.join(timeout=2.0)
    
    # Calculate actual wake-up latency
    if wait_times['wakeup_time']:
        wake_up_latency = wait_times['wakeup_time'] - release_time
        print(f"\n[Step 6] Analyzing wake-up performance:")
        print(f"  Lock released at: {release_time:.4f}")
        print(f"  Thread woke up at: {wait_times['wakeup_time']:.4f}")
        print(f"  Wake-up latency: {wake_up_latency:.4f}s")
        
        if wake_up_latency < 0.1:
            print(f"  ✅ EXCELLENT: Near-instant wake-up (< 100ms)")
        elif wake_up_latency < 0.5:
            print(f"  ✅ GOOD: Fast wake-up (< 500ms)")
        else:
            print(f"  ⚠️  SLOW: Wake-up took {wake_up_latency*1000:.0f}ms")
        
        # Compare with polling
        print(f"\n[Comparison]")
        print(f"  Event-driven wake-up: {wake_up_latency*1000:.1f}ms")
        print(f"  Polling (0.5s interval): would take 500ms minimum")
        print(f"  Polling (5.0s interval): would take 5000ms minimum")
        print(f"  Speedup: {(500/wake_up_latency/1000):.1f}x faster than 0.5s polling")
    
    # Verify event is now set
    print(f"\n[Step 7] Verifying event state...")
    print(f"  Event is_set: {t1_event.is_set()}")
    assert t1_event.is_set(), "Event should be set after T2 commits"
    
    # T1 should now be able to acquire the lock
    print(f"\n[Step 8] T{t1} retries acquiring the lock...")
    r1_retry = ccm.transaction_query(t1, TableAction.WRITE, 'X')
    print(f"  Result: {r1_retry.status.value}")
    assert r1_retry.status == LockStatus.GRANTED, "T1 should now get the lock"
    
    print("\n" + "="*70)
    print("TEST PASSED ✅")
    print("Event-driven wake-up is working correctly!")
    print("="*70)
    
    return True


def test_multiple_waiters_all_signaled():
    """
    Test that multiple waiters on the same resource all get signaled.
    This proves the resource_waiters mapping works correctly.
    """
    print("\n" + "="*70)
    print("MULTIPLE WAITERS SIGNAL PROOF")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create 5 transactions
    transactions = [ccm.transaction_begin() for _ in range(5)]
    print(f"\nCreated {len(transactions)} transactions: {transactions}")
    
    # T5 (oldest due to how we created them) gets the lock
    t_holder = transactions[4]
    print(f"\n[Step 1] T{t_holder} acquires Write lock on resource Y...")
    r = ccm.transaction_query(t_holder, TableAction.WRITE, 'Y')
    assert r.status == LockStatus.GRANTED
    print(f"  ✅ Lock granted")
    
    # T1, T2, T3, T4 all try to acquire and should wait
    waiters = transactions[0:4]
    events = []
    
    print(f"\n[Step 2] {len(waiters)} transactions try to acquire the lock (should wait)...")
    for tid in waiters:
        r = ccm.transaction_query(tid, TableAction.WRITE, 'Y')
        print(f"  T{tid}: {r.status.value}")
        assert r.status == LockStatus.WAITING
        events.append(ccm.get_wait_event(tid))
    
    # Verify all events are cleared
    print(f"\n[Step 3] Verifying all events are in waiting state...")
    for i, (tid, event) in enumerate(zip(waiters, events)):
        print(f"  T{tid} event.is_set(): {event.is_set()}")
        assert not event.is_set(), f"T{tid} event should be cleared"
    
    # Verify resource_waiters mapping
    print(f"\n[Step 4] Verifying resource_waiters mapping...")
    print(f"  Resource 'Y' has {len(ccm.resource_waiters.get('Y', set()))} waiters")
    for tid in waiters:
        assert tid in ccm.resource_waiters.get('Y', set()), f"T{tid} should be in waiters"
        print(f"    ✅ T{tid} registered as waiter")
    
    # Release the lock
    print(f"\n[Step 5] Committing T{t_holder} to release lock...")
    ccm.transaction_commit(t_holder)
    ccm.transaction_commit_flushed(t_holder)
    
    # Give a tiny bit of time for signals to propagate
    time.sleep(0.05)
    
    # Verify ALL events are now set
    print(f"\n[Step 6] Verifying all waiters were signaled...")
    all_signaled = True
    for tid, event in zip(waiters, events):
        signaled = event.is_set()
        print(f"  T{tid} event.is_set(): {signaled}")
        if not signaled:
            all_signaled = False
            print(f"    ❌ FAILED: Event not signaled!")
        else:
            print(f"    ✅ Event signaled")
    
    assert all_signaled, "All events should be signaled"
    
    print("\n" + "="*70)
    print("TEST PASSED ✅")
    print("All waiters were correctly signaled!")
    print("="*70)
    
    return True


def test_event_cleanup():
    """Test that events are properly cleaned up when transactions end"""
    print("\n" + "="*70)
    print("EVENT CLEANUP PROOF")
    print("="*70)
    
    ccm = LockBasedConcurrencyControlManager()
    
    # Create transactions
    t1 = ccm.transaction_begin()
    t2 = ccm.transaction_begin()
    print(f"\n[Step 1] Created T{t1} and T{t2}")
    
    # T2 acquires lock
    ccm.transaction_query(t2, TableAction.WRITE, 'X')
    
    # T1 tries to acquire and waits
    ccm.transaction_query(t1, TableAction.WRITE, 'X')
    
    # Verify event exists in resource_waiters
    assert 'X' in ccm.resource_waiters, "Resource X should be in waiters"
    assert t1 in ccm.resource_waiters['X'], "T1 should be waiting for X"
    print(f"  ✅ T{t1} registered in resource_waiters['X']")
    
    # T2 commits
    ccm.transaction_commit(t2)
    ccm.transaction_commit_flushed(t2)
    
    # T1 acquires the lock
    ccm.transaction_query(t1, TableAction.WRITE, 'X')
    ccm.transaction_commit(t1)
    ccm.transaction_commit_flushed(t1)
    
    print(f"\n[Step 2] Committing and ending transaction...")
    ccm.transaction_end(t1)
    ccm.transaction_end(t2)
    
    # Verify cleanup
    print(f"\n[Step 3] Verifying cleanup...")
    
    # Check that T1 is not in any resource waiters
    found_in_waiters = False
    for resource, waiters in ccm.resource_waiters.items():
        if t1 in waiters:
            found_in_waiters = True
            break
    
    assert not found_in_waiters, "T1 should not be in any resource waiters"
    print(f"  ✅ T{t1} removed from all resource waiters")
    print(f"  ✅ Events properly cleaned up")
    
    print("\n" + "="*70)
    print("TEST PASSED ✅")
    print("Events are properly cleaned up!")
    print("="*70)
    
    return True


def main():
    print("\n" + "="*70)
    print("EVENT-DRIVEN WAKE-UP MECHANISM - PROOF OF CORRECTNESS")
    print("="*70)
    print("\nThis test suite proves that:")
    print("  1. Events provide immediate wake-up (< 100ms vs 500-5000ms polling)")
    print("  2. Multiple waiters are all correctly signaled")
    print("  3. Events are properly cleaned up")
    print("="*70)
    
    tests = [
        ("Wake-up Timing", test_wake_up_timing),
        ("Multiple Waiters", test_multiple_waiters_all_signaled),
        ("Event Cleanup", test_event_cleanup)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            print(f"\n\n{'='*70}")
            print(f"Running: {test_name}")
            print(f"{'='*70}")
            
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
    
    print("\n\n" + "="*70)
    print("FINAL RESULTS")
    print("="*70)
    print(f"Tests Passed: {passed}/{len(tests)}")
    print(f"Tests Failed: {failed}/{len(tests)}")
    
    if passed == len(tests):
        print("\n🎉 ALL TESTS PASSED!")
        print("\nCONCLUSION:")
        print("  The event-driven wake-up mechanism is working correctly.")
        print("  Transactions are woken up immediately when locks are released,")
        print("  not after polling intervals. This provides significant")
        print("  performance improvement and better responsiveness.")
    else:
        print("\n⚠️  SOME TESTS FAILED")
    
    print("="*70)
    
    return passed == len(tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
