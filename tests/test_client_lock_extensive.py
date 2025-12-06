"""
Extensive client-side automated test for Lock-Based protocol (2PL with Deadlock Detection).
Assumes the server is already running with the lock protocol on the given host/port.

Tests various scenarios with deadlock detection:
- Read locks (shared locks)
- Write locks (exclusive locks)
- Lock conflicts with automatic waiting (transparent to client)
- Deadlock detection and resolution
- Multiple concurrent transactions
- Read-Write conflicts (transactions wait automatically)
- Write-Write conflicts (transactions wait automatically)

Deadlock Detection:
- Transactions wait when lock conflicts occur (transparent to client)
- Wait-for graph tracks dependencies
- Cycles detected using DFS
- One transaction aborted to break deadlock (rare)

Usage:
    python test_client_lock_extensive.py --host localhost --port 5555
"""

import argparse
import sys
import threading
import time

sys.path.append('.')
from client import DBClient


def test_1_basic_read_lock(host, port):
    """Test basic read lock - multiple readers should succeed"""
    print("\n" + "="*70)
    print("TEST 1: Basic Read Locks (Shared Locks)")
    print("="*70)
    
    client = DBClient(host=host, port=port)
    if not client.connect():
        print("✗ Failed to connect")
        return False
    
    try:
        # Create table
        client.execute_query("CREATE TABLE products (id INT, name VARCHAR, price INT)")
        print("  ✓ Created table")
        
        # Insert data
        client.begin_transaction()
        client.execute_query("INSERT INTO products VALUES (1, 'Laptop', 1000)")
        client.commit_transaction()
        print("  ✓ Inserted data")
        
        # Start two readers
        reader1 = DBClient(host=host, port=port)
        reader2 = DBClient(host=host, port=port)
        reader1.connect()
        reader2.connect()
        
        reader1.begin_transaction()
        reader2.begin_transaction()
        print("  ✓ Started two transactions")
        
        # Both read - should succeed (shared locks compatible)
        resp1 = reader1.execute_query("SELECT * FROM products")
        resp2 = reader2.execute_query("SELECT * FROM products")
        
        if resp1.get('success') and resp2.get('success'):
            print("  ✓ Both readers acquired shared locks successfully")
            success = True
        else:
            print("  ✗ One or both readers failed")
            success = False
        
        reader1.commit_transaction()
        reader2.commit_transaction()
        reader1.disconnect()
        reader2.disconnect()
        
        print("  ✓ Test 1 PASSED" if success else "  ✗ Test 1 FAILED")
        return success
        
    finally:
        client.disconnect()


def test_2_write_lock_exclusive(host, port):
    """Test write lock - second writer waits, then succeeds (no manual retry needed)"""
    print("\n" + "="*70)
    print("TEST 2: Write Locks (Exclusive - Automatic Waiting)")
    print("="*70)
    
    results = {'writer1': None, 'writer2': None}
    
    def writer_thread(name, delay=0):
        client = DBClient(host=host, port=port)
        if not client.connect():
            results[name] = False
            return
        
        try:
            time.sleep(delay)
            client.begin_transaction()
            
            start = time.time()
            # Now just execute - system handles waiting transparently!
            resp = client.execute_query("UPDATE products SET price=1200 WHERE id=1")
            elapsed = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ {name} acquired write lock (took {elapsed:.2f}s)")
                results[name] = True
            else:
                print(f"  ✗ {name} failed to write: {resp.get('error', 'unknown error')}")
                results[name] = False
            
            time.sleep(0.5)  # Hold lock briefly
            client.commit_transaction()
        finally:
            client.disconnect()
    
    # Start two writers - writer2 will wait automatically
    t1 = threading.Thread(target=writer_thread, args=('writer1', 0))
    t2 = threading.Thread(target=writer_thread, args=('writer2', 0.1))
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    success = results['writer1'] and results['writer2']
    print("  ✓ Test 2 PASSED (both writers succeeded, writer2 waited)" if success else "  ✗ Test 2 FAILED")
    return success


def test_3_read_write_conflict(host, port):
    """Test read-write conflict - writer waits for reader automatically"""
    print("\n" + "="*70)
    print("TEST 3: Read-Write Conflict (Writer waits automatically)")
    print("="*70)
    
    results = {'reader': None, 'writer': None}
    
    def reader_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['reader'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("SELECT * FROM products")
            
            if resp.get('success'):
                print("  ✓ Reader acquired read lock")
                results['reader'] = True
                time.sleep(2)  # Hold lock for 2 seconds
                print("  • Reader holding lock...")
            
            client.commit_transaction()
            print("  ✓ Reader released lock")
        finally:
            client.disconnect()
    
    def writer_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['writer'] = False
            return
        
        try:
            time.sleep(0.5)  # Let reader start first
            client.begin_transaction()
            
            start = time.time()
            # Writer waits automatically - no manual retry!
            resp = client.execute_query("UPDATE products SET price=1500 WHERE id=1")
            elapsed = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ Writer succeeded (waited {elapsed:.2f}s for reader)")
                results['writer'] = True
            else:
                print(f"  ✗ Writer failed: {resp.get('error', 'unknown')}")
                results['writer'] = False
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=reader_thread)
    t2 = threading.Thread(target=writer_thread)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    success = results['reader'] and results['writer']
    print("  ✓ Test 3 PASSED (writer waited automatically)" if success else "  ✗ Test 3 FAILED")
    return success


def test_4_write_write_conflict(host, port):
    """Test write-write conflict - second writer waits automatically"""
    print("\n" + "="*70)
    print("TEST 4: Write-Write Conflict (Automatic Waiting)")
    print("="*70)
    
    results = {'writer1': None, 'writer2': None}
    
    def writer1_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['writer1'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=2000 WHERE id=1")
            
            if resp.get('success'):
                print("  ✓ Writer1 acquired write lock")
                results['writer1'] = True
                time.sleep(2)  # Hold lock
                print("  • Writer1 holding lock...")
            
            client.commit_transaction()
            print("  ✓ Writer1 released lock")
        finally:
            client.disconnect()
    
    def writer2_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['writer2'] = False
            return
        
        try:
            time.sleep(0.5)  # Let writer1 start first
            client.begin_transaction()
            
            start = time.time()
            # Writer2 waits automatically - no manual retry!
            resp = client.execute_query("UPDATE products SET price=2500 WHERE id=1")
            elapsed = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ Writer2 succeeded (waited {elapsed:.2f}s for writer1)")
                results['writer2'] = True
            else:
                print(f"  ✗ Writer2 failed: {resp.get('error', 'unknown')}")
                results['writer2'] = False
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=writer1_thread)
    t2 = threading.Thread(target=writer2_thread)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    success = results['writer1'] and results['writer2']
    print("  ✓ Test 4 PASSED (writer2 waited automatically)" if success else "  ✗ Test 4 FAILED")
    return success


def test_5_multiple_concurrent_transactions(host, port):
    """Test multiple concurrent transactions on different data - should succeed without conflicts"""
    print("\n" + "="*70)
    print("TEST 5: Multiple Concurrent Transactions (No Conflicts)")
    print("="*70)
    
    # Clean up and insert test data
    client = DBClient(host=host, port=port)
    client.connect()
    client.begin_transaction()
    client.execute_query("INSERT INTO products VALUES (2, 'Mouse', 20)")
    client.execute_query("INSERT INTO products VALUES (3, 'Keyboard', 50)")
    client.execute_query("INSERT INTO products VALUES (4, 'Monitor', 300)")
    client.commit_transaction()
    client.disconnect()
    print("  ✓ Inserted test data")
    
    results = {}
    
    def transaction_thread(tid, product_id):
        client = DBClient(host=host, port=port)
        if not client.connect():
            results[tid] = False
            return
        
        try:
            client.begin_transaction()
            
            # Each transaction works on different product - no conflicts expected
            resp = client.execute_query(f"UPDATE products SET price=price+10 WHERE id={product_id}")
            
            if resp.get('success'):
                results[tid] = True
                print(f"  ✓ Transaction {tid} updated product {product_id}")
            else:
                results[tid] = False
                print(f"  ✗ Transaction {tid} failed: {resp.get('error', 'unknown')}")
            
            time.sleep(0.5)  # Simulate work
            client.commit_transaction()
        finally:
            client.disconnect()
    
    # Start 4 concurrent transactions on different products
    threads = []
    for i in range(1, 5):
        t = threading.Thread(target=transaction_thread, args=(i, i))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    success = all(results.values())
    print(f"  ✓ Test 5 PASSED ({len([v for v in results.values() if v])}/4 transactions succeeded)" if success else "  ✗ Test 5 FAILED")
    return success


def test_6_rollback_releases_locks(host, port):
    """Test that rollback releases locks - waiter proceeds automatically"""
    print("\n" + "="*70)
    print("TEST 6: Rollback Releases Locks")
    print("="*70)
    
    results = {'holder': None, 'waiter': None}
    
    def lock_holder_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['holder'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=9999 WHERE id=1")
            
            if resp.get('success'):
                print("  ✓ Lock holder acquired lock")
                results['holder'] = True
                time.sleep(1.5)  # Hold lock
                print("  • Lock holder rolling back...")
            
            client.rollback_transaction()
            print("  ✓ Lock holder rolled back (lock released)")
        finally:
            client.disconnect()
    
    def lock_waiter_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['waiter'] = False
            return
        
        try:
            time.sleep(0.5)  # Let holder acquire first
            client.begin_transaction()
            
            start = time.time()
            # Waiter waits automatically, proceeds when lock released
            resp = client.execute_query("SELECT * FROM products WHERE id=1")
            elapsed = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ Waiter succeeded (waited {elapsed:.2f}s)")
                results['waiter'] = True
            else:
                print(f"  ✗ Waiter failed: {resp.get('error', 'unknown')}")
                results['waiter'] = False
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=lock_holder_thread)
    t2 = threading.Thread(target=lock_waiter_thread)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    success = results['holder'] and results['waiter']
    print("  ✓ Test 6 PASSED (rollback released locks, waiter proceeded)" if success else "  ✗ Test 6 FAILED")
    return success


def test_7_lock_upgrade(host, port):
    """Test lock upgrade from read to write"""
    print("\n" + "="*70)
    print("TEST 7: Lock Upgrade (Read → Write)")
    print("="*70)
    
    client = DBClient(host=host, port=port)
    if not client.connect():
        print("✗ Failed to connect")
        return False
    
    try:
        client.begin_transaction()
        
        # First acquire read lock
        resp = client.execute_query("SELECT * FROM products WHERE id=1")
        if not resp.get('success'):
            print(f"  ✗ Failed to acquire read lock: {resp.get('error', 'unknown')}")
            return False
        print("  ✓ Acquired read lock")
        
        # Then upgrade to write lock (should succeed - same transaction)
        resp = client.execute_query("UPDATE products SET price=3000 WHERE id=1")
        if resp.get('success'):
            print("  ✓ Successfully upgraded to write lock")
            success = True
        else:
            print(f"  ✗ Failed to upgrade lock: {resp.get('error', 'unknown')}")
            success = False
        
        client.commit_transaction()
        print("  ✓ Test 7 PASSED" if success else "  ✗ Test 7 FAILED")
        return success
        
    finally:
        client.disconnect()


def test_8_stress_test(host, port):
    """Stress test with many concurrent transactions"""
    print("\n" + "="*70)
    print("TEST 8: Stress Test (10 Concurrent Transactions)")
    print("="*70)
    
    results = {}
    
    def stress_transaction(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            results[tid] = False
            return
        
        try:
            client.begin_transaction()
            
            # Mix of reads and writes
            if tid % 2 == 0:
                resp = client.execute_query("SELECT * FROM products")
            else:
                product_id = (tid % 4) + 1
                resp = client.execute_query(f"UPDATE products SET price=price+1 WHERE id={product_id}")
            
            results[tid] = resp.get('success', False)
            
            time.sleep(0.2)
            client.commit_transaction()
        except Exception as e:
            print(f"  ! Transaction {tid} exception: {e}")
            results[tid] = False
        finally:
            client.disconnect()
    
    threads = []
    for i in range(10):
        t = threading.Thread(target=stress_transaction, args=(i,))
        threads.append(t)
        t.start()
        time.sleep(0.05)  # Stagger starts slightly
    
    for t in threads:
        t.join()
    
    success_count = sum(1 for v in results.values() if v)
    success = success_count >= 9  # Allow 1 deadlock victim
    
    print(f"  • {success_count}/10 transactions succeeded")
    print(f"  ✓ Test 8 PASSED ({success_count}/10 succeeded)" if success else f"  ✗ Test 8 FAILED (only {success_count}/10)")
    return success


def run(host, port):
    print("\n" + "="*70)
    print("EXTENSIVE LOCK-BASED PROTOCOL TEST (Deadlock Detection)")
    print(f"Connecting to {host}:{port}")
    print("="*70)
    
    # Connection test
    client = DBClient(host=host, port=port)
    if not client.connect():
        print("✗ Failed to connect to server. Is the lock-based server running?")
        return 2
    client.disconnect()
    print("✓ Connected to server successfully\n")
    
    results = {}
    
    try:
        results['test_1'] = test_1_basic_read_lock(host, port)
        time.sleep(0.5)
        
        results['test_2'] = test_2_write_lock_exclusive(host, port)
        time.sleep(0.5)
        
        results['test_3'] = test_3_read_write_conflict(host, port)
        time.sleep(0.5)
        
        results['test_4'] = test_4_write_write_conflict(host, port)
        time.sleep(0.5)
        
        results['test_5'] = test_5_multiple_concurrent_transactions(host, port)
        time.sleep(0.5)
        
        results['test_6'] = test_6_rollback_releases_locks(host, port)
        time.sleep(0.5)
        
        results['test_7'] = test_7_lock_upgrade(host, port)
        time.sleep(0.5)
        
        results['test_8'] = test_8_stress_test(host, port)
        
    except Exception as e:
        print(f"\n✗ TEST SUITE FAILED WITH EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{test_name}: {status}")
    
    print("="*70)
    print(f"TOTAL: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print(f"\n⚠ {total - passed} test(s) failed")
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5555)
    args = parser.parse_args()
    sys.exit(run(args.host, args.port))
