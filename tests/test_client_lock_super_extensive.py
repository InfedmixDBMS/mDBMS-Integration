"""
Super Extensive Lock-Based Protocol Test Suite
Tests complex concurrency scenarios with deadlock detection
"""

import sys
import time
import threading
import argparse
from client import DBClient


def test_1_cascading_waits(host, port):
    """Test cascading waits: T1 holds lock, T2 waits, T3 waits, T1 releases -> T2 proceeds -> T3 proceeds"""
    print("\n" + "="*70)
    print("TEST 1: Cascading Waits (Chain of Waiters)")
    print("="*70)
    
    results = {'t1': None, 't2': None, 't3': None}
    times = {'t2': 0, 't3': 0}
    
    def t1_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t1'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=100 WHERE id=1")
            
            if resp.get('success'):
                print("  ✓ T1 acquired lock")
                results['t1'] = True
                time.sleep(2.0)  # Hold lock for 2 seconds
                print("  • T1 releasing lock...")
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def t2_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t2'] = False
            return
        
        try:
            time.sleep(0.3)  # Let T1 acquire first
            client.begin_transaction()
            
            start = time.time()
            resp = client.execute_query("UPDATE products SET price=200 WHERE id=1")
            times['t2'] = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ T2 acquired lock (waited {times['t2']:.2f}s)")
                results['t2'] = True
                time.sleep(1.0)  # Hold lock for 1 second
                print("  • T2 releasing lock...")
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def t3_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t3'] = False
            return
        
        try:
            time.sleep(0.6)  # Let T1 and T2 start
            client.begin_transaction()
            
            start = time.time()
            resp = client.execute_query("UPDATE products SET price=300 WHERE id=1")
            times['t3'] = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ T3 acquired lock (waited {times['t3']:.2f}s)")
                results['t3'] = True
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=t1_thread)
    t2 = threading.Thread(target=t2_thread)
    t3 = threading.Thread(target=t3_thread)
    
    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()
    
    # Verify timing: T2 should wait ~1.7s, T3 should wait ~2.4s
    success = (results['t1'] and results['t2'] and results['t3'] and 
               times['t2'] > 1.5 and times['t3'] > 2.0)
    
    print("  ✓ Test 1 PASSED (cascading waits worked)" if success else "  ✗ Test 1 FAILED")
    return success


def test_2_multiple_readers_then_writer(host, port):
    """Test multiple concurrent readers, then a writer waits for all"""
    print("\n" + "="*70)
    print("TEST 2: Multiple Readers, Then Writer Waits")
    print("="*70)
    
    results = {'readers': 0, 'writer': None}
    writer_wait_time = [0]
    
    def reader_thread(reader_id):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("SELECT * FROM products WHERE id=1")
            
            if resp.get('success'):
                results['readers'] += 1
                print(f"  ✓ Reader {reader_id} acquired shared lock")
                time.sleep(1.5)  # Hold lock
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def writer_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['writer'] = False
            return
        
        try:
            time.sleep(0.5)  # Let readers start
            client.begin_transaction()
            
            start = time.time()
            resp = client.execute_query("UPDATE products SET price=999 WHERE id=1")
            writer_wait_time[0] = time.time() - start
            
            if resp.get('success'):
                print(f"  ✓ Writer acquired lock (waited {writer_wait_time[0]:.2f}s)")
                results['writer'] = True
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    # Start 5 readers
    readers = [threading.Thread(target=reader_thread, args=(i,)) for i in range(1, 6)]
    writer = threading.Thread(target=writer_thread)
    
    for r in readers:
        r.start()
    writer.start()
    
    for r in readers:
        r.join()
    writer.join()
    
    success = results['readers'] == 5 and results['writer'] and writer_wait_time[0] > 1.0
    print("  ✓ Test 2 PASSED (writer waited for all readers)" if success else "  ✗ Test 2 FAILED")
    return success


def test_3_lock_upgrade_conflict(host, port):
    """Test lock upgrade when another reader exists"""
    print("\n" + "="*70)
    print("TEST 3: Lock Upgrade with Conflict")
    print("="*70)
    
    results = {'reader': None, 'upgrader': None}
    upgrade_wait_time = [0]
    
    def reader_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['reader'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("SELECT * FROM products WHERE id=1")
            
            if resp.get('success'):
                print("  ✓ Reader acquired shared lock")
                results['reader'] = True
                time.sleep(2.0)  # Hold lock
                print("  • Reader releasing lock...")
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def upgrader_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['upgrader'] = False
            return
        
        try:
            time.sleep(0.3)  # Let reader acquire first
            client.begin_transaction()
            
            # First get read lock
            resp1 = client.execute_query("SELECT * FROM products WHERE id=1")
            if resp1.get('success'):
                print("  ✓ Upgrader acquired shared lock")
                
                # Now try to upgrade to write lock
                start = time.time()
                resp2 = client.execute_query("UPDATE products SET price=500 WHERE id=1")
                upgrade_wait_time[0] = time.time() - start
                
                if resp2.get('success'):
                    print(f"  ✓ Upgrader upgraded to exclusive lock (waited {upgrade_wait_time[0]:.2f}s)")
                    results['upgrader'] = True
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=reader_thread)
    t2 = threading.Thread(target=upgrader_thread)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    success = results['reader'] and results['upgrader'] and upgrade_wait_time[0] > 1.5
    print("  ✓ Test 3 PASSED (upgrade waited for other reader)" if success else "  ✗ Test 3 FAILED")
    return success


def test_4_simple_deadlock_detection(host, port):
    """Test simple deadlock: T1 holds A wants B, T2 holds B wants A"""
    print("\n" + "="*70)
    print("TEST 4: Simple Deadlock Detection (2 transactions)")
    print("="*70)
    
    # Setup: Insert two rows
    client = DBClient(host=host, port=port)
    client.connect()
    client.begin_transaction()
    client.execute_query("INSERT INTO products VALUES (5, 'ItemA', 100)")
    client.execute_query("INSERT INTO products VALUES (6, 'ItemB', 200)")
    client.commit_transaction()
    client.disconnect()
    print("  ✓ Setup complete (inserted items A and B)")
    
    results = {'t1': None, 't2': None, 'deadlock_detected': False}
    
    def t1_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t1'] = False
            return
        
        try:
            client.begin_transaction()
            
            # T1: Lock item A
            resp1 = client.execute_query("UPDATE products SET price=price+10 WHERE id=5")
            if resp1.get('success'):
                print("  ✓ T1 acquired lock on Item A")
                time.sleep(0.5)
                
                # T1: Try to lock item B (will wait for T2)
                print("  • T1 requesting lock on Item B...")
                resp2 = client.execute_query("UPDATE products SET price=price+10 WHERE id=6")
                
                if resp2.get('success'):
                    print("  ✓ T1 acquired lock on Item B")
                    results['t1'] = True
                    client.commit_transaction()
                else:
                    print(f"  ! T1 aborted: {resp2.get('error', 'unknown')}")
                    if 'Deadlock' in str(resp2.get('error', '')):
                        results['deadlock_detected'] = True
                    results['t1'] = False
                    client.rollback_transaction()
            
        finally:
            client.disconnect()
    
    def t2_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t2'] = False
            return
        
        try:
            time.sleep(0.2)  # Let T1 acquire A first
            client.begin_transaction()
            
            # T2: Lock item B
            resp1 = client.execute_query("UPDATE products SET price=price+20 WHERE id=6")
            if resp1.get('success'):
                print("  ✓ T2 acquired lock on Item B")
                time.sleep(0.5)
                
                # T2: Try to lock item A (will create deadlock)
                print("  • T2 requesting lock on Item A...")
                resp2 = client.execute_query("UPDATE products SET price=price+20 WHERE id=5")
                
                if resp2.get('success'):
                    print("  ✓ T2 acquired lock on Item A")
                    results['t2'] = True
                    client.commit_transaction()
                else:
                    print(f"  ! T2 aborted: {resp2.get('error', 'unknown')}")
                    if 'Deadlock' in str(resp2.get('error', '')):
                        results['deadlock_detected'] = True
                    results['t2'] = False
                    client.rollback_transaction()
            
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=t1_thread)
    t2 = threading.Thread(target=t2_thread)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Success if deadlock was detected and one transaction completed
    success = results['deadlock_detected'] and (results['t1'] or results['t2'])
    print("  ✓ Test 4 PASSED (deadlock detected, one transaction succeeded)" if success else "  ✗ Test 4 FAILED")
    return success


def test_5_complex_deadlock_cycle(host, port):
    """Test 3-way deadlock cycle: T1->T2->T3->T1"""
    print("\n" + "="*70)
    print("TEST 5: Complex Deadlock (3-transaction cycle)")
    print("="*70)
    
    # Setup: Insert three rows
    client = DBClient(host=host, port=port)
    client.connect()
    client.begin_transaction()
    client.execute_query("INSERT INTO products VALUES (7, 'ItemC', 300)")
    client.execute_query("INSERT INTO products VALUES (8, 'ItemD', 400)")
    client.execute_query("INSERT INTO products VALUES (9, 'ItemE', 500)")
    client.commit_transaction()
    client.disconnect()
    print("  ✓ Setup complete (inserted items C, D, E)")
    
    results = {'t1': None, 't2': None, 't3': None, 'deadlocks': 0}
    
    def t1_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t1'] = False
            return
        
        try:
            client.begin_transaction()
            resp1 = client.execute_query("UPDATE products SET price=price+1 WHERE id=7")
            
            if resp1.get('success'):
                print("  ✓ T1 locked Item C")
                time.sleep(0.5)
                
                resp2 = client.execute_query("UPDATE products SET price=price+1 WHERE id=8")
                
                if resp2.get('success'):
                    print("  ✓ T1 locked Item D")
                    results['t1'] = True
                    client.commit_transaction()
                else:
                    print(f"  ! T1 aborted: {resp2.get('error', 'unknown')}")
                    if 'Deadlock' in str(resp2.get('error', '')):
                        results['deadlocks'] += 1
                    results['t1'] = False
                    client.rollback_transaction()
        finally:
            client.disconnect()
    
    def t2_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t2'] = False
            return
        
        try:
            time.sleep(0.2)
            client.begin_transaction()
            resp1 = client.execute_query("UPDATE products SET price=price+2 WHERE id=8")
            
            if resp1.get('success'):
                print("  ✓ T2 locked Item D")
                time.sleep(0.5)
                
                resp2 = client.execute_query("UPDATE products SET price=price+2 WHERE id=9")
                
                if resp2.get('success'):
                    print("  ✓ T2 locked Item E")
                    results['t2'] = True
                    client.commit_transaction()
                else:
                    print(f"  ! T2 aborted: {resp2.get('error', 'unknown')}")
                    if 'Deadlock' in str(resp2.get('error', '')):
                        results['deadlocks'] += 1
                    results['t2'] = False
                    client.rollback_transaction()
        finally:
            client.disconnect()
    
    def t3_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['t3'] = False
            return
        
        try:
            time.sleep(0.4)
            client.begin_transaction()
            resp1 = client.execute_query("UPDATE products SET price=price+3 WHERE id=9")
            
            if resp1.get('success'):
                print("  ✓ T3 locked Item E")
                time.sleep(0.5)
                
                resp2 = client.execute_query("UPDATE products SET price=price+3 WHERE id=7")
                
                if resp2.get('success'):
                    print("  ✓ T3 locked Item C")
                    results['t3'] = True
                    client.commit_transaction()
                else:
                    print(f"  ! T3 aborted: {resp2.get('error', 'unknown')}")
                    if 'Deadlock' in str(resp2.get('error', '')):
                        results['deadlocks'] += 1
                    results['t3'] = False
                    client.rollback_transaction()
        finally:
            client.disconnect()
    
    t1 = threading.Thread(target=t1_thread)
    t2 = threading.Thread(target=t2_thread)
    t3 = threading.Thread(target=t3_thread)
    
    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()
    
    completed = sum([1 for v in [results['t1'], results['t2'], results['t3']] if v])
    success = results['deadlocks'] > 0 and completed > 0
    
    print(f"  ✓ Test 5 PASSED ({completed} succeeded, {results['deadlocks']} deadlocks detected)" if success else "  ✗ Test 5 FAILED")
    return success


def test_6_high_contention_stress(host, port):
    """Stress test: 20 transactions all competing for same resource"""
    print("\n" + "="*70)
    print("TEST 6: High Contention Stress Test (20 transactions)")
    print("="*70)
    
    results = {'completed': 0, 'failed': 0, 'deadlocks': 0}
    lock = threading.Lock()
    
    def transaction_thread(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            with lock:
                results['failed'] += 1
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=price+1 WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['completed'] += 1
                time.sleep(0.1)  # Brief hold
                client.commit_transaction()
            else:
                with lock:
                    results['failed'] += 1
                    if 'Deadlock' in str(resp.get('error', '')):
                        results['deadlocks'] += 1
                client.rollback_transaction()
        finally:
            client.disconnect()
    
    threads = [threading.Thread(target=transaction_thread, args=(i,)) for i in range(20)]
    
    start = time.time()
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    elapsed = time.time() - start
    
    print(f"  • Completed: {results['completed']}, Failed: {results['failed']}, Deadlocks: {results['deadlocks']}")
    print(f"  • Total time: {elapsed:.2f}s")
    
    # Success if most transactions completed (allow some deadlocks)
    success = results['completed'] >= 15
    print("  ✓ Test 6 PASSED (high contention handled)" if success else "  ✗ Test 6 FAILED")
    return success


def test_7_interleaved_reads_writes(host, port):
    """Test interleaved reads and writes with proper serialization"""
    print("\n" + "="*70)
    print("TEST 7: Interleaved Reads and Writes")
    print("="*70)
    
    results = {'readers': 0, 'writers': 0}
    lock = threading.Lock()
    
    def reader_thread(rid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("SELECT * FROM products WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['readers'] += 1
                time.sleep(0.2)
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def writer_thread(wid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=price+10 WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['writers'] += 1
                time.sleep(0.2)
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    # Interleave 10 readers and 10 writers
    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=reader_thread, args=(i,)))
        threads.append(threading.Thread(target=writer_thread, args=(i,)))
    
    for t in threads:
        t.start()
        time.sleep(0.05)  # Slight stagger
    
    for t in threads:
        t.join()
    
    success = results['readers'] == 10 and results['writers'] == 10
    print(f"  ✓ Test 7 PASSED (all transactions completed)" if success else f"  ✗ Test 7 FAILED (R:{results['readers']}/10, W:{results['writers']}/10)")
    return success


def test_8_long_transaction_vs_short(host, port):
    """Test long-running transaction vs many short transactions"""
    print("\n" + "="*70)
    print("TEST 8: Long Transaction vs Short Transactions")
    print("="*70)
    
    results = {'long': None, 'short': 0}
    lock = threading.Lock()
    
    def long_transaction():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['long'] = False
            return
        
        try:
            client.begin_transaction()
            resp1 = client.execute_query("UPDATE products SET price=1000 WHERE id=1")
            
            if resp1.get('success'):
                print("  ✓ Long transaction acquired lock")
                time.sleep(3.0)  # Hold for 3 seconds
                print("  • Long transaction releasing...")
                results['long'] = True
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def short_transaction(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            time.sleep(0.5)  # Let long transaction start
            client.begin_transaction()
            resp = client.execute_query("SELECT * FROM products WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['short'] += 1
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    long_t = threading.Thread(target=long_transaction)
    short_threads = [threading.Thread(target=short_transaction, args=(i,)) for i in range(5)]
    
    long_t.start()
    for t in short_threads:
        t.start()
    
    long_t.join()
    for t in short_threads:
        t.join()
    
    success = results['long'] and results['short'] == 5
    print(f"  ✓ Test 8 PASSED (long txn completed, {results['short']} short txns waited)" if success else "  ✗ Test 8 FAILED")
    return success


def test_9_rollback_chain_reaction(host, port):
    """Test rollback triggering chain of waiting transactions"""
    print("\n" + "="*70)
    print("TEST 9: Rollback Chain Reaction")
    print("="*70)
    
    results = {'holder': None, 'waiters': 0}
    lock = threading.Lock()
    
    def holder_thread():
        client = DBClient(host=host, port=port)
        if not client.connect():
            results['holder'] = False
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=5000 WHERE id=1")
            
            if resp.get('success'):
                print("  ✓ Holder acquired lock")
                time.sleep(1.5)
                print("  • Holder rolling back...")
                results['holder'] = True
            
            client.rollback_transaction()
        finally:
            client.disconnect()
    
    def waiter_thread(wid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            time.sleep(0.3)  # Let holder acquire
            client.begin_transaction()
            resp = client.execute_query(f"UPDATE products SET price=price+{wid} WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['waiters'] += 1
                    print(f"  ✓ Waiter {wid} acquired lock")
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    holder = threading.Thread(target=holder_thread)
    waiters = [threading.Thread(target=waiter_thread, args=(i,)) for i in range(1, 8)]
    
    holder.start()
    for w in waiters:
        w.start()
    
    holder.join()
    for w in waiters:
        w.join()
    
    success = results['holder'] and results['waiters'] == 7
    print(f"  ✓ Test 9 PASSED (rollback released {results['waiters']} waiters)" if success else "  ✗ Test 9 FAILED")
    return success


def test_10_mixed_operations_multi_table(host, port):
    """Test mixed operations on multiple tables"""
    print("\n" + "="*70)
    print("TEST 10: Mixed Operations on Multiple Tables")
    print("="*70)
    
    # Setup second table
    client = DBClient(host=host, port=port)
    client.connect()
    client.begin_transaction()
    client.execute_query("CREATE TABLE orders (order_id INT, product_id INT, quantity INT)")
    client.execute_query("INSERT INTO orders VALUES (1, 1, 10)")
    client.execute_query("INSERT INTO orders VALUES (2, 2, 20)")
    client.commit_transaction()
    client.disconnect()
    print("  ✓ Setup complete (created orders table)")
    
    results = {'products_txns': 0, 'orders_txns': 0, 'both_txns': 0}
    lock = threading.Lock()
    
    def products_txn(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE products SET price=price+1 WHERE id=1")
            
            if resp.get('success'):
                with lock:
                    results['products_txns'] += 1
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def orders_txn(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp = client.execute_query("UPDATE orders SET quantity=quantity+1 WHERE order_id=1")
            
            if resp.get('success'):
                with lock:
                    results['orders_txns'] += 1
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    def both_txn(tid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            return
        
        try:
            client.begin_transaction()
            resp1 = client.execute_query("UPDATE products SET price=price+1 WHERE id=2")
            resp2 = client.execute_query("UPDATE orders SET quantity=quantity+1 WHERE order_id=2")
            
            if resp1.get('success') and resp2.get('success'):
                with lock:
                    results['both_txns'] += 1
            
            client.commit_transaction()
        finally:
            client.disconnect()
    
    threads = []
    for i in range(5):
        threads.append(threading.Thread(target=products_txn, args=(i,)))
        threads.append(threading.Thread(target=orders_txn, args=(i,)))
        threads.append(threading.Thread(target=both_txn, args=(i,)))
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    success = (results['products_txns'] == 5 and 
               results['orders_txns'] == 5 and 
               results['both_txns'] == 5)
    
    print(f"  ✓ Test 10 PASSED (P:{results['products_txns']}, O:{results['orders_txns']}, B:{results['both_txns']})" 
          if success else "  ✗ Test 10 FAILED")
    return success


def run(host, port):
    print("\n" + "="*70)
    print("SUPER EXTENSIVE LOCK-BASED PROTOCOL TEST")
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
        results['test_1'] = test_1_cascading_waits(host, port)
        time.sleep(0.5)
        
        results['test_2'] = test_2_multiple_readers_then_writer(host, port)
        time.sleep(0.5)
        
        results['test_3'] = test_3_lock_upgrade_conflict(host, port)
        time.sleep(0.5)
        
        results['test_4'] = test_4_simple_deadlock_detection(host, port)
        time.sleep(0.5)
        
        results['test_5'] = test_5_complex_deadlock_cycle(host, port)
        time.sleep(0.5)
        
        results['test_6'] = test_6_high_contention_stress(host, port)
        time.sleep(0.5)
        
        results['test_7'] = test_7_interleaved_reads_writes(host, port)
        time.sleep(0.5)
        
        results['test_8'] = test_8_long_transaction_vs_short(host, port)
        time.sleep(0.5)
        
        results['test_9'] = test_9_rollback_chain_reaction(host, port)
        time.sleep(0.5)
        
        results['test_10'] = test_10_mixed_operations_multi_table(host, port)
        
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
