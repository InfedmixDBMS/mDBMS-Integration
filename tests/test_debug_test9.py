"""Test just Test 9 with debugging"""
import sys
import time
import threading
from client import DBClient

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
            print("  ✗ Holder failed to connect")
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
            else:
                print(f"  ✗ Holder query failed: {resp.get('error')}")
            
            client.rollback_transaction()
            print("  ✓ Holder rolled back")
        finally:
            client.disconnect()
    
    def waiter_thread(wid):
        client = DBClient(host=host, port=port)
        if not client.connect():
            print(f"  ✗ Waiter {wid} failed to connect")
            return
        
        try:
            time.sleep(0.3)  # Let holder acquire
            client.begin_transaction()
            print(f"  • Waiter {wid} requesting lock...")
            
            resp = client.execute_query(f"UPDATE products SET price=price+{wid} WHERE id=1")
            
            print(f"  • Waiter {wid} got response: {resp.get('success')}, error: {resp.get('error', 'none')}")
            
            if resp.get('success'):
                with lock:
                    results['waiters'] += 1
                    print(f"  ✓ Waiter {wid} acquired lock and succeeded")
            else:
                print(f"  ✗ Waiter {wid} failed: {resp.get('error')}")
            
            client.commit_transaction()
        except Exception as e:
            print(f"  ✗ Waiter {wid} exception: {e}")
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
    print(f"\n  Results: holder={results['holder']}, waiters={results['waiters']}/7")
    print("  ✓ Test 9 PASSED" if success else "  ✗ Test 9 FAILED")
    return success

if __name__ == '__main__':
    host = 'localhost'
    port = 5555
    
    # Test connection
    client = DBClient(host=host, port=port)
    if not client.connect():
        print("Failed to connect")
        sys.exit(1)
    client.disconnect()
    print("Connected successfully\n")
    
    test_9_rollback_chain_reaction(host, port)
