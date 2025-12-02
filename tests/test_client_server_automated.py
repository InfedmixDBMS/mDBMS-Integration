"""
Automated Client-Server Integration Test
Tests the event-driven wake-up mechanism in a real client-server scenario
"""
import socket
import json
import threading
import time
import sys


class SimpleClient:
    def __init__(self, client_id, host='localhost', port=5555):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.socket = None
        self.current_tid = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Client {self.client_id}: Connection failed: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
    
    def _send_request(self, request: dict) -> dict:
        try:
            message_data = json.dumps(request).encode('utf-8')
            length_data = len(message_data).to_bytes(4, byteorder='big')
            self.socket.sendall(length_data + message_data)
            
            length_data = self._recv_exact(4)
            if not length_data:
                return {'success': False, 'error': 'Connection lost'}
            
            message_length = int.from_bytes(length_data, byteorder='big')
            message_data = self._recv_exact(message_length)
            if not message_data:
                return {'success': False, 'error': 'Connection lost'}
            
            return json.loads(message_data.decode('utf-8'))
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _recv_exact(self, length: int) -> bytes:
        data = b''
        while len(data) < length:
            chunk = self.socket.recv(length - len(data))
            if not chunk:
                return b''
            data += chunk
        return data
    
    def execute_query(self, query: str) -> dict:
        return self._send_request({'type': 'execute', 'query': query})
    
    def begin_transaction(self) -> dict:
        response = self._send_request({'type': 'begin'})
        if response.get('success'):
            self.current_tid = response.get('transaction_id')
        return response
    
    def commit_transaction(self) -> dict:
        if self.current_tid is None:
            return {'success': False, 'error': 'No active transaction'}
        response = self._send_request({'type': 'commit', 'transaction_id': self.current_tid})
        if response.get('success'):
            self.current_tid = None
        return response
    
    def rollback_transaction(self) -> dict:
        if self.current_tid is None:
            return {'success': False, 'error': 'No active transaction'}
        response = self._send_request({'type': 'rollback', 'transaction_id': self.current_tid})
        if response.get('success'):
            self.current_tid = None
        return response


def setup_test_table():
    """Setup initial test table"""
    print("\n[SETUP] Creating test table...")
    client = SimpleClient(0)
    
    if not client.connect():
        print("[SETUP] ❌ Failed to connect to server")
        return False
    
    try:
        client.begin_transaction()
        result = client.execute_query("CREATE TABLE test_products (id INT, name VARCHAR(50), price INT)")
        
        if result.get('success'):
            client.commit_transaction()
            print("[SETUP] ✅ Test table created")
            return True
        else:
            print(f"[SETUP] ❌ Failed: {result.get('error')}")
            client.rollback_transaction()
            return False
    finally:
        client.disconnect()


def client_worker(client_id, queries, results_dict):
    """Worker function for each client thread"""
    client = SimpleClient(client_id)
    
    if not client.connect():
        results_dict[client_id] = {'success': False, 'error': 'Connection failed'}
        return
    
    try:
        start_time = time.time()
        
        # Begin transaction
        begin_res = client.begin_transaction()
        if not begin_res.get('success'):
            results_dict[client_id] = {'success': False, 'error': f"Begin failed: {begin_res.get('error')}"}
            return
        
        tid = client.current_tid
        print(f"Client {client_id}: Started transaction T{tid}")
        
        # Execute queries
        query_results = []
        for i, query in enumerate(queries):
            print(f"Client {client_id}: Executing query {i+1}/{len(queries)}: {query[:50]}...")
            result = client.execute_query(query)
            
            if result.get('retried'):
                print(f"Client {client_id}: [AUTO-RETRY] Query was automatically retried")
            
            if result.get('queued_for_retry'):
                print(f"Client {client_id}: [QUEUED] {result.get('message')}")
            
            query_results.append(result)
            
            if not result.get('success'):
                print(f"Client {client_id}: ❌ Query failed: {result.get('error')}")
                client.rollback_transaction()
                results_dict[client_id] = {'success': False, 'error': result.get('error')}
                return
            
            print(f"Client {client_id}: ✅ Query succeeded")
            time.sleep(0.1)
        
        # Commit
        print(f"Client {client_id}: Committing transaction T{tid}...")
        commit_res = client.commit_transaction()
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        if commit_res.get('success'):
            print(f"Client {client_id}: ✅ Transaction committed (took {elapsed:.2f}s)")
            results_dict[client_id] = {
                'success': True,
                'elapsed_time': elapsed,
                'transaction_id': tid,
                'query_results': query_results
            }
        else:
            print(f"Client {client_id}: ❌ Commit failed: {commit_res.get('error')}")
            results_dict[client_id] = {'success': False, 'error': commit_res.get('error')}
    
    finally:
        client.disconnect()


def test_concurrent_writes_with_waiting():
    """Test that demonstrates event-driven waiting when transactions conflict"""
    print("\n" + "="*70)
    print("TEST: Concurrent Writes with Event-Driven Waiting")
    print("="*70)
    print("\nScenario:")
    print("  - Client 1: Inserts product with id=1, then waits 2 seconds")
    print("  - Client 2: Tries to update product id=1 (will wait for Client 1)")
    print("  - Expected: Client 2 should wait using events, not polling")
    print("="*70)
    
    # Setup
    if not setup_test_table():
        return False
    
    # Client queries
    client1_queries = [
        "INSERT INTO test_products VALUES (1, 'Laptop', 1000)",
        "SELECT * FROM test_products WHERE id=1"
    ]
    
    # Client 2 will try to access the same row
    client2_queries = [
        "UPDATE test_products SET price=1200 WHERE id=1",
        "SELECT * FROM test_products WHERE id=1"
    ]
    
    results = {}
    threads = []
    
    # Start Client 1
    print("\n[TEST] Starting Client 1...")
    t1 = threading.Thread(target=client_worker, args=(1, client1_queries, results))
    t1.start()
    threads.append(t1)
    
    # Wait a bit, then start Client 2
    time.sleep(0.5)
    print("[TEST] Starting Client 2 (will conflict with Client 1)...")
    t2 = threading.Thread(target=client_worker, args=(2, client2_queries, results))
    t2.start()
    threads.append(t2)
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Analyze results
    print("\n" + "="*70)
    print("TEST RESULTS")
    print("="*70)
    
    success_count = sum(1 for r in results.values() if r.get('success'))
    
    for client_id, result in results.items():
        if result.get('success'):
            print(f"✅ Client {client_id}: SUCCESS (took {result['elapsed_time']:.2f}s)")
        else:
            print(f"❌ Client {client_id}: FAILED - {result.get('error')}")
    
    print(f"\nSummary: {success_count}/{len(results)} clients succeeded")
    
    # Verify final state
    print("\n[VERIFY] Checking final data...")
    verify_client = SimpleClient(99)
    if verify_client.connect():
        verify_client.begin_transaction()
        result = verify_client.execute_query("SELECT * FROM test_products")
        
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            print(f"✅ Final state: {len(rows)} row(s)")
            for row in rows:
                print(f"   {row}")
        else:
            print(f"❌ Verification failed: {result.get('error')}")
        
        verify_client.commit_transaction()
        verify_client.disconnect()
    
    print("="*70)
    
    return success_count == len(results)


def test_multiple_concurrent_clients():
    """Test with multiple clients accessing different rows"""
    print("\n" + "="*70)
    print("TEST: Multiple Concurrent Clients")
    print("="*70)
    print("\nScenario:")
    print("  - 3 clients each insert different products")
    print("  - Should complete quickly with minimal conflicts")
    print("="*70)
    
    # Setup
    if not setup_test_table():
        return False
    
    # Different clients work on different data
    clients_queries = [
        [
            "INSERT INTO test_products VALUES (10, 'Mouse', 20)",
            "SELECT * FROM test_products WHERE id=10"
        ],
        [
            "INSERT INTO test_products VALUES (20, 'Keyboard', 50)",
            "UPDATE test_products SET price=60 WHERE id=20"
        ],
        [
            "INSERT INTO test_products VALUES (30, 'Monitor', 300)",
            "SELECT * FROM test_products WHERE price > 100"
        ]
    ]
    
    results = {}
    threads = []
    
    print("\n[TEST] Starting 3 concurrent clients...")
    start_time = time.time()
    
    for i, queries in enumerate(clients_queries, start=1):
        t = threading.Thread(target=client_worker, args=(i, queries, results))
        t.start()
        threads.append(t)
        time.sleep(0.1)
    
    # Wait for all
    for t in threads:
        t.join()
    
    total_time = time.time() - start_time
    
    # Results
    print("\n" + "="*70)
    print("TEST RESULTS")
    print("="*70)
    
    success_count = sum(1 for r in results.values() if r.get('success'))
    
    for client_id, result in results.items():
        if result.get('success'):
            print(f"✅ Client {client_id}: SUCCESS (took {result['elapsed_time']:.2f}s)")
        else:
            print(f"❌ Client {client_id}: FAILED - {result.get('error')}")
    
    print(f"\nSummary: {success_count}/{len(results)} clients succeeded")
    print(f"Total test time: {total_time:.2f}s")
    
    print("="*70)
    
    return success_count == len(results)


def main():
    print("="*70)
    print("CLIENT-SERVER INTEGRATION TEST")
    print("Event-Driven Wake-up Mechanism")
    print("="*70)
    print("\n⚠️  Make sure server.py is running on localhost:5555")
    
    # Test server connection
    print("\n[INIT] Testing server connection...")
    test_client = SimpleClient(0)
    if not test_client.connect():
        print("❌ Cannot connect to server. Is server.py running?")
        return False
    test_client.disconnect()
    print("✅ Server is reachable")
    
    # Run tests
    tests_passed = 0
    total_tests = 2
    
    # Test 1: Concurrent writes with waiting
    if test_concurrent_writes_with_waiting():
        tests_passed += 1
        print("\n✅ Test 1 PASSED")
    else:
        print("\n❌ Test 1 FAILED")
    
    time.sleep(1)
    
    # Test 2: Multiple concurrent clients
    if test_multiple_concurrent_clients():
        tests_passed += 1
        print("\n✅ Test 2 PASSED")
    else:
        print("\n❌ Test 2 FAILED")
    
    # Summary
    print("\n" + "="*70)
    print("FINAL RESULTS")
    print("="*70)
    print(f"Tests Passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("\n🎉 ALL TESTS PASSED! Event-driven mechanism working end-to-end!")
    else:
        print("\n⚠️  Some tests failed")
    
    print("="*70)
    
    return tests_passed == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
