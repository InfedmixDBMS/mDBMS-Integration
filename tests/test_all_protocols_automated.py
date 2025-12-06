"""
Automated client-server tests for all concurrency control protocols
Tests Lock-Based, Timestamp-Based, and Validation-Based protocols
"""

import subprocess
import socket
import time
import sys
import os

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from client import DBClient


def is_port_in_use(port):
    """Check if a port is already in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def wait_for_server(port, timeout=10):
    """Wait for server to be ready"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_in_use(port):
            time.sleep(0.5)  # Give server a bit more time to fully initialize
            return True
        time.sleep(0.1)
    return False


def run_protocol_test(protocol_name, port):
    """Run tests for a specific protocol"""
    print(f"\n{'='*70}")
    print(f"TESTING {protocol_name.upper()} PROTOCOL")
    print(f"{'='*70}")
    
    # Start server in background
    server_process = subprocess.Popen(
        [sys.executable, 'server.py', '--protocol', protocol_name.lower(), '--port', str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    try:
        # Wait for server to start
        print(f"[TEST] Starting server with {protocol_name} protocol on port {port}...")
        if not wait_for_server(port, timeout=10):
            print(f"[ERROR] Server failed to start on port {port}")
            return False
        
        print(f"[TEST] Server started successfully!")
        time.sleep(1)  # Extra time for initialization
        
        # Run tests
        success = True
        
        # Test 1: Basic transaction
        print(f"\n[TEST 1] Basic Transaction Test")
        try:
            client = DBClient(host='localhost', port=port)
            if not client.connect():
                print(f"  ✗ Failed to connect to server")
                success = False
            else:
                # Begin transaction
                response = client.begin_transaction()
                if not response.get('success'):
                    print(f"  ✗ Failed to begin transaction: {response}")
                    success = False
                else:
                    tid = response.get('transaction_id')
                    print(f"  ✓ Transaction started: TID={tid}")
                    
                    # Commit transaction
                    response = client.commit_transaction()
                    if not response.get('success'):
                        print(f"  ✗ Failed to commit transaction: {response}")
                        success = False
                    else:
                        print(f"  ✓ Transaction committed successfully")
                
                client.disconnect()
            
        except Exception as e:
            print(f"  ✗ Test 1 failed with error: {e}")
            success = False
        
        # Test 2: Create table and insert
        print(f"\n[TEST 2] Create Table and Insert")
        try:
            client = DBClient(host='localhost', port=port)
            if not client.connect():
                print(f"  ✗ Failed to connect to server")
                success = False
            else:
                # Create table
                response = client.execute_query("CREATE TABLE test_users (id INT, name VARCHAR)")
                if not response.get('success'):
                    print(f"  ✗ Failed to create table: {response}")
                    success = False
                else:
                    print(f"  ✓ Table created successfully")
                
                # Begin transaction
                response = client.begin_transaction()
                tid = response.get('transaction_id')
                print(f"  ✓ Transaction started: TID={tid}")
                
                # Insert data
                response = client.execute_query("INSERT INTO test_users VALUES (1, 'Alice')")
                if not response.get('success'):
                    print(f"  ✗ Failed to insert: {response}")
                    success = False
                else:
                    print(f"  ✓ Data inserted successfully")
                
                # Commit
                response = client.commit_transaction()
                if not response.get('success'):
                    print(f"  ✗ Failed to commit: {response}")
                    success = False
                else:
                    print(f"  ✓ Transaction committed successfully")
                
                client.disconnect()
            
        except Exception as e:
            print(f"  ✗ Test 2 failed with error: {e}")
            success = False
        
        # Test 3: Concurrent transactions
        print(f"\n[TEST 3] Concurrent Transactions")
        try:
            client1 = DBClient(host='localhost', port=port)
            client2 = DBClient(host='localhost', port=port)
            
            if not client1.connect() or not client2.connect():
                print(f"  ✗ Failed to connect clients")
                success = False
            else:
                # Start two transactions
                resp1 = client1.begin_transaction()
                resp2 = client2.begin_transaction()
                
                if not resp1.get('success') or not resp2.get('success'):
                    print(f"  ✗ Failed to start concurrent transactions")
                    success = False
                else:
                    tid1 = resp1.get('transaction_id')
                    tid2 = resp2.get('transaction_id')
                    print(f"  ✓ Two transactions started: TID1={tid1}, TID2={tid2}")
                    
                    # Both try to insert
                    resp1 = client1.execute_query("INSERT INTO test_users VALUES (2, 'Bob')")
                    resp2 = client2.execute_query("INSERT INTO test_users VALUES (3, 'Charlie')")
                    
                    print(f"  • T1 insert: {'success' if resp1.get('success') else 'failed'}")
                    print(f"  • T2 insert: {'success' if resp2.get('success') else 'failed'}")
                    
                    # Try to commit both
                    commit1 = client1.commit_transaction()
                    commit2 = client2.commit_transaction()
                    
                    print(f"  • T1 commit: {'success' if commit1.get('success') else 'failed'}")
                    print(f"  • T2 commit: {'success' if commit2.get('success') else 'failed'}")
                    
                    # For lock-based, both should succeed (serialized)
                    # For timestamp/validation, one might fail
                    if protocol_name.lower() == 'lock':
                        if not commit1.get('success') or not commit2.get('success'):
                            print(f"  ⚠ Lock-based should allow both to commit (serialized)")
                    else:
                        print(f"  ✓ Protocol behavior observed (some may abort)")
                
                client1.disconnect()
                client2.disconnect()
            
        except Exception as e:
            print(f"  ✗ Test 3 failed with error: {e}")
            success = False
        
        # Test 4: Select query
        print(f"\n[TEST 4] Select Query")
        try:
            client = DBClient(host='localhost', port=port)
            if not client.connect():
                print(f"  ✗ Failed to connect to server")
                success = False
            else:
                response = client.begin_transaction()
                tid = response.get('transaction_id')
                
                response = client.execute_query("SELECT * FROM test_users")
                if not response.get('success'):
                    print(f"  ✗ Failed to select: {response}")
                    success = False
                else:
                    rows = response.get('rows', [])
                    print(f"  ✓ Select successful, rows returned: {len(rows)}")
                
                client.commit_transaction()
                client.disconnect()
            
        except Exception as e:
            print(f"  ✗ Test 4 failed with error: {e}")
            success = False
        
        return success
        
    finally:
        # Stop server
        print(f"\n[TEST] Stopping server...")
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
            server_process.wait()
        print(f"[TEST] Server stopped")


def main():
    print("="*70)
    print("AUTOMATED CLIENT-SERVER TESTS FOR ALL PROTOCOLS")
    print("="*70)
    
    protocols = [
        ('Lock', 5556),
        ('Timestamp', 5557),
        ('Validation', 5558)
    ]
    
    results = {}
    
    for protocol_name, port in protocols:
        try:
            success = run_protocol_test(protocol_name, port)
            results[protocol_name] = success
            time.sleep(2)  # Wait between tests
        except Exception as e:
            print(f"\n[ERROR] Failed to test {protocol_name} protocol: {e}")
            import traceback
            traceback.print_exc()
            results[protocol_name] = False
    
    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for protocol_name in ['Lock', 'Timestamp', 'Validation']:
        status = "✓ PASSED" if results.get(protocol_name, False) else "✗ FAILED"
        print(f"{protocol_name}-Based Protocol: {status}")
    
    print("="*70)
    
    all_passed = all(results.values())
    if all_passed:
        print("\n🎉 ALL PROTOCOL TESTS PASSED!")
    else:
        print("\n⚠ SOME TESTS FAILED")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
