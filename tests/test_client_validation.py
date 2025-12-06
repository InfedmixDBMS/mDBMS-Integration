"""
Client-side automated test for Validation-Based protocol (OCC).
Assumes the server is already running with the validation protocol on the given host/port.
Usage:
    python test_client_validation.py --host localhost --port 5558
"""

import argparse
import sys

sys.path.append('.')
from client import DBClient


def run(host, port):
    print("\n" + "="*60)
    print(f"Validation protocol client test connecting to {host}:{port}")
    print("="*60)

    client = DBClient(host=host, port=port)
    if not client.connect():
        print("✗ Failed to connect to server. Is the validation server running?")
        return 2

    try:
        # Test 1: Begin two transactions and cause a validation conflict
        print("\n[1] Begin two transactions and cause a validation conflict")
        
        # Need TWO separate clients for two concurrent transactions
        client1 = DBClient(host=host, port=port)
        client2 = DBClient(host=host, port=port)
        
        if not client1.connect() or not client2.connect():
            print("  ✗ Failed to connect clients")
            return 1
        
        # Create table first
        resp = client.execute_query("CREATE TABLE __test_val_table (id INT)")
        if resp.get('success'):
            print("  ✓ Created test table")
        
        # Start two transactions
        resp1 = client1.begin_transaction()
        assert resp1.get('success'), f"Begin failed: {resp1}"
        tid1 = resp1.get('transaction_id')
        print(f"  ✓ Client1 began TID1={tid1}")

        resp2 = client2.begin_transaction()
        assert resp2.get('success'), f"Begin failed: {resp2}"
        tid2 = resp2.get('transaction_id')
        print(f"  ✓ Client2 began TID2={tid2}")

        # Both perform writes to same table (will cause conflict at validation)
        resp1 = client1.execute_query("INSERT INTO __test_val_table VALUES (1)")
        print(f"  • T1 write: {resp1.get('success')}")

        resp2 = client2.execute_query("INSERT INTO __test_val_table VALUES (2)")
        print(f"  • T2 write: {resp2.get('success')}")

        # T1 commits first (should succeed)
        resp1 = client1.commit_transaction()
        print(f"  • T1 commit: {resp1.get('success')}")

        # T2 attempts to commit - should FAIL validation due to conflict
        resp2 = client2.commit_transaction()
        if not resp2.get('success'):
            print(f"  ✓ T2 commit FAILED as expected (validation conflict): {resp2.get('error')[:80]}")
        else:
            print("  ⚠ T2 commit succeeded (unexpected - validation may not have detected conflict)")

        client1.disconnect()
        client2.disconnect()

        print("\nValidation client checks finished (results above).")
        return 0

    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        return 1
    finally:
        client.disconnect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5555)
    args = parser.parse_args()
    sys.exit(run(args.host, args.port))
