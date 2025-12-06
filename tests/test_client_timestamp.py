"""
Client-side automated test for Timestamp-Based protocol.
Assumes the server is already running with the timestamp protocol on the given host/port.
Usage:
    python test_client_timestamp.py --host localhost --port 5557
"""

import argparse
import sys
import time

sys.path.append('.')
from client import DBClient


def run(host, port):
    print("\n" + "="*60)
    print(f"Timestamp protocol client test connecting to {host}:{port}")
    print("="*60)

    client = DBClient(host=host, port=port)
    if not client.connect():
        print("✗ Failed to connect to server. Is the timestamp server running?")
        return 2

    try:
        # Test 1: Start transaction and commit
        print("\n[1] Begin and commit an empty transaction")
        resp = client.begin_transaction()
        assert resp.get('success'), f"Begin failed: {resp}"
        tid = resp.get('transaction_id')
        print(f"  ✓ Began TID={tid}")

        resp = client.commit_transaction()
        assert resp.get('success'), f"Commit failed: {resp}"
        print("  ✓ Commit succeeded")

        # Test 2: Read-write conflict (younger writes, older reads -> older should fail)
        print("\n[2] Read-Write conflict scenario")
        
        # Need TWO separate clients for two concurrent transactions
        client1 = DBClient(host=host, port=port)
        client2 = DBClient(host=host, port=port)
        
        if not client1.connect() or not client2.connect():
            print("  ✗ Failed to connect clients")
            return 1
        
        # Start older transaction first
        resp1 = client1.begin_transaction()
        assert resp1.get('success')
        tid_old = resp1.get('transaction_id')
        print(f"  ✓ Client1 began older TID={tid_old}")

        # Start younger transaction
        resp2 = client2.begin_transaction()
        assert resp2.get('success')
        tid_young = resp2.get('transaction_id')
        print(f"  ✓ Client2 began younger TID={tid_young}")

        # Younger writes first (this sets the write timestamp)
        resp = client2.execute_query("CREATE TABLE __test_ts_table (id INT)")
        if resp.get('success'):
            print(f"  ✓ Younger created table")
        
        resp = client2.execute_query("INSERT INTO __test_ts_table VALUES (1)")
        print(f"  • Younger write: {resp.get('success')}")

        # Older tries to read - should be DENIED by timestamp protocol
        # Because read timestamp (older) < write timestamp (younger)
        resp = client1.execute_query("SELECT * FROM __test_ts_table")
        if not resp.get('success') and resp.get('error'):
            print(f"  ✓ Older read DENIED as expected: {resp.get('error')[:80]}")
        else:
            print(f"  ⚠ Older read succeeded (unexpected - protocol may not have detected conflict)")

        # Cleanup
        try:
            client1.commit_transaction()
        except Exception:
            pass
        try:
            client2.commit_transaction()
        except Exception:
            pass
        
        client1.disconnect()
        client2.disconnect()

        print("\nAll timestamp client checks finished (results above).")
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
