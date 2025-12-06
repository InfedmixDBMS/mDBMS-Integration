import os
import sys
import time
import multiprocessing

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from client import DBClient

def client1_flow():
    client = DBClient()
    client.connect()
    print("[Client 1] BEGIN")
    begin = client.begin_transaction()
    print(f"[Client 1] TID: {client.current_tid}")
    print("[Client 1] UPDATE saldo=6000 WHERE id=1 (hold lock)")
    res = client.execute_query("UPDATE akun SET saldo=6000 WHERE id=1")
    print(f"[Client 1] Update result: {res}")
    time.sleep(2)  # Simulasi proses lama
    print("[Client 1] COMMIT")
    commit = client.commit_transaction()
    print(f"[Client 1] Commit result: {commit}")
    client.disconnect()

def client2_flow(waiting_flag):
    client = DBClient()
    client.connect()
    print("[Client 2] BEGIN")
    begin = client.begin_transaction()
    print(f"[Client 2] TID: {client.current_tid}")
    print("[Client 2] UPDATE saldo=9000 WHERE id=1 (should wait if locked)")
    res = client.execute_query("UPDATE akun SET saldo=9000 WHERE id=1")
    print(f"[Client 2] Update result: {res}")
    if res.get('message') and 'waiting' in res['message'].lower():
        print("[Client 2] Status: WAITING for lock release.")
        waiting_flag.value = 1
    else:
        print("[Client 2] Status: Proceeded (no wait detected)")
    time.sleep(1)
    print("[Client 2] ROLLBACK")
    rollback = client.rollback_transaction()
    print(f"[Client 2] Rollback result: {rollback}")
    client.disconnect()

def print_final_state():
    client = DBClient()
    client.connect()
    print("\nFinal data check (should be saldo=6000):")
    res = client.execute_query("SELECT * FROM akun WHERE id=1")
    print(res)
    client.disconnect()

if __name__ == "__main__":
    waiting_flag = multiprocessing.Value('i', 0)
    p1 = multiprocessing.Process(target=client1_flow)
    p2 = multiprocessing.Process(target=client2_flow, args=(waiting_flag,))
    p1.start()
    time.sleep(0.5)  
    p2.start()
    p1.join()
    p2.join()
    print_final_state()
    print("\nTest result:")
    if waiting_flag.value:
        print("Client 2 detected waiting for lock.")
    else:
        print("Client 2 did NOT wait (check concurrency logic!)")
