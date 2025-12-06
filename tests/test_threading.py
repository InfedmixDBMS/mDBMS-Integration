

import threading
import time
import os
import sys

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if ROOT not in sys.path:
    sys.path.append(ROOT)
if SRC not in sys.path:
    sys.path.append(SRC)
sys.path.append(os.path.join(ROOT, "StorageManager"))

from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from QueryProcessor.query_processor_core import QueryProcessor
from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine

def setup_system():
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm_core = LockBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    return processor

def client_worker(client_id, queries, processor):
    tid = processor.begin_transaction()
    print(f"[Client {client_id}] Transaction started: {tid}")
    for query in queries:
        print(f"[Client {client_id}] Executing: {query}")
        res = processor.execute_query(query, tid)
        if hasattr(res, 'success') and not res.success:
            print(f"[Client {client_id}] Query failed: {res.error}")
        if hasattr(res, 'rows') and res.rows:
            print(f"[Client {client_id}] Result:")
            for row in res.rows.data:
                print(row)
        time.sleep(0.1)
    commit_res = processor.commit_transaction(tid)
    if hasattr(commit_res, 'success') and commit_res.success:
        print(f"[Client {client_id}] Commit success.")
    else:
        print(f"[Client {client_id}] Commit failed: {getattr(commit_res, 'error', 'Unknown error')}")

if __name__ == "__main__":
    if os.path.exists("storage/data/products.dat"):
        os.remove("storage/data/products.dat")
    processor = setup_system()
    # Create table
    print("Creating products table...")
    tid = processor.begin_transaction()
    processor.execute_query("CREATE TABLE products (id INT, name VARCHAR(50), price INT)", tid)
    processor.commit_transaction(tid)
    print("Table created.")
    # Client queries
    queries1 = [
        "INSERT INTO products VALUES (1, 'Laptop', 1000)",
        "INSERT INTO products VALUES (2, 'Mouse', 20)",
        "SELECT * FROM products"
    ]
    queries2 = [
        "INSERT INTO products VALUES (3, 'Keyboard', 50)",
        "UPDATE products SET price = 55 WHERE id = 3",
        "SELECT * FROM products"
    ]
    # Run threads
    t1 = threading.Thread(target=client_worker, args=(1, queries1, processor))
    t2 = threading.Thread(target=client_worker, args=(2, queries2, processor))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    # Final check
    print("\nFinal data check:")
    tid = processor.begin_transaction()
    res = processor.execute_query("SELECT * FROM products", tid)
    processor.commit_transaction(tid)
    if hasattr(res, 'success') and res.success and hasattr(res, 'rows'):
        for row in res.rows.data:
            print(row)
    else:
        print(f"Final select failed: {getattr(res, 'error', 'Unknown error')}")
