import threading
import time
import random
import os
import shutil
import sys

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from QueryProcessor.query_processor_core import QueryProcessor
from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine

def setup_system():
    # Initialize Components
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    
    ccm_core = LockBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    
    optimizer = IntegratedQueryOptimizer()
    
    recovery_manager = IntegratedFailureRecoveryManager()
    
    # Initialize Query Processor
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    return processor

def setup_database(processor):
    print("Setting up database...")
    # Create Tables
    processor.execute_query("CREATE TABLE mahasiswa (id INT, nama VARCHAR(100), ipk FLOAT)")
    processor.execute_query("CREATE TABLE products (id INT, name VARCHAR(100), price FLOAT)")
    print("Database Ready.")

def client_worker(client_id, queries, processor):
    tid = None
    try:
        # 1. Mulai Transaksi
        tid = processor.begin_transaction()
        
        all_success = True
        
        # 2. Eksekusi Query
        for query in queries:
            result = processor.execute_query(query, tid)
            if not result.success:
                print(f"[Client {client_id}] Query Failed: {result.error} (TID: {tid})")
                all_success = False
                break
            time.sleep(0.05) # Simulate processing
        
        if not all_success:
            print(f"[Client {client_id}] Transaction {tid} Rolling back due to query failure.")
            processor.rollback_transaction(tid)
            return

        # 3. Commit Transaksi
        commit_res = processor.commit_transaction(tid)
        if commit_res.success:
            print(f"[Client {client_id}] Transaksi {tid} Berhasil")
            return
        else:
            print(f"[Client {client_id}] Commit Failed: {commit_res.error} (TID: {tid})")
            processor.rollback_transaction(tid)
            return
            
    except Exception as e:
        print(f"[Client {client_id}] Error Exception: {e}")
        if tid is not None:
            try:
                processor.rollback_transaction(tid)
            except:
                pass
        return

def main():
    # Ensure clean state
    if os.path.exists("storage/data/mahasiswa.dat"):
        os.remove("storage/data/mahasiswa.dat")
    if os.path.exists("storage/data/products.dat"):
        os.remove("storage/data/products.dat")
    
    processor = setup_system()
    setup_database(processor)
    
    # Scenario:
    # Client 1: Insert Data Awal (Mahasiswa)
    # Client 2: Update IPK Mahasiswa (Concurrent)
    # Client 3: Insert & Update (Concurrent)
    
    queries_1 = [
        "INSERT INTO mahasiswa VALUES (101, 'Alice', 3.5)",
        "INSERT INTO mahasiswa VALUES (102, 'Bob', 3.2)",
        "INSERT INTO mahasiswa VALUES (103, 'Charlie', 3.8)"
    ]
    
    queries_2 = [
        "UPDATE mahasiswa SET ipk = 3.9 WHERE id = 101",
        "UPDATE mahasiswa SET ipk = 3.6 WHERE id = 102"
    ]
    
    queries_3 = [
        "INSERT INTO mahasiswa VALUES (104, 'David', 3.0)",
        "SELECT * FROM mahasiswa",
        "UPDATE mahasiswa SET ipk = 3.1 WHERE id = 104"
    ]
    
    # Run Client 1 first to populate data
    print("\n--- Running Client 1 (Population) ---")
    t1 = threading.Thread(target=client_worker, args=(1, queries_1, processor))
    t1.start()
    t1.join()
    
    # Verify Data
    print("\n--- Verifying Data after Client 1 ---")
    vt = processor.begin_transaction()
    res = processor.execute_query("SELECT * FROM mahasiswa", vt)
    processor.commit_transaction(vt)
    if res.success:
        if res.rows:
            print(f"Data Count: {len(res.rows.data)}")
            for row in res.rows.data:
                print(row)
        else:
            print("No rows returned")
    else:
        print(f"Failed to read data: {res.error}")

    # Run Client 2 and 3 concurrently
    print("\n--- Running Client 2 & 3 (Concurrent) ---")
    t2 = threading.Thread(target=client_worker, args=(2, queries_2, processor))
    t3 = threading.Thread(target=client_worker, args=(3, queries_3, processor))
    
    t2.start()
    t3.start()
    
    t2.join()
    t3.join()
    
    print("\n--- Final Data Check ---")
    vt = processor.begin_transaction()
    res = processor.execute_query("SELECT * FROM mahasiswa", vt)
    processor.commit_transaction(vt)
    if res.success:
        print("Final Data:")
        if res.rows:
            for row in res.rows.data:
                print(row)
        else:
            print("No rows returned")
    else:
        print(f"Final Select Failed: {res.error}")

if __name__ == "__main__":
    main()
