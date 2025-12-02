import sys
import os
import threading
import time
import random

sys.path.append(os.path.join(os.getcwd(), "ConcurrencyControl", "src"))
sys.path.append(os.path.join(os.getcwd(), "QueryOptimization", "src"))
sys.path.append(os.path.join(os.getcwd(), "StorageManager"))
sys.path.append(os.getcwd())

from QueryProcessor.query_processor_core import QueryProcessor
from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def test_system_integration():
    print("==================================================")
    print("       SYSTEM INTEGRATION TEST")
    print("==================================================")
    
    # Initialize components
    print(f"\n{Colors.OKCYAN}[INIT]{Colors.ENDC} Initializing components...")
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    ccm = LockBasedConcurrencyControlManager()
    concurrency_manager = IntegratedConcurrencyManager(ccm)
    query_optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    # Enable verbose mode
    # storage_manager.setVerbose(True) # Not implemented in real storage manager integration
    concurrency_manager.setVerbose(True)
    
    processor = QueryProcessor(
        optimizer=query_optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    print(f"{Colors.OKCYAN}[INIT]{Colors.ENDC} Components initialized successfully.\n")
    print(60*"=" + "\n")
    
    # Setup initial data
    print(f"\n{Colors.OKCYAN}[SETUP]{Colors.ENDC} Creating initial table...")
    tid_setup = processor.begin_transaction()
    res_setup = processor.execute_query("CREATE TABLE products (id INT, name VARCHAR(50), price INT)", tid_setup)
    if res_setup.success:
        print("✅ Initial table created.")
    else:
        print(f"❌ Failed to create initial table: {res_setup.error}")
    processor.commit_transaction(tid_setup)
    print(60*"=" + "\n")
    
    # Client thread workload
    def client_task(client_id, queries):
        client_color = Colors.OKGREEN if client_id == 1 else Colors.WARNING
        client_tag = f"{client_color}[Client {client_id}]{Colors.ENDC}"
        
        success = False
        
        while not success:
                
            print(f"\n{client_tag} Starting transaction...")
            tid = processor.begin_transaction()
            print(f"{client_tag} Transaction ID: {tid}")
            
            for query in queries:
                print(f"{client_tag} Processing query: {query}")
                
                # 1. Optimization Check
                optimizer = processor.get_optimizer()
                try:
                    plan = optimizer.optimize(query)
                    print(f"{client_tag} Query Plan Generated:\n")
                    print(plan.print_tree() + "\n")
                except Exception as e:
                    print(f"{client_tag} Optimization skipped/failed (expected for DDL/some DML): {e}")

                # 2. Execution (Concurrency & Storage)
                # QP -> Optimizer -> Executor -> CM -> SM
                result = processor.execute_query(query, tid)
                
                if result.success:
                    success = True
                    print(f"{client_tag} ✅ Execution Success: {result.message}")
                    if result.rows:
                        print(f"{client_tag} 📊 Data: {result.rows.data}")
                else:
                    print(f"{client_tag} ❌ Execution Failed: {result.error}. Retrying...")
                    break
                
                time.sleep(random.uniform(0.1, 0.5))
        
        print(f"{client_tag} Committing transaction {tid}...")
        commit_res = processor.commit_transaction(tid)
        if commit_res.success:
             print(f"{client_tag} ✅ Transaction {tid} Committed.")
        else:
             print(f"{client_tag} ❌ Transaction {tid} Commit Failed: {commit_res.error}")

    # Queries
    client1_queries = [
        "INSERT INTO products VALUES (1, 'Laptop', 1000)",
        "INSERT INTO products VALUES (2, 'Mouse', 20)",
        "SELECT * FROM products WHERE price > 30"
    ]
    
    client2_queries = [
        "INSERT INTO products VALUES (3, 'Keyboard', 50)",
        "UPDATE products SET price=100 WHERE id=3",
        "SELECT * FROM products WHERE price > 30"
    ]
    
    # Threads
    t1 = threading.Thread(target=client_task, args=(1, client1_queries))
    t2 = threading.Thread(target=client_task, args=(2, client2_queries))
    
    print(f"\n{Colors.HEADER}[TEST]{Colors.ENDC} Starting concurrent clients...")
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    print(f"\n{Colors.HEADER}[TEST]{Colors.ENDC} Concurrent clients finished.")
    
    # Final Verification
    print(60*"=" + "\n")
    print(f"{Colors.HEADER}[VERIFY]{Colors.ENDC} Final data state:")
    tid_verify = processor.begin_transaction()
    res = processor.execute_query("SELECT * FROM products", tid_verify)
    if res.success and res.rows:
        print(f"✅ Final Rows: {len(res.rows.data)}")
        for row in res.rows.data:
            print(f" - {row}")
    else:
        print(f"❌ Verification failed: {res.error}")
    processor.commit_transaction(tid_verify)
    
    print("\n==================================================")
    print("       TEST SUITE COMPLETED")
    print("==================================================")


if __name__ == "__main__":
    test_system_integration()