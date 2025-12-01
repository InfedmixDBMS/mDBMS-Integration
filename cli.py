import sys
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

def print_welcome():
    print("Welcome to InfedmixDBMS CLI!")
    print("Type 'help' for available commands.")

def print_help():
    print("""
Available commands:
  help                Show this help message
  exit                Exit CLI
  <SQL>               Execute SQL query
  begin               Begin transaction
  commit              Commit transaction
  rollback            Rollback transaction
  show tables         List all tables
  show data <table>   Show all data in table
""")

def cli_loop():
    processor = setup_system()
    print_welcome()
    current_tid = None
    while True:
        try:
            cmd = ""
            prompt = "dbms> "
            while True:
                line = input(prompt)
                if not line and not cmd:
                    break
                cmd += (line + "\n")
                prompt = "...> "
                if ";" in line:
                    break
            cmd = cmd.strip()
            if not cmd:
                continue
            cmd = cmd.replace("\n", " ").strip()
            if cmd.endswith(";"):
                cmd = cmd[:-1].strip()
            if cmd.lower() == "exit":
                print("Exiting DBMS CLI.")
                break
            elif cmd.lower() == "help":
                print_help()
            elif cmd.lower() == "begin":
                current_tid = processor.begin_transaction()
                print(f"Transaction started. TID={current_tid}")
            elif cmd.lower() == "commit":
                if current_tid is None:
                    print("No active transaction.")
                else:
                    res = processor.commit_transaction(current_tid)
                    if hasattr(res, 'success') and res.success:
                        print("Transaction committed.")
                    else:
                        print(f"Commit Error: {getattr(res, 'error', 'Unknown error')}")
                    current_tid = None
            elif cmd.lower() == "rollback":
                if current_tid is None:
                    print("No active transaction.")
                else:
                    res = processor.rollback_transaction(current_tid)
                    if hasattr(res, 'success') and res.success:
                        print("Transaction rolled back.")
                    else:
                        print(f"Rollback Error: {getattr(res, 'error', 'Unknown error')}")
                    current_tid = None
            elif cmd.lower() == "show tables":
                res = processor.execute_query("SHOW TABLES")
                if hasattr(res, 'success') and res.success and hasattr(res, 'rows'):
                    print("Tables:")
                    for row in res.rows.data:
                        print(row)
                else:
                    print("No tables found or error.")
            elif cmd.lower().startswith("show data "):
                table = cmd[len("show data "):].strip()
                res = processor.execute_query(f"SELECT * FROM {table}")
                if hasattr(res, 'success') and res.success and hasattr(res, 'rows'):
                    for row in res.rows.data:
                        print(row)
                else:
                    print("No data or error.")
            else:
                # Sisa = SQL Statement
                sql = cmd
                res = processor.execute_query(sql, current_tid)
                if hasattr(res, 'success') and res.success:
                    print("Query OK.")
                    if hasattr(res, 'rows') and res.rows:
                        for row in res.rows.data:
                            print(row)
                else:
                    print(f"Query Error: {getattr(res, 'error', 'Unknown error')}")
        except KeyboardInterrupt:
            print("\nExiting DBMS CLI.")
            break
        except Exception as e:
            print(f"Error: {e}")
