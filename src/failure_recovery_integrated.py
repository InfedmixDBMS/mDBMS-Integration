from QueryProcessor.interfaces import AbstractFailureRecoveryManager
from QueryProcessor.models import ExecutionResult as ExecutionResult
from FailureRecoveryManager.FailureRecoveryManager.classes.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.FailureRecoveryManager.types.RecoverCriteria import RecoverCriteria
from typing import Optional, Any

class IntegratedFailureRecoveryManager(AbstractFailureRecoveryManager):
    def __init__(self):
        self.verbose = False
        self.tag = "\033[93m[FRM]\033[0m"  # Kuning
    
    def setVerbose(self, verbose: bool):
        self.verbose = verbose
    
    def write_log(self, execution_result: ExecutionResult, table: Optional[str] = None, 
                  key: Optional[Any] = None, old_value: Optional[Any] = None, 
                  new_value: Optional[Any] = None) -> None:
        
        FailureRecoveryManager.write_log(execution_result, table, key, old_value, new_value)
        
        if self.verbose:
            query_type = execution_result.query.strip().upper().split()[0] if execution_result.query else "OPERATION"
            print(f"{self.tag} Logged {query_type} for transaction {execution_result.transaction_id}")
        
        if (execution_result.query and
            execution_result.query.strip().upper().startswith("COMMIT") and
            len(FailureRecoveryManager.buffer) > 10):
            if self.verbose:
                print(f"{self.tag} Triggering checkpoint (buffer size: {len(FailureRecoveryManager.buffer)})")
            FailureRecoveryManager._save_checkpoint()
            if self.verbose:
                print(f"{self.tag} Checkpoint saved")
    
    def log_transaction_start(self, transaction_id: int) -> None:
        exec_result = ExecutionResult(
            success=True,
            transaction_id=transaction_id,
            query="BEGIN TRANSACTION"
        )
        self.write_log(exec_result)
    
    def log_transaction_commit(self, transaction_id: int) -> None:
        exec_result = ExecutionResult(
            success=True,
            transaction_id=transaction_id,
            query="COMMIT"
        )
        self.write_log(exec_result)
    
    def log_transaction_abort(self, transaction_id: int) -> None:
        exec_result = ExecutionResult(
            success=True,
            transaction_id=transaction_id,
            query="ABORT"
        )
        self.write_log(exec_result)
    
    def recover(self) -> bool:
        try:
            if self.verbose:
                print(f"{self.tag} Starting recovery process...")
            
            FailureRecoveryManager.recover(RecoverCriteria())
            
            if self.verbose:
                print(f"{self.tag} Recovery completed successfully")
            return True
            
        except Exception as e:
            if self.verbose:
                print(f"{self.tag} Recovery failed: {e}")
            return False