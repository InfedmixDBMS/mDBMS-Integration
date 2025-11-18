from datetime import datetime
from typing import Dict, Optional
from .models import ExecutionResult, Transaction
from .interfaces import (
    AbstractQueryOptimizer,
    AbstractStorageManager,
    AbstractConcurrencyControlManager,
    AbstractFailureRecoveryManager
)
from .executor import QueryExecutor


class QueryProcessor:
    
    def __init__(
        self,
        optimizer: AbstractQueryOptimizer,
        storage_manager: AbstractStorageManager,
        concurrency_manager: AbstractConcurrencyControlManager,
        recovery_manager: AbstractFailureRecoveryManager
    ):
        self.optimizer = optimizer
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.recovery_manager = recovery_manager
        self.executor = QueryExecutor(storage_manager, concurrency_manager)
        self.active_transactions: Dict[str, Transaction] = {}
        
    def get_optimizer(self) -> AbstractQueryOptimizer:
        return self.optimizer
    
    def get_storage_manager(self) -> AbstractStorageManager:
        return self.storage_manager
    
    def get_concurrency_manager(self) -> AbstractConcurrencyControlManager:
        return self.concurrency_manager
    
    def get_recovery_manager(self) -> AbstractFailureRecoveryManager:
        return self.recovery_manager
    
    def get_executor(self) -> QueryExecutor:
        return self.executor
    
    def execute_query(
        self, 
        query: str, 
        transaction_id: Optional[str] = None
    ) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            # Log query ke FailureRecoveryManager
            if transaction_id:
                self.recovery_manager.log_query(transaction_id, query)
            
            # Get atau create transaction
            transaction = self._get_or_create_transaction(transaction_id)
            transaction.add_query(query)
            
            # Kirim ke QueryOptimizer dan dapatkan QueryPlan tree
            try:
                plan_node = self.optimizer.optimize(query)
            except Exception as e:
                return ExecutionResult(
                    success=False,
                    error=f"Optimization error: {str(e)}",
                    execution_time=self._calculate_execution_time(start_time)
                )
            
            # Eksekusi plan tree menggunakan QueryExecutor
            result = self.executor.execute(
                plan_node,
                transaction
            )
            
            # Update execution time
            result.execution_time = self._calculate_execution_time(start_time)
            
            return result
            
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=f"Query execution failed: {str(e)}",
                execution_time=self._calculate_execution_time(start_time)
            )
    
    def _get_or_create_transaction(self, transaction_id: Optional[str]) -> Transaction:
        if transaction_id and (transaction_id in self.active_transactions):
            return self.active_transactions[transaction_id]
        else:
            transaction = Transaction(transaction_id)
            self.active_transactions[transaction.transaction_id] = transaction
            self.recovery_manager.log_transaction_start(transaction.transaction_id)
            return transaction
    
    def _calculate_execution_time(self, start_time: datetime) -> float:
        return (datetime.now() - start_time).total_seconds()
    
    def _request_lock(
        self, 
        transaction: Transaction, 
        resource_id: str, 
        lock_type: str
    ) -> bool:
        granted = self.concurrency_manager.request_lock(
            transaction.transaction_id,
            resource_id,
            lock_type
        )
        
        if granted:
            transaction.add_lock(resource_id)
        
        return granted
    
    def _abort_transaction(self, transaction: Transaction) -> None:
        transaction.abort()
        self.concurrency_manager.release_all_locks(transaction.transaction_id)
        self.recovery_manager.log_transaction_abort(transaction.transaction_id)
    
    def commit_transaction(self, transaction_id: str) -> ExecutionResult:
        if transaction_id not in self.active_transactions:
            return ExecutionResult(
                success=False,
                error=f"Transaction '{transaction_id}' not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        
        try:
            transaction.commit()
            
            # Release semua lock
            self.concurrency_manager.release_all_locks(transaction_id)
            
            # Log commit
            self.recovery_manager.log_transaction_commit(transaction_id)
            
            # Hapus dari active transactions
            del self.active_transactions[transaction_id]
            
            return ExecutionResult(
                success=True,
                message=f"Transaction '{transaction_id}' committed successfully"
            )
            
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=f"Failed to commit transaction: {str(e)}"
            )
    
    def rollback_transaction(self, transaction_id: str) -> ExecutionResult:
        if transaction_id not in self.active_transactions:
            return ExecutionResult(
                success=False,
                error=f"Transaction '{transaction_id}' not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        
        try:
            # Abort transaction
            self._abort_transaction(transaction)
            
            # Hapus dari active transactions
            del self.active_transactions[transaction_id]
            
            return ExecutionResult(
                success=True,
                message=f"Transaction '{transaction_id}' rolled back successfully"
            )
            
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=f"Failed to rollback transaction: {str(e)}"
            )
