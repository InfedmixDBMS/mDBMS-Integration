from datetime import datetime
from typing import Optional
from ..models import QueryPlan, ExecutionResult, Transaction, TableScanNode
from ..interfaces import (
    AbstractStorageManager,
    AbstractConcurrencyControlManager,
    AbstractFailureRecoveryManager
)
from .execution_visitor import ExecutionVisitor


class QueryExecutor:
    
    def __init__(
        self,
        storage_manager: AbstractStorageManager,
        concurrency_manager: Optional[AbstractConcurrencyControlManager] = None,
        failure_recovery: Optional[AbstractFailureRecoveryManager] = None
    ):
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.failure_recovery = failure_recovery
        self.current_transaction: Optional[int] = None
    
    def execute(self, plan: QueryPlan) -> ExecutionResult:
        visitor = ExecutionVisitor(
            self.storage_manager,
            self.concurrency_manager,
            self.current_transaction
        )
        
        try:
            result_rows = plan.accept(visitor)
            return ExecutionResult(
                success=True,
                data=result_rows,
                rows_affected=len(result_rows.data) if result_rows else 0,
                message="Query executed successfully"
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=str(e),
                message=f"Query execution failed: {str(e)}"
            )
    
    def execute_with_transaction(self, plan: QueryPlan) -> ExecutionResult:
        transaction_id: Optional[int] = None
        
        try:
            if self.concurrency_manager:
                transaction_id = self.concurrency_manager.begin_transaction()
                self.current_transaction = transaction_id
                print(f"Transaction {transaction_id} started")
            
            if not self._acquire_locks(plan, "READ"):
                raise Exception("Failed to acquire locks")
            
            result = self.execute(plan)
            
            if not result.success:
                raise Exception(result.error)
            
            if self.concurrency_manager and transaction_id:
                success = self._flush_to_storage(result)
                
                if success:
                    self.concurrency_manager.commit_transaction(transaction_id)
                    self.concurrency_manager.commit_flushed(transaction_id)
                    self.concurrency_manager.end_transaction(transaction_id)
                    print(f"Transaction {transaction_id} committed successfully")
                else:
                    raise Exception("Failed to flush to storage")
            
            self.current_transaction = None
            return result
            
        except Exception as e:
            if self.concurrency_manager and transaction_id:
                print(f"Rolling back transaction {transaction_id}")
                self.concurrency_manager.rollback_transaction(transaction_id)
                self.concurrency_manager.end_transaction(transaction_id)
            
            self.current_transaction = None
            return ExecutionResult(
                success=False,
                error=str(e),
                message=f"Transaction failed: {str(e)}"
            )
    
    def _acquire_locks(self, plan: QueryPlan, lock_type: str) -> bool:
        if not self.concurrency_manager or not self.current_transaction:
            return True
                
        tables = self._extract_tables(plan)
        
        for table in tables:
            resource_id = f"{table}:0"  # Table-level lock
            success = self.concurrency_manager.request_lock(
                self.current_transaction,
                resource_id,
                lock_type
            )
            if not success:
                print(f"Failed to acquire {lock_type} lock on {table}")
                return False
        
        return True
    
    def _extract_tables(self, plan: QueryPlan) -> list:
        
        tables = []
        
        if isinstance(plan, TableScanNode):
            tables.append(plan.table_name)
        
        if hasattr(plan, 'child') and plan.child:
            tables.extend(self._extract_tables(plan.child))
        if hasattr(plan, 'left_child') and plan.left_child:
            tables.extend(self._extract_tables(plan.left_child))
        if hasattr(plan, 'right_child') and plan.right_child:
            tables.extend(self._extract_tables(plan.right_child))
        
        return list(set(tables))  # Remove duplicates
    
    def _flush_to_storage(self, result: ExecutionResult) -> bool:
        """Flush result ke storage (untuk write operations)"""
        # TODO: Implement flush logic
        return True
