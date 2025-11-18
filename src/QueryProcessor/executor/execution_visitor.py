from datetime import datetime
from typing import Any, Dict, Optional
from .plan_visitor import QueryPlanVisitor
from ..models import (
    Rows,
    ExecutionResult,
    TableScanNode,
    FilterNode,
    ProjectNode,
    SortNode,
    NestedLoopJoinNode,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    CreateTablePlan,
    DropTablePlan
)
from ..interfaces import AbstractStorageManager, AbstractConcurrencyControlManager


class ExecutionVisitor(QueryPlanVisitor):
    def __init__(
        self,
        storage_manager: AbstractStorageManager,
        concurrency_manager: Optional[AbstractConcurrencyControlManager] = None,
        current_transaction: Optional[int] = None
    ):
        self.storage_manager = storage_manager
        self.concurrency_manager = concurrency_manager
        self.current_transaction = current_transaction
    
    def visit_table_scan(self, node: TableScanNode) -> Rows:
        lock_granted = self.concurrency_manager.request_lock(
            self.transaction_id,
            f"{node.table_name}:0",
            "READ"
        )
        
        if not lock_granted:
            raise RuntimeError(f"Failed to acquire READ lock on table: {node.table_name}")
        
        return self.storage_manager.read_table(node.table_name)
    
    def visit_filter(self, node: FilterNode) -> Rows:
        child_rows = node.child.accept(self)
        
        filtered_data = []
        for row in child_rows.data:
            row_dict = {col: val for col, val in zip(child_rows.columns, row)}
            
            if node.condition.evaluate(row_dict):
                filtered_data.append(row)
        
        return Rows(child_rows.columns, filtered_data)
    
    def visit_project(self, node: ProjectNode) -> Rows:
        child_rows = node.child.accept(self)
        
        if '*' in node.columns:
            return child_rows
        
        try:
            column_indices = [child_rows.columns.index(col) for col in node.columns]
        except ValueError as e:
            raise ValueError(f"Column not found during projection: {e}")
        
        projected_data = []
        for row in child_rows.data:
            projected_row = [row[idx] for idx in column_indices]
            projected_data.append(projected_row)
        
        return Rows(node.columns, projected_data)
    
    def visit_sort(self, node: SortNode) -> Rows:
        child_rows = node.child.accept(self)
        
        if not node.order_by:
            if node.limit:
                return Rows(child_rows.columns, child_rows.data[:node.limit])
            return child_rows
        
        sort_indices = []
        for clause in node.order_by:
            try:
                idx = child_rows.columns.index(clause.column)
                sort_indices.append((idx, clause.direction))
            except ValueError:
                raise ValueError(f"ORDER BY column not found: {clause.column}")
        
        def sort_key(row):
            keys = []
            for idx, direction in sort_indices:
                value = row[idx]
                if value is None:
                    value = ""
                
                if direction == "DESC":
                    if isinstance(value, (int, float)):
                        keys.append(-value)
                    else:
                        keys.append(tuple(-ord(c) if c else 0 for c in str(value)))
                else:
                    if isinstance(value, str):
                        keys.append(tuple(ord(c) for c in value))
                    else:
                        keys.append(value)
            return tuple(keys)
        
        sorted_data = sorted(child_rows.data, key=sort_key)
        
        if node.limit:
            sorted_data = sorted_data[:node.limit]
        
        return Rows(child_rows.columns, sorted_data)
    
    def visit_join(self, node: NestedLoopJoinNode) -> Rows:
        left_rows = node.left_child.accept(self)
        right_rows = node.right_child.accept(self)
        
        result_columns = left_rows.columns + right_rows.columns
        result_data = []
        
        for left_row in left_rows.data:
            for right_row in right_rows.data:
                combined_dict = {}
                for col, val in zip(left_rows.columns, left_row):
                    combined_dict[col] = val
                for col, val in zip(right_rows.columns, right_row):
                    combined_dict[col] = val
                
                if node.join_condition.condition.evaluate(combined_dict):
                    combined_row = list(left_row) + list(right_row)
                    result_data.append(combined_row)
        
        return Rows(result_columns, result_data)
    
    
    def visit_insert(self, plan: InsertPlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            lock_granted = self.concurrency_manager.request_lock(
                self.transaction_id,
                f"{plan.table_name}:0", 
                "WRITE"
            )
            
            if not lock_granted:
                raise RuntimeError(f"Failed to acquire WRITE lock on table: {plan.table_name}")
            
            rows = Rows(columns=plan.columns, data=[plan.values])
            
            inserted_rows = self.storage_manager.insert_rows(
                plan.table_name,
                rows,
                self.transaction_id
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ExecutionResult(
                success=True,
                affected_rows=inserted_rows,
                message=f"Inserted {inserted_rows} row(s) into {plan.table_name}",
                execution_time=execution_time,
                transaction_id=self.transaction_id,
                query=f"INSERT INTO {plan.table_name} VALUES ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Insert failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.transaction_id
            )
    
    def visit_update(self, plan: UpdatePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            lock_granted = self.concurrency_manager.request_lock(
                self.transaction_id,
                f"{plan.table_name}:0", 
                "WRITE"
            )
            
            if not lock_granted:
                raise RuntimeError(f"Failed to acquire WRITE lock on table: {plan.table_name}")
            
            condition_dict = self._convert_where_to_dict(plan.where) if plan.where else None
            
            affected_rows = self.storage_manager.update_rows(
                plan.table_name,
                plan.set_clause,
                condition_dict
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            where_desc = f" WHERE {plan.where}" if plan.where else ""
            return ExecutionResult(
                success=True,
                affected_rows=affected_rows,
                message=f"Updated {affected_rows} row(s) in {plan.table_name}{where_desc}",
                execution_time=execution_time,
                transaction_id=self.transaction_id,
                query=f"UPDATE {plan.table_name} SET ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Update failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.transaction_id
            )
    
    def visit_delete(self, plan: DeletePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            lock_granted = self.concurrency_manager.request_lock(
                self.transaction_id,
                f"{plan.table_name}:0", 
                "WRITE"
            )
            
            if not lock_granted:
                raise RuntimeError(f"Failed to acquire WRITE lock on table: {plan.table_name}")
            
            if plan.where:
                condition_dict = self._convert_where_to_dict(plan.where)
            else:
                condition_dict = None
            
            deleted_rows = self.storage_manager.delete_rows(
                plan.table_name,
                condition_dict
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            where_desc = f" WHERE {plan.where}" if plan.where else " (all rows)"
            return ExecutionResult(
                success=True,
                affected_rows=deleted_rows,
                message=f"Deleted {deleted_rows} row(s) from {plan.table_name}{where_desc}",
                execution_time=execution_time,
                transaction_id=self.transaction_id,
                query=f"DELETE FROM {plan.table_name} ..."
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Delete failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.transaction_id
            )
    
    # Operasi DDL
    
    def visit_create_table(self, plan: CreateTablePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            success = self.storage_manager.create_table(
                plan.table_name,
                plan.schema
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if success:
                return ExecutionResult(
                    success=True,
                    message=f"Table '{plan.table_name}' created successfully with {len(plan.schema)} columns",
                    execution_time=execution_time,
                    transaction_id=self.transaction_id,
                    query=f"CREATE TABLE {plan.table_name} ..."
                )
            else:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to create table '{plan.table_name}'",
                    execution_time=execution_time,
                    transaction_id=self.transaction_id
                )
                
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Create table failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.transaction_id
            )
    
    def visit_drop_table(self, plan: DropTablePlan) -> ExecutionResult:
        start_time = datetime.now()
        
        try:
            success = self.storage_manager.drop_table(plan.table_name)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            if success:
                return ExecutionResult(
                    success=True,
                    message=f"Table '{plan.table_name}' dropped successfully",
                    execution_time=execution_time,
                    transaction_id=self.transaction_id,
                    query=f"DROP TABLE {plan.table_name}"
                )
            else:
                return ExecutionResult(
                    success=False,
                    error=f"Failed to drop table '{plan.table_name}'",
                    execution_time=execution_time,
                    transaction_id=self.transaction_id
                )
                
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                success=False,
                error=f"Drop table failed: {str(e)}",
                execution_time=execution_time,
                transaction_id=self.transaction_id
            )
    
    # Helper methods
    
    def _convert_where_to_dict(self, where_condition) -> Dict[str, Any]:
        return {
            where_condition.column: where_condition.value
        }