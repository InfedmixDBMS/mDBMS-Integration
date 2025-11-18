from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import (
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

from ..models import Rows


class QueryPlanVisitor(ABC):
    
    # Operasi SELECT (return Rows)
    
    @abstractmethod
    def visit_table_scan(self, node: 'TableScanNode') -> Any:
        pass
    
    @abstractmethod
    def visit_filter(self, node: 'FilterNode') -> Any:
        pass
    
    @abstractmethod
    def visit_project(self, node: 'ProjectNode') -> Any:
        pass
    
    @abstractmethod
    def visit_sort(self, node: 'SortNode') -> Any:
        pass
    
    @abstractmethod
    def visit_join(self, node: 'NestedLoopJoinNode') -> Any:
        pass
    
    # Operasi DML (return ExecutionResult)
    
    @abstractmethod
    def visit_insert(self, plan: 'InsertPlan') -> Any:
        pass
    
    @abstractmethod
    def visit_update(self, plan: 'UpdatePlan') -> Any:
        pass
    
    @abstractmethod
    def visit_delete(self, plan: 'DeletePlan') -> Any:
        pass
    
    # Operasi DDL (return ExecutionResult)
    
    @abstractmethod
    def visit_create_table(self, plan: 'CreateTablePlan') -> Any:
        pass
    
    @abstractmethod
    def visit_drop_table(self, plan: 'DropTablePlan') -> Any:
        pass