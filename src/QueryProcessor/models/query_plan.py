from abc import ABC, abstractmethod
from typing import Optional, List, Any, Dict, TYPE_CHECKING
from .rows import Rows
from .conditions import WhereCondition, OrderByClause, JoinCondition

if TYPE_CHECKING:
    from ..executor.plan_visitor import QueryPlanVisitor


class QueryPlan(ABC):
    
    def __init__(self):
        self.cost: Optional[float] = None  # Set by optimizer, None by default
    
    @abstractmethod
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        pass
    
    @abstractmethod
    def get_node_type(self) -> str:
        pass
    
    def print_tree(self, indent: int = 0, prefix: str = "") -> str:
        return self._print_tree_recursive(indent, prefix, True)
    
    @abstractmethod
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        pass
    
    def get_cost(self) -> Optional[float]:
        return self.cost
    
    def set_cost(self, cost: float) -> None:
        self.cost = cost


class TableScanNode(QueryPlan):
    
    def __init__(self, table_name: str, alias: Optional[str] = None):
        super().__init__()
        self.table_name = table_name
        self.alias = alias
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_table_scan(self)
    
    def get_node_type(self) -> str:
        return "TableScan"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        result = f"{prefix}{connector}TableScan: {self.table_name}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        if self.alias:
            return f"TableScan(table={self.table_name} AS {self.alias})"
        return f"TableScan(table={self.table_name})"


class FilterNode(QueryPlan):
    
    def __init__(self, child: QueryPlan, condition: WhereCondition, alias: Optional[str] = None):
        super().__init__()
        self.child = child
        self.condition = condition
        self.alias = alias
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_filter(self)
    
    def get_node_type(self) -> str:
        return "Filter"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        alias_str = f" AS {self.alias}" if self.alias else ""
        result = f"{prefix}{connector}Filter: {self.condition}{alias_str}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        
        # Print child with updated prefix
        child_prefix = prefix + ("    " if is_last else "│   ")
        result += self.child._print_tree_recursive(indent + 1, child_prefix, True)
        return result
    
    def __repr__(self) -> str:
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"FilterNode(condition={self.condition}{alias_str})"


class ProjectNode(QueryPlan):
    
    def __init__(self, child: QueryPlan, columns: List[str], alias: Optional[str] = None):
        super().__init__()
        self.child = child
        self.columns = columns
        self.alias = alias
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_project(self)
    
    def get_node_type(self) -> str:
        return "Project"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        columns_str = ", ".join(self.columns[:3])  # Show first 3 columns
        if len(self.columns) > 3:
            columns_str += f", ... ({len(self.columns)} total)"
        alias_str = f" AS {self.alias}" if self.alias else ""
        result = f"{prefix}{connector}Project: [{columns_str}]{alias_str}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        
        # Print child with updated prefix
        child_prefix = prefix + ("    " if is_last else "│   ")
        result += self.child._print_tree_recursive(indent + 1, child_prefix, True)
        return result
    
    def __repr__(self) -> str:
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"ProjectNode(columns={self.columns}{alias_str})"


class SortNode(QueryPlan):
    
    def __init__(
        self,
        child: QueryPlan,
        order_by: List[OrderByClause],
        limit: Optional[int] = None,
        alias: Optional[str] = None
    ):
        super().__init__()
        self.child = child
        self.order_by = order_by
        self.limit = limit
        self.alias = alias
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_sort(self)
    
    def get_node_type(self) -> str:
        return "Sort"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        order_str = ", ".join([f"{o.column} {o.direction}" for o in self.order_by[:2]])
        if len(self.order_by) > 2:
            order_str += f", ... ({len(self.order_by)} columns)"
        limit_str = f" LIMIT {self.limit}" if self.limit else ""
        alias_str = f" AS {self.alias}" if self.alias else ""
        result = f"{prefix}{connector}Sort: [{order_str}]{limit_str}{alias_str}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        
        # Print child with updated prefix
        child_prefix = prefix + ("    " if is_last else "│   ")
        result += self.child._print_tree_recursive(indent + 1, child_prefix, True)
        return result
    
    def __repr__(self) -> str:
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"SortNode(order_by={self.order_by}, limit={self.limit}{alias_str})"


class NestedLoopJoinNode(QueryPlan):
    
    def __init__(
        self,
        left_child: QueryPlan,
        right_child: QueryPlan,
        join_condition: JoinCondition,
        alias: Optional[str] = None
    ):
        super().__init__()
        self.left_child = left_child
        self.right_child = right_child
        self.join_condition = join_condition
        self.alias = alias
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_join(self)
    
    def get_node_type(self) -> str:
        return "NestedLoopJoin"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        alias_str = f" AS {self.alias}" if self.alias else ""
        result = f"{prefix}{connector}NestedLoopJoin: {self.join_condition.join_type}{alias_str}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Condition: {self.join_condition.condition}]\n"
        
        # Print children with updated prefix
        child_prefix = prefix + ("    " if is_last else "│   ")
        
        # Left child
        result += self.left_child._print_tree_recursive(indent + 1, child_prefix, False)
        
        # Right child
        result += self.right_child._print_tree_recursive(indent + 1, child_prefix, True)
        
        return result
    
    def __repr__(self) -> str:
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"NestedLoopJoinNode(condition={self.join_condition}{alias_str})"



class InsertPlan(QueryPlan):
    
    def __init__(
        self,
        table_name: str,
        columns: List[str],
        values: List[Any]
    ):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.values = values
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_insert(self)
    
    def get_node_type(self) -> str:
        return "Insert"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        values_str = str(self.values)[:50]  # Truncate long values
        if len(str(self.values)) > 50:
            values_str += "..."
        result = f"{prefix}{connector}Insert: {self.table_name}\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Columns: {self.columns}]\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Values: {values_str}]\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        return f"InsertPlan(table={self.table_name}, columns={len(self.columns)})"


class UpdatePlan(QueryPlan):
    
    def __init__(
        self,
        table_name: str,
        set_clause: Dict[str, Any],
        where: Optional[WhereCondition] = None
    ):
        super().__init__()
        self.table_name = table_name
        self.set_clause = set_clause
        self.where = where
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_update(self)
    
    def get_node_type(self) -> str:
        return "Update"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        set_str = ", ".join([f"{k}={v}" for k, v in list(self.set_clause.items())[:3]])
        if len(self.set_clause) > 3:
            set_str += "..."
        where_str = f"WHERE {self.where}" if self.where else "No WHERE"
        result = f"{prefix}{connector}Update: {self.table_name}\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [SET: {set_str}]\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [{where_str}]\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        return f"UpdatePlan(table={self.table_name}, sets={len(self.set_clause)})"


class DeletePlan(QueryPlan):
    
    def __init__(
        self,
        table_name: str,
        where: Optional[WhereCondition] = None
    ):
        super().__init__()
        self.table_name = table_name
        self.where = where
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_delete(self)
    
    def get_node_type(self) -> str:
        return "Delete"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        where_str = f"WHERE {self.where}" if self.where else "No WHERE (all rows)"
        result = f"{prefix}{connector}Delete: {self.table_name}\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [{where_str}]\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        return f"DeletePlan(table={self.table_name})"


class CreateTablePlan(QueryPlan):
    
    def __init__(
        self,
        table_name: str,
        schema: Dict[str, str]
    ):
        super().__init__()
        self.table_name = table_name
        self.schema = schema
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        return visitor.visit_create_table(self)
    
    def get_node_type(self) -> str:
        return "CreateTable"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        connector = "└── " if is_last else "├── "
        schema_str = ", ".join([f"{k} {v}" for k, v in list(self.schema.items())[:3]])
        if len(self.schema) > 3:
            schema_str += f", ... ({len(self.schema)} columns)"
        result = f"{prefix}{connector}CreateTable: {self.table_name}\n"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Schema: {schema_str}]\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        return f"CreateTablePlan(table={self.table_name}, columns={len(self.schema)})"


class DropTablePlan(QueryPlan):
    
    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = table_name
    
    def accept(self, visitor: 'QueryPlanVisitor') -> Any:
        """Delegate to visitor's visit_drop_table method."""
        return visitor.visit_drop_table(self)
    
    def get_node_type(self) -> str:
        return "DropTable"
    
    def _print_tree_recursive(self, indent: int, prefix: str, is_last: bool) -> str:
        """Print DropTable node."""
        connector = "└── " if is_last else "├── "
        result = f"{prefix}{connector}DropTable: {self.table_name}\n"
        cost_str = f"{self.cost:.1f}" if self.cost is not None else "N/A"
        result += f"{prefix}{'    ' if is_last else '│   '}  [Cost: {cost_str}]\n"
        return result
    
    def __repr__(self) -> str:
        return f"DropTablePlan(table={self.table_name})"