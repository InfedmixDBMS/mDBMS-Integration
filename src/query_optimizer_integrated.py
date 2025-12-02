from QueryProcessor.interfaces import AbstractQueryOptimizer
from QueryOptimization.src.optimizer.optimization_engine import OptimizationEngine
from QueryOptimization.src.parser.parser import ParsedQuery
from QueryOptimization.src.tree.query_tree import QueryTree
from QueryOptimization.src.tree.nodes import ConditionNode, ConditionLeaf, ConditionOperator
from QueryProcessor.models import (
    QueryPlan,
    TableScanNode,
    FilterNode,
    ProjectNode,
    SortNode,
    NestedLoopJoinNode,
    WhereCondition,
    LogicalCondition,
    OrderByClause,
    JoinCondition,
    ComparisonOperator,
    LogicalOperator,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    CreateTablePlan,
    DropTablePlan
)
from typing import Optional, List, Union
import re


class IntegratedQueryOptimizer(AbstractQueryOptimizer):

    def __init__(self):
        self.engine = OptimizationEngine()
        
    def optimize(self, query: str) -> QueryPlan:
        # Simple regex for DDL/DML
        if re.match(r'^\s*CREATE\s+TABLE', query, re.IGNORECASE):
            return self._parse_create_table(query)
        elif re.match(r'^\s*INSERT\s+INTO', query, re.IGNORECASE):
            return self._parse_insert(query)
        elif re.match(r'^\s*DELETE\s+FROM', query, re.IGNORECASE):
            return self._parse_delete(query)
        elif re.match(r'^\s*UPDATE', query, re.IGNORECASE):
            return self._parse_update(query)
        elif re.match(r'^\s*DROP\s+TABLE', query, re.IGNORECASE):
            return self._parse_drop_table(query)
            
        parsed_query = self.engine.parse_query(query)
        # parsed_query.print_tree()
        return self.convert_parsed_to_plan(parsed_query)
    
    def convert_parsed_to_plan(self, parsed_query: ParsedQuery) -> QueryPlan:
        return self._convert_tree_node(parsed_query.query_tree)
    
    def _convert_tree_node(self, node: QueryTree) -> QueryPlan:
        node_type = node.type
        
        if node_type == "TABLE":
            # Leaf node: TableScanNode
            table_name = node.val
            alias = getattr(node, 'alias', None)  # Get alias if it exists
            return TableScanNode(table_name=table_name, alias=alias)
        
        elif node_type == "SELECT":
            # Filter node dengan WHERE condition
            if len(node.childs) == 0:
                raise ValueError("SELECT node harus memiliki child node")
            
            child_plan = self._convert_tree_node(node.childs[0])
            condition = self._convert_condition(node.val)
            return FilterNode(child = child_plan, condition = condition)
        
        elif node_type == "PROJECT":
            # Project node dengan SELECT columns
            if len(node.childs) == 0:
                raise ValueError("PROJECT node harus memiliki child node")
            
            child_plan = self._convert_tree_node(node.childs[0])
            columns = node.val if isinstance(node.val, list) else [node.val]
            return ProjectNode(child=child_plan, columns=columns)
        
        elif node_type == "ORDER-BY":
            # Sort node dengan ORDER BY clause
            if len(node.childs) == 0:
                raise ValueError("ORDER-BY node harus memiliki child node")
            
            child_plan = self._convert_tree_node(node.childs[0])
            order_by_clauses = self._parse_order_by(node.val)
            return SortNode(child=child_plan, order_by=order_by_clauses)
        
        elif node_type == "JOIN":
            # Join node dengan kondisi JOIN
            if len(node.childs) < 2:
                raise ValueError("JOIN node harus memiliki minimal 2 child nodes")
            
            left_plan = self._convert_tree_node(node.childs[0])
            right_plan = self._convert_tree_node(node.childs[1])
            
            # Extract table names untuk JoinCondition
            left_table = self._extract_table_name(node.childs[0])
            right_table = self._extract_table_name(node.childs[1])
            
            # Konversi join condition (None for CROSS JOIN)
            if node.val is None:
                # CROSS JOIN (no condition)
                return NestedLoopJoinNode(
                    left_child=left_plan,
                    right_child=right_plan,
                    join_condition=None
                )
            else:
                # INNER JOIN with condition
                condition = self._convert_condition(node.val)
                join_condition = JoinCondition(
                    left_table=left_table,
                    right_table=right_table,
                    condition=condition,
                    join_type="INNER JOIN"
                )
                
                return NestedLoopJoinNode(
                    left_child=left_plan,
                    right_child=right_plan,
                    join_condition=join_condition
                )
        
        elif node_type == "GROUP-BY":
            # TODO: Implementasi GROUP BY belum ada di QueryPlan
            # Sementara skip GROUP BY dan convert child saja
            if len(node.childs) > 0:
                return self._convert_tree_node(node.childs[0])
            raise ValueError("GROUP-BY node belum diimplementasikan")
        
        elif node_type == "HAVING":
            # TODO: Implementasi HAVING belum ada di QueryPlan
            if len(node.childs) > 0:
                return self._convert_tree_node(node.childs[0])
            raise ValueError("HAVING node belum diimplementasikan")
        
        elif node_type == "CARTESIAN-PRODUCT":
            # TODO: Implementasi Cartesian Product belum ada di QueryPlan
            raise ValueError("CARTESIAN-PRODUCT belum diimplementasikan")
        
        elif node_type == "NATURAL-JOIN":
            # TODO: Implementasi Natural Join belum ada di QueryPlan
            raise ValueError("NATURAL-JOIN belum diimplementasikan")
        
        elif node_type == "HASH-JOIN":
            # TODO: Implementasi Hash Join belum ada di QueryPlan
            raise ValueError("HASH-JOIN belum diimplementasikan")
        
        elif node_type == "LIMIT":
            # TODO: LIMIT bisa ditambahkan ke SortNode
            raise ValueError("LIMIT node belum diimplementasikan secara terpisah")
        
        else:
            raise ValueError(f"Unknown node type: {node_type}")
    
    def _convert_condition(self, condition_node: ConditionNode) -> Union[WhereCondition, LogicalCondition]:
        
        if condition_node is None:
            return None
        
        if isinstance(condition_node, ConditionLeaf):
            return self._parse_simple_condition(condition_node.condition)
        
        elif isinstance(condition_node, ConditionOperator):
            left_cond = self._convert_condition(condition_node.left)
            right_cond = self._convert_condition(condition_node.right)
            
            operator = LogicalOperator.from_string(condition_node.operator)
            return LogicalCondition(
                operator=operator,
                conditions=[left_cond, right_cond]
            )
        
        else:
            raise ValueError(f"Unknown condition type: {type(condition_node)}")
    
    def _parse_simple_condition(self, condition_str: str) -> WhereCondition:
        """
        Parse string kondisi sederhana seperti "age > 25" menjadi WhereCondition
        """
        condition_str = condition_str.strip()
        
        operators = ['>=', '<=', '!=', '=', '>', '<', 'LIKE', 'IN', 'BETWEEN']
        
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op, 1)
                if len(parts) == 2:
                    column = parts[0].strip()
                    value_str = parts[1].strip()
                    
                    # Parse value
                    value = self._parse_value(value_str)
                    
                    return WhereCondition(
                        column=column,
                        operator=ComparisonOperator.from_string(op),
                        value=value
                    )
        
        raise ValueError(f"Cannot parse condition: {condition_str}")
    
    def _parse_value(self, value_str: str):
        """
        Parse nilai dari string menjadi tipe data yang sesuai
        """
        value_str = value_str.strip()
        
        # String literal (quoted)
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Integer
        try:
            return int(value_str)
        except ValueError:
            pass
        
        # Float
        try:
            return float(value_str)
        except ValueError:
            pass
        
        # Boolean
        if value_str.upper() == 'TRUE':
            return True
        elif value_str.upper() == 'FALSE':
            return False
        elif value_str.upper() == 'NULL':
            return None
        
        if '.' in value_str or value_str.replace('_', '').isalnum():
            from QueryProcessor.models.conditions import ColumnReference
            col_ref = ColumnReference(value_str)
            return col_ref
        
        return value_str
    
    def _parse_order_by(self, order_by_val) -> List[OrderByClause]:
        """
        Parse ORDER BY value menjadi list of OrderByClause
        """
        if isinstance(order_by_val, list):
            clauses = []
            for item in order_by_val:
                parts = item.split()
                column = parts[0]
                direction = parts[1] if len(parts) > 1 else "ASC"
                clauses.append(OrderByClause(column=column, direction=direction))
            return clauses
        else:
            # Single order by
            parts = str(order_by_val).split()
            column = parts[0]
            direction = parts[1] if len(parts) > 1 else "ASC"
            return [OrderByClause(column=column, direction=direction)]
    
    def _extract_table_name(self, node: QueryTree) -> str:
        """
        Ekstrak nama tabel dari QueryTree node (recursive)
        """
        if node.type == "TABLE":
            return node.val
        elif len(node.childs) > 0:
            return self._extract_table_name(node.childs[0])
        else:
            return "unknown_table"
    
    def _parse_create_table(self, query: str) -> CreateTablePlan:
        # CREATE TABLE table_name (col1 type1, col2 type2)
        match = re.match(r'^\s*CREATE\s+TABLE\s+(\w+)\s*\((.+)\)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        schema = {}
        for col_def in columns_str.split(','):
            parts = col_def.strip().split()
            if len(parts) >= 2:
                col_name = parts[0]
                col_type = parts[1]
                schema[col_name] = col_type
        
        return CreateTablePlan(table_name=table_name, schema=schema)

    def _parse_insert(self, query: str) -> InsertPlan:
        # INSERT INTO table_name VALUES (val1, val2)
        match = re.match(r'^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        values_str = match.group(2)
        
        values = []
        for val in values_str.split(','):
            val = val.strip()
            parsed_val = self._parse_value(val)
            values.append(parsed_val)
            
        return InsertPlan(table_name=table_name, columns=[], values=values)

    def _parse_delete(self, query: str) -> DeletePlan:
        # DELETE FROM table_name [WHERE condition]
        match = re.match(r'^\s*DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DELETE syntax")
            
        table_name = match.group(1)
        where_clause = match.group(2)
        
        where_condition = None
        if where_clause:
            where_condition = self._parse_simple_condition(where_clause)
            
        return DeletePlan(table_name=table_name, where=where_condition)

    def _parse_update(self, query: str) -> UpdatePlan:
        # UPDATE table_name SET col=val [WHERE condition]
        match = re.match(r'^\s*UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid UPDATE syntax")
            
        table_name = match.group(1)
        set_clause_str = match.group(2)
        where_clause = match.group(3)
        
        set_clause = {}
        for assignment in set_clause_str.split(','):
            parts = assignment.split('=')
            if len(parts) == 2:
                col = parts[0].strip()
                val = self._parse_value(parts[1].strip())
                set_clause[col] = val
        
        where_condition = None
        if where_clause:
            where_condition = self._parse_simple_condition(where_clause)
            
        return UpdatePlan(table_name=table_name, set_clause=set_clause, where=where_condition)

    def _parse_drop_table(self, query: str) -> DropTablePlan:
        match = re.match(r'^\s*DROP\s+TABLE\s+(\w+)', query, re.IGNORECASE)
        if not match:
            raise ValueError("Invalid DROP TABLE syntax")
        return DropTablePlan(table_name=match.group(1))