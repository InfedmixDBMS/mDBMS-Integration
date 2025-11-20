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
    LogicalOperator
)
from typing import Optional, List, Union
import re


class IntegratedQueryOptimizer(AbstractQueryOptimizer):

    def __init__(self):
        self.engine = OptimizationEngine()
        
    def optimize(self, query: str) -> QueryPlan:
        parsed_query = self.engine.parse_query(query)
        parsed_query.print_tree()
        return self.convert_parsed_to_plan(parsed_query)
    
    def convert_parsed_to_plan(self, parsed_query: ParsedQuery) -> QueryPlan:
        return self._convert_tree_node(parsed_query.query_tree)
    
    def _convert_tree_node(self, node: QueryTree) -> QueryPlan:
        node_type = node.type
        
        if node_type == "TABLE":
            # Leaf node: TableScanNode
            table_name = node.val
            return TableScanNode(table_name = table_name)
        
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
            
            # Konversi join condition
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