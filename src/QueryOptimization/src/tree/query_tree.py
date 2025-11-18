"""
QueryTree Module - Core tree structure for representing parsed SQL queries
"""

from typing import List, Optional
from .nodes import NodeType, ConditionNode, ConditionLeaf, ConditionOperator


class QueryTree:
    """
    Represents a node in a query tree structure
    """

    def __init__(self, type: str, val, childs: List['QueryTree'],
                 parent: Optional['QueryTree']):
        """
        Initialize a QueryTree node
        """
        self.type = type
        self.val = val
        self.childs = childs if childs is not None else []
        self.parent = parent

    def add_child(self, child: 'QueryTree'):
        """
        Add a child node to this query tree node
        """
        child.parent = self
        self.childs.append(child)

    def __str__(self):
        """String representation of the node"""
        return f"{self.type}: {self.val}"

    def print_tree(self, level=0):
        """
        Print the tree structure with indentation for visualization
        """
        indent = '  ' * level
        if isinstance(self.val, ConditionNode):
            condition_str = self.condition_node_to_string(self.val)
        else:
            condition_str = self.val
        if isinstance(condition_str, list):
            condition_str = ", ".join(condition_str)
        print(f"{indent}{self.type}: {condition_str}")
        for child in self.childs:
            child.print_tree(level + 1)

    def condition_node_to_string(self, node: ConditionNode) -> str:
        """
        Convert a ConditionNode to its string representation
        """
        if isinstance(node, ConditionLeaf):
            return node.condition
        elif isinstance(node, ConditionOperator):
            left_str = self.condition_node_to_string(node.left)
            right_str = self.condition_node_to_string(node.right)
            return f"({left_str} {node.operator} {right_str})"
        return str(node)