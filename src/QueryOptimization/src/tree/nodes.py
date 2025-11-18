"""
Nodes Module - Defines all node types and data structures for query trees
"""

from enum import Enum
from typing import Union


class NodeType(Enum):
    """Enumeration of different node types in query trees"""
    PROJECT = "PROJECT"
    SELECT = "SELECT"
    TABLE = "TABLE"
    JOIN = "JOIN"
    CARTESIAN_PRODUCT = "CARTESIAN-PRODUCT"
    NATURAL_JOIN = "NATURAL-JOIN"
    HASH_JOIN = "HASH-JOIN"
    UPDATE = "UPDATE"
    SET = "SET"
    ORDER_BY = "ORDER-BY"
    GROUP_BY = "GROUP-BY"
    HAVING = "HAVING"
    LIMIT = "LIMIT"
    CONDITION = "CONDITION"

    def __str__(self):
        return self.value


class ConditionNode:
    """Base class for all condition nodes in query tree representations"""
    pass


class ConditionOperator(ConditionNode):
    """
    Represents a logical operator (AND, OR) between conditions
    """

    def __init__(self, operator: str, left: 'ConditionNode',
                 right: 'ConditionNode'):
        self.operator = operator
        self.left = left
        self.right = right

    def __eq__(self, other):
        if not isinstance(other, ConditionOperator):
            return False
        return (self.operator == other.operator and
                self.left == other.left and
                self.right == other.right)

    def __repr__(self):
        return f"({self.left} {self.operator} {self.right})"


class ConditionLeaf(ConditionNode):
    """
    Represents a basic condition in SQL queries
    """

    def __init__(self, condition: str):
        self.condition = condition

    def __eq__(self, other):
        if not isinstance(other, ConditionLeaf):
            return False
        return self.condition == other.condition

    def __repr__(self):
        return self.condition


ConditionNode = Union[ConditionOperator, ConditionLeaf]