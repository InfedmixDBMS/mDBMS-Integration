"""
ParsedQuery Module - Represents a fully parsed SQL query
"""

from typing import Optional
from .query_tree import QueryTree


class ParsedQuery:
    """
    Represents a parsed SQL query with its tree structure and metadata
    """

    def __init__(self, query: str, query_tree: QueryTree):
        """
        Initialize a ParsedQuery with SQL string and query tree

        """
        self.query_tree = query_tree
        self.query = query
        self.tables = []
        self.collect_tables(self.query_tree)

    def collect_tables(self, node: QueryTree):
        """
        Collect table names from the query tree
        """
        if node.type == "TABLE":
            table_name = node.val
            if table_name not in self.tables:
                self.tables.append(table_name)
        for child in node.childs:
            self.collect_tables(child)

    def print_tree(self, node: Optional[QueryTree] = None, level: int = 0):
        """
        Print the query tree structure with optional starting node
        """
        start_node = node if node is not None else self.query_tree
        start_node.print_tree(level)