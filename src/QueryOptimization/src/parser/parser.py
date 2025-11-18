from ..tree.query_tree import QueryTree
from ..tree.nodes import NodeType, ConditionLeaf, ConditionOperator
from ..tree.parsed_query import ParsedQuery
from .lexer import Lexer, KEYWORDS

class Parser:
    def __init__(self):
        self.lexer = Lexer()

    def parse_condition(self, condition_tokens):
        """Parse condition tokens into a ConditionNode tree"""
        if not condition_tokens:
            return None

        # Remove outer parentheses
        while (len(condition_tokens) > 2 and
            condition_tokens[0] == '(' and
            condition_tokens[-1] == ')'):
            condition_tokens = condition_tokens[1:-1]
        
        paren_depth = 0
        or_positions = []
        and_positions = []
        
        for i, token in enumerate(condition_tokens):
            if token == '(':
                paren_depth += 1
            elif token == ')':
                paren_depth -= 1
            elif paren_depth == 0:  
                if token == 'OR':
                    or_positions.append(i)
                elif token == 'AND':
                    and_positions.append(i)

        # Process OR operators
        if or_positions:
            split_idx = or_positions[0]
            left_tokens = condition_tokens[:split_idx]
            right_tokens = condition_tokens[split_idx + 1:]

            left_condition = self.parse_condition(left_tokens)
            right_condition = self.parse_condition(right_tokens)

            if left_condition and right_condition:
                return ConditionOperator("OR", left_condition, right_condition)

        # Process AND operators
        elif and_positions:
            split_idx = and_positions[0]
            left_tokens = condition_tokens[:split_idx]
            right_tokens = condition_tokens[split_idx + 1:]

            left_condition = self.parse_condition(left_tokens)
            right_condition = self.parse_condition(right_tokens)

            if left_condition and right_condition:
                return ConditionOperator("AND", left_condition, right_condition)

        # Base case: single condition
        condition_str = " ".join(condition_tokens)
        return ConditionLeaf(condition_str)


    def parse_query(self, query: str) -> ParsedQuery:
        """Parse SQL query string into ParsedQuery object"""
        tokens = self.lexer.tokenize(query)
        if "SELECT" not in tokens or "FROM" not in tokens:
            raise ValueError("Invalid query syntax")

        select_idx = tokens.index("SELECT")
        from_idx = tokens.index("FROM")
        where_idx = tokens.index("WHERE") if "WHERE" in tokens else len(tokens)
        group_idx = tokens.index("GROUP") if "GROUP" in tokens else len(tokens)
        having_idx = tokens.index("HAVING") if "HAVING" in tokens else len(tokens)
        order_idx = tokens.index("ORDER") if "ORDER" in tokens else len(tokens)

        clause_ends = sorted([where_idx, group_idx, having_idx, order_idx,
                            len(tokens)])

        where_end = min([idx for idx in clause_ends if idx > where_idx],
                        default=len(tokens))
        group_end = min([idx for idx in clause_ends if idx > group_idx],
                        default=len(tokens))
        having_end = min([idx for idx in clause_ends if idx > having_idx],
                        default=len(tokens))
        order_end = len(tokens)

        # Parse SELECT clause
        select_tokens = tokens[select_idx + 1: from_idx]
        select_attrs = []
        current_attr = []
        i = 0

        while i < len(select_tokens):
            token = select_tokens[i]
            if token == ',':
                if current_attr:
                    select_attrs.append(' '.join(current_attr))
                    current_attr = []
            elif token == 'AS':
                # Handle "column AS alias"
                if i + 1 < len(select_tokens) and select_tokens[i + 1] != ',':
                    current_attr.append('AS')
                    current_attr.append(select_tokens[i + 1])
                    i += 1
            else:
                current_attr.append(token)
            i += 1

        if current_attr:
            select_attrs.append(' '.join(current_attr))

        # Parse FROM clause
        from_tokens = tokens[from_idx + 1: where_idx]

        # Filter out JOIN type keywords
        filtered_from_tokens = [t for t in from_tokens
                                if t not in ['INNER', 'LEFT', 'RIGHT', 'OUTER']]

        # Find JOIN positions
        join_positions = []
        for i, token in enumerate(filtered_from_tokens):
            if token == "JOIN":
                join_positions.append(i)
        
        if not join_positions:
            # Simple FROM clause without JOINs
            tables = []
            i = 0
            while i < len(filtered_from_tokens):
                if filtered_from_tokens[i] == ',':
                    i += 1
                    continue
                
                table_name = filtered_from_tokens[i]
                if (i + 1 < len(filtered_from_tokens) and
                        filtered_from_tokens[i + 1] != ',' and
                        filtered_from_tokens[i + 1].upper() not in KEYWORDS):
                    alias = filtered_from_tokens[i + 1]
                    tables.append((table_name, alias))
                    i += 2
                else:
                    tables.append((table_name, None))
                    i += 1
            
            table_nodes = []
            for table_name, alias in tables:
                table_nodes.append(QueryTree(NodeType.TABLE.value, table_name,
                                            [], None))
        else:
            # Handle JOINs
            table_nodes = []
            
            first_table_tokens = filtered_from_tokens[:join_positions[0]]
            if len(first_table_tokens) >= 1:
                table_name = first_table_tokens[0]
                alias = first_table_tokens[1] if len(first_table_tokens) > 1 and first_table_tokens[1].upper() not in KEYWORDS else None
                current_node = QueryTree(NodeType.TABLE.value, table_name, [], None)
                
                for i, join_pos in enumerate(join_positions):
                    if i + 1 < len(join_positions):
                        end_pos = join_positions[i + 1]
                    else:
                        end_pos = len(filtered_from_tokens)
                    
                    join_tokens = filtered_from_tokens[join_pos:end_pos]
                    
                    if len(join_tokens) >= 2 and "ON" in join_tokens:
                        on_idx = join_tokens.index("ON")
                        
                        table_tokens = join_tokens[1:on_idx]
                        join_table_name = table_tokens[0]
                        join_alias = table_tokens[1] if len(table_tokens) > 1 and table_tokens[1].upper() not in KEYWORDS else None
                        
                        condition_tokens = join_tokens[on_idx + 1:]
                        join_condition = self.parse_condition(condition_tokens)
                        
                        join_table = QueryTree(NodeType.TABLE.value, join_table_name, [], None)
                        join_node = QueryTree(NodeType.JOIN.value, join_condition, [], None)
                        join_node.add_child(current_node)
                        join_node.add_child(join_table)
                        
                        current_node = join_node
                
                table_nodes = [current_node]

        # Buat tree
        current_tree = table_nodes[0] if table_nodes else None

        # WHERE 
        if where_idx < len(tokens) and where_end > where_idx + 1:
            condition_tokens = tokens[where_idx + 1: where_end]
            where_condition = self.parse_condition(condition_tokens)
            where_node = QueryTree(NodeType.SELECT.value, where_condition, [], None)
            where_node.add_child(current_tree)
            current_tree = where_node

        # GROUP BY 
        if group_idx < len(tokens) and group_end > group_idx + 1:
            group_tokens = tokens[group_idx + 2: group_end] 
            group_attrs = [t for t in group_tokens if t != ',']
            group_node = QueryTree(NodeType.GROUP_BY.value, group_attrs, [], None)
            group_node.add_child(current_tree)
            current_tree = group_node

        # HAVING 
        if having_idx < len(tokens) and having_end > having_idx + 1:
            having_tokens = tokens[having_idx + 1: having_end]
            having_condition = self.parse_condition(having_tokens)
            having_node = QueryTree(NodeType.HAVING.value, having_condition, [], None)
            having_node.add_child(current_tree)
            current_tree = having_node

        # ORDER BY 
        if order_idx < len(tokens):
            order_tokens = tokens[order_idx + 2: order_end]  
            order_attrs = []
            current_order = []
            for token in order_tokens:
                if token == ',':
                    if current_order:
                        order_attrs.append(' '.join(current_order))
                        current_order = []
                else:
                    current_order.append(token)
            if current_order:
                order_attrs.append(' '.join(current_order))
            
            order_node = QueryTree(NodeType.ORDER_BY.value, order_attrs, [], None)
            order_node.add_child(current_tree)
            current_tree = order_node

        # PROJECT
        root = QueryTree(NodeType.PROJECT.value, select_attrs, [], None)
        root.add_child(current_tree)

        parsed = ParsedQuery(query, root)
        return parsed