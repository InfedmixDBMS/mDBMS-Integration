from typing import Any, List, Union, Dict
from enum import Enum


class ComparisonOperator(Enum):
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    LIKE = "LIKE"
    IN = "IN"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    
    @classmethod
    def from_string(cls, op_str: str) -> 'ComparisonOperator':
        op_str = op_str.upper().strip()
        mapping = {
            "=": cls.EQUALS,
            "!=": cls.NOT_EQUALS,
            ">": cls.GREATER_THAN,
            "<": cls.LESS_THAN,
            ">=": cls.GREATER_EQUAL,
            "<=": cls.LESS_EQUAL,
            "LIKE": cls.LIKE,
            "IN": cls.IN,
            "BETWEEN": cls.BETWEEN,
            "IS NULL": cls.IS_NULL,
            "IS NOT NULL": cls.IS_NOT_NULL
        }
        if op_str in mapping:
            return mapping[op_str]
        raise ValueError(f"Unknown operator: {op_str}")


class LogicalOperator(Enum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    
    @classmethod
    def from_string(cls, op_str: str) -> 'LogicalOperator':
        op_str = op_str.upper().strip()
        mapping = {
            "AND": cls.AND,
            "OR": cls.OR,
            "NOT": cls.NOT
        }
        if op_str in mapping:
            return mapping[op_str]
        raise ValueError(f"Unknown logical operator: {op_str}")


class WhereCondition:
    
    def __init__(
        self,
        column: str,
        operator: Union[ComparisonOperator, str],
        value: Any
    ):
        self.column = column
        # Konversi string ke enum jika perlu
        if isinstance(operator, str):
            self.operator = ComparisonOperator.from_string(operator)
        else:
            self.operator = operator
        self.value = value
    
    def __repr__(self) -> str:
        return f"WhereCondition({self.column} {self.operator.value} {self.value})"
    
    def evaluate(self, row_data: Dict[str, Any]) -> bool:
        if self.column not in row_data:
            return False
        
        left_val = row_data[self.column]
        
        if isinstance(self.value, ColumnReference):
            if self.value.column_name not in row_data:
                return False
            right_val = row_data[self.value.column_name]
        else:
            right_val = self.value
        
        if self.operator == ComparisonOperator.EQUALS:
            return left_val == right_val
        elif self.operator == ComparisonOperator.NOT_EQUALS:
            return left_val != right_val
        elif self.operator == ComparisonOperator.GREATER_THAN:
            return left_val > right_val
        elif self.operator == ComparisonOperator.LESS_THAN:
            return left_val < right_val
        elif self.operator == ComparisonOperator.GREATER_EQUAL:
            return left_val >= right_val
        elif self.operator == ComparisonOperator.LESS_EQUAL:
            return left_val <= right_val
        elif self.operator == ComparisonOperator.LIKE:
            import re
            pattern = str(right_val).replace('%', '.*').replace('_', '.')
            return bool(re.match(pattern, str(left_val)))
        elif self.operator == ComparisonOperator.IN:
            return left_val in right_val if isinstance(right_val, (list, tuple)) else False
        elif self.operator == ComparisonOperator.BETWEEN:
            if isinstance(right_val, (list, tuple)) and len(right_val) == 2:
                return right_val[0] <= left_val <= right_val[1]
            return False
        elif self.operator == ComparisonOperator.IS_NULL:
            return left_val is None
        elif self.operator == ComparisonOperator.IS_NOT_NULL:
            return left_val is not None
        else:
            return False

class ColumnReference:
    def __init__(self, column_name: str):
        self.column_name = column_name
    
    def __repr__(self):
        return f"REF({self.column_name})"

class LogicalCondition:
    
    def __init__(
        self,
        operator: Union[LogicalOperator, str],
        conditions: List[Union[WhereCondition, 'LogicalCondition']]
    ):
        # Konversi string ke enum jika perlu
        if isinstance(operator, str):
            self.operator = LogicalOperator.from_string(operator)
        else:
            self.operator = operator
        self.conditions = conditions
        
        # Validasi: NOT harus punya tepat 1 condition
        if self.operator == LogicalOperator.NOT and len(conditions) != 1:
            raise ValueError("NOT operator requires exactly one condition")
    
    def __repr__(self) -> str:
        return f"LogicalCondition({self.operator.value} of {len(self.conditions)} conditions)"
    
    def evaluate(self, row_data: Dict[str, Any]) -> bool:
        if self.operator == LogicalOperator.AND:
            return all(cond.evaluate(row_data) for cond in self.conditions)
        elif self.operator == LogicalOperator.OR:
            return any(cond.evaluate(row_data) for cond in self.conditions)
        elif self.operator == LogicalOperator.NOT:
            return not self.conditions[0].evaluate(row_data)
        else:
            return False


class OrderByClause:
    
    def __init__(self, column: str, direction: str = "ASC"):
        self.column = column
        self.direction = direction.upper()
        
        if self.direction not in ['ASC', 'DESC']:
            raise ValueError(f"Invalid sort direction: {direction}")
    
    def __repr__(self) -> str:
        return f"OrderByClause({self.column} {self.direction})"


class GroupByClause:
    
    def __init__(
        self,
        columns: List[str],
        having: Union[WhereCondition, LogicalCondition, None] = None
    ):
        self.columns = columns
        self.having = having
    
    def __repr__(self) -> str:
        return f"GroupByClause(columns={self.columns})"


class JoinCondition:
    
    def __init__(
        self,
        left_table: str,
        right_table: str,
        condition: Union[WhereCondition, LogicalCondition],
        join_type: str = "INNER JOIN"
    ):
        self.left_table = left_table
        self.right_table = right_table
        self.condition = condition
        self.join_type = join_type.upper()
    
    def __repr__(self) -> str:
        return f"JoinCondition({self.join_type}: {self.left_table} JOIN {self.right_table})"

