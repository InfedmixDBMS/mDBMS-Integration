from typing import List, Dict, Any


class Rows:
    
    def __init__(self, columns: List[str], data: List[List[Any]]):
        self.columns = columns
        self.data = data
        self.row_count = len(data)
    
    def __repr__(self) -> str:
        return f"Rows(columns={self.columns}, row_count={self.row_count})"
    
    def to_dict(self) -> List[Dict[str, Any]]:
        return [dict(zip(self.columns, row)) for row in self.data]
