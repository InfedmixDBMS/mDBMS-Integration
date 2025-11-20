from QueryProcessor.interfaces import AbstractStorageManager
from typing import List, Dict, Any, Optional
from QueryProcessor.models import Rows

class IntegratedStorageManager(AbstractStorageManager):
    def __init__(self):
        self.tables = {}
        self.verbose = False
        self.tag = "\033[95m[SM]\033[0m" # Magenta

    def setVerbose(self, verbose: bool):
        self.verbose = verbose
    
    def read_table(self, table_name: str, columns: Optional[List[str]] = None) -> Rows:
        if self.verbose:
            print(f"{self.tag} Reading table {table_name}")
        if table_name not in self.tables:
            return Rows([], [])
        return self.tables[table_name]
    
    def write_table(self, table_name: str, rows: Rows) -> int:
        if self.verbose:
            print(f"{self.tag} Writing table {table_name}")
        self.tables[table_name] = rows
        return len(rows.data)
    
    def update_rows(
        self, 
        table_name: str, 
        updates: Dict[str, Any], 
        condition: Optional[Dict[str, Any]] = None
    ) -> int:
        if self.verbose:
            print(f"{self.tag} Updating rows in {table_name}")
        return 0
    
    def delete_rows(self, table_name: str, condition: Optional[Dict[str, Any]] = None) -> int:
        if self.verbose:
            print(f"{self.tag} Deleting rows from {table_name}")
        return 0
    
    def insert_rows(self, table_name: str, rows: Rows, transaction_id: Optional[int] = None) -> int:
        if self.verbose:
            print(f"{self.tag} Inserting rows into {table_name} (Tx: {transaction_id})")
        if table_name not in self.tables:
            self.tables[table_name] = Rows(rows.columns, [])
        
        current_rows = self.tables[table_name]
        current_rows.data.extend(rows.data)
        return len(rows.data)
    
    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        if self.verbose:
            print(f"{self.tag} Creating table {table_name}")
        self.tables[table_name] = Rows(list(schema.keys()), [])
        return True
    
    def drop_table(self, table_name: str) -> bool:
        if self.verbose:
            print(f"{self.tag} Dropping table {table_name}")
        if table_name in self.tables:
            del self.tables[table_name]
            return True
        return False