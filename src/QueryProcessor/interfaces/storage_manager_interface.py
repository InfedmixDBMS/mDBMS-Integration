from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..models import Rows


class AbstractStorageManager(ABC):
    
    @abstractmethod
    def read_table(self, table_name: str, columns: Optional[List[str]] = None) -> Rows:
        pass
    
    @abstractmethod
    def write_table(self, table_name: str, rows: Rows) -> int:
        pass
    
    @abstractmethod
    def update_rows(
        self, 
        table_name: str, 
        updates: Dict[str, Any], 
        condition: Optional[Dict[str, Any]] = None
    ) -> int:
        pass
    
    @abstractmethod
    def delete_rows(self, table_name: str, condition: Optional[Dict[str, Any]] = None) -> int:
        pass
    
    @abstractmethod
    def insert_rows(self, table_name: str, rows: Rows) -> int:
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> bool:
        pass
