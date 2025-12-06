from QueryProcessor.interfaces import AbstractStorageManager
from StorageManager.classes.API import StorageEngine, Schema, DataRetrieval, DataWrite, DataDeletion
from typing import List, Dict, Any, Optional
from QueryProcessor.models import Rows

class IntegratedStorageManager(AbstractStorageManager):
    def __init__(self, engine: StorageEngine):
        self.engine = engine
    
    # DataRetrieval: 
    #   table -> str, 
    #   column -> List[str], 
    #   condition -> List[Condition]

    # Condition: 
    #   column -> str, 
    #   operation -> enum('=', '<>', '>', '<', '>=', '<='), 
    #   operand -> int | str | float

    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        return StorageEngine.read_block(data_retrieval)
    
    # DataWrite: 
    #   table -> str,
    #   column -> List[str],
    #   conditions -> List[Condition],
    #   new_value -> List[T] | None
    
    def write_block(self, data_write: DataWrite) -> int:
        return StorageEngine.write_block(data_write)
    
    # DataDeletion: 
    #   table -> str,
    #   conditions -> List[Condition]

    def delete_block(self, data_deletion: DataDeletion) -> int:
        return StorageEngine.delete_block(data_deletion)
    
    # Schema:
    #   columns:
    #       "id" = IntType(),
    #       "name" = VarCharType(50),
    #       "ipk" = FloatType()
    #   column_order: list(columns.keys())
    #   column_index: {name: i for i, name in enumerate(self.column_order)}
    #   size: int (auto-calculated, ga perlu dihitung manual)
    
    def create_table(self, table_name: str, schema: Schema) -> bool:
        return StorageEngine.create_table(table_name, schema)
    
    def drop_table(self, table_name: str) -> bool:
        return StorageEngine.drop_table(table_name)

    def update_stats(self, table_name:str) -> bool:
        return StorageEngine.update_stats(table_name)

    def defragment(self, table_name:str) -> bool:
        return StorageEngine.defragment(table_name)
    
    def get_next_row_id(self, table: str) -> int:
        return StorageEngine.get_next_row_id(table)
    
    def load_schema_names(self, table: str) -> list:
        return StorageEngine.load_schema_names(table)