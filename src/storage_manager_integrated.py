from QueryProcessor.interfaces import AbstractStorageManager
from StorageManager.classes.API import StorageEngine
from StorageManager.classes.DataModels import DataRetrieval, DataWrite, DataDeletion, Condition, Operation, Schema
from StorageManager.classes.Types import IntType, FloatType, CharType, VarCharType
from StorageManager.classes.Serializer import Serializer
from typing import List, Dict, Any, Optional
from QueryProcessor.models import Rows
import re
import os

class IntegratedStorageManager(AbstractStorageManager):
    def __init__(self, engine: StorageEngine):
        self.engine = engine
    
    def read_table(self, table_name: str, columns: Optional[List[str]] = None) -> Rows:
        serializer = Serializer()
        try:
            # 1. Load schema untuk memfilter internal columns (seperti __special_row_id)
            serializer.load_schema(table_name)
            all_columns = [col['name'] for col in serializer.schema['columns']]
            user_columns = [col for col in all_columns if col != '__special_row_id']
        except Exception:
            user_columns = columns if columns else []

        target_columns = columns if columns else user_columns
        
        # 2. Retrieve data menggunakan StorageEngine
        data_retrieval = DataRetrieval(table_name, target_columns, [])
        
        result_data = self.engine.read_block(data_retrieval)
        
        return Rows(target_columns, result_data)

    def write_table(self, table_name: str, rows: Rows) -> int:
        return self.insert_rows(table_name, rows)

    def insert_rows(self, table_name: str, rows: Rows, transaction_id: int = None) -> int:
        # 1. Jika columns hilang (misal INSERT VALUES), fetch dari schema
        current_columns = rows.columns
        if not current_columns:
            serializer = Serializer()
            try:
                serializer.load_schema(table_name)
                all_columns = [col['name'] for col in serializer.schema['columns']]
                current_columns = [col for col in all_columns if col != '__special_row_id']
            except Exception:
                pass

        # 2. Prepend internal row_id column dan default value (0)
        columns = ["__special_row_id"] + current_columns
        new_values = [[0] + row for row in rows.data]
        
        # 3. Write ke storage
        data_write = DataWrite(table_name, columns, [], new_values)
        
        return StorageEngine.write_block(data_write)

    def update_rows(
        self, 
        table_name: str, 
        updates: Dict[str, Any], 
        condition: Optional[Dict[str, Any]] = None
    ) -> int:
        conditions = self._map_conditions(condition) if condition else []
        
        # 1. Fetch semua user columns untuk mempertahankan non-updated data
        serializer = Serializer()
        serializer.load_schema(table_name)
        all_columns = [col['name'] for col in serializer.schema['columns']]
        user_columns = [col for col in all_columns if col != '__special_row_id']
        
        # 2. Read existing rows yang cocok dengan condition
        data_retrieval = DataRetrieval(table_name, user_columns, conditions)
        rows_data = self.engine.read_block(data_retrieval)
        
        if not rows_data:
            return 0
            
        # 3. Delete rows lama
        data_deletion = DataDeletion(table_name, conditions)
        StorageEngine.delete_block(data_deletion)
        
        # 4. Apply updates di memory
        updated_rows_data = []
        for row in rows_data:
            row_dict = dict(zip(user_columns, row))
            
            for col, val in updates.items():
                if col in row_dict:
                    row_dict[col] = val
            
            updated_row = [row_dict[col] for col in user_columns]
            updated_rows_data.append(updated_row)
            
        # 5. Insert updated rows (sebagai records baru)
        columns = ["__special_row_id"] + user_columns
        new_values = [[0] + row for row in updated_rows_data]
        
        data_write = DataWrite(table_name, columns, [], new_values)
        return StorageEngine.write_block(data_write)

    def delete_rows(self, table_name: str, condition: Optional[Dict[str, Any]] = None) -> int:
        conditions = self._map_conditions(condition) if condition else []
        data_deletion = DataDeletion(table_name, conditions)
        return StorageEngine.delete_block(data_deletion)

    def create_table(self, table_name: str, schema: Dict[str, str]) -> bool:
        # 1. Map schema types ke StorageManager types
        schema_dict = {}
        for col_name, type_str in schema.items():
            schema_dict[col_name] = self._parse_type(type_str)
            
        # 2. Create table di catalog
        storage_schema = Schema(**schema_dict)
        success = StorageEngine.create_table(table_name, storage_schema)
        
        # 3. Pastikan physical data file ada
        if success:
            file_path = f"storage/data/{table_name}.dat"
            if not os.path.exists(file_path):
                with open(file_path, 'wb') as f:
                    pass
                    
        return success

    def drop_table(self, table_name: str) -> bool:
        return StorageEngine.drop_table(table_name)

    def _map_conditions(self, condition: Dict[str, Any]) -> List[Condition]:
        conditions = []
        for col, val in condition.items():
            conditions.append(Condition(col, Operation.EQ, val))
        return conditions

    def _parse_type(self, type_str: str):
        type_str = type_str.lower().strip()
        if type_str == "int":
            return IntType()
        elif type_str == "float":
            return FloatType()
        elif type_str.startswith("varchar"):
            match = re.search(r"varchar\((\d+)\)", type_str)
            length = int(match.group(1)) if match else 255
            return VarCharType(length)
        elif type_str.startswith("char"):
            match = re.search(r"char\((\d+)\)", type_str)
            length = int(match.group(1)) if match else 1
            return CharType(length)
        else:
            return VarCharType(255)
