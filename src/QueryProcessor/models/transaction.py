import uuid
from datetime import datetime
from typing import Optional, List


class Transaction:
    
    counter = 0
    
    def __init__(self, transaction_id: Optional[int] = None):
        self.transaction_id = transaction_id or Transaction.counter
        Transaction.counter += 1
        self.start_time = datetime.now()
        self.status = "ACTIVE"
        self.queries_executed: List[str] = []
        self.locks_held: List[str] = []
    
    def add_query(self, query: str) -> None:
        self.queries_executed.append(query)
    
    def add_lock(self, lock_id: str) -> None:
        self.locks_held.append(lock_id)
    
    def commit(self) -> None:
        self.status = "COMMITTED"
    
    def abort(self) -> None:
        self.status = "ABORTED"
    
    def __repr__(self) -> str:
        return f"Transaction(id={self.transaction_id}, status={self.status})"
