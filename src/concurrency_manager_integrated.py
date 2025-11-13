from QueryProcessor.interfaces import AbstractConcurrencyControlManager
from ConcurrencyControl.src.concurrency_control_manager import ConcurrencyControlManager
from ConcurrencyControl.src.row_action import RowAction
from ConcurrencyControl.src.transaction_status import TransactionStatus
from typing import Optional


class IntegratedConcurrencyManager(AbstractConcurrencyControlManager):
    
    def __init__(self, ccm: ConcurrencyControlManager):
        self.ccm = ccm
    
    def begin_transaction(self) -> int:
        return self.ccm.transaction_begin()
    
    def commit_transaction(self, transaction_id: int) -> bool:
        try:
            response = self.ccm.transaction_commit(transaction_id)
            return True
        except Exception as e:
            print(f"Commit failed for transaction {transaction_id}: {e}")
            return False
    
    def commit_flushed(self, transaction_id: int) -> bool:
        try:
            self.ccm.transaction_commit_flushed(transaction_id)
            return True
        except Exception as e:
            print(f"Commit flush failed for transaction {transaction_id}: {e}")
            return False
    
    def rollback_transaction(self, transaction_id: int) -> bool:
        try:
            self.ccm.transaction_rollback(transaction_id)
            self.ccm.transaction_abort(transaction_id)
            return True
        except Exception as e:
            print(f"Rollback failed for transaction {transaction_id}: {e}")
            return False
    
    def end_transaction(self, transaction_id: int) -> bool:
        try:
            self.ccm.transaction_end(transaction_id)
            return True
        except Exception as e:
            print(f"End transaction failed for transaction {transaction_id}: {e}")
            return False
    
    def request_lock(
        self, 
        transaction_id: int, 
        resource_id: str, 
        lock_type: str
    ) -> bool:
        try:
            row_id = self._parse_resource_id(resource_id)
            
            row_action = RowAction.READ if lock_type == "READ" else RowAction.WRITE
            
            response = self.ccm.transaction_query(transaction_id, row_action, row_id)
            
            if response and hasattr(response, 'query_allowed'):
                if not response.query_allowed:
                    print(f"Lock denied for transaction {transaction_id}: {getattr(response, 'reason', 'Unknown')}")
                return response.query_allowed
            
            return True
            
        except Exception as e:
            print(f"Lock request failed for transaction {transaction_id}: {e}")
            return False
    
    def release_lock(self, transaction_id: int, resource_id: str) -> bool:
        return True
    
    def release_all_locks(self, transaction_id: int) -> bool:
        try:
            return True
        except Exception as e:
            print(f"Release locks failed for transaction {transaction_id}: {e}")
            return False
    
    def check_deadlock(self, transaction_id: int) -> bool:
        try:
            status = self.ccm.transaction_get_status(transaction_id)
            return status == TransactionStatus.FAILED
        except Exception as e:
            print(f"Deadlock check failed for transaction {transaction_id}: {e}")
            return False
    
    def get_transaction_status(self, transaction_id: int) -> str:
        try:
            status = self.ccm.transaction_get_status(transaction_id)
            return status.name
        except Exception:
            return "UNKNOWN"
    
    def _parse_resource_id(self, resource_id: str) -> int:
        parts = resource_id.split(":")
        if len(parts) == 2:
            try:
                return int(parts[1])
            except ValueError:
                return 0
        return 0