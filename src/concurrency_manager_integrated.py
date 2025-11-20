from QueryProcessor.interfaces import AbstractConcurrencyControlManager
from ConcurrencyControl.src.concurrency_control_manager import ConcurrencyControlManager
from ConcurrencyControl.src.row_action import RowAction
from ConcurrencyControl.src.transaction_status import TransactionStatus
from typing import Optional


class IntegratedConcurrencyManager(AbstractConcurrencyControlManager):
    
    def __init__(self, ccm: ConcurrencyControlManager):
        self.ccm = ccm
        self.verbose = False
        self.tag = "\033[94m[CCM]\033[0m" # Blue

    def setVerbose(self, verbose: bool):
        self.verbose = verbose
    
    def begin_transaction(self) -> int:
        tid = self.ccm.transaction_begin()
        if self.verbose:
            print(f"{self.tag} Transaction {tid} started")
        return tid
    
    def commit_transaction(self, transaction_id: int) -> bool:
        try:
            status = self.ccm.transaction_get_status(transaction_id)
            if status != TransactionStatus.ACTIVE:
                if self.verbose:
                    print(f"{self.tag} Commit failed for {transaction_id}: Status is {status}")
                return False
            self.ccm.transaction_commit(transaction_id)
            self.ccm.transaction_commit_flushed(transaction_id)
            self.ccm.transaction_end(transaction_id)
            if self.verbose:
                print(f"{self.tag} Transaction {transaction_id} committed")
            return True
        except Exception as e:
            if self.verbose:
                print(f"{self.tag} Commit failed for transaction {transaction_id}: {e}")
            return False
    
    def commit_flushed(self, transaction_id: int) -> bool:
        try:
            self.ccm.transaction_commit_flushed(transaction_id)
            return True
        except Exception as e:
            print(f"Commit flush failed: {e}")
            return False
    
    def rollback_transaction(self, transaction_id: int) -> bool:
        try:
            status = self.ccm.transaction_get_status(transaction_id)
            if status == TransactionStatus.ACTIVE:
                self.ccm.transaction_abort(transaction_id)
                self.ccm.transaction_end(transaction_id)
                return True
            elif status == TransactionStatus.PARTIALLY_COMMITTED:
                self.ccm.transaction_rollback(transaction_id)
                self.ccm.transaction_abort(transaction_id)
                self.ccm.transaction_end(transaction_id)
                return True
            return False
        except Exception as e:
            print(f"Rollback failed: {e}")
            return False
    
    def end_transaction(self, transaction_id: int) -> bool:
        try:
            # Locks otomatis dilepas oleh CCM saat commit atau abort
            self.ccm.transaction_end(transaction_id)
            return True
        except Exception as e:
            # Force cleanup
            try:
                self.ccm.transaction_abort(transaction_id)
                self.ccm.transaction_end(transaction_id)
                return True
            except:
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
            
            if self.verbose:
                print(f"{self.tag} Transaction {transaction_id} requesting {lock_type} lock on {resource_id}")

            response = self.ccm.transaction_query(transaction_id, row_action, row_id)
            
            if response and hasattr(response, 'query_allowed'):
                if self.verbose:
                    print(f"{self.tag} Lock response for {transaction_id}: {response.query_allowed}")
                return response.query_allowed
            return True
        except Exception as e:
            if self.verbose:
                print(f"{self.tag} Lock request failed for transaction {transaction_id}: {e}")
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