import threading
from QueryProcessor.interfaces import AbstractConcurrencyControlManager
from QueryProcessor.interfaces.concurrency_control_interface import LockResult
from ConcurrencyControl.src.concurrency_control_manager import ConcurrencyControlManager
from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from ConcurrencyControl.src.row_action import TableAction
from ConcurrencyControl.src.transaction_status import TransactionStatus
from ConcurrencyControl.src.concurrency_response import LockStatus
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
            
            response = self.ccm.transaction_commit(transaction_id)
            
            if response and response.status == LockStatus.FAILED:
                if self.verbose:
                    print(f"{self.tag} Commit failed for {transaction_id}: {response.reason}")
                return False
            
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
                self.ccm.transaction_rollback(transaction_id)
            elif status in [TransactionStatus.FAILED, TransactionStatus.ABORTED]:
                if self.verbose:
                    print(f"{self.tag} Transaction {transaction_id} already aborted by protocol")
            else:
                if self.verbose:
                    print(f"{self.tag} Cannot rollback transaction {transaction_id} in state {status}")
                return False
            
            return True
        except Exception as e:
            if self.verbose:
                print(f"{self.tag} Rollback failed for transaction {transaction_id}: {e}")
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
    ) -> LockResult:
        try:
            table_name = resource_id
            
            table_action = TableAction.READ if lock_type == "READ" else TableAction.WRITE
            
            if self.verbose:
                print(f"{self.tag} Transaction {transaction_id} requesting {lock_type} lock on {table_name}")

            response = self.ccm.transaction_query(transaction_id, table_action, table_name)
            
            if response:
                status_str = "FAILED"
                granted = False
                wait_event = None
                
                if response.status == LockStatus.GRANTED:
                    status_str = "GRANTED"
                    granted = True
                elif response.status == LockStatus.WAITING:
                    status_str = "WAITING"
                    granted = False  # NOT granted yet, should retry
                    # Get the event for event-driven waiting (if CCM supports it)
                    if isinstance(self.ccm, LockBasedConcurrencyControlManager):
                        wait_event = self.ccm.get_wait_event(transaction_id)
                elif response.status == LockStatus.FAILED:
                    status_str = "FAILED"
                    granted = False
                
                if self.verbose:
                    print(f"{self.tag} Lock response for {transaction_id}: Status={status_str}, Granted={granted}, BlockedBy={response.blocked_by}, ActiveTransactions={response.active_transactions}")
                
                result = LockResult(granted=granted, status=status_str, wait_time=0.1, blocked_by=response.blocked_by, active_transactions=response.active_transactions)
                # Add wait_event if available
                if wait_event is not None:
                    result.wait_event = wait_event
                return result
            
            return LockResult(granted=True, status="GRANTED", active_transactions=response.active_transactions)
            
        except Exception as e:
            if self.verbose:
                print(f"{self.tag} Lock request failed for transaction {transaction_id}: {e}")
            return LockResult(granted=False, status="FAILED")
    
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