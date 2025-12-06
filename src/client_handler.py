import socket
import threading
import json
import time
from typing import Dict, List, Optional, Tuple
from queue import Queue, PriorityQueue
from dataclasses import dataclass, field
from datetime import datetime
from QueryProcessor.query_processor_core import QueryProcessor
from QueryProcessor.models import ExecutionResult


class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


@dataclass(order=True)
class RetryItem:
    priority: float = field(compare=True)
    client_id: str = field(compare=False)
    transaction_id: int = field(compare=False)
    query: str = field(compare=False)
    failed_by: int = field(compare=False)  # Transaction ID that caused the failure
    wait_event: threading.Event = field(default=None, compare=False)  # Event to wait on


class ClientHandler:

    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, host: str = 'localhost', port: int = 5432, processor: Optional[QueryProcessor] = None):
        if hasattr(self, '_initialized'):
            return
            
        self.host = host
        self.port = port
        self.processor = processor
        self.server_socket = None
        self.running = False
        
        # Client management
        self.clients: Dict[str, socket.socket] = {}
        self.client_threads: Dict[str, threading.Thread] = {}
        self.clients_lock = threading.Lock()
        
        # Transaction tracking
        self.active_transactions: Dict[int, str] = {}  # tid -> client_id
        self.transaction_lock = threading.Lock()
        
        # Retry queue management
        self.retry_queue: PriorityQueue[RetryItem] = PriorityQueue()
        self.waiting_on: Dict[int, List[RetryItem]] = {}  # failed_by_tid -> [RetryItems]
        self.retry_lock = threading.Lock()
        
        # Retry processor thread
        self.retry_thread = None
        
        self._initialized = True
        
        print(f"{Colors.OKCYAN}[SERVER] ClientHandler initialized on {host}:{port}{Colors.ENDC}")
    
    def set_processor(self, processor: QueryProcessor):
        self.processor = processor
    
    def start(self):
        if self.processor is None:
            raise RuntimeError("QueryProcessor not set. Call set_processor() first.")
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"{Colors.OKGREEN}[SERVER] Listening on {self.host}:{self.port}{Colors.ENDC}")
        
        # Start retry processor
        self.retry_thread = threading.Thread(target=self._retry_processor, daemon=True)
        self.retry_thread.start()
        
        # Accept connections
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
    
    def stop(self):
        self.running = False
        
        with self.clients_lock:
            for client_id, client_socket in self.clients.items():
                try:
                    client_socket.close()
                    print(f"{Colors.OKCYAN}[SERVER] Closed connection to {client_id}{Colors.ENDC}")
                except:
                    pass
            self.clients.clear()
        
        if self.server_socket:
            self.server_socket.close()
        
        print(f"{Colors.WARNING}[SERVER] Server stopped{Colors.ENDC}")
    
    def _accept_connections(self):
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_id = f"{address[0]}:{address[1]}"
                
                with self.clients_lock:
                    self.clients[client_id] = client_socket
                
                print(f"{Colors.OKGREEN}[SERVER] ✓ Client connected: {client_id}{Colors.ENDC}")
                
                worker = threading.Thread(
                    target=self._client_worker,
                    args=(client_id, client_socket),
                    daemon=True
                )
                self.client_threads[client_id] = worker
                worker.start()
                
            except OSError:
                if not self.running:
                    break
            except Exception as e:
                if self.running:
                    print(f"{Colors.FAIL}[SERVER] Error accepting connection: {e}{Colors.ENDC}")
    
    def _client_worker(self, client_id: str, client_socket: socket.socket):
        try:
            while self.running:
                # Receive message length first (4 bytes)
                length_data = self._recv_exact(client_socket, 4)
                if not length_data:
                    break
                
                message_length = int.from_bytes(length_data, byteorder='big')
                
                # Receive message body
                message_data = self._recv_exact(client_socket, message_length)
                if not message_data:
                    break
                
                message = json.loads(message_data.decode('utf-8'))
                
                # Handle request
                response = self._handle_request(client_id, message)
                
                # Send response
                self._send_message(client_socket, response)
                
        except json.JSONDecodeError as e:
            print(f"{Colors.FAIL}[SERVER] JSON decode error from client {client_id}: {e}{Colors.ENDC}")
        except Exception as e:
            print(f"{Colors.FAIL}[SERVER] Error handling client {client_id}: {e}{Colors.ENDC}")
            import traceback
            traceback.print_exc()
        finally:
            with self.clients_lock:
                if client_id in self.clients:
                    del self.clients[client_id]
                if client_id in self.client_threads:
                    del self.client_threads[client_id]
            try:
                client_socket.close()
            except:
                pass
            print(f"{Colors.WARNING}[SERVER] ✗ Client disconnected: {client_id}{Colors.ENDC}")
    
    def _recv_exact(self, sock: socket.socket, length: int) -> bytes:
        data = b''
        while len(data) < length:
            chunk = sock.recv(length - len(data))
            if not chunk:
                return b''
            data += chunk
        return data
    
    def _send_message(self, sock: socket.socket, message: dict):
        message_data = json.dumps(message).encode('utf-8')
        length_data = len(message_data).to_bytes(4, byteorder='big')
        sock.sendall(length_data + message_data)
    
    def _handle_request(self, client_id: str, message: dict) -> dict:
        request_type = message.get('type')
        
        if request_type == 'execute':
            return self._handle_execute(client_id, message)
        elif request_type == 'begin':
            return self._handle_begin(client_id)
        elif request_type == 'commit':
            return self._handle_commit(client_id, message)
        elif request_type == 'rollback':
            return self._handle_rollback(client_id, message)
        else:
            return {'success': False, 'error': 'Unknown request type'}
    
    def _handle_execute(self, client_id: str, message: dict) -> dict:
        query = message.get('query', '')
        transaction_id = message.get('transaction_id')
        
        try:
            result = self.processor.execute_query(query, transaction_id)
            
            if not result.success and 'Lock denied' in str(result.error):
                tid = transaction_id
                if tid is None:
                    thread_id = threading.get_ident()
                    with self.processor._lock:
                        tid = self.processor.thread_transactions.get(thread_id)
                
                if tid:
                    self._add_to_retry_queue(client_id, tid, query, result.error)
                    
                    return {
                        'success': False,
                        'error': str(result.error),
                        'queued_for_retry': True,
                        'message': 'Query queued for automatic retry'
                    }
            
            return self._result_to_dict(result)
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_begin(self, client_id: str) -> dict:
        try:
            tid = self.processor.begin_transaction()
            
            with self.transaction_lock:
                self.active_transactions[tid] = client_id
            
            return {'success': True, 'transaction_id': tid}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_commit(self, client_id: str, message: dict) -> dict:
        tid = message.get('transaction_id')
        
        try:
            result = self.processor.commit_transaction(tid)
            
            with self.transaction_lock:
                if tid in self.active_transactions:
                    del self.active_transactions[tid]
            
            self._trigger_retry_for_transaction(tid)
            
            return self._result_to_dict(result)
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _handle_rollback(self, client_id: str, message: dict) -> dict:
        tid = message.get('transaction_id')
        
        try:
            result = self.processor.rollback_transaction(tid)
            
            with self.transaction_lock:
                if tid in self.active_transactions:
                    del self.active_transactions[tid]
            
            self._trigger_retry_for_transaction(tid)
            
            return self._result_to_dict(result)
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _add_to_retry_queue(self, client_id: str, tid: int, query: str, error: str):
        failed_by = None
        
        # timestamp-based priority
        retry_item = RetryItem(
            priority=time.time(),
            client_id=client_id,
            transaction_id=tid,
            query=query,
            failed_by=failed_by or -1
        )
        
        with self.retry_lock:
            if failed_by and failed_by in self.waiting_on:
                self.waiting_on[failed_by].append(retry_item)
            else:
                self.retry_queue.put(retry_item)
        
        print(f"{Colors.OKBLUE}[SERVER] Queued retry for client {client_id}, TID {tid}{Colors.ENDC}")
    
    def _trigger_retry_for_transaction(self, tid: int):
        with self.retry_lock:
            if tid in self.waiting_on:
                items = self.waiting_on.pop(tid)
                for item in items:
                    item.priority = time.time()
                    self.retry_queue.put(item)
                print(f"{Colors.OKBLUE}[SERVER] Triggered {len(items)} retries after TID {tid} completed{Colors.ENDC}")
    
    def _retry_processor(self):
        """Event-driven retry processor - waits on events instead of polling"""
        while self.running:
            try:
                # Try to get a retry item with timeout
                try:
                    retry_item = self.retry_queue.get(timeout=0.5)
                except:
                    continue
                
                # If we have a wait event, use event-driven waiting
                if retry_item.wait_event is not None:
                    print(f"{Colors.OKCYAN}[SERVER] Waiting on event for client {retry_item.client_id}, TID {retry_item.transaction_id}{Colors.ENDC}")
                    
                    # Wait for the event to be signaled (with timeout for safety)
                    signaled = retry_item.wait_event.wait(timeout=30.0)
                    
                    if not signaled:
                        print(f"{Colors.WARNING}[SERVER] Event wait timeout for TID {retry_item.transaction_id}, retrying anyway{Colors.ENDC}")
                else:
                    # Fallback to sleep if no event available
                    time.sleep(0.5)
                
                # Attempt retry
                print(f"{Colors.OKCYAN}[SERVER] Retrying query for client {retry_item.client_id}, TID {retry_item.transaction_id}{Colors.ENDC}")
                
                # Check client connection
                with self.clients_lock:
                    if retry_item.client_id not in self.clients:
                        print(f"{Colors.WARNING}[SERVER] Client {retry_item.client_id} disconnected, skipping retry{Colors.ENDC}")
                        continue
                    client_socket = self.clients[retry_item.client_id]
                
                # Execute query with transaction context
                result = self.processor.execute_query(retry_item.query, retry_item.transaction_id)
                
                # Send result back to client
                response = self._result_to_dict(result)
                response['retried'] = True
                response['original_transaction_id'] = retry_item.transaction_id
                
                try:
                    self._send_message(client_socket, response)
                except Exception as e:
                    print(f"{Colors.WARNING}[SERVER] Failed to send retry result to {retry_item.client_id}: {e}{Colors.ENDC}")
                
                # If still failed, re-queue
                if not result.success and 'Lock denied' in str(result.error):
                    # Get new wait event if available
                    retry_item.priority = time.time()
                    # Event will be refreshed by the CCM on next attempt
                    with self.retry_lock:
                        self.retry_queue.put(retry_item)
                    
            except Exception as e:
                if self.running:
                    print(f"{Colors.FAIL}[SERVER] Retry processor error: {e}{Colors.ENDC}")
                    import traceback
                    traceback.print_exc()
                time.sleep(0.1)
    
    def _result_to_dict(self, result: ExecutionResult) -> dict:
        response = {
            'success': result.success,
            'message': result.message if hasattr(result, 'message') else '',
            'error': str(result.error) if hasattr(result, 'error') and result.error else None,
        }
        
        if hasattr(result, 'rows') and result.rows:
            data = []
            for row in result.rows.data:
                json_row = []
                for val in row:
                    if hasattr(val, 'item'):
                        json_row.append(val.item())
                    elif isinstance(val, (int, float, str, bool, type(None))):
                        json_row.append(val)
                    else:
                        json_row.append(str(val))
                data.append(json_row)
            
            response['rows'] = {
                'columns': result.rows.columns,
                'data': data
            }
        
        if hasattr(result, 'affected_rows'):
            response['affected_rows'] = result.affected_rows
        
        return response
