import socket
import json
import threading
import time
import random
import sys


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


class TestClient:
    
    def __init__(self, client_id, host='localhost', port=5555):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.current_tid = None
        self.color = Colors.OKGREEN if client_id == 1 else Colors.WARNING
        self.tag = f"{self.color}[Client {client_id}]{Colors.ENDC}"
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            print(f"{self.tag} Connected to server at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"{self.tag} Failed to connect: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.connected = False
            print(f"{self.tag} Disconnected from server")
    
    def _send_request(self, request: dict) -> dict:
        if not self.connected:
            return {'success': False, 'error': 'Not connected to server'}
        
        try:
            # Send request
            message_data = json.dumps(request).encode('utf-8')
            length_data = len(message_data).to_bytes(4, byteorder='big')
            self.socket.sendall(length_data + message_data)
            
            # Receive response length
            length_data = self._recv_exact(4)
            if not length_data:
                return {'success': False, 'error': 'Connection lost'}
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            # Receive response
            message_data = self._recv_exact(message_length)
            if not message_data:
                return {'success': False, 'error': 'Connection lost'}
            
            return json.loads(message_data.decode('utf-8'))
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _recv_exact(self, length: int) -> bytes:
        data = b''
        while len(data) < length:
            chunk = self.socket.recv(length - len(data))
            if not chunk:
                return b''
            data += chunk
        return data
    
    def execute_query(self, query: str) -> dict:
        request = {
            'type': 'execute',
            'query': query
        }
        return self._send_request(request)
    
    def begin_transaction(self) -> dict:
        request = {'type': 'begin'}
        response = self._send_request(request)
        if response.get('success'):
            self.current_tid = response.get('transaction_id')
        return response
    
    def commit_transaction(self) -> dict:
        if self.current_tid is None:
            return {'success': False, 'error': 'No active transaction'}
        
        request = {
            'type': 'commit',
            'transaction_id': self.current_tid
        }
        response = self._send_request(request)
        if response.get('success'):
            self.current_tid = None
        return response
    
    def rollback_transaction(self) -> dict:
        if self.current_tid is None:
            return {'success': False, 'error': 'No active transaction'}
        
        request = {
            'type': 'rollback',
            'transaction_id': self.current_tid
        }
        response = self._send_request(request)
        if response.get('success'):
            self.current_tid = None
        return response


def client_task(client_id, queries, host='localhost', port=5555):
    client = TestClient(client_id, host, port)
    
    if not client.connect():
        print(f"{client.tag} ❌ Failed to connect to server")
        return
    
    try:
        success = False
        retry_count = 0
        max_retries = 50
        
        while not success and retry_count < max_retries:
            retry_count += 1
            
            print(f"\n{client.tag} Starting transaction (Attempt {retry_count})...")
            
            # Begin transaction
            begin_res = client.begin_transaction()
            if not begin_res.get('success'):
                print(f"{client.tag} ❌ Failed to begin transaction: {begin_res.get('error')}")
                time.sleep(random.uniform(0.5, 1.0))
                continue
            
            tid = client.current_tid
            print(f"{client.tag} Transaction ID: {tid}")
            
            all_queries_success = True
            
            for query in queries:
                print(f"{client.tag} Processing query: {query}")
                
                # Execute query
                result = client.execute_query(query)
                
                if result.get('retried'):
                    print(f"{client.tag} [INFO] This was an automatic retry")
                
                if result.get('queued_for_retry'):
                    print(f"{client.tag} [INFO] {result.get('message')}")
                
                if result.get('success'):
                    print(f"{client.tag} ✅ Execution Success: {result.get('message', 'OK')}")
                    if result.get('rows'):
                        data = result['rows']['data']
                        if data:
                            print(f"{client.tag} 📊 Data ({len(data)} rows):")
                            for row in data[:5]:  # Show first 5 rows
                                print(f"{client.tag}    {row}")
                            if len(data) > 5:
                                print(f"{client.tag}    ... ({len(data) - 5} more rows)")
                else:
                    print(f"{client.tag} ❌ Execution Failed: {result.get('error')}")
                    all_queries_success = False
                    break
                
                time.sleep(random.uniform(0.1, 0.3))
            
            if not all_queries_success:
                # Rollback and retry
                print(f"{client.tag} Rolling back transaction {tid}...")
                rollback_res = client.rollback_transaction()
                if rollback_res.get('success'):
                    print(f"{client.tag} ✅ Transaction {tid} Rolled Back.")
                else:
                    print(f"{client.tag} ❌ Rollback Failed: {rollback_res.get('error')}")
                
                time.sleep(random.uniform(0.5, 1.5))
                continue
            
            # Commit transaction
            print(f"{client.tag} Committing transaction {tid}...")
            commit_res = client.commit_transaction()
            
            if commit_res.get('success'):
                print(f"{client.tag} ✅ Transaction {tid} Committed.")
                success = True
            else:
                print(f"{client.tag} ❌ Transaction {tid} Commit Failed: {commit_res.get('error')}")
                time.sleep(random.uniform(0.5, 1.5))
        
        if not success:
            print(f"{client.tag} ❌ Failed after {max_retries} attempts")
    
    finally:
        client.disconnect()


def setup_initial_data(host='localhost', port=5555):
    print(f"\n{Colors.OKCYAN}[SETUP]{Colors.ENDC} Creating initial table...")
    
    client = TestClient(0, host, port)
    if not client.connect():
        print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to connect to server")
        return False
    
    try:
        # Begin transaction
        begin_res = client.begin_transaction()
        if not begin_res.get('success'):
            print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to begin transaction")
            return False
        
        # Create table
        result = client.execute_query("CREATE TABLE products (id INT, name VARCHAR(50), price INT)")
        
        if result.get('success'):
            print(f"{Colors.OKGREEN}[SETUP]{Colors.ENDC} ✅ Initial table created.")
            client.commit_transaction()
            return True
        else:
            print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to create table: {result.get('error')}")
            client.rollback_transaction()
            return False
    
    finally:
        client.disconnect()


def verify_final_data(host='localhost', port=5555):
    print(f"\n{Colors.HEADER}[VERIFY]{Colors.ENDC} Final data state:")
    
    client = TestClient(99, host, port)
    if not client.connect():
        print(f"{Colors.FAIL}[VERIFY]{Colors.ENDC} ❌ Failed to connect to server")
        return
    
    try:
        # Begin transaction
        client.begin_transaction()
        
        # Query all data
        result = client.execute_query("SELECT * FROM products")
        
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            print(f"{Colors.OKGREEN}[VERIFY]{Colors.ENDC} ✅ Final Rows: {len(rows)}")
            for row in rows:
                print(f"{Colors.OKGREEN}[VERIFY]{Colors.ENDC}  - {row}")
        else:
            print(f"{Colors.FAIL}[VERIFY]{Colors.ENDC} ❌ Verification failed: {result.get('error')}")
        
        client.commit_transaction()
    
    finally:
        client.disconnect()


def test_concurrent_clients():
    print("=" * 60)
    print("       CLIENT-BASED SYSTEM INTEGRATION TEST")
    print("=" * 60)
    print("\nThis test connects to a running server at localhost:5555")
    print("Make sure server.py is running before starting this test!")
    print("=" * 60)
    
    HOST = 'localhost'
    PORT = 5555
    
    # Wait for user confirmation
    input("\nPress Enter to start the test...")
    
    # Setup initial data
    print("\n" + "=" * 60)
    if not setup_initial_data(HOST, PORT):
        print(f"\n{Colors.FAIL}[ERROR]{Colors.ENDC} Failed to setup initial data. Is the server running?")
        return
    print("=" * 60 + "\n")
    
    # Define client queries
    client1_queries = [
        "INSERT INTO products VALUES (1, 'Laptop', 1000)",
        "INSERT INTO products VALUES (2, 'Mouse', 20)",
        "SELECT * FROM products WHERE price > 30"
    ]
    
    client2_queries = [
        "INSERT INTO products VALUES (3, 'Keyboard', 50)",
        "UPDATE products SET price=100 WHERE id=3",
        "SELECT * FROM products WHERE price > 30"
    ]
    
    # Create client threads
    threads = []
    
    print(f"\n{Colors.HEADER}[TEST]{Colors.ENDC} Starting concurrent clients...")
    print("=" * 60 + "\n")
    
    t1 = threading.Thread(target=client_task, args=(1, client1_queries, HOST, PORT))
    t2 = threading.Thread(target=client_task, args=(2, client2_queries, HOST, PORT))
    
    threads.append(t1)
    threads.append(t2)
    
    # Start all threads
    for t in threads:
        t.start()
        time.sleep(0.1)
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    print(f"\n{Colors.HEADER}[TEST]{Colors.ENDC} Concurrent clients finished.")
    print("=" * 60)
    
    # Verify final state
    verify_final_data(HOST, PORT)
    
    print("\n" + "=" * 60)
    print("       TEST SUITE COMPLETED")
    print("=" * 60)


def test_multiple_clients(num_clients=5):
    print("=" * 60)
    print(f"       STRESS TEST - {num_clients} CONCURRENT CLIENTS")
    print("=" * 60)
    
    HOST = 'localhost'
    PORT = 5555
    
    input("\nPress Enter to start the stress test...")
    
    # Setup
    if not setup_initial_data(HOST, PORT):
        print(f"\n{Colors.FAIL}[ERROR]{Colors.ENDC} Failed to setup initial data.")
        return
    
    queries_templates = [
        [
            "INSERT INTO products VALUES ({id}, 'Product{id}', {price})",
            "SELECT * FROM products WHERE id={id}"
        ],
        [
            "INSERT INTO products VALUES ({id}, 'Item{id}', {price})",
            "UPDATE products SET price={new_price} WHERE id={id}",
            "SELECT * FROM products WHERE price > {threshold}"
        ]
    ]
    
    threads = []
    
    print(f"\n{Colors.HEADER}[STRESS TEST]{Colors.ENDC} Starting {num_clients} concurrent clients...")
    
    for i in range(1, num_clients + 1):
        template = queries_templates[i % len(queries_templates)]
        queries = [
            q.format(
                id=i * 10,
                price=random.randint(10, 200),
                new_price=random.randint(50, 300),
                threshold=random.randint(30, 100)
            ) for q in template
        ]
        
        t = threading.Thread(target=client_task, args=(i, queries, HOST, PORT))
        threads.append(t)
        t.start()
        time.sleep(0.05)
    
    # Wait for completion
    for t in threads:
        t.join()
    
    print(f"\n{Colors.HEADER}[STRESS TEST]{Colors.ENDC} All {num_clients} clients finished.")
    
    # Verify
    verify_final_data(HOST, PORT)
    
    print("\n" + "=" * 60)
    print("       STRESS TEST COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stress":
        # Stress test mode
        num_clients = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        test_multiple_clients(num_clients)
    else:
        # Normal test mode
        test_concurrent_clients()
