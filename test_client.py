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


def test_nested_loop_join():
    """Test nested loop join with two tables"""
    print("=" * 60)
    print("       NESTED LOOP JOIN TEST")
    print("=" * 60)
    
    HOST = 'localhost'
    PORT = 5555
    
    input("\nPress Enter to start the nested loop join test...")
    
    client = TestClient(1, HOST, PORT)
    if not client.connect():
        print(f"{Colors.FAIL}[ERROR]{Colors.ENDC} Failed to connect to server")
        return
    
    try:
        # Setup: Drop existing tables if they exist
        print(f"\n{Colors.OKCYAN}[SETUP]{Colors.ENDC} Cleaning up old tables...")
        client.begin_transaction()
        client.execute_query("DROP TABLE IF EXISTS customers")
        client.execute_query("DROP TABLE IF EXISTS orders")
        client.commit_transaction()
        
        # Setup: Create two tables
        print(f"\n{Colors.OKCYAN}[SETUP]{Colors.ENDC} Creating tables for join test...")
        
        client.begin_transaction()
        
        # Create customers table
        result = client.execute_query("CREATE TABLE customers (id INT, name VARCHAR(50), city VARCHAR(50))")
        if not result.get('success'):
            print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to create customers table: {result.get('error')}")
            client.rollback_transaction()
            return
        print(f"{Colors.OKGREEN}[SETUP]{Colors.ENDC} ✅ Created customers table")
        
        # Create orders table
        result = client.execute_query("CREATE TABLE orders (id INT, customer_id INT, product VARCHAR(50), amount INT)")
        if not result.get('success'):
            print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to create orders table: {result.get('error')}")
            client.rollback_transaction()
            return
        print(f"{Colors.OKGREEN}[SETUP]{Colors.ENDC} ✅ Created orders table")
        
        client.commit_transaction()
        
        # Insert test data
        print(f"\n{Colors.OKCYAN}[SETUP]{Colors.ENDC} Inserting test data...")
        
        client.begin_transaction()
        
        # Insert customers
        customers_data = [
            (1, 'Alice', 'Jakarta'),
            (2, 'Bob', 'Bandung'),
            (3, 'Charlie', 'Surabaya'),
            (4, 'Diana', 'Jakarta')
        ]
        
        for cid, name, city in customers_data:
            result = client.execute_query(f"INSERT INTO customers VALUES ({cid}, '{name}', '{city}')")
            if not result.get('success'):
                print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to insert customer: {result.get('error')}")
                client.rollback_transaction()
                return
        
        print(f"{Colors.OKGREEN}[SETUP]{Colors.ENDC} ✅ Inserted {len(customers_data)} customers")
        
        # Insert orders
        orders_data = [
            (101, 1, 'Laptop', 1000),
            (102, 1, 'Mouse', 20),
            (103, 2, 'Keyboard', 50),
            (104, 3, 'Monitor', 300),
            (105, 3, 'Headset', 80),
            (106, 1, 'Webcam', 60)
        ]
        
        for oid, cid, product, amount in orders_data:
            result = client.execute_query(f"INSERT INTO orders VALUES ({oid}, {cid}, '{product}', {amount})")
            if not result.get('success'):
                print(f"{Colors.FAIL}[SETUP]{Colors.ENDC} ❌ Failed to insert order: {result.get('error')}")
                client.rollback_transaction()
                return
        
        print(f"{Colors.OKGREEN}[SETUP]{Colors.ENDC} ✅ Inserted {len(orders_data)} orders")
        
        client.commit_transaction()
        
        # Verify data was inserted
        print(f"\n{Colors.OKCYAN}[VERIFY]{Colors.ENDC} Verifying inserted data...")
        
        client.begin_transaction()
        
        # Check customers
        result = client.execute_query("SELECT * FROM customers")
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            print(f"{Colors.OKGREEN}[VERIFY]{Colors.ENDC} ✅ Customers table has {len(rows)} rows")
            for row in rows:
                print(f"{Colors.OKCYAN}[VERIFY]{Colors.ENDC}   {row}")
        else:
            print(f"{Colors.FAIL}[VERIFY]{Colors.ENDC} ❌ Failed to query customers: {result.get('error')}")
        
        # Check orders
        result = client.execute_query("SELECT * FROM orders")
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            print(f"{Colors.OKGREEN}[VERIFY]{Colors.ENDC} ✅ Orders table has {len(rows)} rows")
            for row in rows:
                print(f"{Colors.OKCYAN}[VERIFY]{Colors.ENDC}   {row}")
        else:
            print(f"{Colors.FAIL}[VERIFY]{Colors.ENDC} ❌ Failed to query orders: {result.get('error')}")
        
        client.commit_transaction()
        
        # Test nested loop join queries
        print(f"\n{Colors.HEADER}[TEST]{Colors.ENDC} Running nested loop join queries...")
        print("=" * 60)
        
        client.begin_transaction()
        
        # Test 0: Cartesian product (no join condition) to verify basic functionality
        print(f"\n{Colors.BOLD}Test 0: Cartesian Product (no join condition){Colors.ENDC}")
        query = "SELECT * FROM customers, orders"
        print(f"{Colors.OKCYAN}Query:{Colors.ENDC} {query}")
        
        result = client.execute_query(query)
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            columns = result['rows']['columns']
            print(f"{Colors.OKGREEN}✅ Cartesian product successful! Found {len(rows)} rows (should be 12*18=216){Colors.ENDC}")
            print(f"\n{Colors.BOLD}First 3 rows:{Colors.ENDC}")
            print(f"{Colors.BOLD}" + " | ".join(str(c) for c in columns) + f"{Colors.ENDC}")
            print("-" * 100)
            for row in rows[:3]:
                print(" | ".join(str(v) for v in row))
            if len(rows) > 3:
                print(f"... ({len(rows) - 3} more rows)")
        else:
            print(f"{Colors.FAIL}❌ Cartesian product failed: {result.get('error')}{Colors.ENDC}")
        
        # Test 1: Simple join with SELECT *
        print(f"\n{Colors.BOLD}Test 1: Simple JOIN with SELECT * (all columns){Colors.ENDC}")
        query = "SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id"
        print(f"{Colors.OKCYAN}Query:{Colors.ENDC} {query}")
        
        result = client.execute_query(query)
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            columns = result['rows']['columns']
            print(f"{Colors.OKGREEN}✅ Join successful! Found {len(rows)} rows{Colors.ENDC}")
            print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
            print(f"{Colors.BOLD}" + " | ".join(str(c) for c in columns) + f"{Colors.ENDC}")
            print("-" * 100)
            for row in rows[:10]:
                print(" | ".join(str(v) for v in row))
            if len(rows) > 10:
                print(f"... ({len(rows) - 10} more rows)")
        else:
            print(f"{Colors.FAIL}❌ Join failed: {result.get('error')}{Colors.ENDC}")
        
        # Test 2: Join with WHERE clause
        print(f"\n{Colors.BOLD}Test 2: JOIN with WHERE clause (amount > 50){Colors.ENDC}")
        query = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id WHERE amount > 50"
        print(f"{Colors.OKCYAN}Query:{Colors.ENDC} {query}")
        
        result = client.execute_query(query)
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            columns = result['rows']['columns']
            print(f"{Colors.OKGREEN}✅ Filtered join successful! Found {len(rows)} rows{Colors.ENDC}")
            print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
            print(f"{Colors.BOLD}" + " | ".join(str(c) for c in columns) + f"{Colors.ENDC}")
            print("-" * 80)
            for row in rows:
                print(" | ".join(str(v) for v in row))
        else:
            print(f"{Colors.FAIL}❌ Filtered join failed: {result.get('error')}{Colors.ENDC}")
        
        # Test 3: Join with city filter
        print(f"\n{Colors.BOLD}Test 3: JOIN to find customers from Jakarta with their orders{Colors.ENDC}")
        query = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id WHERE city = 'Jakarta'"
        print(f"{Colors.OKCYAN}Query:{Colors.ENDC} {query}")
        
        result = client.execute_query(query)
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            columns = result['rows']['columns']
            print(f"{Colors.OKGREEN}✅ City-filtered join successful! Found {len(rows)} rows{Colors.ENDC}")
            print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
            print(f"{Colors.BOLD}" + " | ".join(str(c) for c in columns) + f"{Colors.ENDC}")
            print("-" * 80)
            for row in rows:
                print(" | ".join(str(v) for v in row))
        else:
            print(f"{Colors.FAIL}❌ City-filtered join failed: {result.get('error')}{Colors.ENDC}")
        
        client.commit_transaction()
        
        print("\n" + "=" * 60)
        print(f"{Colors.OKGREEN}       NESTED LOOP JOIN TEST COMPLETED{Colors.ENDC}")
        print("=" * 60)
    
    finally:
        client.disconnect()


def test_all_sql_commands():
    """Comprehensive test for all SQL commands"""
    print("=" * 60)
    print("       COMPREHENSIVE SQL COMMANDS TEST")
    print("=" * 60)
    
    HOST = 'localhost'
    PORT = 5555
    
    input("\nPress Enter to start comprehensive SQL test...")
    
    client = TestClient(1, HOST, PORT)
    if not client.connect():
        print(f"{Colors.FAIL}[ERROR]{Colors.ENDC} Failed to connect to server")
        return
    
    test_results = {
        'passed': 0,
        'failed': 0,
        'bonus_passed': 0,
        'bonus_failed': 0
    }
    
    def run_test(name, query, is_bonus=False, expected_rows=None, validate_fn=None):
        """Helper function to run a test query"""
        print(f"\n{Colors.BOLD}{'[BONUS] ' if is_bonus else ''}Test: {name}{Colors.ENDC}")
        print(f"{Colors.OKCYAN}Query:{Colors.ENDC} {query}")
        
        result = client.execute_query(query)
        
        if result.get('success'):
            print(f"{Colors.OKGREEN}✅ SUCCESS{Colors.ENDC}")
            
            if result.get('rows'):
                rows = result['rows']['data']
                columns = result['rows']['columns']
                print(f"   Rows returned: {len(rows)}")
                
                if expected_rows is not None and len(rows) != expected_rows:
                    print(f"{Colors.WARNING}   ⚠ Expected {expected_rows} rows, got {len(rows)}{Colors.ENDC}")
                
                if validate_fn and not validate_fn(rows, columns):
                    print(f"{Colors.FAIL}   ❌ Validation failed{Colors.ENDC}")
                    if is_bonus:
                        test_results['bonus_failed'] += 1
                    else:
                        test_results['failed'] += 1
                    return False
                
                # Show sample data
                if rows:
                    print(f"   {Colors.BOLD}Sample data:{Colors.ENDC}")
                    for row in rows[:3]:
                        print(f"     {row}")
                    if len(rows) > 3:
                        print(f"     ... ({len(rows) - 3} more rows)")
            else:
                print(f"   Message: {result.get('message', 'OK')}")
            
            if is_bonus:
                test_results['bonus_passed'] += 1
            else:
                test_results['passed'] += 1
            return True
        else:
            print(f"{Colors.FAIL}❌ FAILED: {result.get('error')}{Colors.ENDC}")
            if is_bonus:
                test_results['bonus_failed'] += 1
            else:
                test_results['failed'] += 1
            return False
    
    try:
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 1: SETUP (BONUS COMMANDS){Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        # BEGIN TRANSACTION (BONUS)
        print(f"\n{Colors.BOLD}[BONUS] Test: BEGIN TRANSACTION{Colors.ENDC}")
        begin_res = client.begin_transaction()
        if begin_res.get('success'):
            print(f"{Colors.OKGREEN}✅ SUCCESS - Transaction ID: {client.current_tid}{Colors.ENDC}")
            test_results['bonus_passed'] += 1
        else:
            print(f"{Colors.FAIL}❌ FAILED: {begin_res.get('error')}{Colors.ENDC}")
            test_results['bonus_failed'] += 1
        
        # DROP TABLE (BONUS) - Clean up
        run_test("DROP TABLE (cleanup)", "DROP TABLE IF EXISTS employee", is_bonus=True)
        run_test("DROP TABLE (cleanup)", "DROP TABLE IF EXISTS department", is_bonus=True)
        
        # COMMIT (BONUS)
        print(f"\n{Colors.BOLD}[BONUS] Test: COMMIT{Colors.ENDC}")
        commit_res = client.commit_transaction()
        if commit_res.get('success'):
            print(f"{Colors.OKGREEN}✅ SUCCESS{Colors.ENDC}")
            test_results['bonus_passed'] += 1
        else:
            print(f"{Colors.FAIL}❌ FAILED: {commit_res.get('error')}{Colors.ENDC}")
            test_results['bonus_failed'] += 1
        
        # CREATE TABLE (BONUS)
        client.begin_transaction()
        
        run_test(
            "CREATE TABLE - department",
            "CREATE TABLE department (id INT, name VARCHAR(50), location VARCHAR(50))",
            is_bonus=True
        )
        
        run_test(
            "CREATE TABLE - employee",
            "CREATE TABLE employee (id INT, name VARCHAR(50), salary INT, department_id INT)",
            is_bonus=True
        )
        
        client.commit_transaction()
        
        # INSERT (BONUS)
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 2: INSERT DATA (BONUS){Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        # Insert departments
        run_test("INSERT - department 1", "INSERT INTO department VALUES (1, 'Engineering', 'Jakarta')", is_bonus=True)
        run_test("INSERT - department 2", "INSERT INTO department VALUES (2, 'RnD', 'Bandung')", is_bonus=True)
        run_test("INSERT - department 3", "INSERT INTO department VALUES (3, 'Marketing', 'Jakarta')", is_bonus=True)
        
        # Insert employees
        run_test("INSERT - employee 1", "INSERT INTO employee VALUES (1, 'Alice', 5000, 1)", is_bonus=True)
        run_test("INSERT - employee 2", "INSERT INTO employee VALUES (2, 'Bob', 3000, 2)", is_bonus=True)
        run_test("INSERT - employee 3", "INSERT INTO employee VALUES (3, 'Charlie', 4000, 1)", is_bonus=True)
        run_test("INSERT - employee 4", "INSERT INTO employee VALUES (4, 'Diana', 6000, 3)", is_bonus=True)
        run_test("INSERT - employee 5", "INSERT INTO employee VALUES (5, 'Eve', 2000, 2)", is_bonus=True)
        run_test("INSERT - employee 6", "INSERT INTO employee VALUES (6, 'Frank', 1500, 1)", is_bonus=True)
        
        client.commit_transaction()
        
        # SELECT and FROM
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 3: SELECT and FROM{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "SELECT * FROM single table",
            "SELECT * FROM employee",
            expected_rows=6
        )
        
        run_test(
            "SELECT specific columns",
            "SELECT name, salary FROM employee",
            expected_rows=6,
            validate_fn=lambda rows, cols: 'name' in cols and 'salary' in cols
        )
        
        run_test(
            "FROM multiple tables (Cartesian product)",
            "SELECT * FROM employee, department",
            expected_rows=18  # 6 employees × 3 departments
        )
        
        client.commit_transaction()
        
        # WHERE
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 4: WHERE clause{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "WHERE with = operator",
            "SELECT * FROM employee WHERE department_id = 1",
            expected_rows=3
        )
        
        run_test(
            "WHERE with > operator",
            "SELECT * FROM employee WHERE salary > 3000",
            expected_rows=3,
            validate_fn=lambda rows, cols: all(row[cols.index('salary')] > 3000 for row in rows if 'salary' in cols)
        )
        
        run_test(
            "WHERE with >= operator",
            "SELECT * FROM employee WHERE salary >= 4000",
            expected_rows=3
        )
        
        run_test(
            "WHERE with < operator",
            "SELECT * FROM employee WHERE salary < 3000",
            expected_rows=2
        )
        
        run_test(
            "WHERE with <= operator",
            "SELECT * FROM employee WHERE salary <= 3000",
            expected_rows=3
        )
        
        run_test(
            "WHERE with <> operator",
            "SELECT * FROM employee WHERE department_id <> 1",
            expected_rows=3
        )
        
        client.commit_transaction()
        
        # JOIN
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 5: JOIN{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "JOIN ON",
            "SELECT * FROM employee JOIN department ON employee.department_id = department.id",
            expected_rows=6
        )
        
        run_test(
            "JOIN ON with WHERE",
            "SELECT * FROM employee JOIN department ON employee.department_id = department.id WHERE salary > 3000",
            expected_rows=3
        )
        
        client.commit_transaction()
        
        # AS (Alias) - BONUS
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 6: AS (Table Alias) - BONUS{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "AS - Table alias in JOIN",
            "SELECT * FROM employee AS e JOIN department AS d ON e.department_id = d.id",
            is_bonus=True,
            expected_rows=6
        )
        
        run_test(
            "AS - Short alias in JOIN with WHERE",
            "SELECT * FROM employee e JOIN department d ON e.department_id = d.id WHERE e.salary > 3000",
            is_bonus=True,
            expected_rows=3
        )
        
        client.commit_transaction()
        
        # ORDER BY
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 7: ORDER BY{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        def validate_ascending_order(rows, cols):
            if not rows or 'salary' not in cols:
                return True
            salary_idx = cols.index('salary')
            salaries = [row[salary_idx] for row in rows]
            return salaries == sorted(salaries)
        
        def validate_descending_order(rows, cols):
            if not rows or 'salary' not in cols:
                return True
            salary_idx = cols.index('salary')
            salaries = [row[salary_idx] for row in rows]
            return salaries == sorted(salaries, reverse=True)
        
        run_test(
            "ORDER BY ASC (numeric)",
            "SELECT * FROM employee ORDER BY salary ASC",
            expected_rows=6,
            validate_fn=validate_ascending_order
        )
        
        run_test(
            "ORDER BY DESC (numeric)",
            "SELECT * FROM employee ORDER BY salary DESC",
            expected_rows=6,
            validate_fn=validate_descending_order
        )
        
        run_test(
            "ORDER BY with string column",
            "SELECT * FROM employee ORDER BY name ASC",
            expected_rows=6
        )
        
        client.commit_transaction()
        
        # LIMIT - BONUS
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 8: LIMIT - BONUS{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "LIMIT with specific number",
            "SELECT * FROM employee LIMIT 3",
            is_bonus=True,
            expected_rows=3
        )
        
        run_test(
            "LIMIT with ORDER BY",
            "SELECT * FROM employee ORDER BY salary DESC LIMIT 2",
            is_bonus=True,
            expected_rows=2
        )
        
        client.commit_transaction()
        
        # UPDATE (BONUS)
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 9: UPDATE{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "UPDATE with simple SET",
            "UPDATE employee SET salary = 5500 WHERE id = 1"
        )
        
        # Verify update
        result = client.execute_query("SELECT salary FROM employee WHERE id = 1")
        if result.get('success') and result.get('rows'):
            rows = result['rows']['data']
            if rows and rows[0][1] == 5500:  # Assuming salary is second column after __row_id
                print(f"{Colors.OKGREEN}   ✓ Update verified: salary = 5500{Colors.ENDC}")
            else:
                print(f"{Colors.WARNING}   ⚠ Update verification: unexpected value{Colors.ENDC}")
        
        run_test(
            "UPDATE with expression (1.05 * salary)",
            "UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000"
        )
        
        # Verify arithmetic update
        result = client.execute_query("SELECT * FROM employee WHERE salary > 5000")
        if result.get('success') and result.get('rows'):
            print(f"{Colors.OKGREEN}   ✓ Arithmetic update executed{Colors.ENDC}")
        
        client.commit_transaction()
        
        # DELETE (BONUS)
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}PHASE 10: DELETE - BONUS{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        # First check how many rows before delete
        result = client.execute_query("SELECT * FROM employee WHERE department_id = 2")
        initial_count = len(result['rows']['data']) if result.get('success') and result.get('rows') else 0
        print(f"   Employees in department 2: {initial_count}")
        
        run_test(
            "DELETE with WHERE condition",
            "DELETE FROM employee WHERE department_id = 2",
            is_bonus=True
        )
        
        # Verify delete
        result = client.execute_query("SELECT * FROM employee WHERE department_id = 2")
        if result.get('success') and result.get('rows'):
            remaining = len(result['rows']['data'])
            if remaining == 0:
                print(f"{Colors.OKGREEN}   ✓ Delete verified: 0 rows remaining{Colors.ENDC}")
            else:
                print(f"{Colors.WARNING}   ⚠ Delete verification: {remaining} rows still exist{Colors.ENDC}")
        
        client.commit_transaction()
        
        # Final verification
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}FINAL VERIFICATION{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        client.begin_transaction()
        
        run_test(
            "Final SELECT - remaining employees",
            "SELECT * FROM employee"
        )
        
        run_test(
            "Complex query - JOIN with WHERE and ORDER BY",
            "SELECT * FROM employee e JOIN department d ON e.department_id = d.id WHERE e.salary > 2000 ORDER BY e.salary DESC"
        )
        
        client.commit_transaction()
        
        # Summary
        print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.HEADER}TEST SUMMARY{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        
        total_passed = test_results['passed'] + test_results['bonus_passed']
        total_failed = test_results['failed'] + test_results['bonus_failed']
        total_tests = total_passed + total_failed
        
        print(f"\n{Colors.BOLD}Core Features:{Colors.ENDC}")
        print(f"  {Colors.OKGREEN}✅ Passed: {test_results['passed']}{Colors.ENDC}")
        print(f"  {Colors.FAIL}❌ Failed: {test_results['failed']}{Colors.ENDC}")
        
        print(f"\n{Colors.BOLD}Bonus Features:{Colors.ENDC}")
        print(f"  {Colors.OKGREEN}✅ Passed: {test_results['bonus_passed']}{Colors.ENDC}")
        print(f"  {Colors.FAIL}❌ Failed: {test_results['bonus_failed']}{Colors.ENDC}")
        
        print(f"\n{Colors.BOLD}Overall:{Colors.ENDC}")
        print(f"  {Colors.OKGREEN}✅ Total Passed: {total_passed}/{total_tests}{Colors.ENDC}")
        print(f"  {Colors.FAIL}❌ Total Failed: {total_failed}/{total_tests}{Colors.ENDC}")
        
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        print(f"  {Colors.BOLD}Success Rate: {success_rate:.1f}%{Colors.ENDC}")
        
        print(f"\n{'=' * 60}")
        print(f"{Colors.OKGREEN}COMPREHENSIVE TEST COMPLETED{Colors.ENDC}")
        print(f"{'=' * 60}")
    
    finally:
        client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "stress":
            # Stress test mode
            num_clients = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            test_multiple_clients(num_clients)
        elif sys.argv[1] == "join":
            # Nested loop join test
            test_nested_loop_join()
        elif sys.argv[1] == "all":
            # Comprehensive SQL commands test
            test_all_sql_commands()
        else:
            print(f"{Colors.FAIL}Unknown test mode: {sys.argv[1]}{Colors.ENDC}")
            print(f"\nUsage:")
            print(f"  python test_client.py           - Normal concurrent test")
            print(f"  python test_client.py stress N  - Stress test with N clients")
            print(f"  python test_client.py join      - Nested loop join test")
            print(f"  python test_client.py all       - Comprehensive SQL commands test")
    else:
        # Normal test mode
        test_concurrent_clients()
