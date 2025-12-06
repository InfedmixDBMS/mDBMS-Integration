"""
Client Entry Point - CLI that connects to the server via socket
"""

import socket
import json
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

class DBClient:
    
    def __init__(self, host='localhost', port=5555):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
        self.current_tid = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            print(f"Connected to server at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False
    
    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.connected = False
            print("Disconnected from server")
    
    def _send_request(self, request: dict) -> dict:
        if not self.connected:
            return {'success': False, 'error': 'Not connected to server'}
        
        try:
            message_data = json.dumps(request).encode('utf-8')
            length_data = len(message_data).to_bytes(4, byteorder='big')
            self.socket.sendall(length_data + message_data)
            
            length_data = self._recv_exact(4)
            if not length_data:
                return {'success': False, 'error': 'Connection lost'}
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
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
    
    def execute_query(self, query: str, timeout: float = 30.0) -> dict:
        request = {
            'type': 'execute',
            'query': query
        }
        if self.current_tid is not None:
            request['transaction_id'] = self.current_tid
        
        response = self._send_request(request)
        
        if response.get('queued_for_retry'):
            response = self._receive_response(timeout=timeout)
        
        return response
    
    def _receive_response(self, timeout: float = 30.0) -> dict:
        """Receive a response message without sending a request first"""
        try:
            old_timeout = self.socket.gettimeout()
            self.socket.settimeout(timeout)
            
            length_data = self._recv_exact(4)
            if not length_data:
                return {'success': False, 'error': 'Connection lost'}
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            message_data = self._recv_exact(message_length)
            if not message_data:
                return {'success': False, 'error': 'Connection lost'}
            
            response = json.loads(message_data.decode('utf-8'))
            
            self.socket.settimeout(old_timeout)
            
            return response
            
        except socket.timeout:
            return {'success': False, 'error': f'Timeout waiting for retry response ({timeout}s)'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
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
        """Rollback current transaction"""
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
    
    def analyze_table(self, table_name: str) -> dict:
        request = {
            'type' : 'analyze',
            'table_name': table_name
        }
        return self._send_request(request)

    def defragment_table(self, table_name: str) -> dict:
        request = {
            'type' : 'defragment',
            'table_name': table_name
        }
        return self._send_request(request)



def print_welcome():
    print(f"{Colors.BOLD}{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}InfedmixDBMS Client CLI{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.OKCYAN}Type 'help' for available commands.{Colors.ENDC}")
    print()


def print_help():
    print(f"""
{Colors.BOLD}Available commands:{Colors.ENDC}
  {Colors.OKBLUE}help{Colors.ENDC}                Show this help message
  {Colors.WARNING}exit{Colors.ENDC}                Exit CLI
  {Colors.OKBLUE}<SQL>{Colors.ENDC}               Execute SQL query
  {Colors.HEADER}begin{Colors.ENDC}               Begin transaction
  {Colors.HEADER}commit{Colors.ENDC}              Commit transaction
  {Colors.HEADER}rollback{Colors.ENDC}            Rollback transaction
  {Colors.OKBLUE}show tables{Colors.ENDC}         List all tables
  {Colors.OKBLUE}show data <table>{Colors.ENDC}   Show all data in table
  {Colors.HEADER}defragment{Colors.ENDC}          Defragment table
  {Colors.HEADER}analyze{Colors.ENDC}             Analyzes table and update statistics 
""")


def cli_loop():
    client = DBClient()
    
    if not client.connect():
        print(f"{Colors.FAIL}Failed to connect to server. Is the server running?{Colors.ENDC}")
        return
    
    print_welcome()
    
    try:
        while True:
            try:
                cmd = ""
                if client.current_tid is None:
                    prompt = f"{Colors.OKCYAN}dbms> {Colors.ENDC}"
                else:
                    prompt = f"{Colors.WARNING}dbms[T{client.current_tid}]> {Colors.ENDC}"
                
                while True:
                    line = input(prompt)
                    if not line and not cmd:
                        break
                    cmd += (line + "\n")
                    prompt = f"{Colors.OKCYAN}...> {Colors.ENDC}"
                    if ";" in line:
                        break
                
                cmd = cmd.strip()
                if not cmd:
                    continue
                
                cmd = cmd.replace("\n", " ").strip()
                if cmd.endswith(";"):
                    cmd = cmd[:-1].strip()
                
                if cmd.lower() == "exit":
                    print(f"{Colors.OKCYAN}Exiting CLI.{Colors.ENDC}")
                    break
                
                elif cmd.lower() == "help":
                    print_help()
                
                elif cmd.lower() == "begin":
                    response = client.begin_transaction()
                    if response.get('success'):
                        tid = response.get('transaction_id')
                        print(f"{Colors.OKGREEN}Transaction started. TID={tid}{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}Error: {response.get('error')}{Colors.ENDC}")
                
                elif cmd.lower() == "commit":
                    response = client.commit_transaction()
                    if response.get('success'):
                        print(f"{Colors.OKGREEN}Transaction committed.{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}Error: {response.get('error')}{Colors.ENDC}")
                
                elif cmd.lower() == "rollback":
                    response = client.rollback_transaction()
                    if response.get('success'):
                        print(f"{Colors.WARNING}Transaction rolled back.{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}Error: {response.get('error')}{Colors.ENDC}")
                
                elif cmd.lower() == "show tables":
                    response = client.execute_query("SHOW TABLES")
                    if response.get('success') and response.get('rows'):
                        print(f"{Colors.BOLD}Tables:{Colors.ENDC}")
                        for row in response['rows']['data']:
                            print(f"  {Colors.OKBLUE}{row}{Colors.ENDC}")
                    else:
                        print(f"{Colors.WARNING}No tables found or error.{Colors.ENDC}")
                
                elif cmd.lower().startswith("show data "):
                    table = cmd[len("show data "):].strip()
                    response = client.execute_query(f"SELECT * FROM {table}")
                    if response.get('success') and response.get('rows'):
                        for row in response['rows']['data']:
                            print(f"{Colors.OKBLUE}{row}{Colors.ENDC}")
                    else:
                        error = response.get('error', 'No data')
                        print(f"{Colors.FAIL}Error: {error}{Colors.ENDC}")
                
                elif cmd.lower().startswith("analyze "):
                    parts = cmd.split(maxsplit=1)
                    if len(parts) < 2:
                        print(f"{Colors.FAIL}ERROR: ANALYZE requires a table name.{Colors.ENDC}")
                        continue
                    table_name = parts[1]
                    response = client.analyze_table(table_name)

                    if response.get("success"):
                        print(f"{Colors.OKGREEN}Table '{table_name}' analyzed succesfully.{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}ERROR analyzing table '{table_name}': {response.get('error', 'Unknown error')}{Colors.ENDC}")

                elif cmd.lower().startswith("defragment "):
                    parts = cmd.split(maxsplit=1)
                    if len(parts) < 2:
                        print(f"{Colors.FAIL}ERROR: DEFRAGMENT requires a table name.{Colors.ENDC}")
                        continue
                    table_name = parts[1]
                    response = client.defragment_table(table_name)

                    if response.get("success"):
                        print(f"{Colors.OKGREEN}Table '{table_name}' defragmented succesfully.{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}ERROR defragmenting table'{table_name}': {response.get('error', 'Unknown error')}{Colors.ENDC}")
                


                else:
                    response = client.execute_query(cmd)
                    
                    if response.get('retried'):
                        print(f"{Colors.OKBLUE}[INFO] This was an automatic retry{Colors.ENDC}")
                    
                    if response.get('queued_for_retry'):
                        print(f"{Colors.OKBLUE}[INFO] {response.get('message')}{Colors.ENDC}")
                    
                    if response.get('success'):
                        print(f"{Colors.OKGREEN}Query OK.{Colors.ENDC}")
                        if response.get('rows'):
                            rows_data = response['rows']['data']
                            if rows_data:
                                cols = response['rows']['columns']
                                print(f"{Colors.BOLD}" + " | ".join(str(c) for c in cols) + f"{Colors.ENDC}")
                                print("-" * 60)
                                for row in rows_data:
                                    print(" | ".join(str(v) for v in row))
                            else:
                                print(f"{Colors.WARNING}(0 rows){Colors.ENDC}")
                        if response.get('affected_rows'):
                            print(f"{Colors.OKGREEN}Affected rows: {response['affected_rows']}{Colors.ENDC}")
                    else:
                        print(f"{Colors.FAIL}Query Error: {response.get('error')}{Colors.ENDC}")
                
            except EOFError:
                print(f"\n{Colors.OKCYAN}Exiting CLI.{Colors.ENDC}")
                break
            except KeyboardInterrupt:
                print(f"\n{Colors.OKCYAN}Exiting CLI.{Colors.ENDC}")
                break
            except Exception as e:
                print(f"{Colors.FAIL}Error: {e}{Colors.ENDC}")
    
    finally:
        client.disconnect()


def main():
    cli_loop()


if __name__ == "__main__":
    main()
