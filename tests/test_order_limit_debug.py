"""
Debug test for ORDER BY and LIMIT functionality
Tests the lexer, parser, optimizer, and executor step by step
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from QueryOptimization.src.parser.lexer import Lexer
from QueryOptimization.src.parser.parser import Parser

def test_lexer():
    """Test if lexer tokenizes ORDER BY and LIMIT correctly"""
    print("=" * 60)
    print("TESTING LEXER")
    print("=" * 60)
    
    lexer = Lexer()
    
    test_queries = [
        "SELECT * FROM employee ORDER BY salary ASC",
        "SELECT * FROM employee ORDER BY salary DESC",
        "SELECT * FROM employee LIMIT 5",
        "SELECT * FROM employee ORDER BY salary DESC LIMIT 3"
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        try:
            tokens = lexer.tokenize(query)
            print(f"Tokens: {tokens}")
            
            # Check for ORDER, BY, LIMIT in tokens
            if "ORDER" in query.upper():
                if "ORDER" in tokens and "BY" in tokens:
                    print("✅ ORDER and BY found in tokens")
                else:
                    print(f"❌ ORDER or BY missing! ORDER in tokens: {'ORDER' in tokens}, BY in tokens: {'BY' in tokens}")
            
            if "LIMIT" in query.upper():
                if "LIMIT" in tokens:
                    print("✅ LIMIT found in tokens")
                else:
                    print("❌ LIMIT missing from tokens!")
                    
        except Exception as e:
            print(f"❌ Lexer error: {e}")
    

def test_parser():
    """Test if parser creates correct AST for ORDER BY and LIMIT"""
    print("\n" + "=" * 60)
    print("TESTING PARSER")
    print("=" * 60)
    
    parser = Parser()
    
    test_queries = [
        "SELECT * FROM employee ORDER BY salary ASC",
        "SELECT * FROM employee LIMIT 5",
        "SELECT * FROM employee ORDER BY salary DESC LIMIT 3"
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        try:
            parsed_query = parser.parse_query(query)
            print(f"✅ Parser succeeded!")
            print(f"Root node type: {parsed_query.query_tree.type}")
            tree = parsed_query.query_tree
            
            # Walk the tree to find ORDER_BY and LIMIT nodes
            def walk_tree(node, depth=0):
                indent = "  " * depth
                print(f"{indent}Node: {node.type}, Value: {node.val}")
                for child in node.childs:
                    walk_tree(child, depth + 1)
            
            walk_tree(tree)
            
        except Exception as e:
            print(f"❌ Parser error: {e}")
            import traceback
            traceback.print_exc()


def test_with_client():
    """Test actual queries through the client-server connection"""
    print("\n" + "=" * 60)
    print("TESTING CLIENT-SERVER")
    print("=" * 60)
    print("Make sure server is running with: python server.py")
    print()
    
    import socket
    import json
    
    def send_query(query, client_id=1):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('localhost', 5555))
            
            message = {
                "client_id": client_id,
                "query": query
            }
            sock.sendall(json.dumps(message).encode('utf-8'))
            
            response = sock.recv(65536).decode('utf-8')
            result = json.loads(response)
            
            sock.close()
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    # First setup test data
    print("Setting up test data...")
    queries = [
        "CREATE TABLE test_order (id INT, value INT, name VARCHAR(50))",
        "INSERT INTO test_order VALUES (1, 100, 'Alice')",
        "INSERT INTO test_order VALUES (2, 50, 'Bob')",
        "INSERT INTO test_order VALUES (3, 200, 'Charlie')",
        "INSERT INTO test_order VALUES (4, 75, 'David')"
    ]
    
    for query in queries:
        result = send_query(query, 999)
        if not result.get("success"):
            print(f"Setup query failed: {query}")
            print(f"Error: {result.get('error', 'Unknown')}")
    
    print("\nTesting ORDER BY and LIMIT...")
    
    test_queries = [
        ("SELECT * FROM test_order ORDER BY value ASC", "ORDER BY ASC"),
        ("SELECT * FROM test_order ORDER BY value DESC", "ORDER BY DESC"),
        ("SELECT * FROM test_order LIMIT 2", "LIMIT"),
        ("SELECT * FROM test_order ORDER BY value DESC LIMIT 2", "ORDER BY + LIMIT"),
    ]
    
    for query, description in test_queries:
        print(f"\n{description}")
        print(f"Query: {query}")
        result = send_query(query, 999)
        
        if result.get("success"):
            rows = result.get("rows", [])
            print(f"✅ Query succeeded! Returned {len(rows)} rows")
            for row in rows[:5]:  # Show first 5 rows
                print(f"   {row}")
        else:
            error = result.get("error", "Unknown error")
            print(f"❌ Query failed: {error}")
    
    # Cleanup
    print("\nCleaning up...")
    send_query("DROP TABLE test_order", 999)


if __name__ == "__main__":
    print("ORDER BY and LIMIT Debug Test")
    print("=" * 60)
    
    # Test each component separately
    test_lexer()
    test_parser()
    
    # Ask before testing client-server
    print("\n" + "=" * 60)
    choice = input("Test with actual server? (y/n): ")
    if choice.lower() == 'y':
        test_with_client()
    
    print("\n" + "=" * 60)
    print("Debug test complete!")
