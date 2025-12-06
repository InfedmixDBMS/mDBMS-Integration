"""
Simple test to verify server can be started with different protocols
Tests command-line argument parsing for --protocol flag
"""

import subprocess
import sys
import time
import socket


def is_port_in_use(port):
    """Check if a port is already in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def wait_for_server(port, timeout=10):
    """Wait for server to be ready"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_in_use(port):
            time.sleep(0.3)
            return True
        time.sleep(0.1)
    return False


def test_server_protocol(protocol, port):
    """Test starting server with a specific protocol"""
    print(f"\n{'='*60}")
    print(f"Testing server with --protocol {protocol}")
    print(f"{'='*60}")
    
    # Start server
    process = subprocess.Popen(
        [sys.executable, 'server.py', '--protocol', protocol, '--port', str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    try:
        print(f"Starting server on port {port}...")
        
        if wait_for_server(port, timeout=10):
            print(f"✓ Server started successfully with {protocol} protocol!")
            
            # Give it a moment to fully initialize
            time.sleep(1)
            
            # Try to read some output
            try:
                # Use non-blocking read
                import select
                if select.select([process.stdout], [], [], 0.5)[0]:
                    output = process.stdout.readline()
                    if output:
                        print(f"Server output: {output.strip()}")
            except Exception:
                pass
            
            return True
        else:
            print(f"✗ Server failed to start on port {port}")
            # Try to read error
            stderr_output = process.stderr.read()
            if stderr_output:
                print(f"Error output: {stderr_output}")
            return False
            
    finally:
        print(f"Stopping server...")
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        print(f"Server stopped.\n")


def main():
    print("="*60)
    print("PROTOCOL SELECTION TEST")
    print("="*60)
    print("\nThis test verifies that the server can be started with")
    print("different concurrency control protocols using CLI arguments.")
    
    tests = [
        ('lock', 6001),
        ('timestamp', 6002),
        ('validation', 6003)
    ]
    
    results = {}
    
    for protocol, port in tests:
        try:
            success = test_server_protocol(protocol, port)
            results[protocol] = success
            time.sleep(1)  # Wait between tests
        except Exception as e:
            print(f"✗ Error testing {protocol}: {e}")
            results[protocol] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    
    for protocol in ['lock', 'timestamp', 'validation']:
        status = "✓ PASSED" if results.get(protocol, False) else "✗ FAILED"
        print(f"  --protocol {protocol:12s} : {status}")
    
    print("="*60)
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n✓ All protocol options work correctly!")
        print("\nYou can now start the server with:")
        print("  python server.py --protocol lock        (default)")
        print("  python server.py --protocol timestamp")
        print("  python server.py --protocol validation")
        print("\nOptional arguments:")
        print("  --host <address>   (default: localhost)")
        print("  --port <number>    (default: 5555)")
    else:
        print("\n✗ Some protocol options failed to start")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
