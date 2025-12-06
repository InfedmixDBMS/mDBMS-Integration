import sys
import os
import argparse

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from src.client_handler import ClientHandler
from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
from QueryProcessor.query_processor_core import QueryProcessor
from ConcurrencyControl.src.lock_based_concurrency_control_manager import LockBasedConcurrencyControlManager
from ConcurrencyControl.src.timestamp_based_concurrency_control_manager import TimestampBasedConcurrencyControlManager
from ConcurrencyControl.src.validation_based_concurrency_control_manager import ValidationBasedConcurrencyControlManager
from StorageManager.classes.API import StorageEngine

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

def setup_system(protocol='lock'):
    storage_engine = StorageEngine()
    storage_manager = IntegratedStorageManager(storage_engine)
    
    if protocol.lower() == 'timestamp':
        ccm_core = TimestampBasedConcurrencyControlManager()
        protocol_name = "Timestamp-Based"
    elif protocol.lower() == 'validation':
        ccm_core = ValidationBasedConcurrencyControlManager()
        protocol_name = "Validation-Based (OCC)"
    else:  # Default is Lock-Based
        ccm_core = LockBasedConcurrencyControlManager()
        protocol_name = "Lock-Based (2PL)"
    
    concurrency_manager = IntegratedConcurrencyManager(ccm_core)
    
    optimizer = IntegratedQueryOptimizer()
    recovery_manager = IntegratedFailureRecoveryManager()
    
    processor = QueryProcessor(
        optimizer=optimizer,
        storage_manager=storage_manager,
        concurrency_manager=concurrency_manager,
        recovery_manager=recovery_manager
    )
    
    print(f"{Colors.OKBLUE}[SYSTEM] Concurrency Control Protocol: {protocol_name}{Colors.ENDC}")
    
    return processor


def main():
    print(f"{Colors.BOLD}{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}InfedmixDBMS Server{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='InfedmixDBMS Server')
    parser.add_argument(
        '--protocol',
        type=str,
        choices=['lock', 'timestamp', 'validation'],
        default='lock',
        help='Concurrency control protocol (default: lock)'
    )
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Server host address (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5555,
        help='Server port (default: 5555)'
    )
    
    args = parser.parse_args()
    
    processor = setup_system(protocol=args.protocol)
    
    # Create and start server
    server = ClientHandler(host=args.host, port=args.port, processor=processor)
    
    try:
        server.start()
        
        print(f"\n{Colors.OKGREEN}[SERVER] Server is running on {args.host}:{args.port}{Colors.ENDC}")
        print(f"{Colors.OKGREEN}[SERVER] Press Ctrl+C to stop.{Colors.ENDC}")
        print(f"{Colors.OKCYAN}[SERVER] Waiting for client connections...{Colors.ENDC}\n")
        
        # Keep server running
        import time
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}[SERVER] Shutting down...{Colors.ENDC}")
        server.stop()
        print(f"{Colors.OKCYAN}[SERVER] Goodbye!{Colors.ENDC}")


if __name__ == "__main__":
    main()
