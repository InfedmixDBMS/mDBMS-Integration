import threading
import time
import random
import os
import shutil
import sys

sys.path.append(os.path.join(os.getcwd(), "StorageManager"))

from src.concurrency_manager_integrated import IntegratedConcurrencyManager
from src.storage_manager_integrated import IntegratedStorageManager
from src.query_optimizer_integrated import IntegratedQueryOptimizer
from src.failure_recovery_integrated import IntegratedFailureRecoveryManager
def main():
    from cli import cli_loop
    cli_loop()

if __name__ == "__main__":
    main()
