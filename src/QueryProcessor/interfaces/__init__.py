"""
Interfaces package for Query Processor.
Contains abstract base classes for dependency injection.
"""

from .query_optimizer_interface import AbstractQueryOptimizer
from .storage_manager_interface import AbstractStorageManager
from .concurrency_control_interface import AbstractConcurrencyControlManager
from .failure_recovery_interface import AbstractFailureRecoveryManager

__all__ = [
    'AbstractQueryOptimizer',
    'AbstractStorageManager',
    'AbstractConcurrencyControlManager',
    'AbstractFailureRecoveryManager'
]
