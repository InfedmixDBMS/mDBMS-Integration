from enum import Enum

class TransactionStatus(Enum):
    ACTIVE = 'active'
    PARTIALLY_COMMITTED = 'partially_committed'
    COMMITTED = 'committed'
    FAILED = 'failed'
    ABORTED = 'aborted'
    TERMINATED = 'terminated'