class ConcurrencyResponse:
    def __init__(self, transaction_id: int, query_allowed: bool, reason: str):
        self.transaction_id = transaction_id
        self.query_allowed = query_allowed
        self.reason = reason