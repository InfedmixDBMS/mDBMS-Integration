from abc import ABC, abstractmethod
from ..models import QueryPlan


class AbstractQueryOptimizer(ABC):
    
    @abstractmethod
    def optimize(self, query: str) -> QueryPlan:
        # Parse SQL dan optimize, return QueryPlan tree
        pass

