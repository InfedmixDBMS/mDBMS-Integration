from QueryProcessor import QueryProcessor
from src import (
    IntegratedQueryOptimizer,
    IntegratedConcurrencyManager,
    IntegratedFailureRecoveryManager,
    IntegratedStorageManager
)


def main():
    processor = QueryProcessor(
        optimizer=IntegratedQueryOptimizer(),
        concurrency_manager=IntegratedConcurrencyManager(),
        recovery_manager=IntegratedFailureRecoveryManager(),
        storage_manager=IntegratedStorageManager()
    )
    
    plan = processor.get_optimizer().optimize(
        """
        SELECT
            emp.employee_id,
            emp.name AS employee_name,
            dept.department_name,
            proj.project_name,
            proj.budget
        FROM
            employees emp
        INNER JOIN
            departments dept ON emp.department_id = dept.department_id
        INNER JOIN
            projects proj ON dept.department_id = proj.department_id
        WHERE
            (emp.salary > 80000 AND emp.experience_years >= 5)
            OR proj.budget > 1000000
        ORDER BY
            emp.salary DESC,
            proj.budget DESC
    """
    )
    
    print("\n=== Converted Query Plan ===")
    print(plan.print_tree())
    
    
if __name__ == "__main__":
    main()