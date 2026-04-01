"""
Orchestrator that creates support cases from all open data quality issues.
"""
from src.issues.support_case_service import create_support_cases_from_open_issues


def main():
    created = create_support_cases_from_open_issues()
    if created:
        print(f"Created {created} support case(s) from open issues.")
    return created


if __name__ == "__main__":
    main()