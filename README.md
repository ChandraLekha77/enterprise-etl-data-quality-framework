# Enterprise ETL Data Quality and Source-to-Target Reconciliation Framework

A reusable validation and reconciliation platform for customer, transaction, and operational data received from PostgreSQL, SQL Server, CSV files, and REST APIs.

The platform stages source records, applies documented transformations, validates data quality, reconciles source and target results, and publishes centralized quality results before data is released to downstream dashboards.

## Approved technology stack

- Python, Pandas, and NumPy
- SQL, PostgreSQL, and SQL Server
- SQLAlchemy
- Apache Airflow
- dbt Core
- Great Expectations
- FastAPI
- REST APIs, JSON, and CSV
- pytest
- Git and Docker

No additional application frameworks, orchestration engines, databases, or validation platforms are planned.

## Documentation

- [System architecture](docs/architecture/system_architecture.md)
- [Source-system contracts](docs/architecture/source_systems.md)
- [Transformation and quality rules](docs/architecture/transformation_rules.md)
- [Data dictionary](docs/data_dictionary/data_dictionary.md)
- [Source-to-target mappings](docs/source_to_target_mappings/source_to_target_mapping.md)

## Delivery status

- Day 1: repository structure complete
- Day 2: architecture, contracts, rules, dictionary, and mappings complete
- Runtime implementation: scheduled for subsequent delivery days
