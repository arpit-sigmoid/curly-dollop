# Healthcare Claims ETL Pipeline

A Python ETL pipeline for processing healthcare claims data from Hospital DB to Azure SQL.

## Architecture

```
Hospital DB → Python ETL → Azure SQL
```

## Features

- **Extract**: Pull claims data from hospital database
- **Transform**: Clean, validate, and standardize claims data
- **Load**: Push processed data to Azure SQL
- **Automated CI/CD**: GitHub Actions for testing and deployment

## Project Structure

```
healthcare-etl/
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions CI/CD pipeline
├── etl/
│   ├── __init__.py
│   └── claims_processor.py # ETL processing logic
├── tests/
│   ├── __init__.py
│   └── test_claims_processor.py # Unit tests
└── requirements.txt        # Python dependencies
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from etl.claims_processor import ClaimsProcessor

# Initialize processor
processor = ClaimsProcessor(config={'azure_connection_string': 'your_connection_string'})

# Run complete pipeline
result = processor.run_pipeline(query="SELECT * FROM claims", table_name='claims')

# Get summary statistics
summary = processor.get_claims_summary()
print(summary)
```

## CI/CD Pipeline

The GitHub Actions workflow includes:

- **PR Trigger**: Runs on pull requests to main/develop branches
- **Multi-version Testing**: Tests on Python 3.10, 3.11, and 3.12
- **Linting**: flake8 code quality checks
- **Testing**: pytest with coverage reporting
- **Security Scanning**: Bandit and Safety checks
- **Build**: Package building for deployment

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run with linting
flake8 .
```

## Data Processing Features

- Column name standardization
- Date format conversion
- Data validation (missing fields, invalid amounts)
- Duplicate removal
- Status normalization
- Derived columns (year, month, processing_date)

## Requirements

- Python 3.10+
- pandas
- pytest
- pytest-cov
- flake8
- pyodbc
- sqlalchemy
