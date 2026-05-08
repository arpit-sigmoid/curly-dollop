import pytest
import pandas as pd
from datetime import datetime
from etl.claims_processor import ClaimsProcessor


@pytest.fixture
def processor():
    """Create a ClaimsProcessor instance for testing."""
    return ClaimsProcessor()


@pytest.fixture
def sample_claims_data():
    """Sample claims data for testing."""
    return pd.DataFrame({
        'claim_id': ['CLM001', 'CLM002', 'CLM003'],
        'patient_id': ['PAT001', 'PAT002', 'PAT003'],
        'provider_id': ['PRV001', 'PRV002', 'PRV001'],
        'service_date': ['2024-01-15', '2024-01-16', '2024-01-17'],
        'service_code': ['99213', '99214', '99213'],
        'amount': [150.00, 200.00, 150.00],
        'diagnosis_code': ['J06.9', 'J01.90', 'J06.9'],
        'status': ['pending', 'approved', 'pending']
    })


# Positive Tests
def test_processor_initialization():
    """Test that ClaimsProcessor initializes correctly."""
    processor = ClaimsProcessor()
    assert processor.config == {}
    assert processor.raw_data is None
    assert processor.transformed_data is None


def test_processor_initialization_with_config():
    """Test that ClaimsProcessor initializes with custom config."""
    config = {'azure_connection_string': 'test_connection'}
    processor = ClaimsProcessor(config=config)
    assert processor.config == config


def test_extract_from_hospital_db(processor):
    """Test data extraction from hospital database."""
    query = "SELECT * FROM claims"
    df = processor.extract_from_hospital_db(query)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert 'claim_id' in df.columns
    assert 'patient_id' in df.columns
    assert processor.raw_data is not None


def test_transform_claims(processor):
    """Test claims data transformation."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    transformed_df = processor.transform_claims()
    
    assert isinstance(transformed_df, pd.DataFrame)
    assert 'year' in transformed_df.columns
    assert 'month' in transformed_df.columns
    assert 'processing_date' in transformed_df.columns
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['service_date'])


def test_transform_claims_standardizes_columns(processor):
    """Test that column names are standardized."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    transformed_df = processor.transform_claims()
    
    columns = transformed_df.columns.tolist()
    assert all(col == col.lower().replace(' ', '_') for col in columns)


def test_load_to_azure_sql(processor):
    """Test loading data to Azure SQL."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    processor.transform_claims()
    result = processor.load_to_azure_sql()
    
    assert result is True


def test_run_pipeline(processor):
    """Test complete ETL pipeline execution."""
    result = processor.run_pipeline("SELECT * FROM claims")
    
    assert isinstance(result, pd.DataFrame)
    assert processor.transformed_data is not None
    assert len(result) > 0


def test_get_claims_summary(processor):
    """Test getting claims summary statistics."""
    processor.run_pipeline("SELECT * FROM claims")
    summary = processor.get_claims_summary()
    
    assert isinstance(summary, dict)
    assert 'total_claims' in summary
    assert 'total_amount' in summary
    assert 'average_amount' in summary
    assert 'unique_patients' in summary
    assert 'unique_providers' in summary
    assert 'status_distribution' in summary
    assert 'date_range' in summary


# Null Handling Tests
def test_transform_without_extract(processor):
    """Test that transform raises error without prior extract."""
    with pytest.raises(ValueError, match="No data to transform"):
        processor.transform_claims()


def test_load_without_transform(processor):
    """Test that load raises error without prior transform."""
    with pytest.raises(ValueError, match="No data to load"):
        processor.load_to_azure_sql()


def test_get_summary_without_pipeline(processor):
    """Test that get_claims_summary raises error without running pipeline."""
    with pytest.raises(ValueError, match="No transformed data available"):
        processor.get_claims_summary()


# Edge Cases
def test_transform_removes_missing_required_fields(processor):
    """Test that records with missing required fields are removed."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    processor.raw_data.loc[0, 'claim_id'] = None
    processor.raw_data.loc[1, 'patient_id'] = None
    
    transformed_df = processor.transform_claims()
    
    assert None not in transformed_df['claim_id'].values
    assert None not in transformed_df['patient_id'].values


def test_transform_removes_invalid_amounts(processor):
    """Test that records with invalid amounts are removed."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    processor.raw_data.loc[0, 'amount'] = -50.00
    processor.raw_data.loc[1, 'amount'] = 0.00
    
    transformed_df = processor.transform_claims()
    
    assert all(transformed_df['amount'] > 0)


def test_transform_removes_duplicate_claims(processor):
    """Test that duplicate claims are removed."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    # Add duplicate claim
    duplicate_row = processor.raw_data.iloc[0].copy()
    processor.raw_data = pd.concat([processor.raw_data, duplicate_row.to_frame().T], ignore_index=True)
    
    transformed_df = processor.transform_claims()
    
    assert len(transformed_df) == 5  # Original 5 records, 1 duplicate removed


def test_transform_normalizes_status(processor):
    """Test that status values are normalized to lowercase."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    processor.raw_data['status'] = ['PENDING', 'APPROVED', 'Pending', 'Approved', 'pending']
    
    transformed_df = processor.transform_claims()
    
    assert all(transformed_df['status'] == transformed_df['status'].str.lower())


def test_transform_sorts_by_date(processor):
    """Test that data is sorted by service date."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    transformed_df = processor.transform_claims()
    
    assert transformed_df['service_date'].is_monotonic_increasing


def test_empty_dataset_handling(processor):
    """Test handling of empty dataset."""
    processor.raw_data = pd.DataFrame()
    
    with pytest.raises(ValueError):
        processor.transform_claims()


def test_single_claim_processing(processor):
    """Test processing of a single claim."""
    processor.raw_data = pd.DataFrame({
        'claim_id': ['CLM001'],
        'patient_id': ['PAT001'],
        'provider_id': ['PRV001'],
        'service_date': ['2024-01-15'],
        'service_code': ['99213'],
        'amount': [150.00],
        'diagnosis_code': ['J06.9'],
        'status': ['pending']
    })
    
    transformed_df = processor.transform_claims()
    
    assert len(transformed_df) == 1
    assert transformed_df.iloc[0]['claim_id'] == 'CLM001'


def test_large_dataset_processing(processor):
    """Test processing of large dataset."""
    # Create large dataset
    large_data = {
        'claim_id': [f'CLM{i:06d}' for i in range(10000)],
        'patient_id': [f'PAT{i%1000:04d}' for i in range(10000)],
        'provider_id': [f'PRV{i%100:04d}' for i in range(10000)],
        'service_date': ['2024-01-15'] * 10000,
        'service_code': ['99213'] * 10000,
        'amount': [150.00] * 10000,
        'diagnosis_code': ['J06.9'] * 10000,
        'status': ['pending'] * 10000
    }
    processor.raw_data = pd.DataFrame(large_data)
    
    transformed_df = processor.transform_claims()
    
    assert len(transformed_df) == 10000


# Data Type Tests
def test_preserves_numeric_types(processor):
    """Test that numeric types are preserved."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    transformed_df = processor.transform_claims()
    
    assert pd.api.types.is_numeric_dtype(transformed_df['amount'])
    assert pd.api.types.is_numeric_dtype(transformed_df['year'])
    assert pd.api.types.is_numeric_dtype(transformed_df['month'])


def test_preserves_datetime_types(processor):
    """Test that datetime types are preserved."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    transformed_df = processor.transform_claims()
    
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['service_date'])
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['processing_date'])


def test_custom_table_name(processor):
    """Test loading to custom table name."""
    processor.extract_from_hospital_db("SELECT * FROM claims")
    processor.transform_claims()
    result = processor.load_to_azure_sql(table_name='custom_claims_table')
    
    assert result is True


# Integration Tests
def test_full_pipeline_integration(processor):
    """Test complete pipeline from extract to summary."""
    processor.run_pipeline("SELECT * FROM claims", table_name='claims')
    summary = processor.get_claims_summary()
    
    assert summary['total_claims'] == 5
    assert summary['total_amount'] == 950.0  # Sum of sample data
    assert summary['unique_patients'] == 5
    assert summary['unique_providers'] == 3
