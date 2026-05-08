"""
Healthcare Claims ETL Processor
Extracts, transforms, and loads healthcare claims data from Hospital DB to Azure SQL.
"""

import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClaimsProcessor:
    """Processes healthcare claims data through ETL pipeline."""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the claims processor with configuration."""
        self.config = config or {}
        self.raw_data = None
        self.transformed_data = None
    
    def extract_from_hospital_db(self, query: str) -> pd.DataFrame:
        """
        Extract claims data from hospital database.
        
        Args:
            query: SQL query to extract claims data
            
        Returns:
            pd.DataFrame: Raw claims data
        """
        logger.info(f"Extracting data with query: {query}")
        # Simulate database extraction
        # In production: use pyodbc or sqlalchemy to connect to hospital DB
        sample_data = {
            'claim_id': ['CLM001', 'CLM002', 'CLM003', 'CLM004', 'CLM005'],
            'patient_id': ['PAT001', 'PAT002', 'PAT003', 'PAT004', 'PAT005'],
            'provider_id': ['PRV001', 'PRV002', 'PRV001', 'PRV003', 'PRV002'],
            'service_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],
            'service_code': ['99213', '99214', '99213', '99215', '99214'],
            'amount': [150.00, 200.00, 150.00, 250.00, 200.00],
            'diagnosis_code': ['J06.9', 'J01.90', 'J06.9', 'I10', 'J01.90'],
            'status': ['pending', 'approved', 'pending', 'approved', 'pending']
        }
        df = pd.DataFrame(sample_data)
        self.raw_data = df
        logger.info(f"Extracted {len(df)} claims records")
        return df
    
    def transform_claims(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Transform claims data for Azure SQL loading.
        
        Args:
            df: Raw claims data (uses self.raw_data if None)
            
        Returns:
            pd.DataFrame: Transformed claims data
        """
        df = df or self.raw_data
        if df is None or df.empty:
            raise ValueError("No data to transform. Run extract first.")
        
        logger.info("Starting data transformation")
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Convert date to datetime
        df['service_date'] = pd.to_datetime(df['service_date'])
        
        # Add derived columns
        df['year'] = df['service_date'].dt.year
        df['month'] = df['service_date'].dt.month
        df['processing_date'] = datetime.now()
        
        # Validate data
        df = self._validate_claims(df)
        
        # Normalize status values
        df['status'] = df['status'].str.lower()
        
        # Sort by service date
        df = df.sort_values('service_date')
        
        self.transformed_data = df
        logger.info(f"Transformed {len(df)} claims records")
        return df
    
    def _validate_claims(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate claims data and remove invalid records.
        
        Args:
            df: Claims data to validate
            
        Returns:
            pd.DataFrame: Validated claims data
        """
        # Remove records with missing required fields
        required_fields = ['claim_id', 'patient_id', 'provider_id', 'service_date', 'amount']
        df = df.dropna(subset=required_fields)
        
        # Remove records with invalid amounts
        df = df[df['amount'] > 0]
        
        # Remove duplicate claims
        df = df.drop_duplicates(subset=['claim_id'])
        
        logger.info(f"Validation: {len(df)} valid records after filtering")
        return df
    
    def load_to_azure_sql(self, df: Optional[pd.DataFrame] = None, table_name: str = 'claims') -> bool:
        """
        Load transformed claims data to Azure SQL.
        
        Args:
            df: Transformed claims data (uses self.transformed_data if None)
            table_name: Target table name in Azure SQL
            
        Returns:
            bool: True if load successful, False otherwise
        """
        df = df or self.transformed_data
        if df is None:
            raise ValueError("No data to load. Run transform first.")
        
        logger.info(f"Loading {len(df)} records to Azure SQL table: {table_name}")
        
        # Simulate Azure SQL load
        # In production: use pyodbc or sqlalchemy to connect to Azure SQL
        # Example:
        # import pyodbc
        # conn = pyodbc.connect(self.config['azure_connection_string'])
        # for index, row in df.iterrows():
        #     cursor.execute(f"INSERT INTO {table_name} VALUES (...)")
        
        logger.info("Data loaded successfully to Azure SQL")
        return True
    
    def run_pipeline(self, query: str, table_name: str = 'claims') -> pd.DataFrame:
        """
        Run the complete ETL pipeline.
        
        Args:
            query: SQL query for extraction
            table_name: Target table name for loading
            
        Returns:
            pd.DataFrame: Final transformed data
        """
        logger.info("Starting ETL pipeline")
        
        # Extract
        self.extract_from_hospital_db(query)
        
        # Transform
        self.transform_claims()
        
        # Load
        self.load_to_azure_sql(table_name=table_name)
        
        logger.info("ETL pipeline completed successfully")
        return self.transformed_data
    
    def get_claims_summary(self) -> Dict:
        """
        Get summary statistics of processed claims.
        
        Returns:
            Dict: Summary statistics
        """
        if self.transformed_data is None:
            raise ValueError("No transformed data available. Run pipeline first.")
        
        df = self.transformed_data
        
        summary = {
            'total_claims': len(df),
            'total_amount': df['amount'].sum(),
            'average_amount': df['amount'].mean(),
            'unique_patients': df['patient_id'].nunique(),
            'unique_providers': df['provider_id'].nunique(),
            'status_distribution': df['status'].value_counts().to_dict(),
            'date_range': {
                'start': df['service_date'].min().strftime('%Y-%m-%d'),
                'end': df['service_date'].max().strftime('%Y-%m-%d')
            }
        }
        
        return summary
