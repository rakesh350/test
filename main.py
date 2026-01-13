"""
SAP EAM Data Validation Framework
Validates data from SAP Excel exports against multiple downstream databases
"""

import pandas as pd
import pyodbc
import cx_Oracle
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """Handles connections to different database types"""
    
    @staticmethod
    def connect_mssql(config: Dict) -> pyodbc.Connection:
        """Connect to MS SQL Server"""
        try:
            conn_str = (
                f"DRIVER={{{config['driver']}}};"
                f"SERVER={config['host']},{config.get('port', 1433)};"
                f"DATABASE={config['database']};"
                f"UID={config['username']};"
                f"PWD={config['password']};"
                f"TrustServerCertificate=yes;"
            )
            conn = pyodbc.connect(conn_str, timeout=30)
            logger.info(f"Connected to MS SQL: {config['host']}/{config['database']}")
            return conn
        except Exception as e:
            logger.error(f"MS SQL connection failed: {str(e)}")
            raise
    
    @staticmethod
    def connect_oracle(config: Dict) -> cx_Oracle.Connection:
        """Connect to Oracle Database"""
        try:
            dsn = cx_Oracle.makedsn(
                config['host'],
                config.get('port', 1521),
                service_name=config.get('service_name', config.get('sid'))
            )
            conn = cx_Oracle.connect(
                user=config['username'],
                password=config['password'],
                dsn=dsn,
                encoding="UTF-8"
            )
            logger.info(f"Connected to Oracle: {config['host']}/{config.get('service_name', config.get('sid'))}")
            return conn
        except Exception as e:
            logger.error(f"Oracle connection failed: {str(e)}")
            raise
    
    @staticmethod
    def get_connection(db_type: str, config: Dict):
        """Get database connection based on type"""
        if db_type.lower() == 'mssql':
            return DatabaseConnector.connect_mssql(config)
        elif db_type.lower() == 'oracle':
            return DatabaseConnector.connect_oracle(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


class DataValidator:
    """Main validation engine"""
    
    def __init__(self, config_path: str):
        """Initialize validator with configuration"""
        self.config = self._load_config(config_path)
        self.sap_data = None
        self.validation_results = []
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise
    
    def load_sap_data(self) -> pd.DataFrame:
        """Load SAP EAM Excel data"""
        try:
            excel_path = self.config['sap_excel']['file_path']
            sheet_name = self.config['sap_excel'].get('sheet_name', 0)
            
            self.sap_data = pd.read_excel(excel_path, sheet_name=sheet_name)
            
            # Clean column names
            self.sap_data.columns = self.sap_data.columns.str.strip()
            
            # Convert all data to string for comparison (handle NaN)
            self.sap_data = self.sap_data.fillna('')
            self.sap_data = self.sap_data.astype(str)
            
            logger.info(f"Loaded {len(self.sap_data)} records from SAP Excel")
            logger.info(f"SAP Columns: {list(self.sap_data.columns)}")
            return self.sap_data
            
        except Exception as e:
            logger.error(f"Failed to load SAP data: {str(e)}")
            raise
    
    def _build_query(self, system_config: Dict) -> str:
        """Build SQL query based on mapping configuration"""
        table = system_config['table_name']
        schema = system_config.get('schema', '')
        
        # Get column mappings
        mappings = system_config['column_mappings']
        db_columns = [m['target_column'] for m in mappings]
        
        # Build SELECT clause
        select_clause = ', '.join(db_columns)
        
        # Build full table name
        full_table = f"{schema}.{table}" if schema else table
        
        # Build WHERE clause if key columns specified
        where_clause = ""
        if 'key_columns' in system_config and system_config['key_columns']:
            where_clause = " WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in system_config['key_columns']])
        
        query = f"SELECT {select_clause} FROM {full_table}{where_clause}"
        logger.info(f"Generated query: {query}")
        return query
    
    def fetch_downstream_data(self, system_name: str, system_config: Dict) -> pd.DataFrame:
        """Fetch data from downstream system"""
        try:
            db_type = system_config['db_type']
            db_config = system_config['connection']
            
            # Get database connection
            conn = DatabaseConnector.get_connection(db_type, db_config)
            
            # Build and execute query
            query = self._build_query(system_config)
            df = pd.read_sql(query, conn)
            
            # Clean data
            df = df.fillna('')
            df = df.astype(str)
            
            conn.close()
            
            logger.info(f"Fetched {len(df)} records from {system_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch data from {system_name}: {str(e)}")
            raise
    
    def _create_comparison_key(self, row: pd.Series, key_columns: List[str]) -> str:
        """Create a unique key for comparison"""
        key_values = [str(row.get(col, '')).strip() for col in key_columns]
        return '|'.join(key_values)
    
    def validate_system(self, system_name: str, system_config: Dict) -> Dict:
        """Validate data for a single downstream system"""
        logger.info(f"Starting validation for {system_name}")
        
        try:
            # Fetch downstream data
            downstream_data = self.fetch_downstream_data(system_name, system_config)
            
            # Get key columns for matching
            key_columns_sap = system_config.get('key_columns', [])
            
            # Create mapping dictionary
            column_map = {m['source_column']: m['target_column'] 
                         for m in system_config['column_mappings']}
            
            # Prepare comparison
            matched_records = []
            mismatched_records = []
            missing_in_downstream = []
            extra_in_downstream = []
            
            # Create lookup dictionaries
            sap_dict = {}
            for _, row in self.sap_data.iterrows():
                key = self._create_comparison_key(row, key_columns_sap)
                sap_dict[key] = row
            
            downstream_dict = {}
            key_columns_downstream = [column_map[col] for col in key_columns_sap if col in column_map]
            for _, row in downstream_data.iterrows():
                key = self._create_comparison_key(row, key_columns_downstream)
                downstream_dict[key] = row
            
            # Compare records
            for key, sap_row in sap_dict.items():
                if key in downstream_dict:
                    downstream_row = downstream_dict[key]
                    mismatches = []
                    
                    # Compare each mapped column
                    for source_col, target_col in column_map.items():
                        sap_value = str(sap_row.get(source_col, '')).strip()
                        db_value = str(downstream_row.get(target_col, '')).strip()
                        
                        if sap_value != db_value:
                            mismatches.append({
                                'column': source_col,
                                'sap_value': sap_value,
                                'downstream_value': db_value
                            })
                    
                    if mismatches:
                        mismatched_records.append({
                            'key': key,
                            'mismatches': mismatches
                        })
                    else:
                        matched_records.append(key)
                else:
                    missing_in_downstream.append({
                        'key': key,
                        'sap_data': {col: sap_row.get(col, '') for col in key_columns_sap}
                    })
            
            # Find extra records in downstream
            for key in downstream_dict:
                if key not in sap_dict:
                    extra_in_downstream.append({
                        'key': key,
                        'downstream_data': {col: downstream_dict[key].get(col, '') 
                                          for col in key_columns_downstream}
                    })
            
            # Compile results
            result = {
                'system_name': system_name,
                'timestamp': datetime.now().isoformat(),
                'total_sap_records': len(self.sap_data),
                'total_downstream_records': len(downstream_data),
                'matched_count': len(matched_records),
                'mismatched_count': len(mismatched_records),
                'missing_in_downstream_count': len(missing_in_downstream),
                'extra_in_downstream_count': len(extra_in_downstream),
                'match_percentage': round((len(matched_records) / len(sap_dict) * 100) if sap_dict else 0, 2),
                'mismatched_records': mismatched_records[:100],  # Limit to first 100
                'missing_records': missing_in_downstream[:100],
                'extra_records': extra_in_downstream[:100]
            }
            
            logger.info(f"Validation completed for {system_name}: "
                       f"{result['matched_count']} matched, "
                       f"{result['mismatched_count']} mismatched, "
                       f"{result['missing_in_downstream_count']} missing")
            
            return result
            
        except Exception as e:
            logger.error(f"Validation failed for {system_name}: {str(e)}")
            return {
                'system_name': system_name,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run_validation(self):
        """Run validation for all configured systems"""
        logger.info("=" * 80)
        logger.info("Starting SAP EAM Data Validation")
        logger.info("=" * 80)
        
        # Load SAP data
        self.load_sap_data()
        
        # Validate each downstream system
        for system_name, system_config in self.config['downstream_systems'].items():
            result = self.validate_system(system_name, system_config)
            self.validation_results.append(result)
        
        # Generate reports
        self.generate_reports()
        
        logger.info("=" * 80)
        logger.info("Validation completed")
        logger.info("=" * 80)
    
    def generate_reports(self):
        """Generate validation reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_dir = Path("validation_reports")
        report_dir.mkdir(exist_ok=True)
        
        # Summary report
        summary_data = []
        for result in self.validation_results:
            if 'error' not in result:
                summary_data.append({
                    'System': result['system_name'],
                    'SAP Records': result['total_sap_records'],
                    'Downstream Records': result['total_downstream_records'],
                    'Matched': result['matched_count'],
                    'Mismatched': result['mismatched_count'],
                    'Missing in Downstream': result['missing_in_downstream_count'],
                    'Extra in Downstream': result['extra_in_downstream_count'],
                    'Match %': result['match_percentage']
                })
        
        summary_df = pd.DataFrame(summary_data)
        summary_file = report_dir / f"summary_report_{timestamp}.xlsx"
        
        with pd.ExcelWriter(summary_file, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Detailed sheets for each system
            for result in self.validation_results:
                if 'error' in result:
                    continue
                
                system_name = result['system_name'][:31]  # Excel sheet name limit
                
                # Mismatched records
                if result['mismatched_records']:
                    mismatch_data = []
                    for record in result['mismatched_records']:
                        for mismatch in record['mismatches']:
                            mismatch_data.append({
                                'Key': record['key'],
                                'Column': mismatch['column'],
                                'SAP Value': mismatch['sap_value'],
                                'Downstream Value': mismatch['downstream_value']
                            })
                    
                    if mismatch_data:
                        mismatch_df = pd.DataFrame(mismatch_data)
                        sheet_name = f"{system_name}_Mismatch"[:31]
                        mismatch_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"Reports generated: {summary_file}")
        
        # JSON report for programmatic access
        json_file = report_dir / f"detailed_report_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(self.validation_results, f, indent=2)
        
        logger.info(f"JSON report generated: {json_file}")


def main():
    """Main execution function"""
    try:
        # Initialize validator with config file
        validator = DataValidator('config.json')
        
        # Run validation
        validator.run_validation()
        
        logger.info("Process completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()