"""
Data source abstraction and Excel loading.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pathlib import Path
import pandas as pd
from config import DataSourceConfig


class DataSourceLoader(ABC):
    """Abstract base for data source loaders."""
    
    @abstractmethod
    def load(self, source_path: Path, config: DataSourceConfig) -> pd.DataFrame:
        """Load data from source and return DataFrame."""
        pass


class ExcelSourceLoader(DataSourceLoader):
    """Load data sources from Excel file."""
    
    def load_all_sheets(self, file_path: Path) -> Dict[str, pd.DataFrame]:
        """Load all sheets from Excel file."""
        print(f"\n[IO DEBUG] Loading Excel file: {file_path}")
        sheets = pd.read_excel(file_path, sheet_name=None)
        print(f"[IO DEBUG] Found {len(sheets)} sheets: {list(sheets.keys())}")
        for sheet_name, df in sheets.items():
            print(f"[IO DEBUG]   Sheet '{sheet_name}': {df.shape}, columns: {df.columns.tolist()[:10]}...")  # First 10 columns
        return sheets
    
    def detect_sheet(self, sheets: Dict[str, pd.DataFrame], config: DataSourceConfig) -> Optional[str]:
        """
        Detect which sheet matches a data source config.
        
        First tries to match by keywords in sheet name.
        Then validates by required columns.
        """
        # Try keyword matching first
        for sheet_name in sheets.keys():
            sheet_lower = sheet_name.lower()
            if any(keyword.lower() in sheet_lower for keyword in config.detection_keywords):
                # Validate columns
                columns = sheets[sheet_name].columns.tolist()
                is_valid, _ = config.column_mapping.validate(columns)
                if is_valid:
                    return sheet_name
        
        # Fall back to column validation only
        for sheet_name, df in sheets.items():
            columns = df.columns.tolist()
            is_valid, _ = config.column_mapping.validate(columns)
            if is_valid:
                return sheet_name
        
        return None
    
    def load(self, source_path: Path, config: DataSourceConfig) -> pd.DataFrame:
        """Load specific data source from Excel."""
        sheets = self.load_all_sheets(source_path)
        sheet_name = self.detect_sheet(sheets, config)
        
        if sheet_name is None:
            print(f"[IO ERROR] Could not detect sheet for '{config.name}'")
            print(f"[IO ERROR] Looking for keywords: {config.detection_keywords}")
            print(f"[IO ERROR] Required columns: {config.column_mapping.required_columns}")
            print(f"[IO ERROR] Available sheets and their columns:")
            for sn, df in sheets.items():
                print(f"[IO ERROR]   Sheet '{sn}': {df.columns.tolist()}")
            raise ValueError(
                f"Could not detect sheet for {config.name}. "
                f"Required columns: {config.column_mapping.required_columns}"
            )
        
        print(f"[IO DEBUG] Detected sheet '{sheet_name}' for config '{config.name}'")
        df = sheets[sheet_name]
        
        # Validate required columns
        is_valid, missing = config.column_mapping.validate(df.columns.tolist())
        if not is_valid:
            print(f"[IO ERROR] Missing columns in sheet '{sheet_name}': {missing}")
            print(f"[IO ERROR] Available columns: {df.columns.tolist()}")
            raise ValueError(f"Missing required columns for {config.name}: {missing}")
        
        print(f"[IO DEBUG] Successfully loaded {len(df)} rows from sheet '{sheet_name}'")
        return df


def load_excel_sources(file_path: Path, ar_config: DataSourceConfig, 
                       scheduled_config: DataSourceConfig) -> Dict[str, pd.DataFrame]:
    """
    Load all data sources from Excel file.
    
    Returns dict with keys: 'ar_transactions', 'scheduled_charges'
    """
    loader = ExcelSourceLoader()
    
    return {
        ar_config.name: loader.load(file_path, ar_config),
        scheduled_config.name: loader.load(file_path, scheduled_config)
    }
