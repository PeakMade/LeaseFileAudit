"""
Data source abstraction and Excel loading.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pathlib import Path
import pandas as pd
import re
from config import DataSourceConfig


class DataSourceLoader(ABC):
    """Abstract base for data source loaders."""
    
    @abstractmethod
    def load(self, source_path: Path, config: DataSourceConfig) -> pd.DataFrame:
        """Load data from source and return DataFrame."""
        pass


class ExcelSourceLoader(DataSourceLoader):
    """Load data sources from Excel file."""

    @staticmethod
    def _normalize_sheet_name(sheet_name: str) -> str:
        """Normalize sheet names for resilient keyword matching."""
        normalized = re.sub(r'[^a-z0-9]+', ' ', str(sheet_name).lower()).strip()
        return re.sub(r'\s+', ' ', normalized)

    @classmethod
    def _keyword_score(cls, sheet_name: str, keywords: list[str]) -> int:
        """Return keyword match score for a sheet name."""
        normalized_name = cls._normalize_sheet_name(sheet_name)
        compact_name = normalized_name.replace(' ', '')
        score = 0

        for keyword in keywords:
            normalized_keyword = cls._normalize_sheet_name(keyword)
            compact_keyword = normalized_keyword.replace(' ', '')
            if normalized_keyword and normalized_keyword in normalized_name:
                score += 2
            elif compact_keyword and compact_keyword in compact_name:
                score += 1

        return score
    
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
        
        Evaluates all sheets that satisfy required columns, then chooses the
        best candidate by keyword score and row count.
        """
        candidates = []

        for sheet_name, df in sheets.items():
            columns = df.columns.tolist()
            is_valid, _ = config.column_mapping.validate(columns)
            if not is_valid:
                continue

            keyword_score = self._keyword_score(sheet_name, config.detection_keywords)
            row_count = len(df)
            candidates.append((keyword_score, row_count, sheet_name))

        if not candidates:
            return None

        # Highest keyword score first, then largest row count.
        candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
        selected = candidates[0][2]

        print(
            f"[IO DEBUG] Sheet candidates for '{config.name}': "
            f"{[(name, score, rows) for score, rows, name in candidates]}"
        )
        print(f"[IO DEBUG] Selected best sheet '{selected}' for config '{config.name}'")

        return selected
    
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
