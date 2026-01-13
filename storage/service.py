"""
Storage service for audit run persistence.
"""
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd


class StorageService:
    """
    Manage audit run persistence to disk.
    
    Structure:
    instance/runs/<run_id>/
        inputs_normalized/
            expected_detail.parquet
            actual_detail.parquet
        outputs/
            bucket_results.parquet
            findings.parquet
        run_meta.json
    """
    
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def create_run_dir(self, run_id: str) -> Path:
        """Create directory structure for a new run."""
        run_dir = self.base_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        
        (run_dir / "inputs_normalized").mkdir(exist_ok=True)
        (run_dir / "outputs").mkdir(exist_ok=True)
        
        return run_dir
    
    def save_run(
        self,
        run_id: str,
        expected_detail: pd.DataFrame,
        actual_detail: pd.DataFrame,
        bucket_results: pd.DataFrame,
        findings: pd.DataFrame,
        metadata: Dict[str, Any]
    ):
        """Save complete audit run to disk."""
        run_dir = self.create_run_dir(run_id)
        
        # Save inputs (using CSV instead of Parquet)
        expected_detail.to_csv(run_dir / "inputs_normalized" / "expected_detail.csv", index=False)
        actual_detail.to_csv(run_dir / "inputs_normalized" / "actual_detail.csv", index=False)
        
        # Save outputs (using CSV instead of Parquet)
        bucket_results.to_csv(run_dir / "outputs" / "bucket_results.csv", index=False)
        findings.to_csv(run_dir / "outputs" / "findings.csv", index=False)
        
        # Save metadata
        with open(run_dir / "run_meta.json", "w") as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def load_run(self, run_id: str) -> Dict[str, Any]:
        """Load complete audit run from disk."""
        run_dir = self.base_dir / run_id
        
        if not run_dir.exists():
            raise ValueError(f"Run {run_id} not found")
        
        # Load CSVs with proper date parsing
        expected_detail = pd.read_csv(run_dir / "inputs_normalized" / "expected_detail.csv")
        actual_detail = pd.read_csv(run_dir / "inputs_normalized" / "actual_detail.csv")
        bucket_results = pd.read_csv(run_dir / "outputs" / "bucket_results.csv")
        findings = pd.read_csv(run_dir / "outputs" / "findings.csv")
        
        # Convert date columns to datetime
        date_columns = ['AUDIT_MONTH', 'PERIOD_START', 'PERIOD_END', 'POST_DATE', 'audit_month']
        for df in [expected_detail, actual_detail, bucket_results, findings]:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return {
            "expected_detail": expected_detail,
            "actual_detail": actual_detail,
            "bucket_results": bucket_results,
            "findings": findings,
            "metadata": self.load_metadata(run_id)
        }
    
    def load_metadata(self, run_id: str) -> Dict[str, Any]:
        """Load run metadata."""
        meta_path = self.base_dir / run_id / "run_meta.json"
        with open(meta_path, "r") as f:
            return json.load(f)
    
    def list_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent audit runs."""
        runs = []
        
        if not self.base_dir.exists():
            return runs
        
        for run_dir in sorted(self.base_dir.iterdir(), reverse=True):
            if run_dir.is_dir():
                meta_path = run_dir / "run_meta.json"
                if meta_path.exists():
                    with open(meta_path, "r") as f:
                        meta = json.load(f)
                        meta["run_id"] = run_dir.name
                        runs.append(meta)
                
                if len(runs) >= limit:
                    break
        
        return runs
    
    def get_run_exists(self, run_id: str) -> bool:
        """Check if run exists."""
        return (self.base_dir / run_id).exists()
    
    @staticmethod
    def calculate_file_hash(file_path: Path) -> str:
        """Calculate SHA256 hash of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    @staticmethod
    def generate_run_id() -> str:
        """Generate unique run ID."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"run_{timestamp}"
    
    def create_metadata(
        self,
        run_id: str,
        file_path: Path,
        config_version: str = "v1"
    ) -> Dict[str, Any]:
        """Create run metadata."""
        return {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "config_version": config_version,
            "file_name": file_path.name,
            "file_hash": self.calculate_file_hash(file_path),
            "file_size": file_path.stat().st_size
        }
