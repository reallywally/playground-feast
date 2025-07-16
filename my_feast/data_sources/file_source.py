"""
File-based data source for the Feature Store
"""

import os
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from .base import DataSource


class FileSource(DataSource):
    """
    A data source that reads from files (Parquet, CSV, JSON).
    """

    def __init__(
        self,
        name: str,
        path: str,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        file_format: str = "parquet",
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        super().__init__(
            name, timestamp_field, created_timestamp_column, description, tags
        )
        self.path = path
        self.file_format = file_format.lower()

    def read(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read data from the file"""
        if self.file_format == "parquet":
            df = pd.read_parquet(self.path)
        elif self.file_format == "csv":
            df = pd.read_csv(self.path)
        elif self.file_format == "json":
            df = pd.read_json(self.path)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

        # Filter by columns if specified
        if columns:
            available_columns = [col for col in columns if col in df.columns]
            if available_columns:
                df = df[available_columns]

        # Filter by date range if specified
        if start_date or end_date:
            if not self.timestamp_field:
                raise ValueError("timestamp_field must be specified for date filtering")
            if self.timestamp_field not in df.columns:
                raise ValueError(
                    f"timestamp_field '{self.timestamp_field}' not found in data"
                )

            # Convert timestamp column to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_field]):
                df[self.timestamp_field] = pd.to_datetime(df[self.timestamp_field])

            if start_date:
                df = df[df[self.timestamp_field] >= start_date]
            if end_date:
                df = df[df[self.timestamp_field] <= end_date]

        return df

    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the data source"""
        if self.file_format == "parquet":
            # Use pyarrow for more efficient schema reading
            try:
                parquet_file = pq.ParquetFile(self.path)
                schema = parquet_file.schema.to_arrow_schema()
                return {field.name: str(field.type) for field in schema}
            except Exception:
                # Fallback to pandas
                df = pd.read_parquet(self.path, nrows=1)
                return {col: str(dtype) for col, dtype in df.dtypes.items()}
        else:
            # For CSV and JSON, read a small sample
            if self.file_format == "csv":
                df = pd.read_csv(self.path, nrows=1)
            elif self.file_format == "json":
                df = pd.read_json(self.path, nrows=1)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            return {col: str(dtype) for col, dtype in df.dtypes.items()}

    def validate(self) -> None:
        """Validate the data source configuration"""
        if not self.path:
            raise ValueError("File path cannot be empty")
        if not os.path.exists(self.path):
            raise ValueError(f"File does not exist: {self.path}")
        if self.file_format not in ["parquet", "csv", "json"]:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert file source to dictionary for serialization"""
        data = super().to_dict()
        data.update(
            {"path": self.path, "file_format": self.file_format, "type": "file"}
        )
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileSource":
        """Create file source from dictionary"""
        return cls(
            name=data["name"],
            path=data["path"],
            timestamp_field=data.get("timestamp_field"),
            created_timestamp_column=data.get("created_timestamp_column"),
            file_format=data.get("file_format", "parquet"),
            description=data.get("description"),
            tags=data.get("tags", {}),
        )
