"""
Base data source class for the Feature Store
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd


class DataSource(ABC):
    """
    Abstract base class for all data sources.
    Data sources provide the interface for reading feature data.
    """

    def __init__(
        self,
        name: str,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ):
        self.name = name
        self.timestamp_field = timestamp_field
        self.created_timestamp_column = created_timestamp_column
        self.description = description
        self.tags = tags or {}

    @abstractmethod
    def read(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read data from the source"""
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the data source"""
        pass

    @abstractmethod
    def validate(self) -> None:
        """Validate the data source configuration"""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert data source to dictionary for serialization"""
        return {
            "name": self.name,
            "timestamp_field": self.timestamp_field,
            "created_timestamp_column": self.created_timestamp_column,
            "description": self.description,
            "tags": self.tags,
        }

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"

    def __repr__(self) -> str:
        return self.__str__()
