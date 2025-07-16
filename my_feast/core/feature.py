"""
Feature class for the Feature Store
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from .types import ValueType


@dataclass
class Feature:
    """
    A feature is a named attribute of an entity.
    Features are used to define the schema of a feature view.
    """

    name: str
    dtype: ValueType
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}

    def __str__(self) -> str:
        return f"Feature(name='{self.name}', dtype={self.dtype.value})"

    def __repr__(self) -> str:
        return self.__str__()

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature to dictionary for serialization"""
        return {
            "name": self.name,
            "dtype": self.dtype.value,
            "description": self.description,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Feature":
        """Create feature from dictionary"""
        return cls(
            name=data["name"],
            dtype=ValueType(data["dtype"]),
            description=data.get("description"),
            tags=data.get("tags", {}),
        )

    def validate(self) -> None:
        """Validate feature configuration"""
        if not self.name:
            raise ValueError("Feature name cannot be empty")
        if not isinstance(self.dtype, ValueType):
            raise ValueError("Feature dtype must be a ValueType")
        if not self.name.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                "Feature name must contain only alphanumeric characters, hyphens, and underscores"
            )
