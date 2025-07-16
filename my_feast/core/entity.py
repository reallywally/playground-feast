"""
Entity class for the Feature Store
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from .types import ValueType


@dataclass
class Entity:
    """
    An entity is a primary key used to fetch features.
    Entities are used to join features across different feature views.
    """

    name: str
    value_type: ValueType = ValueType.STRING
    join_keys: Optional[List[str]] = None
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.join_keys is None:
            self.join_keys = [self.name]
        if self.tags is None:
            self.tags = {}

    def __str__(self) -> str:
        return f"Entity(name='{self.name}', value_type={self.value_type.value}, join_keys={self.join_keys})"

    def __repr__(self) -> str:
        return self.__str__()

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary for serialization"""
        return {
            "name": self.name,
            "value_type": self.value_type.value,
            "join_keys": self.join_keys,
            "description": self.description,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Entity":
        """Create entity from dictionary"""
        return cls(
            name=data["name"],
            value_type=ValueType(data["value_type"]),
            join_keys=data["join_keys"],
            description=data.get("description"),
            tags=data.get("tags", {}),
        )

    def validate(self) -> None:
        """Validate entity configuration"""
        if not self.name:
            raise ValueError("Entity name cannot be empty")
        if not self.join_keys:
            raise ValueError("Entity must have at least one join key")
        if not all(isinstance(key, str) for key in self.join_keys):
            raise ValueError("All join keys must be strings")
