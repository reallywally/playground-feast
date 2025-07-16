"""
FeatureView class for the Feature Store
"""

from typing import List, Optional, Dict, Any
from datetime import timedelta
from dataclasses import dataclass, field
from .entity import Entity
from .feature import Feature
from ..data_sources.base import DataSource


@dataclass
class FeatureView:
    """
    A feature view is a logical group of features that share the same data source and entities.
    Feature views are used to define the schema and properties of features.
    """

    name: str
    entities: List[Entity]
    features: List[Feature]
    source: DataSource
    ttl: Optional[timedelta] = None
    online: bool = True
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}

    def __str__(self) -> str:
        return f"FeatureView(name='{self.name}', entities={len(self.entities)}, features={len(self.features)})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_feature_names(self) -> List[str]:
        """Get list of feature names"""
        return [feature.name for feature in self.features]

    def get_entity_names(self) -> List[str]:
        """Get list of entity names"""
        return [entity.name for entity in self.entities]

    def get_join_keys(self) -> List[str]:
        """Get all join keys from entities"""
        join_keys = []
        for entity in self.entities:
            join_keys.extend(entity.join_keys)
        return list(set(join_keys))  # Remove duplicates

    def get_feature_by_name(self, name: str) -> Optional[Feature]:
        """Get feature by name"""
        for feature in self.features:
            if feature.name == name:
                return feature
        return None

    def get_entity_by_name(self, name: str) -> Optional[Entity]:
        """Get entity by name"""
        for entity in self.entities:
            if entity.name == name:
                return entity
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature view to dictionary for serialization"""
        return {
            "name": self.name,
            "entities": [entity.to_dict() for entity in self.entities],
            "features": [feature.to_dict() for feature in self.features],
            "source": self.source.to_dict(),
            "ttl": self.ttl.total_seconds() if self.ttl else None,
            "online": self.online,
            "description": self.description,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeatureView":
        """Create feature view from dictionary"""
        from ..data_sources.file_source import FileSource

        entities = [Entity.from_dict(entity_data) for entity_data in data["entities"]]
        features = [
            Feature.from_dict(feature_data) for feature_data in data["features"]
        ]

        # Create data source based on type
        source_data = data["source"]
        if source_data["type"] == "file":
            source = FileSource.from_dict(source_data)
        else:
            raise ValueError(f"Unsupported source type: {source_data['type']}")

        ttl = timedelta(seconds=data["ttl"]) if data.get("ttl") else None

        return cls(
            name=data["name"],
            entities=entities,
            features=features,
            source=source,
            ttl=ttl,
            online=data.get("online", True),
            description=data.get("description"),
            tags=data.get("tags", {}),
        )

    def validate(self) -> None:
        """Validate feature view configuration"""
        if not self.name:
            raise ValueError("FeatureView name cannot be empty")
        if not self.entities:
            raise ValueError("FeatureView must have at least one entity")
        if not self.features:
            raise ValueError("FeatureView must have at least one feature")
        if not self.source:
            raise ValueError("FeatureView must have a data source")

        # Validate entities
        for entity in self.entities:
            entity.validate()

        # Validate features
        for feature in self.features:
            feature.validate()

        # Validate source
        self.source.validate()

        # Check for duplicate feature names
        feature_names = [feature.name for feature in self.features]
        if len(feature_names) != len(set(feature_names)):
            raise ValueError("Feature names must be unique within a FeatureView")

        # Check for duplicate entity names
        entity_names = [entity.name for entity in self.entities]
        if len(entity_names) != len(set(entity_names)):
            raise ValueError("Entity names must be unique within a FeatureView")
