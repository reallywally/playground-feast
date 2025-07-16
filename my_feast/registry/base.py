"""
Base registry class for the Feature Store
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from ..core.entity import Entity
from ..core.feature_view import FeatureView
from ..core.feature_service import FeatureService


class Registry(ABC):
    """
    Abstract base class for all registry implementations.
    Registry stores and manages feature metadata.
    """

    def __init__(self):
        pass

    @abstractmethod
    def apply_entity(self, entity: Entity) -> None:
        """Apply an entity to the registry"""
        pass

    @abstractmethod
    def apply_feature_view(self, feature_view: FeatureView) -> None:
        """Apply a feature view to the registry"""
        pass

    @abstractmethod
    def apply_feature_service(self, feature_service: FeatureService) -> None:
        """Apply a feature service to the registry"""
        pass

    @abstractmethod
    def get_entity(self, name: str) -> Optional[Entity]:
        """Get an entity by name"""
        pass

    @abstractmethod
    def get_feature_view(self, name: str) -> Optional[FeatureView]:
        """Get a feature view by name"""
        pass

    @abstractmethod
    def get_feature_service(self, name: str) -> Optional[FeatureService]:
        """Get a feature service by name"""
        pass

    @abstractmethod
    def list_entities(self) -> List[Entity]:
        """List all entities"""
        pass

    @abstractmethod
    def list_feature_views(self) -> List[FeatureView]:
        """List all feature views"""
        pass

    @abstractmethod
    def list_feature_services(self) -> List[FeatureService]:
        """List all feature services"""
        pass

    @abstractmethod
    def delete_entity(self, name: str) -> None:
        """Delete an entity by name"""
        pass

    @abstractmethod
    def delete_feature_view(self, name: str) -> None:
        """Delete a feature view by name"""
        pass

    @abstractmethod
    def delete_feature_service(self, name: str) -> None:
        """Delete a feature service by name"""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Clean up registry resources"""
        pass

    def apply_objects(self, objects: List[Any]) -> None:
        """Apply multiple objects to the registry"""
        for obj in objects:
            if isinstance(obj, Entity):
                self.apply_entity(obj)
            elif isinstance(obj, FeatureView):
                self.apply_feature_view(obj)
            elif isinstance(obj, FeatureService):
                self.apply_feature_service(obj)
            else:
                raise ValueError(f"Unsupported object type: {type(obj)}")

    def get_all_objects(self) -> Dict[str, List[Any]]:
        """Get all objects from the registry"""
        return {
            "entities": self.list_entities(),
            "feature_views": self.list_feature_views(),
            "feature_services": self.list_feature_services(),
        }
