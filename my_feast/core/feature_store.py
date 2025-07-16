"""
FeatureStore class - Main API for the Feature Store
"""

import os
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import pandas as pd
from .entity import Entity
from .feature_view import FeatureView
from .feature_service import FeatureService
from .types import FeatureReference
from ..registry.sqlite_registry import SQLiteRegistry
from ..online_store.memory_store import MemoryOnlineStore
from ..offline_store.parquet_store import ParquetOfflineStore
from ..config.config import FeatureStoreConfig


class FeatureStore:
    """
    Main Feature Store class that provides the primary API for feature operations.
    """

    def __init__(
        self,
        repo_path: Optional[str] = None,
        config: Optional[FeatureStoreConfig] = None,
    ):
        """
        Initialize the Feature Store.

        Args:
            repo_path: Path to the feature repository
            config: Feature store configuration
        """
        self.repo_path = repo_path or "."

        # Load configuration
        if config:
            self.config = config
        else:
            config_path = os.path.join(self.repo_path, "feature_store.yaml")
            if os.path.exists(config_path):
                self.config = FeatureStoreConfig.from_yaml(config_path)
            else:
                self.config = FeatureStoreConfig()

        # Initialize components
        self._init_registry()
        self._init_online_store()
        self._init_offline_store()

    def _init_registry(self):
        """Initialize the registry"""
        registry_path = os.path.join(self.repo_path, self.config.registry_path)
        self.registry = SQLiteRegistry(registry_path)

    def _init_online_store(self):
        """Initialize the online store"""
        if self.config.online_store_type == "memory":
            self.online_store = MemoryOnlineStore()
        else:
            raise ValueError(
                f"Unsupported online store type: {self.config.online_store_type}"
            )

    def _init_offline_store(self):
        """Initialize the offline store"""
        if self.config.offline_store_type == "parquet":
            self.offline_store = ParquetOfflineStore()
        else:
            raise ValueError(
                f"Unsupported offline store type: {self.config.offline_store_type}"
            )

    def apply(self, objects: List[Union[Entity, FeatureView, FeatureService]]) -> None:
        """
        Apply entities, feature views, and feature services to the registry.

        Args:
            objects: List of objects to apply
        """
        for obj in objects:
            if isinstance(obj, Entity):
                self.registry.apply_entity(obj)
            elif isinstance(obj, FeatureView):
                self.registry.apply_feature_view(obj)
            elif isinstance(obj, FeatureService):
                self.registry.apply_feature_service(obj)
            else:
                raise ValueError(f"Unsupported object type: {type(obj)}")

    def get_online_features(
        self,
        features: List[str],
        entity_rows: List[Dict[str, Any]],
        feature_service: Optional[FeatureService] = None,
    ) -> pd.DataFrame:
        """
        Get online features for real-time inference.

        Args:
            features: List of feature references in format "feature_view:feature_name"
            entity_rows: List of entity key-value pairs
            feature_service: Optional feature service to use

        Returns:
            DataFrame with entity keys and feature values
        """
        if feature_service:
            # Use feature service to get features
            feature_refs = feature_service.get_feature_references()
        else:
            # Parse feature references from strings
            feature_refs = []
            for feature_str in features:
                if ":" in feature_str:
                    fv_name, f_name = feature_str.split(":", 1)
                    feature_refs.append(
                        FeatureReference(feature_view_name=fv_name, feature_name=f_name)
                    )
                else:
                    raise ValueError(f"Invalid feature format: {feature_str}")

        # Group features by feature view
        fv_features = {}
        for ref in feature_refs:
            if ref.feature_view_name not in fv_features:
                fv_features[ref.feature_view_name] = []
            fv_features[ref.feature_view_name].append(ref.feature_name)

        # Get features from each feature view
        result_df = pd.DataFrame(entity_rows)

        for fv_name, feature_names in fv_features.items():
            # Get feature view from registry
            feature_view = self.registry.get_feature_view(fv_name)
            if not feature_view:
                raise ValueError(f"Feature view '{fv_name}' not found")

            # Read features from online store
            fv_df = self.online_store.read_features(
                feature_view=feature_view,
                entity_rows=entity_rows,
                feature_names=feature_names,
            )

            # Merge with result
            if not fv_df.empty:
                join_keys = feature_view.get_join_keys()
                result_df = result_df.merge(
                    fv_df[join_keys + feature_names], on=join_keys, how="left"
                )

        return result_df

    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        features: List[str],
        feature_service: Optional[FeatureService] = None,
    ) -> pd.DataFrame:
        """
        Get historical features for training or batch scoring.

        Args:
            entity_df: DataFrame with entity keys and timestamps
            features: List of feature references in format "feature_view:feature_name"
            feature_service: Optional feature service to use

        Returns:
            DataFrame with entity keys, timestamps, and feature values
        """
        if feature_service:
            # Use feature service to get features
            feature_refs = feature_service.get_feature_references()
        else:
            # Parse feature references from strings
            feature_refs = []
            for feature_str in features:
                if ":" in feature_str:
                    fv_name, f_name = feature_str.split(":", 1)
                    feature_refs.append(
                        FeatureReference(feature_view_name=fv_name, feature_name=f_name)
                    )
                else:
                    raise ValueError(f"Invalid feature format: {feature_str}")

        # Get unique feature view names
        fv_names = list(set(ref.feature_view_name for ref in feature_refs))

        # Get feature views from registry
        feature_views = []
        for fv_name in fv_names:
            fv = self.registry.get_feature_view(fv_name)
            if not fv:
                raise ValueError(f"Feature view '{fv_name}' not found")
            feature_views.append(fv)

        # Get historical features from offline store
        return self.offline_store.get_historical_features(
            entity_df=entity_df, feature_views=feature_views, feature_refs=feature_refs
        )

    def materialize_incremental(
        self, end_date: datetime, feature_views: Optional[List[str]] = None
    ) -> None:
        """
        Materialize incremental features to online store.

        Args:
            end_date: End date for materialization
            feature_views: List of feature view names to materialize (all if None)
        """
        # Get feature views to materialize
        if feature_views:
            fvs = []
            for fv_name in feature_views:
                fv = self.registry.get_feature_view(fv_name)
                if not fv:
                    raise ValueError(f"Feature view '{fv_name}' not found")
                fvs.append(fv)
        else:
            fvs = self.registry.list_feature_views()

        # Materialize each feature view
        for fv in fvs:
            if fv.online:  # Only materialize online feature views
                # Get latest features from offline store
                df = self.offline_store.pull_latest_from_table_or_query(
                    feature_view=fv, end_date=end_date
                )

                # Write to online store
                if not df.empty:
                    self.online_store.write_features(
                        feature_view=fv, df=df, timestamp=end_date
                    )

    def materialize(
        self,
        start_date: datetime,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        """
        Materialize features to online store for a date range.

        Args:
            start_date: Start date for materialization
            end_date: End date for materialization
            feature_views: List of feature view names to materialize (all if None)
        """
        # Get feature views to materialize
        if feature_views:
            fvs = []
            for fv_name in feature_views:
                fv = self.registry.get_feature_view(fv_name)
                if not fv:
                    raise ValueError(f"Feature view '{fv_name}' not found")
                fvs.append(fv)
        else:
            fvs = self.registry.list_feature_views()

        # Materialize each feature view
        for fv in fvs:
            if fv.online:  # Only materialize online feature views
                # Get features from offline store
                df = self.offline_store.pull_latest_from_table_or_query(
                    feature_view=fv, start_date=start_date, end_date=end_date
                )

                # Write to online store
                if not df.empty:
                    self.online_store.write_features(
                        feature_view=fv, df=df, timestamp=end_date
                    )

    def list_entities(self) -> List[Entity]:
        """List all entities in the registry"""
        return self.registry.list_entities()

    def list_feature_views(self) -> List[FeatureView]:
        """List all feature views in the registry"""
        return self.registry.list_feature_views()

    def list_feature_services(self) -> List[FeatureService]:
        """List all feature services in the registry"""
        return self.registry.list_feature_services()

    def get_entity(self, name: str) -> Optional[Entity]:
        """Get an entity by name"""
        return self.registry.get_entity(name)

    def get_feature_view(self, name: str) -> Optional[FeatureView]:
        """Get a feature view by name"""
        return self.registry.get_feature_view(name)

    def get_feature_service(self, name: str) -> Optional[FeatureService]:
        """Get a feature service by name"""
        return self.registry.get_feature_service(name)

    def teardown(self) -> None:
        """Clean up feature store resources"""
        self.registry.teardown()
        self.online_store.teardown()

    def get_metadata(self) -> Dict[str, Any]:
        """Get feature store metadata"""
        return {
            "repo_path": self.repo_path,
            "config": self.config.to_dict(),
            "registry": self.registry.get_metadata(),
            "online_store": self.online_store.get_metadata(),
        }
