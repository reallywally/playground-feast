"""
Memory online store implementation for the Feature Store
"""

import threading
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from .base import OnlineStore
from ..core.feature_view import FeatureView


class MemoryOnlineStore(OnlineStore):
    """
    Memory-based online store implementation.
    Stores features in memory using dictionaries for fast access.
    """

    def __init__(self):
        super().__init__()
        # Dictionary to store features: {feature_view_name: {entity_key: {feature_name: value}}}
        self._features: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # Dictionary to store timestamps: {feature_view_name: {entity_key: timestamp}}
        self._timestamps: Dict[str, Dict[str, datetime]] = {}
        # Lock for thread safety
        self._lock = threading.RLock()

    def write_features(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Write features to the memory store"""
        self.validate_feature_data(feature_view, df)

        if timestamp is None:
            timestamp = datetime.now()

        join_keys = feature_view.get_join_keys()
        feature_names = feature_view.get_feature_names()

        with self._lock:
            # Initialize feature view storage if not exists
            if feature_view.name not in self._features:
                self._features[feature_view.name] = {}
                self._timestamps[feature_view.name] = {}

            # Process each row
            for _, row in df.iterrows():
                # Create entity key from join keys
                entity_key = self._create_entity_key(row, join_keys)

                # Store features for this entity
                self._features[feature_view.name][entity_key] = {}
                for feature_name in feature_names:
                    if feature_name in row:
                        self._features[feature_view.name][entity_key][feature_name] = (
                            row[feature_name]
                        )

                # Store timestamp
                self._timestamps[feature_view.name][entity_key] = timestamp

    def read_features(
        self,
        feature_view: FeatureView,
        entity_rows: List[Dict[str, Any]],
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read features from the memory store"""
        self.validate_entity_rows(feature_view, entity_rows)

        if feature_names is None:
            feature_names = feature_view.get_feature_names()

        join_keys = feature_view.get_join_keys()

        result_rows = []

        with self._lock:
            # Check if feature view exists
            if feature_view.name not in self._features:
                # Return empty dataframe with correct schema
                columns = join_keys + feature_names
                return pd.DataFrame(columns=columns)

            # Process each entity row
            for entity_row in entity_rows:
                entity_key = self._create_entity_key(entity_row, join_keys)

                # Start with entity values
                result_row = entity_row.copy()

                # Add feature values if entity exists
                if entity_key in self._features[feature_view.name]:
                    features = self._features[feature_view.name][entity_key]
                    for feature_name in feature_names:
                        if feature_name in features:
                            result_row[feature_name] = features[feature_name]
                        else:
                            result_row[feature_name] = None
                else:
                    # Entity not found, set all features to None
                    for feature_name in feature_names:
                        result_row[feature_name] = None

                result_rows.append(result_row)

        return pd.DataFrame(result_rows)

    def delete_features(self, feature_view: FeatureView) -> None:
        """Delete all features for a feature view"""
        with self._lock:
            if feature_view.name in self._features:
                del self._features[feature_view.name]
            if feature_view.name in self._timestamps:
                del self._timestamps[feature_view.name]

    def teardown(self) -> None:
        """Clean up memory store resources"""
        with self._lock:
            self._features.clear()
            self._timestamps.clear()

    def _create_entity_key(self, row: Dict[str, Any], join_keys: List[str]) -> str:
        """Create a unique entity key from join key values"""
        key_values = []
        for join_key in sorted(join_keys):  # Sort for consistent ordering
            key_values.append(f"{join_key}={row[join_key]}")
        return "|".join(key_values)

    def get_feature_view_names(self) -> List[str]:
        """Get list of feature view names in the store"""
        with self._lock:
            return list(self._features.keys())

    def get_entity_count(self, feature_view_name: str) -> int:
        """Get count of entities for a feature view"""
        with self._lock:
            if feature_view_name in self._features:
                return len(self._features[feature_view_name])
            return 0

    def get_metadata(self) -> Dict[str, Any]:
        """Get store metadata"""
        with self._lock:
            metadata = {"type": "memory", "feature_views": {}, "total_entities": 0}

            for fv_name, entities in self._features.items():
                entity_count = len(entities)
                metadata["feature_views"][fv_name] = {
                    "entity_count": entity_count,
                    "features": (
                        list(list(entities.values())[0].keys()) if entities else []
                    ),
                }
                metadata["total_entities"] += entity_count

            return metadata

    def clear(self) -> None:
        """Clear all data from the store"""
        with self._lock:
            self._features.clear()
            self._timestamps.clear()
