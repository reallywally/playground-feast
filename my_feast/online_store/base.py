"""
Base online store class for the Feature Store
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from ..core.feature_view import FeatureView
from ..core.types import FeatureReference


class OnlineStore(ABC):
    """
    Abstract base class for all online store implementations.
    Online stores provide low-latency feature serving.
    """

    def __init__(self):
        pass

    @abstractmethod
    def write_features(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Write features to the online store"""
        pass

    @abstractmethod
    def read_features(
        self,
        feature_view: FeatureView,
        entity_rows: List[Dict[str, Any]],
        feature_names: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Read features from the online store"""
        pass

    @abstractmethod
    def delete_features(self, feature_view: FeatureView) -> None:
        """Delete all features for a feature view"""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Clean up online store resources"""
        pass

    def get_feature_references(
        self, feature_view: FeatureView, feature_names: Optional[List[str]] = None
    ) -> List[FeatureReference]:
        """Get feature references for a feature view"""
        if feature_names is None:
            feature_names = feature_view.get_feature_names()

        references = []
        for feature_name in feature_names:
            if feature_name in feature_view.get_feature_names():
                references.append(
                    FeatureReference(
                        feature_view_name=feature_view.name, feature_name=feature_name
                    )
                )
            else:
                raise ValueError(
                    f"Feature '{feature_name}' not found in feature view '{feature_view.name}'"
                )

        return references

    def validate_entity_rows(
        self, feature_view: FeatureView, entity_rows: List[Dict[str, Any]]
    ) -> None:
        """Validate entity rows against feature view join keys"""
        join_keys = feature_view.get_join_keys()

        for i, row in enumerate(entity_rows):
            for join_key in join_keys:
                if join_key not in row:
                    raise ValueError(f"Entity row {i} missing join key '{join_key}'")

    def validate_feature_data(
        self, feature_view: FeatureView, df: pd.DataFrame
    ) -> None:
        """Validate feature data against feature view schema"""
        # Check if join keys are present
        join_keys = feature_view.get_join_keys()
        for join_key in join_keys:
            if join_key not in df.columns:
                raise ValueError(f"Join key '{join_key}' not found in data")

        # Check if timestamp field is present if specified
        if feature_view.source.timestamp_field:
            if feature_view.source.timestamp_field not in df.columns:
                raise ValueError(
                    f"Timestamp field '{feature_view.source.timestamp_field}' not found in data"
                )

        # Check if feature columns are present
        feature_names = feature_view.get_feature_names()
        for feature_name in feature_names:
            if feature_name not in df.columns:
                raise ValueError(f"Feature '{feature_name}' not found in data")
