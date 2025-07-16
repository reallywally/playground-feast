"""
Base offline store class for the Feature Store
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
import pandas as pd
from ..core.feature_view import FeatureView
from ..core.types import FeatureReference


class OfflineStore(ABC):
    """
    Abstract base class for all offline store implementations.
    Offline stores provide historical feature data for training and batch scoring.
    """

    def __init__(self):
        pass

    @abstractmethod
    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        feature_views: List[FeatureView],
        feature_refs: List[FeatureReference],
    ) -> pd.DataFrame:
        """Get historical features for training or batch scoring"""
        pass

    @abstractmethod
    def pull_latest_from_table_or_query(
        self,
        feature_view: FeatureView,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Pull latest features from data source"""
        pass

    @abstractmethod
    def write_logged_features(
        self, feature_view: FeatureView, df: pd.DataFrame
    ) -> None:
        """Write logged features to offline store"""
        pass

    @abstractmethod
    def offline_write_batch(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
        progress_callback: Optional[callable] = None,
    ) -> None:
        """Write batch features to offline store"""
        pass

    def validate_entity_df(self, entity_df: pd.DataFrame) -> None:
        """Validate entity dataframe"""
        if entity_df.empty:
            raise ValueError("Entity dataframe cannot be empty")

    def validate_feature_views(
        self, feature_views: List[FeatureView], feature_refs: List[FeatureReference]
    ) -> None:
        """Validate feature views against feature references"""
        fv_names = {fv.name for fv in feature_views}

        for ref in feature_refs:
            if ref.feature_view_name not in fv_names:
                raise ValueError(f"Feature view '{ref.feature_view_name}' not found")

        # Validate that all referenced features exist
        for ref in feature_refs:
            fv = next(fv for fv in feature_views if fv.name == ref.feature_view_name)
            if ref.feature_name not in fv.get_feature_names():
                raise ValueError(
                    f"Feature '{ref.feature_name}' not found in feature view '{ref.feature_view_name}'"
                )

    def perform_point_in_time_join(
        self,
        entity_df: pd.DataFrame,
        feature_views: List[FeatureView],
        feature_refs: List[FeatureReference],
    ) -> pd.DataFrame:
        """Perform point-in-time join between entity dataframe and feature views"""
        result_df = entity_df.copy()

        # Group feature references by feature view
        fv_refs = {}
        for ref in feature_refs:
            if ref.feature_view_name not in fv_refs:
                fv_refs[ref.feature_view_name] = []
            fv_refs[ref.feature_view_name].append(ref.feature_name)

        # Join each feature view
        for fv in feature_views:
            if fv.name in fv_refs:
                feature_names = fv_refs[fv.name]
                fv_df = self._get_feature_view_data(fv, entity_df)

                if not fv_df.empty:
                    # Perform point-in-time join
                    result_df = self._join_feature_view(
                        result_df, fv_df, fv, feature_names
                    )

        return result_df

    def _get_feature_view_data(
        self, feature_view: FeatureView, entity_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get feature view data for entities"""
        # Read from data source
        df = feature_view.source.read()

        # Filter by entity values if possible
        join_keys = feature_view.get_join_keys()
        entity_values = {}

        for join_key in join_keys:
            if join_key in entity_df.columns:
                entity_values[join_key] = entity_df[join_key].unique().tolist()

        # Filter dataframe by entity values
        for join_key, values in entity_values.items():
            if join_key in df.columns:
                df = df[df[join_key].isin(values)]

        return df

    def _join_feature_view(
        self,
        entity_df: pd.DataFrame,
        feature_df: pd.DataFrame,
        feature_view: FeatureView,
        feature_names: List[str],
    ) -> pd.DataFrame:
        """Join feature view data with entity dataframe"""
        join_keys = feature_view.get_join_keys()

        # Check if all join keys exist in both dataframes
        for join_key in join_keys:
            if join_key not in entity_df.columns:
                raise ValueError(f"Join key '{join_key}' not found in entity dataframe")
            if join_key not in feature_df.columns:
                raise ValueError(
                    f"Join key '{join_key}' not found in feature dataframe"
                )

        # Select only required columns
        columns_to_select = join_keys + feature_names
        columns_to_select = [
            col for col in columns_to_select if col in feature_df.columns
        ]
        feature_df = feature_df[columns_to_select]

        # Perform join
        if (
            feature_view.source.timestamp_field
            and feature_view.source.timestamp_field in entity_df.columns
        ):
            # Point-in-time join with timestamp
            result_df = self._point_in_time_join_with_timestamp(
                entity_df, feature_df, join_keys, feature_view.source.timestamp_field
            )
        else:
            # Simple join
            result_df = entity_df.merge(feature_df, on=join_keys, how="left")

        return result_df

    def _point_in_time_join_with_timestamp(
        self,
        entity_df: pd.DataFrame,
        feature_df: pd.DataFrame,
        join_keys: List[str],
        timestamp_field: str,
    ) -> pd.DataFrame:
        """Perform point-in-time join with timestamp"""
        # This is a simplified implementation
        # In a real implementation, this would be more sophisticated
        return entity_df.merge(feature_df, on=join_keys, how="left")
