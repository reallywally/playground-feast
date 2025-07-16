"""
Parquet offline store implementation for the Feature Store
"""

from typing import List, Optional
from datetime import datetime
import pandas as pd
from .base import OfflineStore
from ..core.feature_view import FeatureView
from ..core.types import FeatureReference


class ParquetOfflineStore(OfflineStore):
    """
    Parquet-based offline store implementation.
    Reads historical features from parquet files.
    """

    def __init__(self):
        super().__init__()

    def get_historical_features(
        self,
        entity_df: pd.DataFrame,
        feature_views: List[FeatureView],
        feature_refs: List[FeatureReference],
    ) -> pd.DataFrame:
        """Get historical features for training or batch scoring"""
        self.validate_entity_df(entity_df)
        self.validate_feature_views(feature_views, feature_refs)

        # Perform point-in-time join
        return self.perform_point_in_time_join(entity_df, feature_views, feature_refs)

    def pull_latest_from_table_or_query(
        self,
        feature_view: FeatureView,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Pull latest features from data source"""
        # Read from the feature view's data source
        df = feature_view.source.read(start_date=start_date, end_date=end_date)

        # If we have a timestamp field, get the latest records per entity
        if feature_view.source.timestamp_field:
            timestamp_field = feature_view.source.timestamp_field
            join_keys = feature_view.get_join_keys()

            # Sort by timestamp and get the latest record for each entity
            df = df.sort_values(timestamp_field)
            df = df.groupby(join_keys).tail(1)

        return df

    def write_logged_features(
        self, feature_view: FeatureView, df: pd.DataFrame
    ) -> None:
        """Write logged features to offline store"""
        # For parquet offline store, we might write to a separate logging directory
        # This is a simplified implementation
        import os

        # Create logs directory if it doesn't exist
        logs_dir = f"logs/{feature_view.name}"
        os.makedirs(logs_dir, exist_ok=True)

        # Write to timestamped parquet file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = f"{logs_dir}/logged_features_{timestamp}.parquet"
        df.to_parquet(log_path)

    def offline_write_batch(
        self,
        feature_view: FeatureView,
        df: pd.DataFrame,
        progress_callback: Optional[callable] = None,
    ) -> None:
        """Write batch features to offline store"""
        # For parquet offline store, we might write to a batch directory
        # This is a simplified implementation
        import os

        # Create batch directory if it doesn't exist
        batch_dir = f"batch/{feature_view.name}"
        os.makedirs(batch_dir, exist_ok=True)

        # Write to timestamped parquet file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_path = f"{batch_dir}/batch_features_{timestamp}.parquet"

        # Write in chunks if dataframe is large
        chunk_size = 10000
        if len(df) > chunk_size:
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                chunk_path = f"{batch_dir}/batch_features_{timestamp}_chunk_{i // chunk_size}.parquet"
                chunk.to_parquet(chunk_path)

                if progress_callback:
                    progress_callback(i + len(chunk), len(df))
        else:
            df.to_parquet(batch_path)
            if progress_callback:
                progress_callback(len(df), len(df))

    def _point_in_time_join_with_timestamp(
        self,
        entity_df: pd.DataFrame,
        feature_df: pd.DataFrame,
        join_keys: List[str],
        timestamp_field: str,
    ) -> pd.DataFrame:
        """Perform point-in-time join with timestamp"""
        # This is a more sophisticated implementation for parquet
        result_rows = []

        # Ensure timestamp columns are datetime
        if timestamp_field in entity_df.columns:
            entity_df[timestamp_field] = pd.to_datetime(entity_df[timestamp_field])
        if timestamp_field in feature_df.columns:
            feature_df[timestamp_field] = pd.to_datetime(feature_df[timestamp_field])

        # For each entity row, find the latest features as of the timestamp
        for _, entity_row in entity_df.iterrows():
            # Filter feature data for this entity
            entity_filter = True
            for join_key in join_keys:
                if join_key in feature_df.columns:
                    entity_filter = entity_filter & (
                        feature_df[join_key] == entity_row[join_key]
                    )

            entity_features = feature_df[entity_filter]

            # Filter by timestamp if available
            if timestamp_field in entity_row and timestamp_field in feature_df.columns:
                entity_timestamp = entity_row[timestamp_field]
                entity_features = entity_features[
                    entity_features[timestamp_field] <= entity_timestamp
                ]

            # Get the latest feature values
            if not entity_features.empty:
                if timestamp_field in entity_features.columns:
                    latest_features = entity_features.sort_values(timestamp_field).tail(
                        1
                    )
                else:
                    latest_features = entity_features.tail(1)

                # Combine with entity data
                result_row = entity_row.to_dict()
                for col in latest_features.columns:
                    if col not in join_keys and col != timestamp_field:
                        result_row[col] = latest_features[col].iloc[0]
                result_rows.append(result_row)
            else:
                # No features found, add entity row with null features
                result_rows.append(entity_row.to_dict())

        return pd.DataFrame(result_rows)

    def get_feature_data_for_materialization(
        self,
        feature_view: FeatureView,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Get feature data for materialization to online store"""
        df = self.pull_latest_from_table_or_query(feature_view, start_date, end_date)

        # Ensure all required columns are present
        required_columns = (
            feature_view.get_join_keys() + feature_view.get_feature_names()
        )
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing columns in feature data: {missing_columns}")

        return df[required_columns]
