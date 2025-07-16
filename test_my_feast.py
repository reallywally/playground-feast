"""
Test script for My Feast Feature Store implementation
"""

import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# Add the current directory to the path
sys.path.insert(0, ".")

from my_feast import (
    Entity,
    Feature,
    FeatureView,
    FeatureService,
    FeatureStore,
    ValueType,
    FileSource,
)


def test_basic_functionality():
    """Test basic functionality of the feature store"""
    print("Testing basic functionality...")

    # Create test data
    test_data = pd.DataFrame(
        {
            "driver_id": [1001, 1002, 1003],
            "conv_rate": [0.8, 0.9, 0.7],
            "acc_rate": [0.95, 0.88, 0.92],
            "avg_daily_trips": [100, 150, 80],
            "event_timestamp": [
                datetime.now() - timedelta(hours=1),
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=3),
            ],
        }
    )

    # Save test data to parquet
    os.makedirs("test_data", exist_ok=True)
    test_data.to_parquet("test_data/driver_stats.parquet")

    # Define entity
    driver_entity = Entity(
        name="driver_id", value_type=ValueType.INT64, description="Driver identifier"
    )

    # Define features
    conv_rate_feature = Feature(
        name="conv_rate", dtype=ValueType.FLOAT, description="Conversion rate"
    )

    acc_rate_feature = Feature(
        name="acc_rate", dtype=ValueType.FLOAT, description="Acceptance rate"
    )

    avg_trips_feature = Feature(
        name="avg_daily_trips", dtype=ValueType.INT64, description="Average daily trips"
    )

    # Define data source
    driver_source = FileSource(
        name="driver_stats_source",
        path="test_data/driver_stats.parquet",
        timestamp_field="event_timestamp",
    )

    # Define feature view
    driver_fv = FeatureView(
        name="driver_stats",
        entities=[driver_entity],
        features=[conv_rate_feature, acc_rate_feature, avg_trips_feature],
        source=driver_source,
        ttl=timedelta(hours=24),
    )

    # Define feature service
    driver_service = FeatureService(
        name="driver_features",
        features=[
            "driver_stats:conv_rate",
            "driver_stats:acc_rate",
            "driver_stats:avg_daily_trips",
        ],
    )

    # Initialize feature store
    store = FeatureStore(repo_path="test_repo")

    # Apply objects to registry
    store.apply([driver_entity, driver_fv, driver_service])

    print("✓ Successfully applied entities, feature views, and services")

    # Test materialization
    store.materialize_incremental(end_date=datetime.now())
    print("✓ Successfully materialized features")

    # Test online feature retrieval
    entity_rows = [{"driver_id": 1001}, {"driver_id": 1002}, {"driver_id": 1003}]

    online_features = store.get_online_features(
        features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
        entity_rows=entity_rows,
    )

    print(f"✓ Online features retrieved: {len(online_features)} rows")
    print(online_features)

    # Test historical features
    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(minutes=30),
            ],
        }
    )

    historical_features = store.get_historical_features(
        entity_df=entity_df,
        features=["driver_stats:conv_rate", "driver_stats:avg_daily_trips"],
    )

    print(f"✓ Historical features retrieved: {len(historical_features)} rows")
    print(historical_features)

    # Test feature service
    service_features = store.get_online_features(
        features=[], entity_rows=entity_rows, feature_service=driver_service
    )

    print(f"✓ Feature service worked: {len(service_features)} rows")
    print(service_features)

    # Test listing functions
    entities = store.list_entities()
    feature_views = store.list_feature_views()
    feature_services = store.list_feature_services()

    print(f"✓ Entities: {len(entities)}")
    print(f"✓ Feature views: {len(feature_views)}")
    print(f"✓ Feature services: {len(feature_services)}")

    # Test metadata
    metadata = store.get_metadata()
    print(f"✓ Metadata retrieved: {metadata}")

    # Clean up
    store.teardown()
    print("✓ Feature store cleaned up")

    # Remove test files
    import shutil

    if os.path.exists("test_data"):
        shutil.rmtree("test_data")
    if os.path.exists("test_repo"):
        shutil.rmtree("test_repo")

    print("✓ Test files cleaned up")
    print("All tests passed! 🎉")


if __name__ == "__main__":
    test_basic_functionality()
