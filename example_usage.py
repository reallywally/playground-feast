"""
Example usage of My Feast Feature Store

This example demonstrates how to:
1. Define entities and features
2. Create feature views and services
3. Apply definitions to the registry
4. Materialize features to online store
5. Retrieve online and historical features
"""

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

from my_feast import (
    Entity,
    Feature,
    FeatureView,
    FeatureService,
    FeatureStore,
    ValueType,
    FileSource,
)


def create_sample_data():
    """Create sample driver statistics data"""
    # Create sample data
    np.random.seed(42)
    n_drivers = 100

    data = []
    for driver_id in range(1001, 1001 + n_drivers):
        for days_ago in range(7):
            timestamp = datetime.now() - timedelta(
                days=days_ago, hours=np.random.randint(0, 24)
            )
            data.append(
                {
                    "driver_id": driver_id,
                    "conv_rate": np.random.uniform(0.5, 0.95),
                    "acc_rate": np.random.uniform(0.8, 0.99),
                    "avg_daily_trips": np.random.randint(50, 200),
                    "total_earnings": np.random.uniform(100, 500),
                    "event_timestamp": timestamp,
                    "created": timestamp,
                }
            )

    df = pd.DataFrame(data)

    # Save to parquet file
    import os

    os.makedirs("data", exist_ok=True)
    df.to_parquet("data/driver_stats.parquet")
    print(f"Created sample data with {len(df)} records")
    return df


def main():
    """Main example function"""
    print("🚀 My Feast Feature Store Example")
    print("=" * 50)

    # 1. Create sample data
    print("\n1. Creating sample data...")
    create_sample_data()

    # 2. Define Entity
    print("\n2. Defining entity...")
    driver = Entity(
        name="driver_id", value_type=ValueType.INT64, description="Driver identifier"
    )
    print(f"✓ Entity: {driver}")

    # 3. Define Features
    print("\n3. Defining features...")
    features = [
        Feature(name="conv_rate", dtype=ValueType.FLOAT, description="Conversion rate"),
        Feature(name="acc_rate", dtype=ValueType.FLOAT, description="Acceptance rate"),
        Feature(
            name="avg_daily_trips",
            dtype=ValueType.INT64,
            description="Average daily trips",
        ),
        Feature(
            name="total_earnings", dtype=ValueType.FLOAT, description="Total earnings"
        ),
    ]

    for feature in features:
        print(f"✓ Feature: {feature}")

    # 4. Define Data Source
    print("\n4. Defining data source...")
    driver_stats_source = FileSource(
        name="driver_hourly_stats_source",
        path="data/driver_stats.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    print(f"✓ Data Source: {driver_stats_source}")

    # 5. Define Feature View
    print("\n5. Defining feature view...")
    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        features=features,
        source=driver_stats_source,
        ttl=timedelta(days=1),
        online=True,
        description="Driver hourly statistics",
    )
    print(f"✓ Feature View: {driver_stats_fv}")

    # 6. Define Feature Service
    print("\n6. Defining feature service...")
    driver_ranking_service = FeatureService(
        name="driver_ranking_service",
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
        description="Features for driver ranking model",
    )
    print(f"✓ Feature Service: {driver_ranking_service}")

    # 7. Initialize Feature Store
    print("\n7. Initializing feature store...")
    store = FeatureStore(repo_path="my_feature_repo")
    print("✓ Feature Store initialized at: my_feature_repo")

    # 8. Apply definitions to registry
    print("\n8. Applying definitions to registry...")
    store.apply([driver, driver_stats_fv, driver_ranking_service])
    print("✓ Applied all definitions to registry")

    # 9. Materialize features to online store
    print("\n9. Materializing features to online store...")
    store.materialize_incremental(end_date=datetime.now())
    print("✓ Features materialized successfully")

    # 10. Get online features
    print("\n10. Retrieving online features...")
    entity_rows = [
        {"driver_id": 1001},
        {"driver_id": 1002},
        {"driver_id": 1003},
        {"driver_id": 1004},
        {"driver_id": 1005},
    ]

    online_features = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips",
        ],
        entity_rows=entity_rows,
    )

    print(f"✓ Retrieved online features for {len(online_features)} drivers:")
    print(online_features.head())

    # 11. Get historical features
    print("\n11. Retrieving historical features...")
    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002, 1003, 1004, 1005],
            "event_timestamp": [
                datetime.now() - timedelta(hours=1),
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=3),
                datetime.now() - timedelta(hours=4),
                datetime.now() - timedelta(hours=5),
            ],
        }
    )

    historical_features = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:total_earnings",
        ],
    )

    print(f"✓ Retrieved historical features for {len(historical_features)} drivers:")
    print(historical_features.head())

    # 12. Use feature service
    print("\n12. Using feature service...")
    service_features = store.get_online_features(
        features=[], entity_rows=entity_rows, feature_service=driver_ranking_service
    )

    print(f"✓ Retrieved features using service for {len(service_features)} drivers:")
    print(service_features.head())

    # 13. Show metadata
    print("\n13. Feature store metadata...")
    entities = store.list_entities()
    feature_views = store.list_feature_views()
    feature_services = store.list_feature_services()

    print(f"✓ Entities: {len(entities)}")
    print(f"✓ Feature Views: {len(feature_views)}")
    print(f"✓ Feature Services: {len(feature_services)}")

    metadata = store.get_metadata()
    print(
        f"✓ Registry: {metadata['registry']['entity_count']} entities, {metadata['registry']['feature_view_count']} feature views"
    )

    # 14. Cleanup
    print("\n14. Cleaning up...")
    store.teardown()

    # Remove sample data and repo
    import shutil
    import os

    if os.path.exists("data"):
        shutil.rmtree("data")
    if os.path.exists("my_feature_repo"):
        shutil.rmtree("my_feature_repo")

    print("✓ Cleanup completed")

    print("\n" + "=" * 50)
    print("🎉 Example completed successfully!")
    print("\nNext steps:")
    print("- Explore different data sources (CSV, JSON)")
    print("- Try different online/offline store configurations")
    print("- Build ML models using the retrieved features")
    print("- Set up scheduled materialization")


if __name__ == "__main__":
    main()
