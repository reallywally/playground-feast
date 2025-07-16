"""
SQLite registry implementation for the Feature Store
"""

import sqlite3
import json
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
from .base import Registry
from ..core.entity import Entity
from ..core.feature_view import FeatureView
from ..core.feature_service import FeatureService


class SQLiteRegistry(Registry):
    """
    SQLite-based registry implementation.
    Stores feature metadata in a SQLite database.
    """

    def __init__(self, db_path: str = "registry.db"):
        super().__init__()
        self.db_path = db_path
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the SQLite database with required tables"""
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Create entities table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS entities (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create feature_views table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_views (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create feature_services table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_services (
                    name TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.commit()

    def apply_entity(self, entity: Entity) -> None:
        """Apply an entity to the registry"""
        entity.validate()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO entities (name, data, updated_timestamp)
                VALUES (?, ?, ?)
            """,
                (entity.name, json.dumps(entity.to_dict()), datetime.now()),
            )
            conn.commit()

    def apply_feature_view(self, feature_view: FeatureView) -> None:
        """Apply a feature view to the registry"""
        feature_view.validate()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO feature_views (name, data, updated_timestamp)
                VALUES (?, ?, ?)
            """,
                (feature_view.name, json.dumps(feature_view.to_dict()), datetime.now()),
            )
            conn.commit()

    def apply_feature_service(self, feature_service: FeatureService) -> None:
        """Apply a feature service to the registry"""
        feature_service.validate()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO feature_services (name, data, updated_timestamp)
                VALUES (?, ?, ?)
            """,
                (
                    feature_service.name,
                    json.dumps(feature_service.to_dict()),
                    datetime.now(),
                ),
            )
            conn.commit()

    def get_entity(self, name: str) -> Optional[Entity]:
        """Get an entity by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM entities WHERE name = ?", (name,))
            result = cursor.fetchone()

            if result:
                return Entity.from_dict(json.loads(result[0]))
            return None

    def get_feature_view(self, name: str) -> Optional[FeatureView]:
        """Get a feature view by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM feature_views WHERE name = ?", (name,))
            result = cursor.fetchone()

            if result:
                return FeatureView.from_dict(json.loads(result[0]))
            return None

    def get_feature_service(self, name: str) -> Optional[FeatureService]:
        """Get a feature service by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM feature_services WHERE name = ?", (name,))
            result = cursor.fetchone()

            if result:
                return FeatureService.from_dict(json.loads(result[0]))
            return None

    def list_entities(self) -> List[Entity]:
        """List all entities"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM entities ORDER BY name")
            results = cursor.fetchall()

            return [Entity.from_dict(json.loads(row[0])) for row in results]

    def list_feature_views(self) -> List[FeatureView]:
        """List all feature views"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM feature_views ORDER BY name")
            results = cursor.fetchall()

            return [FeatureView.from_dict(json.loads(row[0])) for row in results]

    def list_feature_services(self) -> List[FeatureService]:
        """List all feature services"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM feature_services ORDER BY name")
            results = cursor.fetchall()

            return [FeatureService.from_dict(json.loads(row[0])) for row in results]

    def delete_entity(self, name: str) -> None:
        """Delete an entity by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM entities WHERE name = ?", (name,))
            conn.commit()

    def delete_feature_view(self, name: str) -> None:
        """Delete a feature view by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM feature_views WHERE name = ?", (name,))
            conn.commit()

    def delete_feature_service(self, name: str) -> None:
        """Delete a feature service by name"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM feature_services WHERE name = ?", (name,))
            conn.commit()

    def teardown(self) -> None:
        """Clean up registry resources"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def get_metadata(self) -> Dict[str, Any]:
        """Get registry metadata"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Count entities
            cursor.execute("SELECT COUNT(*) FROM entities")
            entity_count = cursor.fetchone()[0]

            # Count feature views
            cursor.execute("SELECT COUNT(*) FROM feature_views")
            feature_view_count = cursor.fetchone()[0]

            # Count feature services
            cursor.execute("SELECT COUNT(*) FROM feature_services")
            feature_service_count = cursor.fetchone()[0]

            return {
                "db_path": self.db_path,
                "entity_count": entity_count,
                "feature_view_count": feature_view_count,
                "feature_service_count": feature_service_count,
            }
