"""
Configuration management for the Feature Store
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
import yaml
import os


@dataclass
class FeatureStoreConfig:
    """
    Configuration for the Feature Store
    """

    project: str = "default_project"
    registry_path: str = "registry.db"
    online_store_type: str = "memory"
    online_store_config: Optional[Dict[str, Any]] = None
    offline_store_type: str = "parquet"
    offline_store_config: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.online_store_config is None:
            self.online_store_config = {}
        if self.offline_store_config is None:
            self.offline_store_config = {}

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "FeatureStoreConfig":
        """Load configuration from YAML file"""
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

        with open(yaml_path, "r") as f:
            config_data = yaml.safe_load(f)

        # Parse configuration
        project = config_data.get("project", "default_project")
        registry_path = config_data.get("registry", "registry.db")

        # Parse online store configuration
        online_store_config = config_data.get("online_store", {})
        if isinstance(online_store_config, dict):
            online_store_type = online_store_config.get("type", "memory")
        else:
            online_store_type = "memory"
            online_store_config = {}

        # Parse offline store configuration
        offline_store_config = config_data.get("offline_store", {})
        if isinstance(offline_store_config, dict):
            offline_store_type = offline_store_config.get("type", "parquet")
        else:
            offline_store_type = "parquet"
            offline_store_config = {}

        return cls(
            project=project,
            registry_path=registry_path,
            online_store_type=online_store_type,
            online_store_config=online_store_config,
            offline_store_type=offline_store_type,
            offline_store_config=offline_store_config,
        )

    def to_yaml(self, yaml_path: str) -> None:
        """Save configuration to YAML file"""
        config_data = {
            "project": self.project,
            "registry": self.registry_path,
            "online_store": {
                "type": self.online_store_type,
                **self.online_store_config,
            },
            "offline_store": {
                "type": self.offline_store_type,
                **self.offline_store_config,
            },
        }

        with open(yaml_path, "w") as f:
            yaml.safe_dump(config_data, f, default_flow_style=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "project": self.project,
            "registry_path": self.registry_path,
            "online_store_type": self.online_store_type,
            "online_store_config": self.online_store_config,
            "offline_store_type": self.offline_store_type,
            "offline_store_config": self.offline_store_config,
        }

    def validate(self) -> None:
        """Validate configuration"""
        if not self.project:
            raise ValueError("Project name cannot be empty")

        if not self.registry_path:
            raise ValueError("Registry path cannot be empty")

        supported_online_stores = ["memory", "redis", "sqlite"]
        if self.online_store_type not in supported_online_stores:
            raise ValueError(f"Unsupported online store type: {self.online_store_type}")

        supported_offline_stores = ["parquet", "bigquery", "spark"]
        if self.offline_store_type not in supported_offline_stores:
            raise ValueError(
                f"Unsupported offline store type: {self.offline_store_type}"
            )

    def get_online_store_config(self, key: str, default: Any = None) -> Any:
        """Get online store configuration value"""
        return self.online_store_config.get(key, default)

    def get_offline_store_config(self, key: str, default: Any = None) -> Any:
        """Get offline store configuration value"""
        return self.offline_store_config.get(key, default)

    def set_online_store_config(self, key: str, value: Any) -> None:
        """Set online store configuration value"""
        self.online_store_config[key] = value

    def set_offline_store_config(self, key: str, value: Any) -> None:
        """Set offline store configuration value"""
        self.offline_store_config[key] = value
