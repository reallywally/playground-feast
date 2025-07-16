"""
FeatureService class for the Feature Store
"""

from typing import List, Optional, Dict, Any, Union
from dataclasses import dataclass, field
from .feature_view import FeatureView
from .types import FeatureReference


@dataclass
class FeatureService:
    """
    A feature service is a logical grouping of features from multiple feature views.
    Feature services are used to define which features should be retrieved together.
    """

    name: str
    features: List[Union[FeatureView, FeatureReference, str]]
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}

    def __str__(self) -> str:
        return f"FeatureService(name='{self.name}', features={len(self.features)})"

    def __repr__(self) -> str:
        return self.__str__()

    def get_feature_references(self) -> List[FeatureReference]:
        """Get list of feature references from all sources"""
        references = []

        for feature in self.features:
            if isinstance(feature, FeatureView):
                # Add all features from the feature view
                for feature_name in feature.get_feature_names():
                    references.append(
                        FeatureReference(
                            feature_view_name=feature.name, feature_name=feature_name
                        )
                    )
            elif isinstance(feature, FeatureReference):
                references.append(feature)
            elif isinstance(feature, str):
                # Parse string format "feature_view:feature_name"
                if ":" in feature:
                    fv_name, f_name = feature.split(":", 1)
                    references.append(
                        FeatureReference(feature_view_name=fv_name, feature_name=f_name)
                    )
                else:
                    raise ValueError(f"Invalid feature reference format: {feature}")
            else:
                raise ValueError(f"Unsupported feature type: {type(feature)}")

        return references

    def get_feature_view_names(self) -> List[str]:
        """Get list of unique feature view names"""
        feature_view_names = set()

        for feature in self.features:
            if isinstance(feature, FeatureView):
                feature_view_names.add(feature.name)
            elif isinstance(feature, FeatureReference):
                feature_view_names.add(feature.feature_view_name)
            elif isinstance(feature, str):
                if ":" in feature:
                    fv_name, _ = feature.split(":", 1)
                    feature_view_names.add(fv_name)

        return list(feature_view_names)

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature service to dictionary for serialization"""
        features_data = []

        for feature in self.features:
            if isinstance(feature, FeatureView):
                features_data.append(
                    {"type": "feature_view", "data": feature.to_dict()}
                )
            elif isinstance(feature, FeatureReference):
                features_data.append(
                    {
                        "type": "feature_reference",
                        "data": {
                            "feature_view_name": feature.feature_view_name,
                            "feature_name": feature.feature_name,
                        },
                    }
                )
            elif isinstance(feature, str):
                features_data.append({"type": "string", "data": feature})

        return {
            "name": self.name,
            "features": features_data,
            "description": self.description,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeatureService":
        """Create feature service from dictionary"""
        features = []

        for feature_data in data["features"]:
            if feature_data["type"] == "feature_view":
                features.append(FeatureView.from_dict(feature_data["data"]))
            elif feature_data["type"] == "feature_reference":
                ref_data = feature_data["data"]
                features.append(
                    FeatureReference(
                        feature_view_name=ref_data["feature_view_name"],
                        feature_name=ref_data["feature_name"],
                    )
                )
            elif feature_data["type"] == "string":
                features.append(feature_data["data"])

        return cls(
            name=data["name"],
            features=features,
            description=data.get("description"),
            tags=data.get("tags", {}),
        )

    def validate(self) -> None:
        """Validate feature service configuration"""
        if not self.name:
            raise ValueError("FeatureService name cannot be empty")
        if not self.features:
            raise ValueError("FeatureService must have at least one feature")

        # Validate each feature
        for feature in self.features:
            if isinstance(feature, FeatureView):
                feature.validate()
            elif isinstance(feature, FeatureReference):
                if not feature.feature_view_name:
                    raise ValueError("FeatureReference must have a feature_view_name")
                if not feature.feature_name:
                    raise ValueError("FeatureReference must have a feature_name")
            elif isinstance(feature, str):
                if not feature:
                    raise ValueError("String feature reference cannot be empty")
                if ":" not in feature:
                    raise ValueError(
                        "String feature reference must be in format 'feature_view:feature_name'"
                    )
            else:
                raise ValueError(f"Unsupported feature type: {type(feature)}")

        # Check for duplicate feature references
        references = self.get_feature_references()
        reference_strings = [str(ref) for ref in references]
        if len(reference_strings) != len(set(reference_strings)):
            raise ValueError(
                "Feature references must be unique within a FeatureService"
            )
