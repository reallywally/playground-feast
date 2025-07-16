"""
Core types for the Feature Store
"""

from enum import Enum
from typing import Dict, Any, List
from dataclasses import dataclass
import numpy as np


class ValueType(Enum):
    """Value types for features and entities"""

    UNKNOWN = "unknown"
    BYTES = "bytes"
    STRING = "string"
    INT32 = "int32"
    INT64 = "int64"
    DOUBLE = "double"
    FLOAT = "float"
    BOOL = "bool"
    UNIX_TIMESTAMP = "unix_timestamp"
    BYTES_LIST = "bytes_list"
    STRING_LIST = "string_list"
    INT32_LIST = "int32_list"
    INT64_LIST = "int64_list"
    DOUBLE_LIST = "double_list"
    FLOAT_LIST = "float_list"
    BOOL_LIST = "bool_list"
    UNIX_TIMESTAMP_LIST = "unix_timestamp_list"


# Type mapping for numpy/pandas types
NUMPY_TYPE_MAP = {
    ValueType.INT32: np.int32,
    ValueType.INT64: np.int64,
    ValueType.FLOAT: np.float32,
    ValueType.DOUBLE: np.float64,
    ValueType.BOOL: np.bool_,
    ValueType.STRING: np.str_,
    ValueType.BYTES: np.bytes_,
    ValueType.UNIX_TIMESTAMP: np.datetime64,
}


@dataclass
class FeatureReference:
    """Reference to a feature in a feature view"""

    feature_view_name: str
    feature_name: str

    def __str__(self):
        return f"{self.feature_view_name}:{self.feature_name}"


@dataclass
class EntityReference:
    """Reference to an entity"""

    name: str
    join_keys: List[str]


@dataclass
class FeatureServiceReference:
    """Reference to a feature service"""

    name: str
    features: List[FeatureReference]


# Type aliases
FeatureMap = Dict[str, Any]
EntityMap = Dict[str, Any]
FeatureViewMap = Dict[str, Any]
