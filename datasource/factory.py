from .models import SourceConfig
from .database import Database
from .base import DataSource
from typing import Type, Dict

class DataSourceFactory:
    _source_map: Dict[str, Type[DataSource]] = {
        "db": Database
    }

    @classmethod
    def get_data_source(cls, config: SourceConfig) -> DataSource:
        source_class = cls._source_map.get(config.source_type)
        if source_class is None:
            raise ValueError(f"Unknown source type: {config.source_type}")
        return source_class(config)

    @classmethod
    def register_data_source(cls, source_type: str, source_class: Type[DataSource]) -> None:
        if not issubclass(source_class, DataSource):
            raise TypeError("source_class must inherit from DataSource")
        cls._source_map[source_type] = source_class
