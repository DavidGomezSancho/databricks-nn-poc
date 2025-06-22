import datetime
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from .models import SourceConfig

class DataSource(ABC):
    def __init__(self, config: SourceConfig):
        self.config = config
        
    @abstractmethod
    def read_source(self) -> DataFrame:
        pass

    @abstractmethod
    def write_to_raw(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def read_from_raw(self, date: datetime.date) -> DataFrame:
        pass

    @abstractmethod
    def write_to_delta(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def read_from_delta(self) -> DeltaTable:
        pass

    @abstractmethod
    def get_raw_location(self, date: datetime.date) -> str:
        pass

    @abstractmethod
    def get_delta_location(self) -> str:
        pass
