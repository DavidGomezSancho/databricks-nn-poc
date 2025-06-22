import datetime
from pyspark.sql import DataFrame
from databricks.sdk.runtime import *
from delta.tables import DeltaTable
from .base import DataSource
from .models import SourceConfig

class Database(DataSource):
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self.user = dbutils.secrets.get(scope="jdbc-secrets", key="sql-user")
        self.password = dbutils.secrets.get(scope="jdbc-secrets", key="sql-password")

    def read_source(self) -> DataFrame:
        jdbc_url = (
            f"{self.config.connection_string};"
            f"database={self.config.database_name};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;"
            "loginTimeout=60;"
        )

        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", self.config.schema_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
            
        return df

    def write_to_raw(self, df: DataFrame) -> None:
        today = datetime.datetime.today()
        df.write.mode("overwrite").parquet(self.get_raw_location(today))

    def read_from_raw(self, date: datetime.date) -> DataFrame:
        return spark.read.parquet(self.get_raw_location(date))
    
    def write_to_delta(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").save(self.get_delta_location())

    def read_from_delta(self) -> DeltaTable:
        return DeltaTable.forPath(spark, self.get_delta_location())
        # return spark.read.format("delta").load(self.get_delta_location())

    def get_raw_location(self, date: datetime.date) -> str:
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')

        return f"abfss://demo-container@adlspocmetadatann.dfs.core.windows.net/raw/{self.config.source_name}/{self.config.database_name}/{self.config.schema_name}/{year}/{month}/{day}/"
    
    def get_delta_location(self) -> str:
        return f"abfss://demo-container@adlspocmetadatann.dfs.core.windows.net/delta/{self.config.source_name}/{self.config.database_name}/{self.config.schema_name}/"
