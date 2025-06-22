from databricks.sdk.runtime import *
url = dbutils.secrets.get(scope="adls-secrets", key="url")
token = dbutils.secrets.get(scope="adls-secrets", key="token")

spark.conf.set(url, token)