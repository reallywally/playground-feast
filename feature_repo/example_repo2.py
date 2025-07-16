
from feast import (
    FeatureStore,
)
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("FeastHiveExample") \
    .config("spark.sql.warehouse.dir", "/hadoop/spark/warehouse/feature_mart") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://172.16.10.31:10000") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# fu_ocpn_inf 테이블 조회
df = spark.sql("SELECT * FROM fu_ocpn_inf LIMIT 10")
df.show()

# Feature Store 초기화
store = FeatureStore(repo_path=".")

# Spark DataFrame을 Pandas DataFrame으로 변환
pandas_df = df.toPandas()

# 필요한 경우 여기서 feature store에 데이터를 등록하거나 조회할 수 있습니다
# 예: store.get_historical_features(...)

# Spark 세션 종료
spark.stop()
