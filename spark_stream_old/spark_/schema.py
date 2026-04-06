from pyspark.sql.types import *

schema = StructType() \
    .add("timestamp", StringType()) \
    .add("temperature", DoubleType())