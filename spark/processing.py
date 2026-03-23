from pyspark.sql.functions import col

def process(df):
    return df.filter(col("temperature") > 30)