from etl.AbstractETL import AbstractETL
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType

class StoreETL(AbstractETL):
    def __init__(self, spark, logger):
        super().__init__(spark, logger)

    def extract(self, source):
        self.logger.info(f"Extracting data from {source}...")
        df = self.spark.read.csv(source, header=True, inferSchema=True)
        return df
    
    def transform(self, df):
        self.logger.info("Transforming data...")
        for column in df.columns:
            df = df.withColumnRenamed(column, column.strip())
        
        return df

    def load(self, df, destination):
        self.logger.info(f"Loading data into {destination}...")
        df.write.mode("overwrite").csv(destination, header=True)
