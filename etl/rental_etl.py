from etl.AbstractETL import AbstractETL
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

class RentalETL(AbstractETL):
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
        if "return_date" in df.columns:
            df = df.withColumn(
                "return_date",
                F.when(F.col("return_date").isNotNull(), F.to_timestamp(F.trim(F.col("return_date"))))
                .otherwise(F.lit("2024-12-24 00:00:00").cast(TimestampType()))
            )
        df = df.dropDuplicates(["rental_id"]).orderBy("rental_id")    
        return df

    def load(self, df, destination):
        self.logger.info(f"Loading data into {destination}...")
        df.write.mode("overwrite").csv(destination, header=True)
