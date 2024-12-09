from etl.AbstractETL import AbstractETL
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, ShortType, ByteType, IntegerType


class FilmETL(AbstractETL):
    column_rules = {
        "film_id": {
            "regex": "[^0-9]",
            "type": ShortType(),
            "nullable": False
        },
        "release_year": {
            "regex": "[^0-9]",
            "type": ShortType(),
            "nullable": False
        },
        "original_language_id": {
            "regex": None,
            "type": ByteType(),
            "nullable": False,
            "default": 0
        },
        "rental_rate": {
            "regex": "[^0-9.]",
            "type": DecimalType(4, 2),
            "nullable": False,
            "format_number": 2
        },
        "length": {
            "regex": "[^0-9]",
            "type": ShortType(),
            "nullable": True
        },
        "replacement_cost": {
            "regex": "[^0-9.]",
            "type": DecimalType(5, 3),
            "nullable": False,
            "format_number": 2
        },
        "num_voted_users": {
            "regex": "[^0-9]",
            "type": IntegerType(),
            "nullable": False
        }
    }

    def __init__(self, spark, logger):
        super().__init__(spark, logger)

    def extract(self, source):
        self.logger.info(f"Extracting data from {source}...")
        df = self.spark.read.csv(source, header=True, inferSchema=True)
        return df
    
    

    def transform(self, df):
        self.logger.info("Transforming data film ...")
        for column in df.columns:
            df = df.withColumnRenamed(column, column.strip())

        for column, rules in self.column_rules.items():
            if column in df.columns:
                self.logger.info(f"Cleaning column: {column}")
                
                if "regex" in rules and rules["regex"]:
                    self.logger.info(f"Add regex {rules['regex']} to column {column}")
                    df = df.withColumn(column, F.regexp_replace(F.col(column), rules["regex"], ""))
                
                if "type" in rules:
                    self.logger.info(f"Casting column {column} to type {rules['type']}")
                    df = df.withColumn(column, F.col(column).cast(rules["type"]))
                
                if "nullable" in rules and not rules["nullable"]:
                    default_value = rules.get("default", None)
                    if default_value is not None:
                        self.logger.info(f"Replace null values  in the  column {column} to {default_value}")
                        df = df.withColumn(column, F.when(F.col(column).isNull(), default_value).otherwise(F.col(column)))
                
                if "format_number" in rules:
                    self.logger.info(f"Formatting numbers in the column {column} to {rules['format_number']} decimal")
                    df = df.withColumn(column, F.format_number(F.col(column), rules["format_number"]))
            else:
                self.logger.warning(f"The column {column} does not exist in the DataFrame. Cleaning is omitted.")
        
        if "film_id" in df.columns:
            self.logger.info("Deleting duplicate data and order by 'film_id'")
            df = df.dropDuplicates(["film_id"]).orderBy("film_id")
        
        return df

    def load(self, df, destination):
        self.logger.info(f"Loading data into {destination}...")
        df.write.mode("overwrite").csv(destination, header=True)
