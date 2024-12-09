from etl.AbstractETL import AbstractETL
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

class CustomerETL(AbstractETL):
    column_rules = {
        "customer_id": {
            "type": IntegerType(),
            "nullable": False
        },
        "email": {
            "regex": "[\\x00-\\x1F]+",  
            "transformations": [
                {"function": F.trim},
                {"function": F.upper},
                {"function": lambda col: F.when(F.length(col) > 50, col.substr(1, 50)).otherwise(col)}
            ],
            "type": StringType(),
            "nullable": False
        },
        "first_name": {
            "transformations": [
                {"function": F.trim},
                {"function": F.upper}
            ],
            "type": StringType(),
            "nullable": False
        },
        "last_name": {
            "transformations": [
                {"function": F.trim},
                {"function": F.upper}
            ],
            "type": StringType(),
            "nullable": False
        },
        "columns_to_drop": ["customer_id_old", "segment"]
    }

    def __init__(self, spark, logger):
        super().__init__(spark, logger)

    def extract(self, source):
        self.logger.info(f"Extracting data from {source}1...")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").option("charset", "UTF-8").csv(source)

        return df
    
    def transform(self, df):
        self.logger.info("Transforming data...")
        for column in df.columns:
            df = df.withColumnRenamed(column, column.strip())
            
        for column, rules in self.column_rules.items():
            if column in df.columns and column != "columns_to_drop":
                self.logger.info(f"Cleaning column: {column}")
                
                if "regex" in rules:
                    self.logger.info(f"Add regex {rules['regex']} to column {column}")
                    df = df.withColumn(column, F.regexp_replace(F.col(column), rules["regex"], ""))
                
                if "transformations" in rules:
                    for transformation in rules["transformations"]:
                        func = transformation["function"]
                        self.logger.info(f"Add transformation {func} to column {column}")
                        df = df.withColumn(column, func(F.col(column)))
                
                if "type" in rules:
                    self.logger.info(f"Casting column {column} to type {rules['type']}")
                    df = df.withColumn(column, F.col(column).cast(rules["type"]))
        
        columns_to_drop = self.column_rules.get("columns_to_drop", [])
        self.logger.info(f"Drop columns: {columns_to_drop}")
        df = df.drop(*columns_to_drop)
        
        if "customer_id" in df.columns:
            self.logger.info("Delete duplicate data and order by 'customer_id'")
            df = df.dropDuplicates(["customer_id"]).orderBy("customer_id")
        
        return df

    def load(self, df, destination):
        self.logger.info(f"Loading data into {destination}...")
        df.write.mode("overwrite").csv(destination, header=True)
