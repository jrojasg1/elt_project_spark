from abc import ABC, abstractmethod

class AbstractETL(ABC):
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
    
    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    def run(self):
        self.logger.info("Starting ETL process...")
        self.extract()
        self.transform()
        self.load()
        self.logger.info("ETL process completed successfully.")
