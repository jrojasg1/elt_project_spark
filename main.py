from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import *
from etl.film_etl import FilmETL
from etl.inventory_etl import InventoryETL
from etl.rental_etl import RentalETL
from etl.customer_etl import CustomerETL
from observability.logger import logger

spark = SparkSession.builder.master(SPARK_MASTER).appName(SPARK_APP_NAME).getOrCreate()

# Crear objetos de ETL
film_model =  FilmETL(spark, logger)
inventory_model = InventoryETL(spark, logger)
rental_model = RentalETL(spark, logger)
customer_model = CustomerETL(spark, logger)

# Ejecución del ETL

df_film = film_model.extract(INPUT_CSV_FILM_PATH)
transformed_film = film_model.transform(df_film)
load_film_transformated = film_model.load(transformed_film, OUTPUT_CSV_FILM_PATH)

df_inventory = inventory_model.extract(INPUT_CSV_INVENTORY_PATH)
transformed_inventory = inventory_model.transform(df_inventory)
load_inventory_transformated = inventory_model.load(transformed_inventory, OUTPUT_CSV_INVENTORY_PATH)

df_rental = rental_model.extract(INPUT_CSV_RENTAL_PATH)
transformed_rental = rental_model.transform(df_rental)
load_rental_transformated = rental_model.load(transformed_rental, OUTPUT_CSV_RENTAL_PATH)

df_customer = customer_model.extract(INPUT_CSV_CUSTOMER_PATH)
transformed_customer = customer_model.transform(df_customer)
load_customer_transformated = customer_model.load(transformed_customer, OUTPUT_CSV_CUSTOMER_PATH)
 

logger.info("ETL process finished.")
