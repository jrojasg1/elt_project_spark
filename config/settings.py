
SPARK_MASTER = "local[*]" 
SPARK_APP_NAME = "ETL_Application"  

INPUT_CSV_FILM_PATH = "data/film.csv"
INPUT_CSV_INVENTORY_PATH = "data/inventory.csv"
INPUT_CSV_RENTAL_PATH = "data/rental.csv"
INPUT_CSV_CUSTOMER_PATH = "data/customer.csv"
INPUT_CSV_STORE_PATH = "data/store.csv"


OUTPUT_CSV_FILM_PATH = "data/film_transformed"
OUTPUT_CSV_INVENTORY_PATH = "data/inventory_transformed"
OUTPUT_CSV_RENTAL_PATH = "data/rental_transformed"
OUTPUT_CSV_CUSTOMER_PATH = "data/customer_transformed"
OUTPUT_CSV_STORE_PATH = "data/store_transformed"

LOG_FILE = "observability/etl.log"  
END_POINT_TRACING = "http://localhost:4319/v1/traces"

METRICS_SERVER_PORT = 8000  
