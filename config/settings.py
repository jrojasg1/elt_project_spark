""" # config/settings.py

SPARK_MASTER = "local[*]"  # Para entornos locales, en producción usar un cluster.
SPARK_APP_NAME = "ETL Application"
DATABASE_URL = "jdbc:mysql://localhost:3306/mydb"
S3_BUCKET = "s3://my-bucket/output/"
 """

# Configuración de Spark
SPARK_MASTER = "local[*]"  # Configuración para ejecutar Spark localmente (usa el clúster en producción).
SPARK_APP_NAME = "CSV_ETL_Application"  # Nombre de la aplicación Spark.

# Configuración de rutas de archivos CSV
INPUT_CSV_PATH = "data/input.csv"  # Ruta del archivo CSV de entrada.
OUTPUT_CSV_PATH = "data/output.csv"  # Ruta del archivo CSV de salida.

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

LOG_FILE = "observability/etl.log"  # Archivo donde se guardarán los logs.
END_POINT_TRACING = "http://localhost:4319/v1/traces"
# Configuración adicional
CSV_OPTIONS = {
    "header": True,  # Indica si el archivo CSV tiene encabezado.
    "inferSchema": True  # Intenta inferir el esquema de las columnas automáticamente.
}

# Configuración de métricas
METRICS_SERVER_PORT = 8000  # Puerto para servir métricas a Prometheus.
