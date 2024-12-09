import logging
from config.settings import LOG_FILE
def setup_logger():
    logger = logging.getLogger("ETLLogger")
    
    # Evitar agregar múltiples manejadores si el logger ya está configurado
    if not logger.handlers:
        # Configuración para escribir en un archivo
        file_handler = logging.FileHandler(LOG_FILE)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        logger.setLevel(logging.INFO)

    return logger


logger = setup_logger()
