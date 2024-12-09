# elt_project_spark


## Descripción General

Esta aplicación ETL está diseñada para procesar y transformar datos de manera eficiente, modular y escalable. Su arquitectura permite manejar flujos de datos específicos de diferentes entidades (como `film`, `customer`, `inventory`, etc.) y realizar tareas de extracción, transformación y carga con PySpark.


## Estructura del Proyecto

```plaintext
etl-app/
├── config/
│   └── settings.py       # Configuraciones generales de la aplicación
├── observability/
│   ├── logger.py         # Registro de logs
│   ├── metrics.py        # Gestión de métricas
│   └── tracing.py        # Rastreo de flujo
├── etl/
│   ├── __init__.py       # Inicializador del módulo ETL
│   ├── base_etl.py       # Clase base con lógica común para ETL
│   ├── film_etl.py       # Flujo ETL específico para 'film'
│   ├── customer_etl.py   # Flujo ETL específico para 'customer'
│   ├── inventory_etl.py  # Flujo ETL específico para 'inventory'
│   └── rental_etl.py     # Flujo ETL específico para 'rental'
├── data/
│   ├── customers.csv     # Datos fuente de clientes
│   ├── films.csv         # Datos fuente de películas
│   ├── inventory.csv     # Datos fuente del inventario
│   └── rentals.csv       # Datos fuente de alquileres
├── main.py               # Punto de entrada principal de la aplicación
└── requirements.txt      # Dependencias del proyecto

```

## 1. Modularidad

La estructura de directorios permite separar las responsabilidades y mantener un diseño limpio y fácil de mantener:

- **`config/`**: Centraliza todas las configuraciones de la aplicación (como rutas, credenciales, parámetros de ejecución). Esto facilita la reutilización y los cambios en configuraciones sin afectar otras partes del código.
- **`observability/`**: Agrupa funcionalidades relacionadas con la supervisión y monitoreo del ETL, como logs, métricas y rastreo. Esto asegura trazabilidad y detección eficiente de errores.
- **`etl/`**: Contiene las clases específicas para cada flujo de datos ETL, organizadas por entidad (`film`, `inventory`, etc.) y una clase base que centraliza lógica común. Esto reduce la duplicación de código.
- **`data/`**: Almacena los archivos fuente utilizados en los procesos ETL. Centralizar los datos facilita el acceso y permite una fácil configuración de rutas dinámicas.

---

## 2. Escalabilidad

- **Clases ETL independientes**: Cada flujo (como `FilmETL`, `CustomerETL`) está separado, pero hereda de una clase base (`AbstractETL.py`). Esto permite agregar nuevos flujos ETL simplemente añadiendo nuevas clases sin modificar las existentes.
- **Procesamiento distribuido**: Al usar frameworks como PySpark, el diseño es escalable para manejar grandes volúmenes de datos.

---

## 3. Reusabilidad

- **Clase base ETL (`AbstractETL.py`)**: Contiene métodos comunes  lo que estandariza la forma en que se implementan los procesos ETL (`extract`, `transform`, `load`) y facilita extender la lógica para flujos ETL específicos. Esto minimiza redundancias.
- **Configuraciones en `config/`**: Los valores dinámicos, como las rutas de datos, pueden ser reutilizados por múltiples componentes.

---

## 4. Observabilidad y Depuración

La carpeta `observability/` proporciona herramientas clave para supervisar el rendimiento:

- **`logger.py`**: Registra eventos y errores durante el ETL.
- **`tracing.py`**: Rastrea el flujo de datos, lo cual es esencial para entender cuellos de botella o problemas.

Esto garantiza que el sistema sea auditable y fácil de depurar.

---

## 5. Flexibilidad

- **Estructura flexible para múltiples fuentes**: El diseño permite trabajar con diversas fuentes (CSV, bases de datos, APIs, etc.) agregando extractores específicos.
- **Adaptación a diferentes formatos de datos**: Gracias al diseño modular, se pueden modificar transformaciones o añadir nuevas reglas de validación sin afectar el resto de la aplicación.

---

## 6. Mantenimiento

El diseño sigue el principio **SOLID**, con separación de responsabilidades, lo que facilita:

- Agregar nuevas funcionalidades sin alterar el código existente.
- Localizar errores rápidamente gracias a la división por módulos.

---

## 7. Aplicación Práctica

- **Integración en `main.py`**: El archivo principal actúa como punto de entrada para coordinar los flujos ETL y ejecutar procesos específicos según sea necesario.
- **Dependencias centralizadas en `requirements.txt`**: Esto asegura que las dependencias necesarias para la ejecución del ETL estén claras y organizadas.

---

## Conclusión

El diseño modular y escalable de esta aplicación ETL permite procesar datos de manera eficiente y flexible, mientras que las herramientas de observabilidad y la clara división de responsabilidades aseguran que el sistema sea fácil de mantener, extender y depurar.
