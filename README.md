# ETLProyect
# Proyecto de ETL - Consolidados de LO, HOST y Logs

Este proyecto de ETL (Extract, Transform, Load) está diseñado para procesar, transformar y consolidar datos de diferentes fuentes: LiveOffice (LO), Host Reports y Logs. El flujo de trabajo de este proyecto incluye la extracción de datos desde archivos de entrada, su transformación mediante funciones específicas, y la carga de los datos procesados en archivos Excel para su posterior análisis.

## Flujo de trabajo

1. **Extracción de Datos (Extract)**:
    - **LiveOffice**: Se extraen los informes de LiveOffice Card Reports desde una carpeta específica.
    - **Host Reports**: Se extraen los informes de Host Reports (incluyendo `TransactionLookup` y `rpttransactiondetailbytid`).
    - **Logs**: Se extraen los datos de Logs (incluyendo datos de ATM y Cash Advance).

2. **Transformación de Datos (Transform)**:
    - **LiveOffice**: Los datos de LiveOffice se transforman en un formato adecuado para la comparación y consolidación con los datos de Host Reports y Logs.
    - **Host Reports**: Dependiendo del tipo de archivo (TransactionLookup o rpttransactiondetailbytid), los datos se transforman adecuadamente.
    - **Logs**: Se transforman los datos de ATM y Cash Advance para su posterior análisis y consolidación con los otros conjuntos de datos.

3. **Consolidación y Procesamiento con Spark**:
    - Se procesan los datos de LiveOffice, Host Reports y Logs utilizando Apache Spark para realizar las uniones (joins) y cálculos necesarios.
    - La consolidación de los datos se realiza en base a columnas clave, como `seqNumber` y `AuthNumber`.

4. **Carga de Datos (Load)**:
    - Después de la transformación y consolidación de los datos, los resultados se exportan a archivos Excel.
    - Se utilizan funciones modulares para la carga de los datos, lo que permite una fácil reutilización y mantenimiento.

## Estructura del Proyecto

La estructura del proyecto está organizada en módulos que manejan cada una de las fases del proceso ETL:

Project/ │ ├── Task/ │ ├── Extraction/ │ │ ├── data_extraction.py # Funciones para extraer datos de LiveOffice y Host │ │ ├── data_extraction_host.py # Funciones para extraer datos de Host Reports │ │ └── data_extraction_logs.py # Funciones para extraer datos de Logs │ ├── Transformation/ │ │ ├── data_transformation.py # Funciones para transformar datos de LiveOffice │ │ ├── data_transformation_host.py# Funciones para transformar datos de Host Reports │ │ ├── data_transformation_logs.py# Funciones para transformar datos de Logs │ │ ├── spark_LO_HOST.py # Funciones para procesamiento con Spark (LO y Host) │ │ ├── spark_LOGS_LO_HOST.py # Funciones para procesamiento con Spark (Logs) │ │ └── utils.py # Funciones auxiliares │ ├── Load/ │ │ └── load.py # Funciones para cargar datos en archivos Excel │ └── main.py # Archivo principal que coordina el proceso ETL ├── ArchivosEntrada/ # Carpeta con los archivos de entrada (LO, Host, Logs) ├── ArchivosSalida/ # Carpeta donde se guardan los archivos de salida └── README.md # Documento con la descripción del proyecto


## Descripción de las Funciones

- **Extracción de Datos**:
    - `extract_data()`: Extrae los datos de LiveOffice.
    - `extract_host_data()`: Extrae los datos de Host Reports.
    - `extract_logs()`: Extrae los datos de Logs.

- **Transformación de Datos**:
    - `transform_data()`: Realiza la transformación de los datos de LiveOffice.
    - `transform_transaction_lookup()`: Transforma los datos de `TransactionLookup` de Host Reports.
    - `transform_rpttransactiondetailbytid()`: Transforma los datos de `rpttransactiondetailbytid` de Host Reports.
    - `transform_logs()`: Transforma los datos de Logs (ATM y Cash Advance).

- **Procesamiento con Spark**:
    - `process_with_spark()`: Realiza las uniones de los datos de LiveOffice y Host Reports utilizando Apache Spark.
    - `process_logs_with_spark()`: Realiza las uniones de los datos de Logs con los datos consolidados de LiveOffice y Host Reports.

- **Carga de Datos**:
    - `load_to_excel()`: Carga los DataFrames procesados en archivos Excel.

## Requerimientos

El proyecto requiere las siguientes dependencias:

- Python 3.x
- Apache Spark (para procesamiento de datos con Spark)
- Pandas (para manipulación de datos y exportación a Excel)
- openpyxl (para trabajar con archivos Excel)

Se recomienda usar un entorno virtual para gestionar las dependencias:

```bash
pip install -r requirements.txt
