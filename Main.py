# main.py

import pandas as pd
import os
import time
from Task.Extraction.data_extraction import extract_data
from Task.Transformation.data_transformation import transform_data
from Task.Extraction.data_extraction_host import extract_host_data
from Task.Transformation.data_transformation_host import transform_transaction_lookup, transform_rpttransactiondetailbytid
from Task.Transformation.utils import get_excel_files
from Task.Transformation.spark_LO_HOST import process_with_spark
from Task.Extraction.data_extraction_logs import extract_logs
from Task.Transformation.data_transformation_logs import transform_logs
from Task.Transformation.spark_LOGS_LO_HOST import process_logs_with_spark
from Task.Load.data_load import load_to_excel  # Importa la función de carga



start_time = time.time()
print("Iniciando el proceso ETL...")


#---------------------------------------------------------------------------------PATHS----------------------------------------------------------------------------------------------

# Rutas LO
folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\LOCardReports'
output_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosSalida\Concatenado2.xlsx'
# Rutas HOST
host_folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\HostReports'
output_path3 = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosSalida\Concatenado3.xlsx'
# Rutas Logs
Logs_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\Logs'


#-------------------------------------------------------------------------LiveOffice Card Reports----------------------------------------------------------------------------------------------

# Extracción
raw_data = extract_data(folder_path)
# Transformación
transformed_data = transform_data(raw_data)

# Guardar resultado usando la función load_to_excel
load_to_excel(transformed_data, output_path)


#--------------------------------------------------------------------------Host Card Reports----------------------------------------------------------------------------------------------
# Extracción de datos
file_list = get_excel_files(host_folder_path)
dataframes = []

for file_path in file_list:
    df_host, file_type = extract_host_data(file_path)
    if df_host is not None:
        # Transformación según el tipo de archivo
        if file_type == "TransactionLookup":
            df_host = transform_transaction_lookup(df_host)
        elif file_type == "rpttransactiondetailbytid":
            df_host = transform_rpttransactiondetailbytid(df_host)
        
        dataframes.append(df_host)

# Combinación de datos y exportación
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)
    load_to_excel(combined_df, output_path3)
else:
    print("No se generaron datos para exportar.")


#--------------------------------------------------------------------------SPARK LO-HOST CONSOLIDATION ----------------------------------------------------------------------------------------------

if dataframes:
    # Procesar con Spark
    print("Iniciando el procesamiento con Spark...")
    result_df, result_dfHOST = process_with_spark(transformed_data, combined_df, output_path3)  

    # Exportar a Excel usando la función load_to_excel
    load_to_excel(result_df, output_path)

    # Verificación: Chequear si el archivo se exportó exitosamente
    if os.path.exists(output_path3):
        print(f"Archivo exportado exitosamente a {output_path3}")
    else:
        print("No se pudo exportar el archivo.")
else:
    print("No se generaron datos para exportar.")


#-----------------------------------------------------------------------------Logs Extraction-------------------------------------------------------------------------------

# Extraer datos
df_Logscompleto = extract_logs(Logs_path)

# Crear un DataFrame unificado
df_LogsUnified = pd.DataFrame()

# Transformar datos de ATM
dfLogsATM = transform_logs(df_Logscompleto, 'ATM')
df_LogsUnified = pd.concat([df_LogsUnified, dfLogsATM], ignore_index=True)

# Transformar datos de Cash Advance
dfLogsCA = transform_logs(df_Logscompleto, 'CashAdvance')
df_LogsUnified = pd.concat([df_LogsUnified, dfLogsCA], ignore_index=True)

# Mostrar y exportar el DataFrame unificado usando la función load_to_excel
print(df_LogsUnified)
load_to_excel(df_LogsUnified, output_path)


#------------------------------------------------------------------SPARK Consolidado logs------------------------------------------------------------------------------

# Procesar los logs con Spark y obtener los DataFrames finales
result_df1, result_df2 = process_logs_with_spark(df_LogsUnified, result_df, result_dfHOST)

# Guardar los resultados en un archivo Excel usando la función load_to_excel
with pd.ExcelWriter(output_path3, engine='openpyxl') as writer:
    result_df1.to_excel(writer, sheet_name='LOGSvsLO-HOST', index=False)  # Guardar df1 en 'Hoja1'
    result_df2.to_excel(writer, sheet_name='LOGSvsHOST', index=False)  # Guardar df2 en 'Hoja2'

print(f"Archivo exportado exitosamente a {output_path3}")

#------------------------------------------------------------------------------------------------------------------------------------------------------

print("Proceso ETL completado.")
# Capturar el tiempo final
end_time = time.time()
# Calcular y mostrar el tiempo transcurrido
elapsed_time = end_time - start_time
print(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos")

#------------------------------------------------------------------------------------------------------------------------------------------------------
