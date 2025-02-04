# main.py

import pandas as pd
import os
import time
from Task.Extraction.data_extraction import extract_data
from Task.Extraction.data_extraction import extract_data_tr_bb
from Task.Transformation.data_transformation import transform_data
from Task.Transformation.data_transformation import transform_data_tr_bb
from Task.Extraction.data_extraction_host import extract_host_data
from Task.Transformation.data_transformation_host import transform_transaction_lookup, transform_rpttransactiondetailbytid
from Task.Transformation.utils import get_excel_files
from Task.Transformation.spark_LO_HOST import process_with_spark
from Task.Extraction.data_extraction_logs import extract_logs
from Task.Transformation.data_transformation_logs import transform_logs
from Task.Transformation.spark_LOGS_LO_HOST import process_logs_with_spark
from Task.Transformation.spark_LOGS_LO_TR_BB import process_logs_with_spark_tr_bb
from Task.Load.data_load import load_to_excel  # Importa la función de carga
from Task.Load.data_load import load_to_excel_tr_bb 


start_time = time.time()
print("Iniciando el proceso ETL...")


#---------------------------------------------------------------------------------PATHS----------------------------------------------------------------------------------------------

# Rutas LO
folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\LOCardReports'
output_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosSalida\Concatenado2.xlsx'
folder_path_tr_bb = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\LO_TR_BBReports'

# Rutas HOST
host_folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\HostReports'
output_path3 = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosSalida\Concatenado3.xlsx'
# Rutas Logs
Logs_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\Logs'


#-------------------------------------------------------------------------LiveOffice Card Reports----------------------------------------------------------------------------------------------

def process_live_office(folder_path, output_path):
    raw_data = extract_data(folder_path)
    transformed_data = transform_data(raw_data)
    load_to_excel(transformed_data, output_path)
    return transformed_data


#-------------------------------------------------------------------------LiveOffice TR && BB Reports----------------------------------------------------------------------------------------------


def process_live_office_tr_bb(folder_path_tr_bb, output_path3):
    raw_data_tr_bb = extract_data_tr_bb(folder_path_tr_bb)
    transformed_data_tr_bb = transform_data_tr_bb(raw_data_tr_bb)
    load_to_excel_tr_bb(transformed_data_tr_bb, output_path3)
    return transformed_data_tr_bb

#--------------------------------------------------------------------------Host Card Reports----------------------------------------------------------------------------------------------


def process_host_reports(host_folder_path, output_path3):
    file_list = get_excel_files(host_folder_path)
    dataframes = []

    for file_path in file_list:
        df_host, file_type = extract_host_data(file_path)
        if df_host is not None:
            df_host = (
                transform_transaction_lookup(df_host) if file_type == "TransactionLookup" 
                else transform_rpttransactiondetailbytid(df_host) if file_type == "rpttransactiondetailbytid"
                else df_host
            )
            dataframes.append(df_host)

    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        load_to_excel(combined_df, output_path3)
        return combined_df
    else:
        print("No se generaron datos para exportar.")
        return None

#--------------------------------------------------------------------------SPARK LO-HOST CONSOLIDATION ----------------------------------------------------------------------------------------------

def process_spark_lo_host(transformed_data, combined_df, output_path, output_path3):
    if combined_df is not None:
        print("Iniciando el procesamiento con Spark...")
        result_df, result_dfHOST = process_with_spark(transformed_data, combined_df, output_path3)

        load_to_excel(result_df, output_path)

        if os.path.exists(output_path3):
            print(f"Archivo exportado exitosamente a {output_path3}")
        else:
            print("No se pudo exportar el archivo.")
    else:
        print("No se generaron datos para exportar.")


#----------------------------------------------------------------------------- Logs Extraction-------------------------------------------------------------------------------

def process_logs(Logs_path, output_path, output_path3):
    logs_data = extract_logs(Logs_path)
    df_LogsUnified = pd.DataFrame()

    for log_type in ['ATM', 'CashAdvance']:
        df_logs = transform_logs(logs_data, log_type)
        df_LogsUnified = pd.concat([df_LogsUnified, df_logs], ignore_index=True)

    dfLogsTR = transform_logs(logs_data, 'TicketRedemption')
    dfLogsBB = transform_logs(logs_data, 'BillBreaking')

    load_to_excel(dfLogsTR, output_path)
    load_to_excel(dfLogsBB, output_path3)

    return df_LogsUnified, dfLogsTR, dfLogsBB

#------------------------------------------------------------------SPARK Consolidado logs------------------------------------------------------------------------------

def process_spark_logs(df_LogsUnified, result_df, result_dfHOST, output_path3):
    result_df1, result_df2 = process_logs_with_spark(df_LogsUnified, result_df, result_dfHOST)

    with pd.ExcelWriter(output_path3, engine='openpyxl') as writer:
        result_df1.to_excel(writer, sheet_name='LOGSvsLO-HOST', index=False)
        result_df2.to_excel(writer, sheet_name='LOGSvsHOST', index=False)

    print(f"Archivo exportado exitosamente card transactions a {output_path3}")

#------------------------------------------------------------------SPARK Consolidado logs vs LO (TR-BB)------------------------------------------------------------------------------

def process_spark_logs_tr_bb(dfLogsTR, dfLogsBB, transformed_data_tr_bb, output_path3):
    result_df1, result_df2 = process_logs_with_spark_tr_bb(dfLogsTR, dfLogsBB, transformed_data_tr_bb)

    with pd.ExcelWriter(output_path3, engine='openpyxl') as writer:
        result_df1.to_excel(writer, sheet_name='LOGSvsLO-TR', index=False)
        result_df2.to_excel(writer, sheet_name='LOGSvsLO-BB', index=False)

#------------------------------------------------------------------------------------------------------------------------------------------------------

def main():
    start_time = time.time()
    print("Iniciando el proceso ETL...")

    # Definir rutas
    folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\LOCardReports'
    output_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\Concatenado2.xlsx'
    folder_path_tr_bb = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\LO_TR_BBReports'
    host_folder_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\HostReports'
    output_path3 = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosSalida\Concatenado3.xlsx'
    Logs_path = r'C:\Users\paula\UNIR\MasterBigDatayVisualAnalytics\cuatrimestre2\TFM-TFE\Entrega3\TESIS_Maestria_ETL\ETLProyect\ArchivosEntrada\Logs'

    #  Procesos
    transformed_data = process_live_office(folder_path, output_path)
    transformed_data_tr_bb = process_live_office_tr_bb(folder_path_tr_bb, output_path3)
    combined_df = process_host_reports(host_folder_path, output_path3)

    process_spark_lo_host(transformed_data, combined_df, output_path, output_path3)
    df_LogsUnified, dfLogsTR, dfLogsBB = process_logs(Logs_path, output_path, output_path3)
    process_spark_logs(df_LogsUnified, transformed_data, combined_df, output_path3)
    process_spark_logs_tr_bb(dfLogsTR, dfLogsBB, transformed_data_tr_bb, output_path3)

    print("Proceso ETL completado.")
    print(f"Tiempo total de ejecución: {time.time() - start_time:.2f} segundos")


if __name__ == "__main__":
    main()

#------------------------------------------------------------------------------------------------------------------------------------------------------
