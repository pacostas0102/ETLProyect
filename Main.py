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
from pathlib import Path



start_time = time.time()
print("Iniciando el proceso ETL...")


#-------------------------------------------------------------------------LiveOffice Card Reports----------------------------------------------------------------------------------------------

def process_live_office(folder_path, output_path):
    raw_data = extract_data(folder_path)
    transformed_data = transform_data(raw_data)
    load_to_excel(transformed_data, output_path,'locardreport')
    return transformed_data


#-------------------------------------------------------------------------LiveOffice TR && BB Reports----------------------------------------------------------------------------------------------


def process_live_office_tr_bb(folder_path_tr_bb, output_path3, option):
    raw_data_tr_bb = extract_data_tr_bb(folder_path_tr_bb, option)
    transformed_data_tr_bb = transform_data_tr_bb(raw_data_tr_bb)
    load_to_excel(transformed_data_tr_bb, output_path3,'lotr-bbreport')
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
        #print(type(df_host))
        load_to_excel(df_host, output_path3,'hostcardreports')    
        #dataframes.append(df_host)

    #if df_host:
        #combined_df = pd.concat(dataframes, ignore_index=True)
        #dataF = pd.DataFrame(dataframes)
       # load_to_excel(df_host, output_path3,'hostcardreports')
    return df_host, file_type
    #else:
     #   print("There were not data to export")
      #  return None

#--------------------------------------------------------------------------SPARK LO-HOST CONSOLIDATION ----------------------------------------------------------------------------------------------

def process_spark_lo_host(transformed_data, combined_df, host_name, output_path, output_path3):
    if combined_df is not None:
        print("Spark process begins...")
        result_df = process_with_spark(transformed_data, combined_df, host_name, output_path3)

        load_to_excel(result_df, output_path,'lo-host')

        if os.path.exists(output_path):
            print(f"File successfully exported in: {output_path}")
        else:
            print("File couldn't be exported.")

        return result_df    
    else:
        print("There isn't any data to export")
        return combined_df 
       


#----------------------------------------------------------------------------- Logs Extraction-------------------------------------------------------------------------------

def process_logs(Logs_path, output_path, option):
    logs_data, detected_types = extract_logs(Logs_path,option,output_path)
 #   df_LogsUnified = pd.DataFrame()
    dfLogs = transform_logs(logs_data, option, output_path, detected_types)
    return dfLogs 

#------------------------------------------------------------------SPARK Consolidado logs------------------------------------------------------------------------------

def process_spark_logs(df_LogsUnified, combined_df, dfHOST,host_name, output_path):
    result_df1, result_df2 = process_logs_with_spark(df_LogsUnified, combined_df, dfHOST,host_name)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        result_df1.to_excel(writer, sheet_name='LOGSvsLO-HOST', index=False)
        result_df2.to_excel(writer, sheet_name='LOGSvsHOST', index=False)

    print(f"Archivo exportado exitosamente card transactions a {output_path}")

#------------------------------------------------------------------SPARK Consolidado logs vs LO (TR-BB)------------------------------------------------------------------------------

def process_spark_logs_tr_bb(dfLogs, transformed_data_tr_bb, output_path):
    #result_df1, result_df2 = process_logs_with_spark_tr_bb(dfLogsTR, dfLogsBB, transformed_data_tr_bb)
    result_df1  = process_logs_with_spark_tr_bb(dfLogs, transformed_data_tr_bb, output_path)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        result_df1.to_excel(writer, sheet_name='LOGSvsLO-TR', index=False)
        #result_df2.to_excel(writer, sheet_name='LOGSvsLO-BB', index=False)

#------------------------------------------------------------------------------------------------------------------------------------------------------

def main():
    start_time = time.time()
    print("Iniciando el proceso ETL...")

    # Definir rutas
    folder_path = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosEntrada\LOCardReports'
    output_path = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosSalida\Concatenado2.xlsx'
    folder_path_tr_bb = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosEntrada\LO_TR_BBReports'
    host_folder_path = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosEntrada\HostReports'
    output_path3 = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosSalida\Concatenado3.csv'    
    output_path2 = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosSalida\Concatenado3.xlsx'
    Logs_path = r'C:\Users\Paula\Documents\LiveOffice\LOGS\variances\ETLProject\ETLProyect\ArchivosEntrada\Logs'

    option = input("What type of variance do you want to analyze? (tickets/cards/bills): ").strip().lower()

    if option == "tickets":
        print("You selected TICKET variance.")        
        df_TransformedLogs = process_logs(Logs_path, output_path3, option)
        transformed_data_tr_bb = process_live_office_tr_bb(folder_path_tr_bb, output_path, option)
        result1 = process_spark_logs_tr_bb(df_TransformedLogs, transformed_data_tr_bb, output_path2)
        
    elif option == "cards":
        print("You selected CARD variance.")
        transformed_data = process_live_office(folder_path, output_path)
        df_host, host_name = process_host_reports(host_folder_path, output_path)
        combined_df = process_spark_lo_host(transformed_data, df_host, host_name, output_path, output_path3)
        df_TransformedLogs = process_logs(Logs_path, output_path3, option)
        result2= process_spark_logs(df_TransformedLogs, combined_df, df_host,host_name, output_path2)


    elif option == "bills":
        print("You selected BILL variance.")
        df_TransformedLogs = process_logs(Logs_path, output_path3, option)
        transformed_data_tr_bb = process_live_office_tr_bb(folder_path_tr_bb, output_path, option)
        #result1 = process_spark_logs_tr_bb(df_TransformedLogs, transformed_data_tr_bb, output_path2)

    else:
        print("Invalid option. Please enter 'tickets', 'cards', or 'bills'.")




    #  Procesos

    #transformed_data = process_live_office(folder_path, output_path)
    #transformed_data_tr_bb = process_live_office_tr_bb(folder_path_tr_bb, output_path3)
    #combined_df = process_host_reports(host_folder_path, output_path3)

    #process_spark_lo_host(transformed_data, combined_df, output_path, output_path3)
    #df_LogsUnified, dfLogsTR, dfLogsBB = process_logs(Logs_path, output_path, output_path)
    #process_spark_logs(df_LogsUnified, transformed_data, combined_df, output_path3)
    #process_spark_logs_tr_bb(dfLogsTR, dfLogsBB, transformed_data_tr_bb, output_path)

    print("Proceso ETL completado.")
    print(f"Tiempo total de ejecución: {time.time() - start_time:.2f} segundos")


if __name__ == "__main__":
    main()

#------------------------------------------------------------------------------------------------------------------------------------------------------
