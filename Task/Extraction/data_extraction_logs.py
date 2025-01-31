# Task/Extraction/data_extraction_logs.py

import os
import pandas as pd


def extract_logs(Logs_path, column_names=["FilteredData"]):
    logs_dataframes = []
    
    for subcarpeta in os.listdir(Logs_path):
        ruta_subcarpeta = os.path.join(Logs_path, subcarpeta)
        if os.path.isdir(ruta_subcarpeta) and os.path.basename(ruta_subcarpeta).lower() in ['atm', 'cashadvance', 'ticketredemption', 'billbreaking']:
            print(f"Procesando: {ruta_subcarpeta}")
            for archivo in os.listdir(ruta_subcarpeta):
                if archivo.endswith('.jrn'):  # Filtrar solo archivos con extensión .jrn
                    ruta_completa = os.path.join(ruta_subcarpeta, archivo)
                    logs_df = pd.read_csv(ruta_completa, sep=';', encoding='utf-8', names=column_names, header=None)
                    logs_dataframes.append(logs_df)
    
    df_Logscompleto = pd.concat(logs_dataframes, ignore_index=True)
    return df_Logscompleto

