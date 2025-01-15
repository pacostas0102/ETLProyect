import os
import pandas as pd

def extract_host_data(file_path):
    """
    Extrae datos desde un archivo específico según su tipo.
    """
    file_name = os.path.basename(file_path)

    if "TransactionLookup" in file_name:
        # Lectura específica para TransactionLookup
        df_host = pd.read_excel(file_path, skiprows=4)
        df_host = df_host[3:]  # Eliminar las filas de encabezado original (0, 1 y 2)
        df_host.reset_index(drop=True, inplace=True)
        df_host['file_name'] = file_name
        print("Archivo procesado: TransactionLookup")
        return df_host, "TransactionLookup"

    elif "rpttransactiondetailbytid" in file_name:
        # Lectura específica para rpttransactiondetailbytid
        df_host = pd.read_excel(file_path, skiprows=5)
        df_host = df_host[4:]  # Eliminar las filas de encabezado original (0, 1, 2, y 3)
        df_host.reset_index(drop=True, inplace=True)
        df_host['file_name'] = file_name
        print("Archivo procesado: rpttransactiondetailbytid")
        return df_host, "rpttransactiondetailbytid"

    else:
        print(f"No se procesó el archivo: {file_name}")
        return None, None
