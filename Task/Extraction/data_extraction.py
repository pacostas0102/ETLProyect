#Librerías

import os
import pandas as pd


# -------------------------------------------------------------------------
# Función de extracción
# -------------------------------------------------------------------------
def extract_data(folder_path):
    file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(('.xls', '.xlsx'))]
    dataframes = []
    for file in file_list:
        df = pd.read_excel(file, sheet_name="Sheet2", skiprows=3, header=None)
        file_name = os.path.basename(file)
        new_columns = df.iloc[2].astype(str) + df.iloc[3].astype(str)
        df.columns = new_columns  # Asignar los nuevos encabezados
        df = df[4:]  # Eliminar las filas de encabezado original (0, 1 y 2)
        df.reset_index(drop=True, inplace=True)
        df['file_name'] = file_name  # Agregar el nombre del archivo
        dataframes.append((df, file_name))
    return dataframes