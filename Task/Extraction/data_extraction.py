#Librerías

import os
import pandas as pd


# -------------------------------------------------------------------------
# Función de extracción card
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
        print("Nombre del archivo:", file_name)
    return dataframes
# -------------------------------------------------------------------------
# Función de extracción tr and bb
# -------------------------------------------------------------------------
def extract_data_tr_bb(folder_path_tr_bb):
    # Obtener una lista de todos los archivos Excel en la carpeta
    file_list = [os.path.join(folder_path_tr_bb, f) for f in os.listdir(folder_path_tr_bb) if f.endswith(('.xls', '.xlsx'))]
    
    dataframes = []
    for file in file_list:
        file_name = os.path.basename(file)
        # Leer archivos dependiendo de su nombre
        if "Voucher_Redemption_Transaction" in file_name:
            print("El archivo contiene 'Voucher Redemption' en el nombre.")
            df_tr = pd.read_excel(file, skiprows=15, header=None)
            dataframes.append(('TR', df_tr, file_name))  # Guardamos la fuente de los datos (TR)
        elif "Bill_Breaking_Transaction" in file_name:
            print("El archivo contiene 'Bill Breaking' en el nombre.")
            df_bb = pd.read_excel(file, sheet_name="Sheet2", skiprows=3, header=None)
            dataframes.append(('BB', df_bb, file_name))  # Guardamos la fuente de los datos (BB)
    
    return dataframes