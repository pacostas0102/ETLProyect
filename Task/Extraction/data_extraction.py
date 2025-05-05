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
        df.columns = new_columns  
        df = df[4:]  
        df.reset_index(drop=True, inplace=True)
        df['file_name'] = file_name  
        dataframes.append((df, file_name))
        print("File Name:", file_name)

    #dataframesLO = pd.DataFrame(df)  

    return dataframes
# -------------------------------------------------------------------------
# Función de extracción tr and bb
# -------------------------------------------------------------------------
def extract_data_tr_bb(folder_path_tr_bb, option):
    file_list = [os.path.join(folder_path_tr_bb, f) for f in os.listdir(folder_path_tr_bb) if f.endswith(('.xls', '.xlsx'))]
    
    dataframes = []
    for file in file_list:
        file_name = os.path.basename(file)
        if option.lower() == "tickets" and "Voucher_Redemption_Transaction" in file_name:
            print("The File has 'Voucher Redemption' included in the file name.")
            df_tr = pd.read_excel(file, skiprows=15, header=None)
            dataframes.append(('TR', df_tr, file_name))  

        elif option.lower() == "bills" and "Bill_Breaking_Transaction" in file_name:
            print("The File has 'Bill Breaking' included in the file name.")
            df_bb = pd.read_excel(file, sheet_name="Sheet2", skiprows=3, header=None)
            dataframes.append(('BB', df_bb, file_name))   
    
    return dataframes