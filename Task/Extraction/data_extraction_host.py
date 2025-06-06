import os
import pandas as pd

def extract_host_data(file_path):
    """
    Data is extracted by their host report characteristics 
    """
    file_name = os.path.basename(file_path)

    if "TransactionLookup" in file_name:
        print (file_path)
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == '.xls':
                df_host = pd.read_excel(file_path, skiprows=4, engine='xlrd')
                #df_host = df_host[1:] 
                df_host.reset_index(drop=True, inplace=True)
                df_host['file_name'] = file_name
                print("Data extracted from : TransactionLookup")

            elif ext == '.xlsx':
                df_host = pd.read_excel(file_path, skiprows=4, engine='openpyxl')
            else:
                raise ValueError(f"Archivo no soportado: {file_path}")
        except Exception as e:
            print(f"Error leyendo archivo: {file_path} -> {e}")
            df_host = None
        #df_host = pd.read_excel(file_path, skiprows=4, engine='openpyxl')
        #df_host = df_host[3:] 
        #df_host.reset_index(drop=True, inplace=True)
        #df_host['file_name'] = file_name
        #print("Data extracted from : TransactionLookup")
        return df_host, "TransactionLookup"

    elif "rpttransactiondetailbytid" in file_name:
        df_host = pd.read_excel(file_path)#, skiprows=5)
        #new_columns = df_host.iloc[2].astype(str) + df_host.iloc[3].astype(str)
        #df_host.columns = new_columns  
        #df_host = df_host[1:]  
        df_host.reset_index(drop=True, inplace=True)
        df_host['file_name'] = file_name
        print("Data extracted from : rpttransactiondetailbytid")
        #df_host = df_host.dropna(axis=1, how='all')
        return df_host, "rpttransactiondetailbytid"

    else:
        print(f"The following file couldn't be processed : {file_name}")
        return None, None
