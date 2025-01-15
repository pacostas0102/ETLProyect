# Task/Transform/data_transformation_logs.py

import pandas as pd

def transform_logs(df_Logscompleto, log_type):
    if log_type == 'ATM':
        # Filtrar filas que contienen el texto "ATM. 'Posting Transaction Result"
        dfLogs = df_Logscompleto[
            df_Logscompleto.apply(lambda row: row.astype(str).str.contains("ATM.     'Posting Transaction Result ").any(), axis=1)
        ]
    elif log_type == 'CashAdvance':
        # Filtrar filas que contienen el texto "Cash Advance Transaction Info. 'Posting Transaction Result"
        dfLogs = df_Logscompleto[
            df_Logscompleto.apply(lambda row: row.astype(str).str.contains("Cash Advance Transaction Info. 'Posting Transaction Result").any(), axis=1)
        ]
    else:
        raise ValueError("Tipo de log desconocido")

    # Usar expresiones regulares para extraer los valores
    dfLogs['seqNumber'] = dfLogs["FilteredData"].str.extract(r'"seqNumber":"(\d+)"').astype(str)
    dfLogs['AuthNumber'] = dfLogs["FilteredData"].str.extract(r'"AuthNumber":"(\d+)"').astype(str)
    dfLogs['CardNumber'] = dfLogs["FilteredData"].str.extract(r'"CardNumber":"(\d+)"').astype(str)
    dfLogs['HostIP'] = dfLogs["FilteredData"].str.extract(r'"HostIP":"([^"]+)"').astype(str)
    dfLogs['Amount'] = dfLogs["FilteredData"].str.extract(r'"Amount":([0-9]+(?:\.[0-9]+)?)')
    dfLogs['DispensedTotal'] = dfLogs["FilteredData"].str.extract(r'"DispensedTotal":([0-9]+(?:\.[0-9]+)?)')
    dfLogs['TransactionType'] = dfLogs["FilteredData"].str.extract(r'"TransactionType":"([^"]+)"').astype(str)
    dfLogs['Status'] = dfLogs["FilteredData"].str.extract(r'"Status":"([^"]+)"').astype(str)
    
    if log_type == 'ATM':
        dfLogs['JournalName'] = (
            dfLogs["FilteredData"]
            .str.extract(r'\d+\s+([^\s]+)\s+\'Posting Transaction Result')  # Extraer el texto relevante
            .squeeze()  # Convertir a una Serie si es un DataFrame con una sola columna
            .str.replace(r'\.', '', regex=True)  # Eliminar el punto
        ).astype(str)
    elif log_type == 'CashAdvance':
        dfLogs['JournalName'] = (
            dfLogs["FilteredData"]
            .str.extract(r'------------>\s*(.*?)\s*Transaction Info\.')  # Extraer el texto relevante
            .squeeze()  # Convertir a una Serie si es un DataFrame con una sola columna
            .str.replace(r'\.', '', regex=True)  # Eliminar el punto
        ).astype(str)

    return dfLogs
