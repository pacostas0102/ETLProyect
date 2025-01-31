#Librerías
from Task.Transformation.utils import clean_and_convert_columns
import pandas as pd

# -------------------------------------------------------------------------
# Función de Transformación
# -------------------------------------------------------------------------


def transform_atm(df):
    df.insert(loc=df.columns.get_loc("nanSTATUS"), column="FEE($)", value=0)
    df['DISPENSEDTOTAL($)'] = pd.to_numeric(df['DISPENSEDTOTAL($)'], errors='coerce').fillna(0).astype(float)
    df['TRANSAMOUNT($)'] = pd.to_numeric(df['TRANSAMOUNT($)'], errors='coerce').astype(float)
    df = df.drop(['DISPENSED QTYS$1', 'nan$5', 'nan$10', 'nan$20', 'nan$50', 'nan$100'], axis=1)
    df['TRANSTYPE'] = df['TRANSTYPE'].astype(str)        
    df['SEQUENCENUMBER'] = df['SEQUENCENUMBER'].astype(str)        
    df['nanCARD#'] = df['nanCARD#'].astype(str)        
    df['HOSTSEQ NUMBER'] = df['HOSTSEQ NUMBER'].astype(str)
    # limpiar la columna TRANSACTIONTYPE
    df["TRANSTYPE"] = df["TRANSTYPE"].str.strip()  # Elimina espacios antes y después
    df["TRANSTYPE"] = df["TRANSTYPE"].str.replace(r"\s+", " ", regex=True)  # Reemplaza múltiples espacios con uno solo
    df = df.rename(columns={"nanSTATUS": "STATUS", "nanCARD#": "CARDNUMBER", "TRANSTYPE": "TRANSACTIONTYPE"})
    return df

def transform_cash_advance(df):
    status_column_index = df.columns.get_loc("FEE($)")
    df['TRANSAMOUNT($)'] = pd.to_numeric(df['TRANSAMOUNT($)'], errors='coerce').astype(float)
    df.insert(loc=status_column_index, column="DISPENSEDTOTAL($)", value=0)
    df.insert(loc=status_column_index, column="COINSTOTAL($)", value=0)
    df = clean_and_convert_columns(df)
    df = df.loc[df["TRANSACTIONTYPE"] != "Credit"]
    df = df.rename(columns={"nanSTATUS": "STATUS"})
    return df

def transform_debit_dispense(df):
    status_column_index = df.columns.get_loc("FEE($)")
    df.insert(loc=status_column_index, column="DISPENSEDTOTAL($)", value=0)
    df.insert(loc=status_column_index, column="COINSTOTAL($)", value=0)
    df = clean_and_convert_columns(df)
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.strip()  # Elimina espacios antes y después
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.replace(r"\s+", " ", regex=True)  # Reemplaza múltiples espacios con uno solo
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.strip()  # Elimina espacios antes y después
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.replace(r"\s+", " ", regex=True)  # Reemplaza múltiples espacios con uno solo
        
    df = df.rename(columns={"nanSTATUS": "STATUS"})
    return df

def transform_pos_ticket_purchase(df):
    status_column_index = df.columns.get_loc("FEE($)")
    df.insert(loc=status_column_index, column="DISPENSEDTOTAL($)", value=0)
    df.insert(loc=status_column_index, column="COINSTOTAL($)", value=0)
    df = clean_and_convert_columns(df)
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.strip()  # Elimina espacios antes y después
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.replace(r"\s+", " ", regex=True)  # Reemplaza múltiples espacios con uno solo
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.strip()  # Elimina espacios antes y después
    df["TRANSACTIONTYPE"] = df["TRANSACTIONTYPE"].str.replace(r"\s+", " ", regex=True)  # Reemplaza múltiples espacios con uno solo
        
    df = df.rename(columns={"nanSTATUS": "STATUS"})
    return df