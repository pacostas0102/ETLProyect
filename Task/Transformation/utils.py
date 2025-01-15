import os

# para el LiveOffice
def clean_and_convert_columns(df):
    df['TRANSACTIONTYPE'] = df['TRANSACTIONTYPE'].str.strip().replace(r"\s+", " ", regex=True).astype(str)
    df['SEQUENCENUMBER'] = df['SEQUENCENUMBER'].astype(str)
    df['CARDNUMBER'] = df['CARDNUMBER'].astype(str)
    df['HOSTSEQ NUMBER'] = df['HOSTSEQ NUMBER'].astype(str)
    return df

#para el Host
def get_excel_files(folder_path):
    """
    Obtiene todos los archivos Excel en una carpeta.
    """
    return [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(('.xls', '.xlsx'))]
