import pandas as pd

def transform_transaction_lookup(df):
    """
    Aplica transformaciones a archivos TransactionLookup.
    """
    df['Seq'] = df['Seq'].astype(str)
    df['PAN'] = df['PAN'].astype(str)
    df['Amt. Req'] = df['Amt. Req'].str.replace(',', '', regex=False).astype(str)
    df['Amt. Disp'] = df['Amt. Disp'].str.replace(',', '', regex=False).astype(str)
    df['Amt. Req'] = pd.to_numeric(df['Amt. Req'].str.replace('$', '', regex=False), errors='coerce').astype(float)
    df['Amt. Disp'] = pd.to_numeric(df['Amt. Disp'].str.replace('$', '', regex=False), errors='coerce').astype(float)
    df = df.fillna(0)
    print("Transformaciones aplicadas a TransactionLookup")
    return df

def transform_rpttransactiondetailbytid(df):
    """
    Aplica transformaciones a archivos rpttransactiondetailbytid.
    """
    df = df.dropna(axis=1, how='all')  # Eliminar columnas vacías
    print("Transformaciones aplicadas a rpttransactiondetailbytid")
    return df
