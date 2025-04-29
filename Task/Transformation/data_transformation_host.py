import pandas as pd

def transform_transaction_lookup(df):
    df['Seq'] = df['Seq'].astype(str)
    df['PAN'] = df['PAN'].astype(str)
    df['Amt. Req'] = df['Amt. Req'].str.replace(',', '', regex=False).astype(str)
    df['Amt. Disp'] = df['Amt. Disp'].str.replace(',', '', regex=False).astype(str)
    df['Amt. Req'] = pd.to_numeric(df['Amt. Req'].str.replace('$', '', regex=False), errors='coerce').astype(float)
    df['Amt. Disp'] = pd.to_numeric(df['Amt. Disp'].str.replace('$', '', regex=False), errors='coerce').astype(float)
    df = df.fillna(0)
    print("Transform methods where applied to TransactionLookup")
    return df

def transform_rpttransactiondetailbytid(df):
    df = df.dropna(axis=1, how='all')
    header_row = df[df.apply(lambda row: row.astype(str).str.contains("Card Number").any(), axis=1)].index[0]
    df.columns = df.iloc[header_row]
    df = df[header_row + 1:].reset_index(drop=True)
    df = df.dropna(thresh=4)
    df = df[~df["Card Number"].astype(str).str.contains("Card Number|Business Date", na=False)]
    df.columns = df.columns.str.strip()
    df = df[df['Card Number'].notna()]
    df['Switch Seq.'] = df['Switch Seq.'].astype(str)
    #df = df.dropna(axis=1, how='all')
    #df = df[df['Switch Seq.'].str.match(r'^\d{4}$')] #Debit transactions only
    # df = pd.DataFrame (df)
    print("Transform methods where applied to rpttransactiondetailbytid")
    return df
