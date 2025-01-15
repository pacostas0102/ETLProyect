import pandas as pd
from Task.Transformation.data_specifictransformation import (
    transform_atm,
    transform_cash_advance,
    transform_debit_dispense,
    transform_pos_ticket_purchase,
)

def transform_data(dataframes):
    transformed_dataframes = []
    for df, file_name in dataframes:
        # Transformación Nulos
        df = df.dropna(axis=1, how='all')
        df = df.fillna(0)

        # Renombrar y filtrar la columna 'DATE & TIME'
        target_column = df.filter(like="DATE & TIME").columns[0]
        df = df.rename(columns={target_column: "DATE & TIME"})
        df = df.loc[df["DATE & TIME"] != "Totals:"]

        # Transformaciones específicas por tipo de archivo
        if "ATM" in file_name:
            df = transform_atm(df)
        elif "Cash_Advance" in file_name:
            df = transform_cash_advance(df)
        elif "Debit_Dispense" in file_name:
            df = transform_debit_dispense(df)
        elif "Pos_Ticket_Purchase" in file_name:
            df = transform_pos_ticket_purchase(df)
        else:
            print(f"Archivo no clasificado: {file_name}")

        transformed_dataframes.append(df)

    return pd.concat(transformed_dataframes, ignore_index=True)
