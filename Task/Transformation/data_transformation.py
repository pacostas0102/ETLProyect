import pandas as pd
from Task.Transformation.data_specifictransformation import (
    transform_atm,
    transform_cash_advance,
    transform_debit_dispense,
    transform_pos_ticket_purchase
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


def transform_data_tr_bb(dataframes):
    transformed_data = []
    
    for data_type, df, file_name in dataframes:
        if data_type == 'TR':
            # Transformaciones específicas para "Voucher Redemption"
            new_columns = df.iloc[0].astype(str) + df.iloc[1].astype(str)
            df.columns = new_columns  # Asignar los nuevos encabezados
            df = df[2:]  # Eliminar las filas de encabezado original
            df.reset_index(drop=True, inplace=True)
            df['file_name'] = file_name  # Agregar nombre del archivo
            
            # Eliminación de columnas con NaN
            df = df.dropna(axis=1, how='all')
            
            # Renombrar columna de fecha y hora
            target_column = df.filter(like="DATE & TIME").columns[0]
            df = df.rename(columns={target_column: "DATE & TIME"})
            df = df.loc[df["DATE & TIME"] != "Totals:"]
            
            # Convertir columnas a tipo float
            columnas_a_convertir = ["TRANSAMOUNT($)", "DISPENSED QTYS$1", "nan$5", "nan$10", "nan$20", "nan$50", "nan$100"]
            df[columnas_a_convertir] = df[columnas_a_convertir].astype(float)
            df.fillna(0, inplace=True)
            transformed_data.append(('TR', df))
        
        elif data_type == 'BB':
            # Transformaciones específicas para "Bill Breaking"
            new_columns = df.iloc[2].astype(str) + df.iloc[3].astype(str)
            df.columns = new_columns  # Asignar los nuevos encabezados
            df = df[4:]  # Eliminar las filas de encabezado original
            df.reset_index(drop=True, inplace=True)
            df['file_name'] = file_name  # Agregar nombre del archivo
            
            # Eliminación de columnas con NaN
            df = df.dropna(axis=1, how='all')
            
            # Renombrar columna de fecha y hora
            target_column = df.filter(like="DATE & TIME").columns[0]
            df = df.rename(columns={target_column: "DATE & TIME"})
            df = df.loc[df["DATE & TIME"] != "Totals:"]
            
            transformed_data.append(('BB', df))
    
    return transformed_data
