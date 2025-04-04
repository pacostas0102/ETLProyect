# Task/Load/load.py

import pandas as pd
import os

def load_to_excel(dataframe, output_path,sheet_name):
    try:
            # Verificar si el archivo existe
            if not os.path.exists(output_path):
                # Si no existe, crear nuevo archivo con la hoja especificada
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    dataframe.to_excel(writer, index=False, sheet_name=sheet_name)
            else:
                # Si ya existe, agregar una nueva hoja
                with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists='new') as writer:
                    dataframe.to_excel(writer, index=False, sheet_name=sheet_name)

            print(f"✅ Archivo exportado exitosamente en '{output_path}', hoja '{sheet_name}'")

    except Exception as e:
        print(f"❌ Error al exportar el archivo: {e}")

# Función para cargar los DataFrames en un archivo Excel
def load_to_excel_tr_bb(transformed_data, output_path3):
    with pd.ExcelWriter(output_path3, engine='openpyxl') as writer:
        for data_type, df in transformed_data:
            if data_type == 'TR':
                df.to_excel(writer, sheet_name='TRLO', index=False)  # Guardar "TR" en la hoja 'TRLO'
            elif data_type == 'BB':
                df.to_excel(writer, sheet_name='BBLO', index=False)  # Guardar "BB" en la hoja 'BBLO'
