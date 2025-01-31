# Task/Load/load.py

import pandas as pd

def load_to_excel(dataframe, output_path):
    """
    Carga un DataFrame en un archivo Excel.
    
    :param dataframe: El DataFrame que se desea exportar.
    :param output_path: La ruta del archivo Excel de destino.
    """
    try:
        dataframe.to_excel(output_path, index=False, engine='openpyxl')
        print(f"Archivo exportado exitosamente a {output_path}")
    except Exception as e:
        print(f"Error al exportar el archivo: {e}")

# Función para cargar los DataFrames en un archivo Excel
def load_to_excel_tr_bb(transformed_data, output_path3):
    with pd.ExcelWriter(output_path3, engine='openpyxl') as writer:
        for data_type, df in transformed_data:
            if data_type == 'TR':
                df.to_excel(writer, sheet_name='TRLO', index=False)  # Guardar "TR" en la hoja 'TRLO'
            elif data_type == 'BB':
                df.to_excel(writer, sheet_name='BBLO', index=False)  # Guardar "BB" en la hoja 'BBLO'
