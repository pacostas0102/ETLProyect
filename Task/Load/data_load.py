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
