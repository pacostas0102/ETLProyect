import os
import pandas as pd

def extract_logs(Logs_path, option, output_path, column_names=["FilteredData"]):
    logs_dataframes = []
    detected_types = []
    found_filters = {filtro: 0 for filtro in ["SMS", "TicketRedemption", "ATM", "CashAdvance", "BillBreaking"]}

    # Mapear las opciones del usuario a las carpetas correctas
    folder_mapping = {
        "tickets": ["ticketredemption", "sms"],
        "cards": ["atm", "cashadvance"],
        "bills": ["billbreaking"]
    }

    # Definir los filtros por tipo de log
    filters = {
        'ATM': ["ATM.     'Posting Transaction Result",  '"action":"Posting Transaction Result"'],
        'CashAdvance': ["Cash Advance Transaction Info. 'Posting Transaction Result"],
        'TicketRedemption': ['Posting: {"type":"TicketRedemption"'],
        'BillBreaking': ['Posting: {"type":"BillBreak"'],
        'SMS': [
            'Receiving from Konami Server <?xml version="1.0" encoding="UTF-8" standalone="no"?><Message Command="Redeem Ticket"'
        ]
    }

    # Relacionar la opción ingresada con los filtros adecuados
    filter_mapping = {
        "tickets": filters["SMS"] + filters["TicketRedemption"],
        "cards": filters["ATM"] + filters["CashAdvance"],
        "bills": filters["BillBreaking"]
    }



    # Obtener las carpetas y filtros correctos
    selected_folders = folder_mapping.get(option.lower(), [])
    selected_filters = filter_mapping.get(option.lower(), [])

    if not selected_filters:
        raise ValueError("Unknown log type option")

    for subcarpeta in os.listdir(Logs_path):
        ruta_subcarpeta = os.path.join(Logs_path, subcarpeta)

        # Procesar solo las carpetas seleccionadas
        if os.path.isdir(ruta_subcarpeta) and subcarpeta.lower() in selected_folders:
            print(f"Processing: {ruta_subcarpeta}")

            for archivo in os.listdir(ruta_subcarpeta):
                if archivo.lower().endswith(('.jrn', '.log')):  
                    ruta_completa = os.path.join(ruta_subcarpeta, archivo)
                    
                    filtered_lines = []
                    with open(ruta_completa, 'r', encoding='utf-8') as file:
                        for line in file:
                            line = line.strip()
                            for filtro in selected_filters:
                                if filtro in line:
                                    filtered_lines.append(line)
                                    # Contar cuántas líneas coinciden con cada filtro
                                    for key, values in filters.items():
                                        if filtro in values:
                                            found_filters[key] += 1
             
                    
                    if filtered_lines:
                        logs_df = pd.DataFrame(filtered_lines, columns=column_names)
                        logs_dataframes.append(logs_df)
            detected_types.append(subcarpeta)         

    # Combinar los logs extraídos en un solo DataFrame
    if logs_dataframes:
        df_logs = pd.concat(logs_dataframes, ignore_index=True)
        print("✅ Logs Extraction finished")
    else:
        print("⚠️ No logs found for the selected option.")
        df_logs = pd.DataFrame(columns=column_names)

    # Mostrar cuántas líneas se encontraron para cada tipo de log
    for key, count in found_filters.items():
        if count > 0:
            print(f"✅ Se encontraron {count} líneas para {key}")
        elif key in filter_mapping[option.lower()]:  # Solo avisar de los filtros esperados
            print(f"⚠️ No se encontraron datos para {key}")

    print (detected_types)        

    return df_logs, detected_types
