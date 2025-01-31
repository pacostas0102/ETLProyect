import os
import pandas as pd

def transform_logs(df_Logscompleto, log_type):
    """Transforma los logs de diferentes tipos según el tipo de transacción."""
    if log_type == 'ATM':
        filtro = "ATM.     'Posting Transaction Result"
    elif log_type == 'CashAdvance':
        filtro = "Cash Advance Transaction Info. 'Posting Transaction Result"
    elif log_type == 'TicketRedemption':
        filtro = 'Posting: {"type":"TicketRedemption"'
    elif log_type == 'BillBreaking':
        filtro = ' Posting: {"type":"BillBreak"'
    else:
        raise ValueError("Tipo de log desconocido")

    # Filtrar datos específicos
    dfLogs = df_Logscompleto[
        df_Logscompleto.apply(lambda row: row.astype(str).str.contains(filtro).any(), axis=1)
    ]
    
    # Extraer campos comunes
    dfLogs['seqNumber'] = dfLogs["FilteredData"].str.extract(r'"seqNumber":"(\d+)"').astype(str)
    dfLogs['Amount'] = dfLogs["FilteredData"].str.extract(r'"Amount":([0-9]+(?:\.[0-9]+)?)')
    dfLogs['DispensedTotal'] = dfLogs["FilteredData"].str.extract(r'"DispensedTotal":([0-9]+(?:\.[0-9]+)?)')
    dfLogs['Status'] = dfLogs["FilteredData"].str.extract(r'"Status":"([^"]+)"').astype(str)

    if log_type in ['ATM', 'CashAdvance']:
        dfLogs['AuthNumber'] = dfLogs["FilteredData"].str.extract(r'"AuthNumber":"(\d+)"').astype(str)
        dfLogs['CardNumber'] = dfLogs["FilteredData"].str.extract(r'"CardNumber":"(\d+)"').astype(str)
        dfLogs['HostIP'] = dfLogs["FilteredData"].str.extract(r'"HostIP":"([^"]+)"').astype(str)
        dfLogs['TransactionType'] = dfLogs["FilteredData"].str.extract(r'"TransactionType":"([^"]+)"').astype(str)

        if log_type == 'ATM':
            dfLogs['JournalName'] = dfLogs["FilteredData"].str.extract(r'\d+\s+([^\s]+)\s+\'Posting Transaction Result').squeeze().str.replace(r'\.', '', regex=True).astype(str)
        elif log_type == 'CashAdvance':
            dfLogs['JournalName'] = dfLogs["FilteredData"].str.extract(r'------------>\s*(.*?)\s*Transaction Info\.').squeeze().str.replace(r'\.', '', regex=True).astype(str)

    elif log_type in ['TicketRedemption', 'BillBreaking']:
        print ("Entre a Ticket Y Bill")
        dfLogs['TicketData'] = dfLogs["FilteredData"].str.extract(r'"TicketData":"([^"]+)"').astype(str)
        dfLogs['Type'] = dfLogs["FilteredData"].str.extract(r'"type"\s*:\s*"([^"]+)"', expand=False)
        for i, denom in enumerate([1, 5, 10, 20, 50, 100], start=1):
            dfLogs[f'BillDenom_{i}_{denom}'] = dfLogs["FilteredData"].str.extract(rf'"BillCount_{i}":([0-9]+(?:\.[0-9]+)?)')
        dfLogs['JournalName'] = log_type  # Nombre del tipo de log como identificador
        
    print (dfLogs)
    return dfLogs


    #-------------------------------------------------------------------------------------------------------------------------

    