import os
import pandas as pd

def transform_logs(dfLogs, log_type, output_path, detected_types):
    print ("I'm transforming Filtered Data column")
    dfLogs1 = pd.DataFrame()

    if log_type in ['cards']:
        # Extraer campos comunes
        dfLogs['seqNumber'] = dfLogs["FilteredData"].str.extract(r'"seqNumber":"(\d+)"').astype(str)
        dfLogs['Amount'] = dfLogs["FilteredData"].str.extract(r'"Amount":([0-9]+(?:\.[0-9]+)?)')
        dfLogs['DispensedTotal'] = dfLogs["FilteredData"].str.extract(r'"DispensedTotal":([0-9]+(?:\.[0-9]+)?)')
        dfLogs['Status'] = dfLogs["FilteredData"].str.extract(r'"Status":"([^"]+)"').astype(str)
        dfLogs['AuthNumber'] = dfLogs["FilteredData"].str.extract(r'"AuthNumber":"(\d+)"').astype(str)
        dfLogs['CardNumber'] = dfLogs["FilteredData"].str.extract(r'"CardNumber":"(\d+)"').astype(str)
        dfLogs['HostIP'] = dfLogs["FilteredData"].str.extract(r'"HostIP":"([^"]+)"').astype(str)
        dfLogs['TransactionType'] = dfLogs["FilteredData"].str.extract(r'"TransactionType":"([^"]+)"').astype(str)
        if 'ATM' or 'CASHADVANCE' in detected_types:
            dfLogs['Type'] = dfLogs["FilteredData"].str.extract(r'"type"\s*:\s*"([^"]+)"', expand=False)
    elif log_type in ['bills']:
        print ("BillBreaking logs filter")
        dfLogs['TimeDate'] = dfLogs["FilteredData"].str.extract( r'<TimeDate>([^<]+)</TimeDate>').astype(str)
        dfLogs['seqNumber'] = dfLogs["FilteredData"].str.extract(r'"seqNumber":"(\d+)"').astype(str)
        dfLogs['Amount'] = dfLogs["FilteredData"].str.extract(r'"Amount":([0-9]+(?:\.[0-9]+)?)')
        dfLogs['DispensedTotal'] = dfLogs["FilteredData"].str.extract(r'"DispensedTotal":([0-9]+(?:\.[0-9]+)?)')        
        dfLogs['Status'] = dfLogs["FilteredData"].str.extract(r'"Status":"([^"]+)"').astype(str)
        dfLogs['TicketData'] = dfLogs["FilteredData"].str.extract(r'"TicketData":"([^"]+)"').astype(str)
        dfLogs['Type'] = dfLogs["FilteredData"].str.extract(r'"type"\s*:\s*"([^"]+)"', expand=False)
        for i, denom in enumerate([1, 2, 3, 4, 5, 6], start=1):
            dfLogs[f'BillCount_0{i}_{denom}'] = dfLogs["FilteredData"].str.extract(rf'"BillCount_0{i}":([0-9]+(?:\.[0-9]+)?)')
            dfLogs[f'CoinCount_0{i}_{denom}'] = dfLogs["FilteredData"].str.extract(rf'"CoinCount_0{i}":([0-9]+(?:\.[0-9]+)?)')            
        dfLogs['JournalName'] = log_type  
    elif log_type in ['tickets']:
        print ("TicketRedemption logs filter")
        dfLogs['TimeDate'] = dfLogs["FilteredData"].str.extract( r'<TimeDate>([^<]+)</TimeDate>').astype(str)
        dfLogs['seqNumber'] = dfLogs["FilteredData"].str.extract(r'"seqNumber":"(\d+)"').astype(str)
        dfLogs['Amount'] = dfLogs["FilteredData"].str.extract(r'"Amount":([0-9]+(?:\.[0-9]+)?)')
        dfLogs['DispensedTotal'] = dfLogs["FilteredData"].str.extract(r'"DispensedTotal":([0-9]+(?:\.[0-9]+)?)')        
        dfLogs['Status'] = dfLogs["FilteredData"].str.extract(r'"Status":"([^"]+)"').astype(str)
        dfLogs['Barcode'] = dfLogs["FilteredData"].str.extract(r'<Barcode>(\d+)</Barcode>').astype(str) 
        dfLogs['TicketData'] = dfLogs["FilteredData"].str.extract(r'"TicketData":"([^"]+)"').astype(str)
        dfLogs['Type'] = dfLogs["FilteredData"].str.extract(r'"type"\s*:\s*"([^"]+)"', expand=False)
        for i, denom in enumerate([1, 2, 3, 4, 5, 6], start=1):
            dfLogs[f'BillCount_0{i}_{denom}'] = dfLogs["FilteredData"].str.extract(rf'"BillCount_0{i}":([0-9]+(?:\.[0-9]+)?)')
            dfLogs[f'CoinCount_0{i}_{denom}'] = dfLogs["FilteredData"].str.extract(rf'"CoinCount_0{i}":([0-9]+(?:\.[0-9]+)?)')            
        dfLogs['JournalName']= dfLogs["FilteredData"].str.extract( r'(Receiving from Konami Server|TicketRedemption)').squeeze()
        
   
    dfLogs.to_csv(output_path, index=False)
 #   print (dfLogs1)
    return dfLogs

    #-------------------------------------------------------------------------------------------------------------------------

    