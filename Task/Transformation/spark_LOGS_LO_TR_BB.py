from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col




def process_logs_with_spark_tr_bb(dfLogs, transformed_data_tr_bb, output_path):

    #Validation

    duplicates = dfLogs[dfLogs.duplicated(subset=['seqNumber', 'TicketBarcode'], keep=False)]

    # Identify the groups in which the duplicated has Completed and partial dispensed
    mask = duplicates.groupby(['seqNumber', 'TicketBarcode'])['Status'].transform(lambda x: set(x) == {'COMPLETED', 'PARTIALDISPENSING'})

    # Delette Partial Dispensing status
    filtered_duplicates = duplicates[mask]
    to_drop = filtered_duplicates[filtered_duplicates['Status'] == 'PARTIALDISPENSING']

    #Delete the registers from the original 
    df_cleaned = dfLogs.drop(index=to_drop.index)

    print(df_cleaned)


    dflogtr = dfLogs[dfLogs['JournalName'] == 'TicketRedemption']
    dflogsms = dfLogs[dfLogs['JournalName'] == 'Receiving from Konami Server']

    dflogsms = dflogsms.dropna(axis=1, how='all')

    def buscar_seq(barcode):
        match = dflogtr[dflogtr['TicketBarcode'].str.contains(barcode)]
        if not match.empty:
            return match['seqNumber'].values[0]  # Puedes ajustar si hay múltiples matches
        return None

    dflogsms['seqNumber'] = dflogsms['TicketBarcode'].apply(buscar_seq)
    # Filtrar los registros donde JournalName sea 'TicketRedemption'
    #dfLogs_filtered = dfLogs[dfLogs["JournalName"] == "TicketRedemption"]
#    print (type(transformed_data_tr_bb))
#    df_pandasLOTR = pd.DataFrame(transformed_data_tr_bb) 
    
    print('Im into logs spark process for ticket redemption')
            
    # Inicializar la sesión de Spark
    spark = SparkSession.builder.appName("Logs_LO TR_BB Comparative").getOrCreate()   

    df_sparkLOGSTR = spark.createDataFrame(dflogtr)
    df_sparkLOTR = spark.createDataFrame(transformed_data_tr_bb)
    df_sparkLOSMS = spark.createDataFrame(dflogsms)

    df_sparkLOGSTR.printSchema()
    df_sparkLOSMS.printSchema()

    #SMSbarcodes = [row['TicketBarcode'] for row in df_sparkLOSMS.select("TicketBarcode").distinct().collect()]


    #for barcode in SMSbarcodes:
    #    matching_rows = df_sparkLOGSTR.filter(col("TicketBarcode").contains(barcode))
    #    print(f"Resultados para barcode: {barcode}")
    #    matching_rows.select("seqNumber", "TicketBarcode")

    # Limpia el TicketBarcode de los logs
    #df_sparkLOGSTR = df_sparkLOGSTR.withColumn("TicketBarcode_clean", F.regexp_extract("TicketBarcode", r'^(\d+)', 1))

    # Explota múltiples valores en TICKETBARCODE_FILLED usando split y explode
    #df_sparkLOTR = df_sparkLOTR.withColumn("BarcodeExploded", F.explode(F.split(F.col("VOUCHERSDATA"), ",")))

    #df_sparkLOTR = df_sparkLOTR.withColumn("Barcode_clean", F.regexp_extract("BarcodeExploded", r'^(\d+)', 1))

    # Asigna alias a los DataFrames
    df_sparkLOSMS = df_sparkLOSMS.alias("sms")
    df_sparkLOGSTR = df_sparkLOGSTR.alias("logstr")
    df_sparkLOTR = df_sparkLOTR.alias("lotr")

    # Join entre df_sparkLOSMS y df_sparkLOGSTR usando alias
    matched_df = df_sparkLOSMS.join(
        df_sparkLOGSTR,
        col("sms.seqNumber") == col("logstr.seqNumber"),
        how="left"
    ).drop(col("logstr.seqNumber")).drop(col("logstr.TicketBarcode"))

    # Segundo join con df_sparkLOTR
    matched_df1 = matched_df.join(
        df_sparkLOTR,
        matched_df["seqNumber"] == col("lotr.SEQUENCENUMBER_FILLED"),
        how="left"
    ).drop(col("lotr.SEQUENCENUMBER_FILLED")).drop(col("lotr.TicketBarcode"))

    # Si hay múltiples matches por TicketBarcode, tomamos el primero por orden arbitrario
    #windowSpec = Window.partitionBy("TicketBarcode_clean").orderBy("SEQUENCENUMBER_FILLED")
    #unique_matches = matched_df.withColumn("row_num", row_number().over(windowSpec)).filter(F.col("row_num") == 1)

    result_df = matched_df1.withColumn(
        "found_in_Systems",
        F.when(F.col("SEQUENCENUMBER_FILLED").isNotNull(), True).otherwise(False)
    )#.orderBy(F.col("TimeDate").asc())

    pandas_df = result_df.toPandas()
            #pandas_df = dfLOGSTR.toPandas()
            #pandas_df.to_csv(output_path, index=False)
            
            # Finalizar la sesión de Spark
    spark.stop()


        #print(pandas_df)
    return pandas_df
    
    #return None
