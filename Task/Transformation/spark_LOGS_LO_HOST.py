from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd

def process_logs_with_spark(df_LogsUnified, result_dflO, dfhost, host_name):
    
    print (host_name)
    # Iniciar la sesión de Spark
    spark = SparkSession.builder.appName("Logs_LO_Host Comparative").getOrCreate()

    # Convertir los DataFrames de Pandas a Spark
    dfLogs = spark.createDataFrame(df_LogsUnified)
    dfLOHOST = spark.createDataFrame(result_dflO)

    dfLogs.printSchema()
    dfLOHOST.printSchema()

    # Realizar el join con Logs y LOHOST
    joined_df = dfLogs.join(
        dfLOHOST,
        dfLogs["seqNumber"] == dfLOHOST["SEQUENCENUMBER"],
        "left"
    )
    sorted_df1 = joined_df.orderBy("seqNumber")

    # Agregar columna de indicador
    result_df2 = sorted_df1.withColumn(
        "found_in_Systems",
        F.when(F.col("SEQUENCENUMBER").isNotNull(), True).otherwise(False)
    )

    # Realizar el join con Logs y HOST

    #if host_name == 'TransactionLookup' :

     #   dfHOST = spark.createDataFrame(dfhost)

     #   sorted_df1 = dfLogs.join(
     #       dfHOST,
     #       dfLogs["AuthNumber"] == dfHOST["seq"],
     #       "left"
     #   )

        # Agregar columna de indicador
     #   result_df3 = sorted_df1.withColumn(
     #       "found_in_datastream",
     #       F.when(F.col("seq").isNotNull(), True).otherwise(False)
     #   )
    if host_name == 'rpttransactiondetailbytid' : 
        print ('Im comparing CDS with LOGS')
        dfhost = dfhost.dropna(axis=1, how='all')
        numeric_columns = ['Amt Req\'d', 'Fee Req\'d', 'Amt Disp.', 'Fee Amt.']

        for col in numeric_columns:
            dfhost[col] = dfhost[col].astype(float)      


        schema = StructType([
            StructField("Terminal DateTime", StringType(), True),   # Fecha y hora como string
            StructField("TranType", StringType(), True),            # Tipo de transacción (W/D)
            StructField("From/To", StringType(), True),             # Cuenta origen o destino (DDA)
            StructField("Card Number", StringType(), True),         # Número de tarjeta enmascarado
            StructField("Amt Req'd", StringType(), True),           # Monto solicitado
            StructField("Fee Req'd", StringType(), True),           # Comisión solicitada
            StructField("Amt Disp.", DoubleType(), True),           # Monto entregado
            StructField("Fee Amt.", StringType(), True),            # Comisión aplicada
            StructField("Seq", StringType(), True),         # Secuencia del terminal
            StructField("Response", StringType(), True),            # Respuesta (EMV, EXPIRED, etc.)
            StructField("Issuer", StringType(), True),              # Emisor (VNT, STR, etc.)
            StructField("Switch Seq.", StringType(), True),         # Secuencia del switch (puede ser numérica o no)
            StructField("file_name", StringType(), True),
        ])

        dfHOST = spark.createDataFrame(dfhost, schema=schema)

        dfHOST = dfHOST.withColumn("last_4_digits", F.regexp_extract(F.col("Card Number"), r"(\d{4})$", 1))

        sorted_df1 = dfLogs.join(
            dfHOST,
            dfLogs["AuthNumber"] == dfHOST["Seq"],
            "left"
        )

        # Agregar columna de indicador
        result_df3 = sorted_df1.withColumn(
            "found_in_CDS",
            F.when(F.col("Seq").isNotNull(), True).otherwise(False)
        )  

    # Convertir los DataFrames de Spark a Pandas para exportarlos
    pandas_df1 = result_df2.toPandas()
    pandas_df2 = result_df3.toPandas()

    return pandas_df1, pandas_df2