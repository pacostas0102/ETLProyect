from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pandas as pd

def process_with_spark(df_combined, df_host, host_name, output_path):
    
    df_host = df_host.dropna(axis=1, how='all')

    print("host_name")
    print(host_name)
     
    if "rpttransactiondetailbytid" in host_name:

        print(' Im comparing CDS report with LO reports')

        numeric_columns = ['Amt Req\'d', 'Fee Req\'d', 'Amt Disp.', 'Fee Amt.']

        for col in numeric_columns:
            df_host[col] = df_host[col].astype(float)


        # Iniciar sesión de Spark
        spark = SparkSession.builder.appName("Host_LO Comparative1").getOrCreate()
        
        # Convertir Pandas DataFrame a Spark DataFrame
        dfLO = spark.createDataFrame(df_combined)

        dfLO.printSchema()

        schema = StructType([
            StructField("Terminal DateTime", StringType(), True),   # Fecha y hora como string
            StructField("TranType", StringType(), True),            # Tipo de transacción (W/D)
            StructField("From/To", StringType(), True),             # Cuenta origen o destino (DDA)
            StructField("Card Number", StringType(), True),         # Número de tarjeta enmascarado
            StructField("Amt Req'd", StringType(), True),           # Monto solicitado
            StructField("Fee Req'd", StringType(), True),           # Comisión solicitada
            StructField("Amt Disp.", DoubleType(), True),           # Monto entregado
            StructField("Fee Amt.", StringType(), True),            # Comisión aplicada
            StructField("Term  Seq.", StringType(), True),         # Secuencia del terminal
            StructField("Response", StringType(), True),            # Respuesta (EMV, EXPIRED, etc.)
            StructField("Issuer", StringType(), True),              # Emisor (VNT, STR, etc.)
            StructField("Switch Seq.", StringType(), True),         # Secuencia del switch (puede ser numérica o no)
            StructField("file_name", StringType(), True),
        ])

        dfHOST = spark.createDataFrame(df_host, schema=schema)
        dfHOST.printSchema()

       
            
        dfLO = dfLO.withColumnRenamed("HOSTSEQ NUMBER", "hostseq")
        dfHOST = dfHOST.withColumnRenamed("Term  Seq.", "Seq")
        dfLO = dfLO.filter(F.col("TRANSACTIONTYPE") != "Balance inquiry")

        dfLO.printSchema()
            
        dfHOST = dfHOST.withColumn("last_4_digits", F.regexp_extract(F.col("Card Number"), r"(\d{4})$", 1))
            
        joined_df = dfLO.join(
            dfHOST,
            dfLO["hostseq"] == dfHOST["Seq"],
            "left"
        )
            
        result_df = joined_df.withColumn(
            "found_in_cds",
            F.when(F.col("Seq").isNotNull(), True).otherwise(False)
        ).orderBy(F.col("DATE & TIME").asc())


        #print(result_df.head(10)) 
    elif "TransactionLookup" in host_name:
        print(' Im comparing DataStream report with LO reports')
        # Iniciar sesión de Spark
        spark = SparkSession.builder.appName("Host_LO Comparative2").getOrCreate()

        # Convertir Pandas DataFrame a Spark DataFrame
        dfLO = spark.createDataFrame(df_combined)
        dfHOST = spark.createDataFrame(df_host)
        dfLO.printSchema()
        dfHOST.printSchema()

        dfLO = dfLO.withColumnRenamed("HOSTSEQ NUMBER", "hostseq_number")
        dfHOST = dfHOST.withColumnRenamed("Seq", "seq")

            
        dfLO = dfLO.filter(F.col("TRANSACTIONTYPE") != "Balance inquiry")

            
        joined_df = dfLO.join(
            dfHOST,
            dfLO["hostseq_number"] == dfHOST["seq"],
            "left"
        )
            
        result_df = joined_df.withColumn(
            "found_in_datastream",
            F.when(F.col("seq").isNotNull(), True).otherwise(False)
        )
    

    # Convertir el DataFrame de Spark a Pandas
    pandas_df1 = result_df.toPandas()
    #pandas_df2 = dfHOST.toPandas()

    spark.stop()

    return pandas_df1#,pandas_df2
