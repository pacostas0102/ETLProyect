from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_logs_with_spark_tr_bb(dfLogsTR, dfLogsbb, transformed_data_tr_bb):

    df_bb = next((df for data_type, df in transformed_data_tr_bb if data_type == 'BB'), None)
    df_tr = next((df for data_type, df in transformed_data_tr_bb if data_type == 'TR'), None)


    # Iniciar la sesión de Sparkspark = SparkSession.builder.appName("Logs_LO TR_BB Comparative").getOrCreate()
    spark = SparkSession.builder.appName("Logs_LO TR_BB Comparative").getOrCreate()

    #Pandas a Spark
    #print(dfLogsTR.head())
    #dfLogsTR = dfLogsTR.toPandas()
    dfLOGSTR = spark.createDataFrame(dfLogsTR)
    dfLogsBB = spark.createDataFrame(dfLogsbb)


    #print (df_tr)
    dfLOTR = spark.createDataFrame(df_tr)
    dfLOBB = spark.createDataFrame(df_bb) 


    #Renombre de columnas
    #dfLO = dfLO.withColumnRenamed("HOSTSEQ NUMBER", "hostseq_number")
    #dfHOST = dfHOST.withColumnRenamed("Seq", "seq")

    # Filtrar el DataFrame dfLO
    #dfLO = dfLO.filter(F.col("STATUS") != "Denied")
    #dfLO = dfLO.filter(F.col("TRANSACTIONTYPE") != "Balance inquiry")


    #Verificación
    print('Dataframe de LiveOffice')
    dfLOBB.printSchema()
    print('Dataframe de Logs')
    dfLogsBB.printSchema()

    #dfLOGSTR = dfLOGSTR.filter(F.col("SEQUENCENUMBER") != "")


    #-Join Logs con LOHOSTUNIFIED
    joined_df1 = dfLOGSTR.join(
        dfLOTR,
        dfLOGSTR["seqNumber"] == dfLOTR["SEQUENCENUMBER"],
        "left"
    )

    sorted_df1 = joined_df1.orderBy("seqNumber")


    # Agregar columna de indicador

    result_df2 = sorted_df1.withColumn(
        "found_in_Systems",
        F.when(F.col("SEQUENCENUMBER").isNotNull(), True).otherwise(False)
    )

    result_df2.show()

    #-Join Logs con HOST
    joined_df2 = dfLogsBB.join(
        dfLOBB,
        dfLogsBB["seqNumber"] == dfLOBB["SEQUENCENUMBER"],
        "left"
    )

    sorted_df2 = joined_df2.orderBy("seqNumber")

    # Agregar columna de indicador
    result_df3 = sorted_df2.withColumn(
        "found_in_datastream",
        F.when(F.col("SEQUENCENUMBER").isNotNull(), True).otherwise(False)
    )

    result_df3.show()

    # Convertir los DataFrames de Spark a Pandas para exportarlos
    pandas_df1 = result_df2.toPandas()
    pandas_df2 = result_df3.toPandas()

    return pandas_df1, pandas_df2