from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_logs_with_spark(df_LogsUnified, result_df, result_dfHOST):
    # Iniciar la sesión de Spark
    spark = SparkSession.builder.appName("Logs_LO_Host Comparative").getOrCreate()

    # Convertir los DataFrames de Pandas a Spark
    dfLogs = spark.createDataFrame(df_LogsUnified)
    dfLOHOST = spark.createDataFrame(result_df)
    dfHOST = spark.createDataFrame(result_dfHOST)

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
        F.when(F.col("seqNumber").isNotNull(), True).otherwise(False)
    )

    # Realizar el join con Logs y HOST
    sorted_df1 = dfLogs.join(
        dfHOST,
        dfLogs["AuthNumber"] == dfHOST["seq"],
        "left"
    )

    # Agregar columna de indicador
    result_df3 = sorted_df1.withColumn(
        "found_in_datastream",
        F.when(F.col("seq").isNotNull(), True).otherwise(False)
    )

    # Convertir los DataFrames de Spark a Pandas para exportarlos
    pandas_df1 = result_df2.toPandas()
    pandas_df2 = result_df3.toPandas()

    return pandas_df1, pandas_df2