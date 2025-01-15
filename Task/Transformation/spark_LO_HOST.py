from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def process_with_spark(df_combined, df_host, output_path):
    # Iniciar sesión de Spark
    spark = SparkSession.builder.appName("Host_LO Comparative").getOrCreate()

    # Convertir Pandas DataFrame a Spark DataFrame
    dfLO = spark.createDataFrame(df_combined)
    dfHOST = spark.createDataFrame(df_host)

    # Renombrar columnas
    dfLO = dfLO.withColumnRenamed("HOSTSEQ NUMBER", "hostseq_number")
    dfHOST = dfHOST.withColumnRenamed("Seq", "seq")

    # Filtrar el DataFrame dfLO
    dfLO = dfLO.filter(F.col("TRANSACTIONTYPE") != "Balance inquiry")

    # Unir ambos DataFrames
    joined_df = dfLO.join(
        dfHOST,
        dfLO["hostseq_number"] == dfHOST["seq"],
        "left"
    )

    # Agregar columna de indicador
    result_df = joined_df.withColumn(
        "found_in_datastream",
        F.when(F.col("seq").isNotNull(), True).otherwise(False)
    )

    # Convertir el DataFrame de Spark a Pandas
    pandas_df1 = result_df.toPandas()    
    pandas_df2 = dfHOST.toPandas()

    return pandas_df1,pandas_df2
