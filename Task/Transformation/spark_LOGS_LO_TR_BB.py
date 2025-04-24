from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd

def process_logs_with_spark_tr_bb(dfLogs, transformed_data_tr_bb, output_path):
    # Filtrar los registros donde JournalName sea 'TicketRedemption'
    dfLogs_filtered = dfLogs[dfLogs["JournalName"] == "TicketRedemption"]
#    print (type(transformed_data_tr_bb))
#    df_pandasLOTR = pd.DataFrame(transformed_data_tr_bb) 
    if not dfLogs_filtered.empty:
        print('Im into logs spark process for ticket redemption')
            
            # Inicializar la sesión de Spark
        spark = SparkSession.builder.appName("Logs_LO TR_BB Comparative").getOrCreate()
            
            # Convertir los DataFrames de Pandas a Spark
        df_sparkLOGSTR = spark.createDataFrame(dfLogs)        
        df_sparkLOTR = spark.createDataFrame(transformed_data_tr_bb)
            #dfLOTR = spark.createDataFrame(transformed_data_tr_bb)
            
        print('Esquema de DataFrame de Logs:')
        df_sparkLOGSTR.printSchema()
        print('Esquema de DataFrame de Transformed Data:')
        df_sparkLOTR.printSchema()

            #dfLOTR.printSchema()
            
            # Realizar la unión de los DataFrames solo con los registros filtrados
        joined_df = df_sparkLOGSTR.join(
            df_sparkLOTR,
            df_sparkLOGSTR["seqNumber"] == df_sparkLOTR["SEQUENCENUMBER"],
            "left"
        )
            
            # Agregar columna de indicador
        result_df = joined_df.withColumn(
            "found_in_Systems",
            F.when(F.col("SEQUENCENUMBER").isNotNull(), True).otherwise(False)
        )
            
            #result_df.show()
            
            # Convertir DataFrame de Spark a Pandas y exportar a CSV
        pandas_df = result_df.toPandas()
            #pandas_df = dfLOGSTR.toPandas()
            #pandas_df.to_csv(output_path, index=False)
            
            # Finalizar la sesión de Spark
        spark.stop()


        #print(pandas_df)
    return pandas_df
    
    return None
