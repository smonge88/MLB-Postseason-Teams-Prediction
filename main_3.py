####MAIN 1###

#-------------------------------------------------------------------------------------------------------------------------------
#Imports y creación de la sesión Spark

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType, ArrayType, DecimalType, DoubleType
from pyspark.sql.functions import explode, col, round, sum, count, lit, expr, percentile_approx, regexp_replace
from pyspark.sql.functions import *
import os
from functions import read_file, convert_labels, remove_rows, join_sets, join_counts, join_off_def, join_cycle, count_players, build_df_year, union_df
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.appName("Proyecto Samuel Part3")\
                            .config("spark.driver.extraClassPath","postgresql-42.2.14.jar")\
                            .config("spark.executor.extraClassPath","postgresql-42.2.14.jar")\
                            .config("spark.jars","postgresql-42.2.14.jar")\
                            .getOrCreate()

#-------------------------------------------------------------------------------------------------------------------------------
#Genero los dataframes de interés por año con la función build_df_year
df_2010=build_df_year(2010)
df_2009=build_df_year(2009)
df_2008=build_df_year(2008)
df_2007=build_df_year(2007)
df_2006=build_df_year(2006)


#-------------------------------------------------------------------------------------------------------------------------------
#Aplico la función union_df para unir todos los dataframes de cada año creados, primero los agrupo en una lista para luego ingresarlos a la función
list_df_years=[df_2010,df_2009,df_2008,df_2007,df_2006]
df_final = union_df(list_df_years)


#-------------------------------------------------------------------------------------------------------------------------------
#Escribo el dataframe final como un archivo csv, en caso de ocuparse
df_final.coalesce(1).write.csv("df_final_part_3",header=True)


#-------------------------------------------------------------------------------------------------------------------------------
#Escribo el dataframe final que contiene todos los dataframes por año en un solo archivo en una DB de postgres
df_final \
   .write \
   .format("jdbc") \
   .mode('overwrite') \
   .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
   .option("driver", "org.postgresql.Driver")\
   .option("user", "postgres") \
   .option("password", "testPassword") \
   .option("dbtable", "df_final3") \
   .save()



