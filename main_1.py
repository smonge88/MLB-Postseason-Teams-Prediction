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

spark = SparkSession.builder.appName("Proyecto Samuel Part1")\
                            .config("spark.driver.extraClassPath","postgresql-42.2.14.jar")\
                            .config("spark.executor.extraClassPath","postgresql-42.2.14.jar")\
                            .config("spark.jars","postgresql-42.2.14.jar")\
                            .getOrCreate()

#-------------------------------------------------------------------------------------------------------------------------------
#Genero los dataframes de interés por año con la función build_df_year
df_2021=build_df_year(2021)
df_2019=build_df_year(2019)
df_2018=build_df_year(2018)
df_2017=build_df_year(2017)
df_2016=build_df_year(2016)


#-------------------------------------------------------------------------------------------------------------------------------
#Aplico la función union_df para unir todos los dataframes de cada año creados, primero los agrupo en una lista para luego ingresarlos a la función
list_df_years=[df_2021,df_2019,df_2018,df_2017,df_2016]
df_final = union_df(list_df_years)


#-------------------------------------------------------------------------------------------------------------------------------
#Escribo el dataframe final como un archivo csv, en caso de ocuparse
df_final.coalesce(1).write.csv("df_final_part_1",header=True)


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
   .option("dbtable", "df_final1") \
   .save()


 