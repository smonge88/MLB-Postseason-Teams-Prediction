####Functions####

#-------------------------------------------------------------------------------------------------------------------------------
#Imports y creación de la sesión Spark

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType, ArrayType, DecimalType, DoubleType
from pyspark.sql.functions import explode, col, round, sum, count, lit, expr, percentile_approx, regexp_replace
from pyspark.sql.functions import *
import os

spark = SparkSession.builder.appName("Proyecto Samuel").getOrCreate()


#-------------------------------------------------------------------------------------------------------------------------------
#Función 1. read_file: lectura de los archivos csv desde la dirección dada
def read_file(name_file,year):
    folder = "Files/"+str(year)+"/"
    file=name_file+"_"+str(year)+".csv"
    df_file = spark.read.option("delimiter", ",").option("header", True).option('inferSchema',True).csv(os.path.join(folder,file))
    return df_file

#-------------------------------------------------------------------------------------------------------------------------------
#Función 2. convert_labels: convierto etiquetas de string (Y/N) a numérico binario
def convert_labels(df,col,name):
    df_converted=df.withColumn(name,when(col == "Y",1).otherwise(0))
    df_converted=df_converted.drop(col)
    return df_converted

#-------------------------------------------------------------------------------------------------------------------------------
#Función 3. remove_rows: remuevo filas cuyo valor en la columna especificada sea menor o mayor que valor umbral definido
def remove_rows(df,colu,criterio,val):
    if criterio=="mayor que":
        filtered_df=df.filter(col(colu)>=val)
    elif criterio=="menor que":
        filtered_df=df.filter(col(colu)<=val)
    elif criterio=="dos equipos":
        filtered_df=df.where(col(colu) != val)
    return filtered_df

#-------------------------------------------------------------------------------------------------------------------------------
#Función 4. join_sets: hago join de los subsets relacionados con los equipos (DS1,DS3,DS5,DS7,DS7)
def join_sets(list_sets,colu):
    df=list_sets[0].join(list_sets[1],[colu],"inner") \
     .join(list_sets[2],[colu],"inner")\
     .join(list_sets[3],[colu],"inner")\
     .join(list_sets[4],[colu],"inner")\
     .sort([colu])
    return df
    
#-------------------------------------------------------------------------------------------------------------------------------
#Función 5. join_counts: hago join de los subsets relacionados con los conteos de jugadores según la métrica especificada (colu) para los DS2 y DS4
def join_counts(list_counts,colu):
    df=list_counts[0].join(list_counts[1],[colu],"fullouter")\
                     .join(list_counts[2],[colu],"fullouter")\
                     .join(list_counts[3],[colu],"fullouter")\
                     .join(list_counts[4],[colu],"fullouter")\
                     .join(list_counts[5],[colu],"fullouter")\
                     .join(list_counts[6],[colu],"fullouter")\
                     .join(list_counts[7],[colu],"fullouter")\
                     .join(list_counts[8],[colu],"fullouter")\
                     .join(list_counts[9],[colu],"fullouter")\
                     .sort([colu])\
                     .na.fill(value=0)
    return df

#-------------------------------------------------------------------------------------------------------------------------------
#Función 6. join_off_def: hago join de los subsets relacionados con los conteos de jugadores en ofensiva y con los de defensiva
def join_off_def(list_counts,colu):
    df=list_counts[0].join(list_counts[1],[colu],"fullouter")\
                     .sort([colu])\
                     .na.fill(value=0)
    return df

#-------------------------------------------------------------------------------------------------------------------------------
#Función 7. join_cycle: join general con ciclo for que permite hacer unión de la lista de sets que se ingrese sin importar su tamaño
#Función hecha como una opción a explorar *** Sin embargo, no se usó en el programa por tratarse de un ciclo for!!
def join_cycle(list_join,colu,type_join):
    for i in range(len(list_join)-1):
        df=list_join[0].join(list_join[i+1],[colu],type_join).na.fill(value=0)               
    df=df.sort([colu])
    return df

#-------------------------------------------------------------------------------------------------------------------------------
#Función 8. count_players: permite contar y ordenar la lista de jugadores que cumplen con cierto criterio, agrupándolos por el equipo    
def count_players(df,colu_group,colu_count,name_new_col,criterio,val):
    df_pre_count=remove_rows(df,colu_count,criterio,val)
    df_count=df_pre_count.groupBy(col(colu_group)) \
                .agg(count(col(colu_count)).alias(name_new_col))\
                .sort(col(colu_group))         
    return df_count

#-------------------------------------------------------------------------------------------------------------------------------
#Función 9. build_df_year: función que utiliza todas las funciones anteriores con el objetivo de retornar un data frame que recopila todos
#los atributos de interés (como columnas) de este proyecto por todos los equipos (como filas) para un año en específico   
def build_df_year(year):
    
    #Read files
    tbat=read_file("tbat",year)
    pbat=read_file("pbat",year)
    tpit=read_file("tpit",year)
    ppit=read_file("ppit",year)
    tfie=read_file("tfie",year)
    tmis=read_file("tmis",year)
    post=read_file("post",year)
    
    #Reassigning tmis schema (attendance and payroll): removing $ sign and commas
    tmis = tmis.withColumnRenamed("Est. Payroll","Payroll")
    tmis = tmis.withColumn('Attendance', regexp_replace('Attendance', ',', ''))
    tmis = tmis.withColumn('Payroll', regexp_replace('Payroll', r'\$', ''))\
               .withColumn('Payroll', regexp_replace('Payroll', ',', ''))
    tmis = tmis.withColumn('Attendance', tmis['Attendance'].cast("float"))\
               .withColumn('Payroll', tmis['Payroll'].cast("float"))
    
    #TBAT (ds1) cols left: Tm,BatAge,R/G,PA,R,H,HR,SB,CS,BB,SO,BA,OBP,SLG,GDP,HBP,SH,SF,IBB,LOB [20]
    remove_cols_tbat = ("#Bat","G","AB","RBI","2B","3B","CS","OPS","OPS+","TB")
    ds1=tbat.drop(*remove_cols_tbat)
    ds1=ds1.withColumnRenamed("BB","bBB").withColumnRenamed("H","bH").withColumnRenamed("HBP","bHBP")\
       .withColumnRenamed("HR","bHR").withColumnRenamed("LOB","bLOB").withColumnRenamed("R","bR").withColumnRenamed("SO","bSO")

    #PBAT (ds2) cols left: Rk,Tm,G,PA,R,H,HR,RBI,SB,BB,SO,BA,OBP,SLG,OPS,GDP [16]   "Name"
    remove_cols_pbat = ("Age","PAge","Lg","AB","2B","3B","CS","OPS+","TB","HBP","SH","SF","IBB","Pos Summary","Name-additional")
    ds2=pbat.drop(*remove_cols_pbat)
    ds2=ds2.withColumnRenamed("Tm","Abb")

    #TPIT (ds3) cols left: Tm,PAge,RA/G,ERA,CG,tSho,cSho,SV,IP,H,R,HR,BB,SO,HBP,BK,WP,WHIP,LOB [19]
    remove_cols_tpit = ("#P","W","L","W-L%","G","GS","GF","ER","IBB","BF","ERA+","FIP","H9","HR9","BB9","SO9","SO/W")
    ds3=tpit.drop(*remove_cols_tpit)
    ds3=ds3.withColumnRenamed("BB","pBB").withColumnRenamed("H","pH").withColumnRenamed("HBP","pHBP")\
       .withColumnRenamed("HR","pHR").withColumnRenamed("LOB","pLOB").withColumnRenamed("R","pR").withColumnRenamed("SO","pSO")

    #PPIT (ds4) cols left: Rk,Tm,W,L,ERA,CG,SHO,SV,IP,H,R,HR,BB,SO,HBP,WP,WHIP [17]   "Name"
    remove_cols_ppit = ("Age","Lg","W-L%","G","GS","GF","ER","IBB","BK","BF","ERA+","FIP","H9","HR9","BB9","SO9","SO/W","Name-additional")
    ds4=ppit.drop(*remove_cols_ppit)
    ds4=ds4.withColumnRenamed("Tm","Abb")

    #TFIE (ds5) cols left: Tm,E,DP [3]
    remove_cols_tfie = ("#Fld","RA/G","DefEff","G","GS","CG","Inn","Ch","PO","A","Fld%","Rtot","Rtot/yr","Rdrs","Rdrs/yr","Rgood")
    ds5=tfie.drop(*remove_cols_tfie) 

    #TMIS (ds6) cols left: Tm,Attendance, Payroll [3]
    remove_cols_tmis = ("Attend/G","BatAge","PAge","BPF","PPF","#HOF","#A-S","#a-tA-S","Time","Chall","Succ","Succ%","Managers")
    ds6=tmis.drop(*remove_cols_tmis)
    
    #Filter rows (hitters with more than 100 Plate Appearances)
    ds2=remove_rows(ds2,"PA","mayor que",100)
    ds2=remove_rows(ds2,"Abb","dos equipos","TOT")
    
    #Filter rows (pitchers with more than 20 Innings Pitched)
    ds4=remove_rows(ds4,"IP","mayor que",70)
    ds4=remove_rows(ds4,"Abb","dos equipos","TOT")
    
    #Convert postseason labels to numerical
    ds7=convert_labels(post,post.Postseason,"Post")
    
    #Initial join for datasets using Tm as parameter
    list_datasets=[ds1,ds3,ds5,ds6,ds7]
    df_subsets=join_sets(list_datasets,"Tm")
    
    # Offensive count columns from DS2 (Player Batting Stats)
    count_off_hits=count_players(ds2,"Abb","H","qbH","mayor que",100)
    count_off_runs=count_players(ds2,"Abb","R","qbR","mayor que",80)
    count_off_hrs=count_players(ds2,"Abb","HR","qbHR","mayor que",25)
    count_off_rbis=count_players(ds2,"Abb","RBI","qbRBI","mayor que",80)
    count_off_sb=count_players(ds2,"Abb","SB","qbSB","mayor que",10)
    count_off_bb=count_players(ds2,"Abb","BB","qbBB","mayor que",40)
    count_off_so=count_players(ds2,"Abb","SO","qbSO","mayor que",100)
    count_off_ba=count_players(ds2,"Abb","BA","qbBA","mayor que",0.3)
    count_off_obp=count_players(ds2,"Abb","OBP","qbOBP","mayor que",0.36)
    count_off_slg=count_players(ds2,"Abb","SLG","qbSLG","mayor que",0.5)

    list_counts_off=[count_off_hits,count_off_runs,count_off_hrs,count_off_rbis,count_off_sb,\
                 count_off_bb,count_off_so,count_off_ba,count_off_obp,count_off_slg]
    
    df_off_counts=join_counts(list_counts_off,"Abb")
    
    # Defensive count columns from DS4 (Player Pitching Stats) para un minimo de 70 IP
    count_def_w=count_players(ds4,"Abb","W","qpW","mayor que",10) 
    count_def_l=count_players(ds4,"Abb","L","qpL","menor que",4) 
    count_def_era=count_players(ds4,"Abb","ERA","qpERA","menor que",3.50) 
    count_def_so=count_players(ds4,"Abb","SO","qpSO","mayor que",100) 
    count_def_hits=count_players(ds4,"Abb","H","qpH","menor que",100) 
    count_def_hrs=count_players(ds4,"Abb","HR","qpHR","menor que",15)
    count_def_bb=count_players(ds4,"Abb","BB","qpBB","menor que",40) 
    count_def_runs=count_players(ds4,"Abb","R","qpR","menor que",50) 
    count_def_whip=count_players(ds4,"Abb","WHIP","qpWHIP","mayor que",1.2)  
    count_def_wp=count_players(ds4,"Abb","WP","qpWP","menor que",3)  

    list_counts_def=[count_def_w,count_def_l,count_def_era,count_def_so,count_def_hits,count_def_hrs,\
                 count_def_bb,count_def_runs,count_def_whip,count_def_wp]
    
    df_def_counts=join_counts(list_counts_def,"Abb")
    
    # Join offensive and defensive count columns player stats together (DS2 and DS4)
    list_ds2_ds4=[df_off_counts,df_def_counts]
    ds2_d4_joined=join_off_def(list_ds2_ds4,"Abb")
    
    # Join DS2 and DS4 to the rest of the subsets
    list_all_DS=[df_subsets,ds2_d4_joined]
    df_total=join_off_def(list_all_DS,"Abb")
    
    return df_total

#-------------------------------------------------------------------------------------------------------------------------------
#Función 10. union_cycle_df: función que sirve para unir un dataframe debajo del otro, al mantener las columnas. Trabaja con una lista
#de los dataframes que tiene que unir
#Función hecha como una opción a explorar *** Sin embargo, no se usó en el programa por tratarse de un ciclo for!!
def union_cycle_df(list_df):
    df=list_df[0]
    for i in range(len(list_df)-1):
        df=df.union(list_df[i+1])
    return df
    
#-------------------------------------------------------------------------------------------------------------------------------
#Función 11. union_df: misma función anterior pero no utiliza ciclo for para hacer la unión de dataframes. El primer if corresponde a la
#ejecución del main_1 y el segundo if corresponde a la ejecución del main_2
def union_df(list_df):
    df=list_df[0].union(list_df[1])\
                 .union(list_df[2])\
                 .union(list_df[3])\
                 .union(list_df[4])
    return df

#-------------------------------------------------------------------------------------------------------------------------------
#Función 12. union_df_2: misma función anterior pero ocupa únicamente como entrada una lista con 2 dataframes. Esto para probar union_df
#en pytest de manera indirecta.  
def union_df_2(list_df):
    df=list_df[0].union(list_df[1])
    return df