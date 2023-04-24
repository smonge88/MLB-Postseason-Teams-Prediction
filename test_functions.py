from .functions import read_file, convert_labels, remove_rows, join_sets, join_counts, join_off_def, join_cycle, count_players, build_df_year, union_df, union_df_2

#Para los tests, se utilizan ejemplos varios de dataframes reducidos de los datasets que se están utilizando para el desarrollo del proyecto.
#Se pretende demostrar la misma funcionalidad que tienen los datasets de verdad al usarlos como una muestra representativa de los datasets verdaderos.

#**************************************************************************************
#Test 1: lectura de csv desde la carpeta Files (donde se encuentran el resto de folders y archivos de los datasets del proyecto
def test_read_file(spark_session):
    df=read_file('test_post',2020)
    correct_df=spark_session.createDataFrame([("Arizona Diamondbacks","ARI","N"),
                                            ("Atlanta Braves","ATL","Y"),
                                            ("Baltimore Orioles","BAL","N")],
                                           ["Tm","Abb","Postseason"])
    assert df.collect()==correct_df.collect()

#**************************************************************************************
#Test 2: se prueba la función convert_labels, para convertir la etiqueta de postemporada de str a binario (1/0).
def test_convert_labels(spark_session):
    df=spark_session.createDataFrame([("Arizona Diamondbacks","ARI","N"),
                                            ("Los Angeles Dodgers","LAD","Y"),
                                            ("Minnesota Twins","MIN","N"),
                                            ("San Diego Padres","SDP","N")],
                                           ["Tm","Abb","Postseason"])
                                           
    df_converted=convert_labels(df, df.Postseason,"Post")
    
    correct_df=spark_session.createDataFrame([("Arizona Diamondbacks","ARI",0),
                                            ("Los Angeles Dodgers","LAD",1),
                                            ("Minnesota Twins","MIN",0),
                                            ("San Diego Padres","SDP",0)],
                                           ["Tm","Abb","Post"])
    
    assert df_converted.collect()==correct_df.collect()


#**************************************************************************************
#Test 3: se prueba la función remove_rows (la condición mayor que). Se filtran únicamente los jugadores con 20 o más homeruns.
def test_remove_rows(spark_session):
    df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (5,"Joey Gallo","TOT",38,146),
                                            (6,"Joey Gallo","TEX",21,82),
                                            (7,"Joey Gallo","NYY",17,64)],
                                            ["ID","Name","Tm","HR","SO"])
    
    df_rows_removed=remove_rows(df,"HR","mayor que",20)
    
    correct_df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (5,"Joey Gallo","TOT",38,146),
                                            (6,"Joey Gallo","TEX",21,82)],
                                            ["ID","Name","Tm","HR","SO"])
   
    assert df_rows_removed.collect()==correct_df.collect()


# **************************************************************************************
#Test 4: se prueba la función remove_rows (la condición menor que). Se filtran únicamente los jugadores con 100 o menos strikeouts.
def test_remove_rows_2(spark_session):
    df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (5,"Joey Gallo","TOT",38,146),
                                            (6,"Joey Gallo","TEX",21,82),
                                            (7,"Joey Gallo","NYY",17,64)],
                                            ["ID","Name","Tm","HR","SO"])
    
    df_rows_removed=remove_rows(df,"SO","menor que",100)
    
    correct_df=spark_session.createDataFrame([(3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (6,"Joey Gallo","TEX",21,82),
                                            (7,"Joey Gallo","NYY",17,64)],
                                            ["ID","Name","Tm","HR","SO"])
   
    assert df_rows_removed.collect()==correct_df.collect()


# **************************************************************************************
#Test 5: se prueba la función remove_rows (eliminar fila que corresponde a las estadísticas totales de un jugador que estuvo en 2 equipos durante
#la misma temporada).
def test_remove_rows_3(spark_session):
    df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (5,"Joey Gallo","TOT",38,146),
                                            (6,"Joey Gallo","TEX",21,82),
                                            (7,"Joey Gallo","NYY",17,64)],
                                            ["ID","Name","Tm","HR","SO"])
    
    df_rows_removed=remove_rows(df,"Tm","dos equipos","TOT")
    
    correct_df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (6,"Joey Gallo","TEX",21,82),
                                            (7,"Joey Gallo","NYY",17,64)],
                                            ["ID","Name","Tm","HR","SO"])
   
    assert df_rows_removed.collect()==correct_df.collect()

    
# **************************************************************************************
#Test 6: se prueba la función convert_labels, para convertir la etiqueta de postemporada de str a binario (1/0).
def test_join_sets(spark_session):
    df1=spark_session.createDataFrame([("Arizona Diamondbacks",298,632),
                                            ("Los Angeles Dodgers",163,741),
                                            ("Minnesota Twins",214,763),
                                            ("San Diego Padres",236,699)],
                                           ["Tm","HR","SO"])
                                           
    df2=spark_session.createDataFrame([("Arizona Diamondbacks",34,69),
                                            ("Los Angeles Dodgers",56,45),
                                            ("Minnesota Twins",49,98),
                                            ("San Diego Padres",74,55)],
                                           ["Tm","Errors","GDP"])
    
    df3=spark_session.createDataFrame([("Arizona Diamondbacks",3.65,2.03),
                                            ("Los Angeles Dodgers",2.96,1.99),
                                            ("Minnesota Twins",4.32,2.26),
                                            ("San Diego Padres",6.01,2.59)],
                                           ["Tm","ERA","WHIP"])    
    
    df4=spark_session.createDataFrame([("Arizona Diamondbacks",2,3),
                                            ("Los Angeles Dodgers",0,1),
                                            ("Minnesota Twins",1,4),
                                            ("San Diego Padres",3,2)],
                                           ["Tm","qHR","qSO"])
    
    df5=spark_session.createDataFrame([("Arizona Diamondbacks","ARI",0),
                                            ("Los Angeles Dodgers","LAD",1),
                                            ("Minnesota Twins","MIN",0),
                                            ("San Diego Padres","SDP",0)],
                                           ["Tm","Abb","Post"])    
                                           
    list_df=[df1,df2,df3,df4,df5]
                                           
    df_sets_joined=join_sets(list_df, "Tm")
    
    correct_df=spark_session.createDataFrame([("Arizona Diamondbacks",298,632,34,69,3.65,2.03,2,3,"ARI",0),
                                            ("Los Angeles Dodgers",163,741,56,45,2.96,1.99,0,1,"LAD",1),
                                            ("Minnesota Twins",214,763,49,98,4.32,2.26,1,4,"MIN",0),
                                            ("San Diego Padres",236,699,74,55,6.01,2.59,3,2,"SDP",0)],
                                           ["Tm","HR","SO","Errors","GDP","ERA","WHIP","qHR","qSO","Abb","Post"])
    
    assert df_sets_joined.collect()==correct_df.collect()

    
# **************************************************************************************
#Test 7: se prueba la función join_off_def qeu agrega dos DF en uno solo conservando todas las filas de ambos (tanto las que tienen en común
#como las que no.
def test_join_off_def(spark_session):
    df_off=spark_session.createDataFrame([("Arizona Diamondbacks",1,0),
                                            ("Los Angeles Dodgers",0,1),
                                            ("Minnesota Twins",2,3),
                                            ("San Diego Padres",4,1)],
                                           ["Tm","qbHR","qbSO"])
                                           
    df_def=spark_session.createDataFrame([("Tampa Bay Rays",2,4),
                                            ("Los Angeles Dodgers",1,1),
                                            ("Minnesota Twins",0,2),
                                            ("San Diego Padres",2,1)],
                                           ["Tm","qpERA","qpSO"])
                                           
    list_df=[df_off,df_def]
                                           
    df_sets_joined=join_off_def(list_df, "Tm")
    
    correct_df=spark_session.createDataFrame([("Arizona Diamondbacks",1,0,0,0),
                                            ("Los Angeles Dodgers",0,1,1,1),
                                            ("Minnesota Twins",2,3,0,2),
                                            ("San Diego Padres",4,1,2,1),
                                            ("Tampa Bay Rays",0,0,2,4)],
                                           ["Tm","qbHR","qbSO","qpERA","qpSO"])
    
    assert df_sets_joined.collect()==correct_df.collect()
    
#Nota 1: La función join_counts trabaja de la misma forma que join_off_def; con la diferencia de que ocupa como entrada una lista de 10
#dataframes, mientras que join_off_def únicamente lo hace con 2. La funcionalidad es la misma, por esta razón no se ejecuta el test respectivo.
 
# **************************************************************************************
#Test 8: se prueba la función count_players, la cual agrupa jugadores por equipo y los cuenta según un criterio determinado. Los equipos que
#no tengan jugadores que cumplan la condición, son eliminados del dataframe resultante. Se prueba la condición jugadores con "mayor que" 25 homeruns.
 
def test_count_players(spark_session):
    df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (5,"Joey Gallo","TEX",21,82),
                                            (6,"Joey Gallo","NYY",17,64),
                                            (7,"Carlos Correa","HOU",28,110)],
                                            ["ID","Name","Tm","HR","SO"])
    
    df_counted=count_players(df,"Tm","HR","qbHR","mayor que",25)
    
    correct_df=spark_session.createDataFrame([("ATL",1),
                                            ("HOU",2)],
                                            ["Tm","qbHR"])
   
    assert df_counted.collect()==correct_df.collect()

# **************************************************************************************
# Test 9: se prueba la función count_players. Se prueba la condición jugadores con "menor que" 100 strikeouts.
 
def test_count_players2(spark_session):
    df=spark_session.createDataFrame([(1,"Ronald Acuna Jr","ATL",28,120),
                                            (2,"Jose Altuve","HOU",35,131),
                                            (3,"Gleyber Torres","NYY",21,84),
                                            (4,"JD Martinez","BOS",13,96),
                                            (5,"Joey Gallo","TEX",21,82),
                                            (6,"Joey Gallo","NYY",17,64),
                                            (7,"Carlos Correa","HOU",28,110)],
                                            ["ID","Name","Tm","HR","SO"])
    
    df_counted=count_players(df,"Tm","SO","qpSO","menor que",100)
    
    correct_df=spark_session.createDataFrame([("BOS",1),
                                            ("NYY",2),
                                            ("TEX",1)],
                                            ["Tm","qpSO"])
   
    assert df_counted.collect()==correct_df.collect()

# **************************************************************************************
#Test 10: se prueba la función union_df_2 que es igual a la función utilizada en el main (union_df). Esta función une dos dataframes uno luego
#del otro por compartir las mismas columnas. Se prueba bajo la función union_df_2 ya que este únicamente requiere del ingreso de una lista de 2
#dataframes, y no de múltiples como sucede con union_df. Pero el algoritmo es el mismo. 

def test_union_df(spark_session):
    df1=spark_session.createDataFrame([("Arizona Diamondbacks",1,0,0,0),
                                            ("Los Angeles Dodgers",0,1,1,1),
                                            ("Minnesota Twins",2,3,0,2),
                                            ("San Diego Padres",4,1,2,1),
                                            ("Tampa Bay Rays",0,0,2,4)],
                                           ["Tm","qbHR","qbSO","qpERA","qpSO"])
                                          
    df2=spark_session.createDataFrame([("Boston Red Sox",0,2,3,1),
                                            ("Los Angeles Angels",2,4,3,0),
                                            ("Kansas City Royals",0,2,1,1),
                                            ("New York Mets",3,1,1,2),
                                            ("Tampa Bay Rays",5,3,1,0)],
                                           ["Tm","qbHR","qbSO","qpERA","qpSO"])
    list_df=[df1,df2]
    df_united=union_df_2(list_df)
    
    correct_df=spark_session.createDataFrame([("Arizona Diamondbacks",1,0,0,0),
                                            ("Los Angeles Dodgers",0,1,1,1),
                                            ("Minnesota Twins",2,3,0,2),
                                            ("San Diego Padres",4,1,2,1),
                                            ("Tampa Bay Rays",0,0,2,4),
                                            ("Boston Red Sox",0,2,3,1),
                                            ("Los Angeles Angels",2,4,3,0),
                                            ("Kansas City Royals",0,2,1,1),
                                            ("New York Mets",3,1,1,2),
                                            ("Tampa Bay Rays",5,3,1,0)],
                                           ["Tm","qbHR","qbSO","qpERA","qpSO"])
    
    assert df_united.collect()==correct_df.collect()

# **************************************************************************************
#Nota: No se ejecuta prueba de build_df_year ya que este comprende la utilización de todas las funciones anteriores que fueron demostradas y
#que cumplen con el objetivo para el que se hicieron.