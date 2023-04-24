# MLB-Postseason-Teams-Prediction
Archivos dentro de la carpeta zip del proyecto:
	_init_.py
	Dockerfile
	build_image.sh
	run_image.sh
	load_jupyter.sh
	run_image_db.sh
	psqlread.py
	psqlread.sh
	postgresql-42.2.14.jar
	main_1.py
	main_2.py
	main_3.py
	main_1.sh
	main_2.sh
	main_3.sh
	functions.py
	conftest.py
	test_functions.py
	Notebook Modelos ML.ipynb
	Folder: Files
	Folder: Reference Files
Creación de los contenedores en Docker:
1.	Descargar y descomprimir el zip en un directorio local.
2.	Abrir el command prompt de su computadora y accesar la dirección en la que guardó la carpeta descomprimida.
3.	Construir la imagen utilizando el comando < docker build --tag proyecto_samuel . >.
4.	Correr la imagen, creando su respectivo contenedor con el comando < docker run -p 8888:8888 -i -t proyecto_samuel /bin/bash >. Este será el contenedor donde se correrá tanto los archivos main.py como posteriormente, el Notebook de Jupyter con el programa de los modelos de ML.
5.	Crear la imagen y correr el contenedor para la base de datos de postgres con el comando < docker run --name proy-samuel-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres >. La contraseña para escribir y leer desde la base de datos creada, será testPassword.
6.	Revisar que ambos contenedores se hayan creado correctamente y estén en ejecución.
Ejecución del código principal en los contenedores:
Para este proyecto, el programa “main.py” se dividió en 3 secuencias diferentes (main_1, main_2, main_3). Los 3 programas construyen el dataframe que se abrirá en Jupyter posteriormente y que se utilizará para desarrollar los modelos de ML. 
La primera secuencia crea un dataframe que comprende un set de datos que va desde el 2021 al 2016 (sin incluir el 2020). La segunda secuencia uno que va desde el 2015 al 2011 y la tercera secuencia trabaja con los datasets del 2010 al 2006, formando así la tercera parte del dataframe principal. Cada uno de estos main, lee los archivos que se encuentran agrupados por año en carpetas distintas dentro del folder de Files.
Por lo tanto, Files, se compone de 15 carpetas  que van desde el 2021 al 2006. Donde cada carpeta contiene 7 archivos csv cada una:
	DS1: Team Batting Stats (tbat_year) --> 30 equipos x 29 columnas atributos de bateo.
	DS2: Players Batting Stats (pbat_year) --> ~1700 bateadores x 31 columnas atributos de bateo.
	DS3: Team Pitching Stats (tpit_year) --> 30 equipos x 36 columnas atributos de pitcheo.
	DS4: Players Pitching Stats (ppit_year) --> ~1100 bateadores x 36 columnas atributos de pitcheo.
	DS5: Team Fielding Stats (tfie_year) --> 30 equipos x 19 columnas atributos de fildeo.
	DS6: Team Miscellaneous Stats (tmis_year) --> 30 equipos x 16 columnas atributos misceláneos.
	DS7: Team Postseason Status (post_year) --> 30 equipos x 2 columnas atributos (abreviatura del equipo y etiqueta de clasificación a Postemporada).
Más allá de los archivos por año que cada uno de los programas “main” lee, no hay diferencias en cuanto al código. Primero llama a la función “build_df_year” para cada crear un dataframe con todos los atributos de interés por año y procesarlos según los criterios que se eligieron como más pertinentes. Esta función contiene a su vez 7 funciones que se construyeron para poder generar el dataframe según los requerimientos.
Luego se crea una lista, donde se van guardando estos dataframes por año. Finalmente se ejecuta la función “union_df”, la cual une en un solo dataframe todos los que se agruparon en dicha lista. Todas estas funciones mencionadas se encuentran en el archivo functions.py.
Con tal de guardar este dataframe final y poder accesarlo luego, se emplea la función write que escribe los dataframes generados (df_final1, df_final2 y df_final3) en la base de datos Postgresql que se ejecutó mediante el otro contenedor. Luego, a través del Notebook de Jupyter, se leerán dichos dataframes directamente desde la base de datos. También se emplea una función de pyspark “write.csv”, que permite crear una carpeta llamada “df_final_part_X”, esto para cada uno de los main. Estos archivos también se guardarán en el contenedor del proyecto.
Siguiendo con la lista de instrucciones para ejecutar el código:
7.	Ejecutar el comando < sh main_1.sh >. Este ejecuta la primera secuencia principal del programa (main_1.py). Una vez se complete la instrucción, proceder con < sh main_2.sh >. De igual forma esperar a que se ejecute la orden y seguir con < sh main_3.sh >. Cada uno de estos comandos puede tardar varios minutos en completarse.
8.	Ejecutar el comando < ls > y revisar la creación exitosa de las carpetas df_final_part_1, df_final_part_2 y df_final_part_3. A su vez, se recomienda ingresar a cada una con el comando <cd> y comprobar la existencia de los archivos csv correspondientes. Recordar que estos traen un nombre alfa-numérico que Spark le asignó.
Ingreso al código dentro del Notebook de Jupyter:
9.	Regresarse al directorio principal del proyecto mediante <cd ..> e ingresar el comando < sh load_jupyter.sh >.
10.	Copiar el último enlace que se obtenga de la ejecución de la orden, y pegarlo en su navegador de preferencia.
11.	Habiendo ingresado al directorio principal en Jupyter, abrir el archivo ipynb Notebook Modelos ML.
12.	Ejecutar todas las celdas del cuaderno y observar los resultados. Es posible que las celdas relacionadas con gráficos o histogramas tarden varios minutos en ejecutarse.
Ejecución de las pruebas unitarias:
13.	En la ventana de la terminal ya en ejecución, salirse del Notebook de Jupyter mediante crtl+c.
14.	Una vez estando de regreso en el directorio principal del contenedor en el que se ha venido trabajando, ejecutar el comando <pytest> o bien <pytest -vv> para tener más información de las pruebas realizadas.
Se trata de 10 pruebas unitarias y todas están alojadas en el archivo test_functions.py.


