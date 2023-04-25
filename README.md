# MLB-Postseason-Teams-Prediction

(Note: The Files folder with the entry data is not included in this repo.)

**Docker cointainers creation:**
1. Download and unzip the zip file into a local directory.
2. Open the command prompt on your computer and navigate to the directory where you saved the unzipped folder.
3. Build the image using the command < docker build --tag project_samuel . >.
4. Run the image, creating its respective container with the command < docker run -p 8888:8888 -i -t project_samuel /bin/bash >. This will be the container where both the main.py files and later the Jupyter Notebook with the ML model program will run.
5. Create the image and run the container for the postgres database with the command < docker run --name proy-samuel-db -e POSTGRES_PASSWORD=testPassword -p 5433:5432 -d postgres >. The password to write to and read from the created database will be testPassword.
6. Check that both containers have been created correctly and are running.

**Main code execution on the containers:**
For this project, the program "main.py" was divided into 3 different sequences (main_1, main_2, main_3). The 3 programs build the dataframe that will be opened in Jupyter later and that will be used to develop the ML models.
The first sequence creates a dataframe that comprises a dataset from 2021 to 2016 (excluding 2020, COVID shortened-season). The second sequence creates one that goes from 2015 to 2011 and the third sequence works with the datasets from 2010 to 2006, thus forming the third part of the main dataframe. Each of these mains reads the files that are grouped by year in different folders inside the Files folder.

Therefore, Files consists of 15 folders ranging from 2021 to 2006. Each folder contains 7 csv files:
- DS1: Team Batting Stats (tbat_year) --> 30 teams x 29 columns batting attributes.
- DS2: Players Batting Stats (pbat_year) --> ~1700 batters x 31 columns batting attributes.
-	DS3: Team Pitching Stats (tpit_year) --> 30 teams x 36 columns batting attributes.
-	DS4: Players Pitching Stats (ppit_year) --> ~1100 batters x 36 columns pitching attributes.
-	DS5: Team Fielding Stats (tfie_year) --> 30 teams x 19 columns fielding attributes.
-	DS6: Team Miscellaneous Stats (tmis_year) --> 30 teams x 16 columns miscellaneous attributes.
-	DS7: Team Postseason Status (post_year) --> 30 teams x 2 column attributes (team abbreviation and Postseason classification tag).

Beyond the year files that each of the "main" programs reads, there are no differences in terms of the code. First, it calls the "build_df_year" function to create a dataframe with all the attributes of interest per year and process them according to the selected criteria. This function contains 7 functions that were built to generate the dataframe according to the requirements.

Then, a list is created, where these dataframes are saved by year. Finally, the "union_df" function is executed, which merges all the dataframes grouped in that list into a single dataframe. All these mentioned functions are located in the "functions.py" file.

In order to save this final dataframe and access it later, the "write" function is used to write the generated dataframes (df_final1, df_final2, and df_final3) to the Postgresql database that was executed through the other container. Then, through the Jupyter Notebook, these dataframes will be read directly from the database. A pyspark function "write.csv" is also used, which allows creating a folder called "df_final_part_X", for each of the main programs. These files will also be saved in the project container.

Continuing with the list of instructions to run the code:
7. Run the command "sh main_1.sh". This executes the first main sequence of the program (main_1.py). Once the instruction is complete, proceed with "sh main_2.sh". Similarly, wait for the command to execute and continue with "sh main_3.sh". Each of these commands can take several minutes to complete.
8. Run the command "ls" and check the successful creation of the df_final_part_1, df_final_part_2, and df_final_part_3 folders. It is also recommended to enter each folder using the "cd" command and check the existence of the corresponding csv files. Remember that these files have an alphanumeric name assigned by Spark.

Entering the Jupyter Notebook:
9. Go back to the main project directory using "cd .." and enter the command "sh load_jupyter.sh".
10. Copy the last link obtained from executing the command and paste it into your preferred browser.
11. Once inside the main directory in Jupyter, open the "ipynb Notebook Modelos ML" file.
12. Run all the cells in the notebook and observe the results. It's possible that the cells related to graphs or histograms may take several minutes to execute.

Unitary tests:
13. In the running terminal window, exit Jupyter Notebook using "ctrl+c".
14. Once back in the main directory of the container where you have been working, run the command "pytest" or "pytest -vv" to get more information about the tests performed. There are 10 unit tests, and all of them are located in the "test_functions.py" file.


