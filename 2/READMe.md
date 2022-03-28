# Population and Insertion Code and pipeline 

The total code for creating the database schema and populating the database is distributed over the following files 

1. convert.py

This file takes one command line argument which is the path to the cssv file containing details about each fragment. The python file then creates a json file which has the structure appropriate for translating into our previously designed system catalogue 

The columns of the csv file are `Fragment Name,Fragmentation Type,Allocated site,Fragmentation Condition,Columns`

All column names should be listed as a coma separated string and should be followed by the table name. 

The condition should include the column name, operator, comparison value without any spaces. 

The fragmentation type should be one of 'V', 'H', 'DH' or 'None'

2. fragment_schema.py 

This file uses the previously created json file in order to crete the system catalogue tables on each site and also create the necessary data tables

3. system_catalogue.py

This file populates the system catalogue 

4. populate.py

This file populates the data tables. it takes in path to 3 csv files as command line arguments. the three csv files should be named as the corresponding table names. The column names should correspond to the ones mentioned in the fragmentation csv file 


example to run 

```
python3 convert.py given.csv
python3 fragmentation_schema.py
python3 system_catalogue.py
python3 populate.py menu.csv user.csv item.csv
```