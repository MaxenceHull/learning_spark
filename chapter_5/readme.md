# Chapter 5: Interacting with External Data Sources

## Goals
* Use a DBMS to read (and write?) data
* Learn how to crate user defined functions

## Basic example of a User Defined Function (UDF)
````
spark-submit udf.py
````

## Basic example of a DBMS (PostgreSQL) with Spark
* Be sure that Docker is installed and run on your machine before running the script

`start.sh` script will:
* Build a new docker image and load a dataset (french regions, departments and cities)
* Query DB using Spark and a psql driver

`````
./start.sh usage
./start.sh run
./start.sh clean
`````