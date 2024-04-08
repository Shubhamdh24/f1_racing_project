-- Databricks notebook source
use catalog hive_metastore;


-- COMMAND ----------

create database if not exists f1_racing_project;

-- COMMAND ----------

create table if not exists f1_racing_project.circuits
(circuit_id INT,
    circuit_ref STRING,
    name STRING,
    location STRING,
    country STRING,
    lat FLOAT,
    lng FLOAT,
    alt INT,
    url STRING) using csv location '/mnt/saf1racing/bronze/circuits.csv' options (header=True);

-- COMMAND ----------

select * from f1_racing_project.circuits;
desc extended f1_racing_project.circuits;

-- COMMAND ----------

select * from f1_racing_project.circuits;

-- COMMAND ----------

create table f1_racing_project.races
(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING) using csv options (path='/mnt/saf1racing/bronze/races.csv',header=True);

-- COMMAND ----------

create table f1_racing_project.constructors
(constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING) using json options (path='/mnt/saf1racing/bronze/constructors.json')

-- COMMAND ----------

use database f1_racing_project;

-- COMMAND ----------

select * from constructors;


-- COMMAND ----------

create table drivers
(driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING) using json options (path='/mnt/saf1racing/bronze/drivers.json');

-- COMMAND ----------

alter table drivers
add column (forename STRING,surname STRING);


-- COMMAND ----------

SELECT * FROM DRIVERS;

-- COMMAND ----------

desc extended drivers;

-- COMMAND ----------

CREATE TABLE RESULTS
(resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING) USING JSON OPTIONS (PATH = '/mnt/saf1racing/bronze/results.json');

-- COMMAND ----------

create table pit_stops
(driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING) using json options(path='/mnt/saf1racing/bronze/pit_stops.json',multiline=True);

-- COMMAND ----------

create table lap_times 
(raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT) using csv options(path='/mnt/saf1racing/bronze/lap_times');

-- COMMAND ----------

select * from lap_times;

-- COMMAND ----------

create table qualifying
(constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT) using json options(path ='/mnt/saf1racing/bronze/qualifying/',multiline=True);

-- COMMAND ----------

select * from qualifying;

-- COMMAND ----------


