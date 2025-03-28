# These are the codes for Hadoop/Hive


1. Creating “warehouse” directory inside “hive” directory
```bash
hdfs dfs -mkdir -p /user/hive/warehouse
``` 

2. Granting permissions to the group
```bash
bzip2 -d 2003 - granting write permission to the group
```

3. For 2003.csv File:
```bash
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/KM2QOA

mv :persistentId?persistentId=doi:10.7910%2FDVN%2FHG7NV7%2FKM2QOA 2003.csv.bz2

bzip2 -d 2003.csv.bz2 - unzip

hdfs dfs -put 2003.csv /user/hive/warehouse  - Copying the new file back to HDFS
```



4. For Airports.csv File:
```bash
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/XTPZZY

mv \:persistentId?persistentId=doi:10.7910%2FDVN%2FHG7NV7%2FXTPZZY Airports.csv

hdfs dfs -put Airports.csv /user/hive/warehouse  - Copy the new file back to HDFS

head -n 10 Airports.csv  -First 10 lines from csv file
```

5. For Carriers.csv File
```bash
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/3NOQ6Q

mv \:persistentId?persistentId=doi:10.7910%2FDVN%2FHG7NV7%2F3NOQ6Q Carriers.csv - renaming the downloaded file as Carriers.csv

hdfs dfs -put Carriers.csv /user/hive/warehouse  - Copy the new file back to HDFS

head -n 10 Carriers.csv  -First 10 lines from csv file
```

6. Creation of hive table and load all the data into it 
```bash
hive
```
```bash
Create database Ali2003FlightInfo; 
Use Ali2003FlightInfo; 
CREATE TABLE IF NOT EXISTS Ali2003 (
Year INT,
Month INT,
DayofMonth INT,
DayOfWeek INT,
DepTime STRING,
CRSDepTime STRING,
ArrTime STRING,
CRSArrTime STRING,
UniqueCarrier STRING,
FlightNum INT,
TailNum INT,
ActualElapsedTime INT,
CRSElapsedTime INT,
AirTime INT,
ArrDelay INT,
DepDelay STRING,
Origin STRING,
Dest STRING,
Distance INT,
TaxiIn INT,
TaxiOut STRING,
Cancelled INT,
CancellationCode STRING,
Diverted INT,
CarrierDelay INT,
WeatherDelay INT,
NASDelay INT,
SecurityDelay INT,
LateAircraftDelay INT,
PRIMARY KEY (UniqueCarrier, Origin, Dest) DISABLE NOVALIDATE
)
COMMENT 'Flight Info'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"seperatorChar" = ",",
"qouteChar" = "\""
);
```
```bash
LOAD DATA INPATH  '/user/hive/warehouse/2003.csv' OVERWRITE INTO TABLE Ali2003; - Loading the data to table “Ali2003”
```
```bash
CREATE TABLE IF NOT EXISTS AliAirports (
iata STRING,
airport STRING,
city STRING,
state STRING,
country STRING,
lat DOUBLE,
long DOUBLE
)
COMMENT 'Airports Info'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"seperatorChar" = ","
);
```
```bash
LOAD DATA INPATH '/user/hive/warehouse/Airports.csv' OVERWRITE INTO TABLE AliAirports;
- Loading the data to table “AliAirports”
```


- Creating a “AliCarriers” table
```bash
CREATE TABLE IF NOT EXISTS AliCarriers (
Code STRING,
Description STRING,
)
COMMENT 'Carriers Table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"seperatorChar" = ","
);
```
```bash
LOAD DATA INPATH '/user/hive/warehouse/Carriers.csv' OVERWRITE INTO TABLE AliCarriers;
- Loading the data to table “AliCarriers”
```

7. Display 100 rows 
```bash
SELECT * FROM Ali2003 LIMIT 100;

SELECT * FROM AliAirports LIMIT 100;

SELECT * FROM AliCarriers LIMIT 100;
```

8. Data Analysis

1.
```bash
SELECT sj.Year, sj.Origin, a.airport,
(round((sum(sj.Arrdelay))/60,2)) as Total_ArrDelay,
(round((sum(sj.Depdelay))/60,2)) as Total_DepDelay,
(round((sum(sj.Arrdelay) + sum(sj.DepDelay))/60,2)) as Total_Delay
FROM Ali2003 sj
INNER JOIN AliAirports a ON sj.Origin = a.iata
GROUP BY sj.Year, sj.Origin, a.airport
ORDER BY Total_Delay DESC
LIMIT 3;
```

2.
```bash
SELECT sj.Year, sj.Dest, air.airport,
  ROUND((SUM(sj.Arrdelay) / 60), 2) AS Total_ArrDelay,
  ROUND((SUM(sj.DepDelay) / 60), 2) AS Total_DepDelay,
  ROUND((SUM(sj.Arrdelay) + SUM(sj.DepDelay)) / 60, 2) AS Total_Delay
FROM Ali2003 sj
INNER JOIN AliAirports air ON sj.Dest = air.iata
GROUP BY sj.Year, sj.Dest, air.airport
ORDER BY Total_Delay DESC LIMIT 3;
```


3.
```bash
SELECT sj.Year, sj.Origin, c.Description,
(round((sum(sj.Arrdelay))/60,2)) as Total_ArrDelay,
(round((sum(sj.Depdelay))/60,2)) as Total_DepDelay,
(round((sum(sj.Arrdelay) + sum(sj.DepDelay))/60,2)) as Total_Delay
FROM Ali2003 sj
INNER JOIN AliCarriers c on sj.UniqueCarrier = c.code
GROUP BY sj.Year, sj.Origin, c.Description
ORDER BY Total_delay DESC LIMIT 3;
```

4.
```bash 
SELECT sj.Year, sj.Dest, c.Description,
(round((sum(sj.Arrdelay))/60,2)) as Total_ArrDelay,
(round((sum(sj.Depdelay))/60,2)) as Total_DepDelay,
(round((sum(sj.Arrdelay) + sum(sj.DepDelay))/60,2)) as Total_Delay
FROM Ali2003 sj
INNER JOIN AliCarriers c on sj.UniqueCarrier = c.code
GROUP BY sj.Year, sj.Dest, c.Description
ORDER BY Total_delay DESC LIMIT 3;
```

5. 
```bash
SELECT Year,
  CASE
    WHEN SUM(ArrDelay) > SUM(DepDelay) THEN ROUND(SUM(ArrDelay) / 60 / 2)
    WHEN SUM(ArrDelay) < SUM(DepDelay) THEN ROUND(SUM(DepDelay) / 60 / 2)
    ELSE 0
  END AS Total_Delay,
  CASE
    WHEN SUM(ArrDelay) > SUM(DepDelay) THEN 'Arrivals'
    WHEN SUM(ArrDelay) < SUM(DepDelay) THEN 'Departures'
    ELSE 'Equal'
  END AS DelayType
FROM Ali2003
WHERE
  ArrDelay IS NOT NULL
  AND DepDelay IS NOT NULL
GROUP BY Year;
```


6. 
```bash
SELECT Year,
(round((sum(Arrdelay))/60,2)) as Total_ArrDelay,
(round((sum(DepDelay))/60,2)) as Total_DepDelay
FROM Ali2003
GROUP BY Year;
```


## Phase 2:

1. Create a new table in Hive 
```bash
CREATE TABLE IF NOT EXISTS Ali_sample LIKE Ali2003;
```

2. Add a new column to the table
```bash 
ALTER TABLE Ali_sample ADD COLUMN IF NOT EXISTS Delayed STRING;
```

3. Perform random sampling and insert into the new table
```bash
INSERT OVERWRITE TABLE Ali_sample
SELECT * FROM Ali2003
WHERE rand() <= 0.1 
DISTRIBUTE BY rand()
SORT BY rand()
LIMIT 30000;

proportion = sample size / total population size = 30,000 / 1048576 = 0.0286102295 rounded off to = 0.0286

INSERT OVERWRITE TABLE Ali_sample
SELECT * FROM Ali2003
WHERE rand() <= 0.0286
DISTRIBUTE BY rand()
SORT BY rand()
LIMIT 30000;
```


4. Check that all records are loaded
```bash
SELECT COUNT(*) FROM Ali_sample;
```

5. Populate the Delayed column
```bash
UPDATE Ali_sample
SET Delayed = CASE
                WHEN ArrDelay <= 0 AND DepDelay <= 0 THEN 'N'
                ELSE 'Y'
             END;
```

5. Extract the sample records
```bash
INSERT OVERWRITE LOCAL DIRECTORY '/path/to/local/directory'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM Ali_sample;
```