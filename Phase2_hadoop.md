# Phase 2 codes for Hadoop & Hive


```bash
wget https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/KM2QOA
```

```bash
mv :persistentId?persistentId=doi:10.7910%2FDVN%2FHG7NV7%2FKM2QOA 2003.csv.bz2
```

```bash
ls -l
```

```bash
bzip2 -d 2003.csv.bz2 - unzip
```

```bash
hdfs dfs -mkdir -p /user/hive/ali
hdfs dfs -chmod g+w /user/hive/ali
hdfs dfs -put 2003.csv /user/hive/ali
```

```bash
hive
```


## Create database AliFlightInfo;

```bash
Use AliFlightInfo;
```
```bash
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
"seperatorChar" = ","
);
```


## Creating Sample Table:
```bash
CREATE TABLE IF NOT EXISTS AliSample
 (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT,
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ","
);
```


```bash
LOAD DATA INPATH  '/user/hive/ali/2003.csv' OVERWRITE INTO TABLE Ali2003; 
```

```bash
INSERT INTO TABLE AliSample
SELECT *
FROM Ali2003
LIMIT 30000;
```
```bash
select count(1) from AliSample;
```


## Creating a new table with a new column Delayed:
```bash
CREATE TABLE IF NOT EXISTS AliSample_new (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT,
-- Adding a new column "Delayed"
    Delayed CHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ","
);
```

```bash
INSERT INTO AliSample_new
SELECT *,
CASE
WHEN ArrDelay <= 0 AND DepDelay <= 0 THEN 'N' ELSE 'Y'
END AS Delayed
FROM AliSample;
```
```bash
select * from AliSample_new limit 10;
```
```bash
drop table AliSample;
ALTER TABLE AliSample_new RENAME TO Ali_sample;
Exit;
ls -l
```

## Making a new directory:

```bash
[hadoop@ip-172-31-22-108 ~]$ hdfs dfs -mkdir selected_data
[hadoop@ip-172-31-22-108 ~]$ hive
hive> SET hive.cli.print.header=true;
Use AliFlightInfo;
```


- Query:
```bash
INSERT OVERWRITE DIRECTORY 'hdfs:///user/hadoop/selected_data_newest/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM Ali_sample;
```

```bash
[hadoop@ip-172-31-22-108 ~]$ hive -e 'SET hive.cli.print.header=true; Use AliFlightInfo; SELECT * FROM Ali_sample; LIMIT 0' \ | sed 's/[\]/,/g'> /home/hadoop/header2.csv
[hadoop@ip-172-31-22-108 ~]$ hadoop fs -get /user/hadoop/selected_data/* /home/hadoop/data
[hadoop@ip-172-31-22-108 ~]$ cat /home/hadoop/header.csv /home/hadoop/data > /home/hadoop/2003_data.csv
```
Since I have done in .pem
Go to Got Bash
```bash
chmod 400 "C:\\Users\\niyu1\\Downloads\\Project_Phase1.pem"
```
```bash
hive -e 'SET hive.cli.print.header=true; Use flight; SELECT * FROM S_sample; LIMIT 0' \ | sed 's/[\]/,/g'> /home/hadoop/header3.csv
```
```bash
hive -e 'SET hive.cli.print.header=true; Use AliFlightinfo; SELECT * FROM Ali_sample; LIMIT 0' \ | sed 's/[\]/,/g'> /home/hadoop/header1.csv
```
```bash
scp -i /Users/ali/Desktop/Ali_project.pem hadoop@ec2-3-145-141-244.us-east-2.compute.amazonaws.com/home/hadoop/2008_data.csv .
```