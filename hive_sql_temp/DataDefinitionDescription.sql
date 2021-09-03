/* 1. Data Types */

--Create table using ARRAY, MAP, STRUCT, and Composite data type
CREATE TABLE employee (
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

--Verify tables creations run in Beeline
!table employee

--Load data
LOAD DATA LOCAL INPATH '/root/employee.txt' OVERWRITE INTO TABLE employee;

--Query the whole table
SELECT * FROM employee;

--Query the ARRAY in the table
SELECT work_place FROM employee;

SELECT work_place[0] AS col_1, work_place[1] AS col_2, work_place[2] AS col_3 FROM employee;

--Query the STRUCT in the table
SELECT gender_age FROM employee;

SELECT gender_age.gender, gender_age.age FROM employee;

--Query the MAP in the table
SELECT skills_score FROM employee;

SELECT name, skills_score['DB'] AS DB, 
skills_score['Perl'] AS Perl, skills_score['Python'] AS Python, 
skills_score['Sales'] as Sales, skills_score['HR'] as HR FROM employee;

SELECT depart_title FROM employee;

SELECT name, depart_title['Product'] AS Product, depart_title['Test'] AS Test,
depart_title['COE'] AS COE, depart_title['Sales'] AS Sales  
FROM employee;

SELECT name, 
depart_title['Product'][0] AS product_col0, 
depart_title['Test'][0] AS test_col0 
FROM employee;

/* 2. Databases */

--Create database without checking if the database already exists.
CREATE DATABASE hivetest;

--Create database and checking if the database already exists.
CREATE DATABASE IF NOT EXISTS hivetest;

--Create database with location, comments, and metadata information
CREATE DATABASE IF NOT EXISTS hivetest
COMMENT 'hive database demo'
LOCATION '/hdfs/directory'
WITH DBPROPERTIES ('creator'='cloudera','date'='2019-07-17');

--Show and describe database with wildcards
SHOW DATABASES;
SHOW DATABASES LIKE 'hive.*';
DESCRIBE DATABASE hivetest;

--Use the database
USE hivetest;

--Show current database
SELECT current_database();

--Drop the empty database.
DROP DATABASE IF EXISTS hivetest;

--Drop database with CASCADE
DROP DATABASE IF EXISTS hivetest CASCADE;

ALTER DATABASE hivetest SET OWNER user cloudera;

/* 3. Table creation */

--Create internal table and load the data
CREATE TABLE IF NOT EXISTS employee_internal (
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an internal table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'home/employee.txt' OVERWRITE INTO TABLE employee_internal;

--Create external table and load the data
CREATE EXTERNAL TABLE IF NOT EXISTS employee_external (
   name string,
   work_place ARRAY<string>,
   gender_age STRUCT<gender:string,age:int>,
   skills_score MAP<string,int>,
   depart_title MAP<STRING,ARRAY<STRING>>
)
COMMENT 'This is an external table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE
LOCATION '/user/cloudera/employee';

LOAD DATA LOCAL INPATH '/home/employee.txt' OVERWRITE INTO TABLE employee_external;

--Temporary tables
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_emp1 (
name string,
work_place ARRAY<string>,
gender_age STRUCT<gender:string,age:int>,
skills_score MAP<string,int>,
depart_title MAP<STRING,ARRAY<STRING>>
); 

CREATE TEMPORARY TABLE tmp_emp2 AS SELECT * FROM tmp_emp1;

CREATE TEMPORARY TABLE tmp_emp3 LIKE tmp_emp1;

--Create Table With Data - CREATE TABLE AS SELECT (CTAS)
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

--Create Table As SELECT (CTAS) with Common Table Expression (CTE) 
CREATE TABLE cte_employee AS
WITH r1 AS (SELECT name FROM r2 WHERE name = 'Michael'),
r2 AS (SELECT name FROM employee WHERE gender_age.gender= 'Male'),
r3 AS (SELECT name FROM employee WHERE gender_age.gender= 'Female')
SELECT * FROM r1 UNION ALL select * FROM r3;

SELECT * FROM cte_employee;

--Create Table Without Data - TWO ways 
--With CTAS
CREATE TABLE empty_ctas_employee AS SELECT * FROM employee_internal WHERE 1=2;

--With LIKE
CREATE TABLE empty_like_employee LIKE employee_internal;

--Check row count for both tables
SELECT COUNT(*) AS row_cnt FROM empty_ctas_employee;
SELECT COUNT(*) AS row_cnt FROM empty_like_employee;

/* 4. Table description */

--Show tables
SHOW TABLES;
SHOW TABLES '*emp*';
SHOW TABLES '*ext*|*cte*';
SHOW TABLE EXTENDED LIKE 'employee_int*';

--Show columns
SHOW COLUMNS IN employee_internal;
DESC employee_internal;

--Show DDL and property
SHOW CREATE TABLE employee_internal;
SHOW TBLPROPERTIES employee_internal;

/* 5. Table clear */

--Drop table 
DROP TABLE IF EXISTS empty_ctas_employee;

DROP TABLE IF EXISTS empty_like_employee;

--Truncate table
SELECT * FROM cte_employee;

TRUNCATE TABLE cte_employee;

SELECT * FROM cte_employee;

/* 6. Table alter */

--Alter table name
ALTER TABLE cte_employee RENAME TO cte_employee_backup;

--Alter table properties, such as comments
ALTER TABLE cte_employee_backup SET TBLPROPERTIES ('comment' = 'New comments');

--Alter table delimiter through SerDe properties
ALTER TABLE employee_internal SET SERDEPROPERTIES ('field.delim' = '$');

--Alter Table File Format
ALTER TABLE employee_internal SET FILEFORMAT RCFILE;

--Alter Table Location
ALTER TABLE employee_internal SET LOCATION 'hdfs://localhost:9000/user/cloudera/employee'; 

--Alter Table Location
ALTER TABLE employee_internal ENABLE NO_DROP; 
ALTER TABLE employee_internal DISABLE NO_DROP; 
ALTER TABLE employee_internal ENABLE OFFLINE;
ALTER TABLE employee_internal DISABLE OFFLINE;

--Alter Table Concatenate to merge small files into larger files
--convert to the file format supported
ALTER TABLE employee_internal SET FILEFORMAT ORC;
 
--convert to the regular file format
ALTER TABLE employee_internal SET FILEFORMAT TEXTFILE;


--Alter columns
--Change column type - before changes
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE name employee_name string AFTER gender_age;

--Verify the changes 
DESC employee_internal; 

--Change column type
ALTER TABLE employee_internal CHANGE employee_name name string COMMENT 'updated' FIRST;

--Verify the changes 
DESC ctas_employee; 

--Add columns to the table
ALTER TABLE ctas_employee ADD COLUMNS (work string);

--Verify the added columns
DESC ctas_employee;      

--Replace all columns
ALTER TABLE ctas_employee REPLACE COLUMNS (name string);

--Verify the replaced all columns
DESC ctas_employee;   

/* 7. Table partitioning */

--Create partition table DDL
CREATE TABLE employee_partitioned
(
  name string,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
PARTITIONED BY (Year INT, Month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

--Check partition table structure
DESC employee_partitioned;

--Show partitions
SHOW PARTITIONS employee_partitioned;

--Add multiple partitions
ALTER TABLE employee_partitioned ADD 
PARTITION (year=2018, month=11)        
PARTITION (year=2018, month=12);

SHOW PARTITIONS employee_partitioned;

--Drop partitions
ALTER TABLE employee_partitioned DROP PARTITION (year=2018, month=11);

ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (year=2017); -- Drop all partitions in 2017

ALTER TABLE employee_partitioned DROP IF EXISTS PARTITION (month=9);

SHOW PARTITIONS employee_partitioned;

--Rename partitions
ALTER TABLE employee_partitioned PARTITION (year=2018, month=12) RENAME TO PARTITION (year=2018,month=10);

SHOW PARTITIONS employee_partitioned;

--Load data to the partition
LOAD DATA LOCAL INPATH '/home/employee.txt' 
OVERWRITE INTO TABLE employee_partitioned
PARTITION (year=2018, month=12);

--Verify data loaded
SELECT name, year, month FROM employee_partitioned;

--Partition table add columns
ALTER TABLE employee_partitioned ADD COLUMNS (work string) CASCADE;

--Change data type for partition columns
ALTER TABLE employee_partitioned PARTITION COLUMN(year string);
--Verify the changes
DESC employee_partitioned;

ALTER TABLE employee_partitioned PARTITION (year=2018) SET FILEFORMAT ORC;
ALTER TABLE employee_partitioned PARTITION (year=2018) ENABLE NO_DROP;
ALTER TABLE employee_partitioned PARTITION (year=2018) ENABLE OFFLINE;
ALTER TABLE employee_partitioned PARTITION (year=2018) DISABLE NO_DROP;
ALTER TABLE employee_partitioned PARTITION (year=2018) DISABLE OFFLINE;
ALTER TABLE employee_partitioned PARTITION (year=2018) CONCATENATE;
ALTER TABLE employee_partitioned PARTITION (year=2018) SET FILEFORMAT TEXTFILE;

/* 8. Table bucketing */

--Prepare data for bucket tables
CREATE TABLE employee_id                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '/home/employee_id.txt' OVERWRITE INTO TABLE employee_id;

--Create the bucket table
CREATE TABLE employee_id_buckets                         
(
  name string,
  employee_id int,
  work_place ARRAY<string>,
  gender_age STRUCT<gender:string,age:int>,
  skills_score MAP<string,int>,
  depart_title MAP<STRING,ARRAY<STRING>>
)
CLUSTERED BY (employee_id) INTO 2 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

set map.reduce.tasks = 2;

set hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE employee_id_buckets SELECT * FROM employee_id;

hdfs dfs -ls /user/hive/warehouse/employee_id_buckets

