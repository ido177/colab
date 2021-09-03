/* 4. INSERT EXPORT IMPORT */

--Insert specified columns
CREATE TABLE emp_simple( -- Create a test table only has primary types
name string,
work_place string
);

--Insert values
INSERT INTO TABLE emp_simple VALUES ('Michael', 'Toronto'),('Lucy', 'Montreal');
SELECT * FROM emp_simple;

DROP TABLE IF EXISTS ctas_employee ;
CREATE TABLE ctas_employee AS SELECT * FROM employee_external;

--INSERT from CTE
WITH a as (SELECT * FROM ctas_employee )
FROM a
INSERT OVERWRITE TABLE employee
SELECT *;

--Create partition table DDL
DROP TABLE IF EXISTS employee_partitioned ;
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

--Dynamic partition is not enabled by default. We need to set following to make it work.
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nostrict;

--Dynamic partition insert
INSERT INTO TABLE employee_partitioned PARTITION(year, month)
SELECT name, array('Toronto') as work_place, 
named_struct("gender","Male","age",30) as gender_age,
map("Python",90) as skills_score,
map("R&D",array('Developer')) as depart_title, 
year(start_date) as year, month(start_date) as month
FROM employee_hr eh
WHERE eh.employee_id = 102;

SHOW PARTITIONS employee_partitioned;

--Verify the inserted row
SELECT name,depart_title,year,month FROM employee_partitioned
WHERE name = 'Steven';

--Export data and metadata of table
EXPORT TABLE employee TO '/tmp/output';

hdfs dfs -ls -R /tmp/output/

--Import as new table
IMPORT TABLE employee_imported FROM '/tmp/output';

--Import as external table 
IMPORT EXTERNAL TABLE empolyee_imported_external 
FROM '/tmp/output'
LOCATION '/tmp/outputext' ; --Note, LOCATION property is optional.

/* 5. ORDER, SORT */

--ORDER, SORT
SELECT name FROM employee ORDER BY name DESC;

--Use more than 1 reducer
SET mapred.reduce.tasks = 2;

SELECT name FROM employee SORT BY name DESC;   

--Use only 1 reducer
SET mapred.reduce.tasks = 1; 

SELECT name FROM employee SORT BY name DESC;   

--Distribute by
SELECT name, employee_id 
FROM employee_hr DISTRIBUTE BY employee_id ; 

--Used with SORT BY
SELECT name, start_date FROM employee_hr DISTRIBUTE BY start_date SORT BY name;

--Cluster by
SELECT name, employee_id FROM employee_hr CLUSTER BY name ;   

