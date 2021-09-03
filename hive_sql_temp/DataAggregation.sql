/* 1. Basic aggregation */

--Aggregation without GROUP BY columns
SELECT count(*) as rowcnt1, count(1) AS rowcnt2 FROM employee;

--Aggregation with GROUP BY columns
SELECT gender_age.gender, count(*) AS row_cnt FROM employee
GROUP BY gender_age.gender;

--Multiple aggregate functions are called in the same SELECT
SELECT gender_age.gender, AVG(gender_age.age) AS avg_age,
count(*) AS row_cnt FROM employee GROUP BY gender_age.gender;

--Aggregate functions are used with CASE WHEN 
SELECT sum(CASE WHEN gender_age.gender = 'Male' THEN gender_age.age
ELSE 0 END)/count(CASE WHEN gender_age.gender = 'Male' THEN 1
ELSE NULL END) AS male_age_avg FROM employee;

--Aggregate functions are used with COALESCE and IF 
SELECT
sum(coalesce(gender_age.age,0)) AS age_sum,
sum(if(gender_age.gender = 'Female',gender_age.age,0))
AS female_age_sum FROM employee;

--Nested aggregate functions are not allowed
--FAILED: SemanticException [Error 10128]: Line 1:11 Not yet supported place for UDAF 'count'
--SELECT avg(count(*)) AS row_cnt FROM employee; 

--Aggregate functions cannot apply to null
--SELECT sum(null), avg(null); 

--Aggregate functions can be also used with DISTINCT keyword to do aggregation on unique values.
SELECT count(distinct gender_age.gender) AS gender_uni_cnt, count(distinct name) AS name_uni_cnt FROM employee;

--Use max/min struct
SELECT gender_age.gender,
max(struct(gender_age.age, name)).col1 as age,
max(struct(gender_age.age, name)).col2 as name
FROM employee
GROUP BY gender_age.gender;

--Use subquery to select unique value before aggregations for better performance
SELECT count(*) AS gender_uni_cnt FROM (SELECT distinct gender_age.gender FROM employee) a;

--Grouping Set
SELECT 
name, 
start_date,
count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS((name, start_date));
--||-- equals to
SELECT 
name, 
start_date, 
count(sin_number) AS sin_cnt 
FROM employee_hr
GROUP BY name, start_date;

SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS(name, start_date);
--||-- equals to
SELECT 
name, null as start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name
UNION ALL
SELECT 
null as name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY start_date;

SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date 
GROUPING SETS((name, start_date), name);
--||-- equals to
SELECT 
name, start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name, start_date
UNION ALL
SELECT 
name, null as start_date, count(sin_number) as sin_cnt 
FROM employee_hr
GROUP BY name;

--Aggregation condition – HAVING
SELECT gender_age.age FROM employee GROUP BY gender_age.age HAVING count(*)=1;
SELECT gender_age.age, count(*) as cnt FROM employee GROUP BY gender_age.age HAVING cnt=1;

/* 2. Window functions */

--Prepare table and data for demonstration
CREATE TABLE IF NOT EXISTS employee_contract
(
name string,
dept_num int,
employee_id int,
salary int,
type string,
start_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH
'/home/employee_contract.txt' 
OVERWRITE INTO TABLE employee_contract;

--window aggregate functions
SELECT 
name, 
dept_num as deptno, 
salary,
count(*) OVER (PARTITION BY dept_num) as cnt,
sum(salary) OVER(PARTITION BY dept_num ORDER BY dept_num) as sum1,
sum(salary) OVER(ORDER BY dept_num) as sum2
FROM employee_contract
ORDER BY deptno, name;

--window sorting functions
SELECT 
name, 
dept_num as deptno, 
salary,
row_number() OVER () as rnum,
rank() OVER (PARTITION BY dept_num ORDER BY salary) as rk, 
dense_rank() OVER (PARTITION BY dept_num ORDER BY salary) as drk,
percent_rank() OVER(PARTITION BY dept_num ORDER BY salary) as prk,
ntile(4) OVER(PARTITION BY dept_num ORDER BY salary) as ntile
FROM employee_contract
ORDER BY deptno, name;

--window analytics function
SELECT 
name,
dept_num as deptno,
salary,
round(cume_dist() OVER (PARTITION BY dept_num ORDER BY salary), 2) as cume,
lead(salary, 2) OVER (PARTITION BY dept_num ORDER BY salary) as lead,
lag(salary, 2, 0) OVER (PARTITION BY dept_num ORDER BY salary) as lag,
first_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as fval,
last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary) as lvalue,
last_value(salary) OVER (PARTITION BY dept_num ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lvalue2
FROM employee_contract 
ORDER BY deptno, salary;

--window expression preceding and following
SELECT 
name, dept_num as dno, salary AS sal,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) win1,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) win2,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) win3,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) win4,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) win5,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS 2 PRECEDING) win6,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS UNBOUNDED PRECEDING) win7
FROM employee_contract
ORDER BY dno, name;

--window expression current_row
SELECT 
name, dept_num as dno, salary AS sal,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND CURRENT ROW) win8,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) win9,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) win10,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) win11,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) win12,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) win13,
max(salary) OVER (PARTITION BY dept_num ORDER BY name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) win14
FROM employee_contract
ORDER BY dno, name;

--window reference
SELECT name, dept_num, salary,
MAX(salary) OVER w1 AS win1,
MAX(salary) OVER w2 AS win2,
MAX(salary) OVER w3 AS win3
FROM employee_contract
WINDOW
w1 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
w2 as w3,
w3 as (PARTITION BY dept_num ORDER BY name ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
;

