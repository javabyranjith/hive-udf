CREATE DATABASE IF NOT EXISTS ranjith;
USE ranjith;
DROP TABLE IF EXISTS empinfo;
CREATE EXTERNAL TABLE empinfo(empid STRING, firstname STRING, lastname STRING, dob STRING, designation STRING, doj STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/<yourusername>/ranjith/hive/data/emp/empinfo/";

DROP TABLE IF EXISTS empcontact;
CREATE EXTERNAL TABLE empcontact(empid STRING, doorno STRING, street STRING, city STRING, country STRING, pincode STRING, phone STRING, mobileno1 STRING, mobileno2 STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/<yourusername>/ranjith/hive/data/emp/empcontact/";

DROP TABLE IF EXISTS empsalary;
CREATE EXTERNAL TABLE empsalary(empid STRING, basic STRING, hra STRING, allowance STRING, deduction STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/<yourusername>/ranjith/hive/data/emp/empsalary/";

--ADD JAR and create function
ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION emp_exp_string AS 'jbr.hiveudf.EmpExperienceUDFString';

--STRING examples
ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION emp_exp_string AS 'jbr.hiveudf.EmpExperienceUDFString';
SELECT emp_exp_string(empid,doj) from empinfo;

--MAP Examples
ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
ADD JAR /prod/hadoop/user/ranjith/emp/brickhouse-0.6.0.jar
CREATE TEMPORARY FUNCTION emp_exp_map AS 'jbr.hiveudf.EmpExperienceUDFMap';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
SELECT emp_exp_map(ei.empid,ei.doj) from ranjith.empinfo ei, ranjith.empsalary es; -- raw data
SELECT to_json(emp_exp_map(ei.empid,ei.doj)) from ranjith.empinfo ei, ranjith.empsalary es; --using brickhouse map udf


--LIST Examples
ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION emp_exp_list AS 'jbr.hiveudf.EmpExperienceUDFList';
SELECT emp_exp_list(firstname) from ranjith.empinfo;

-- OTHER Examples
ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION emp_bonus AS 'jbr.hiveudf.EmpBonusUDF';
--SELECT emp_bonus('100','/user/<username>/mytest/emp/empbonus/'); -- by hdfs location
SELECT emp_bonus('700','/user/<username>/mytest/emp/empbonus/empbonus2.txt'); -- by hdfs file
