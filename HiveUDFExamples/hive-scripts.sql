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
	
	--use Hive config variable
	SET doj=select doj from empinfo where empid='100';
	SELECT emp_exp_string(empid,${hiveconf:doj}) from empinfo;
	
	

--MAP Examples
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
	ADD JAR /prod/hadoop/user/ranjith/emp/brickhouse-0.6.0.jar;
	CREATE TEMPORARY FUNCTION emp_exp_map AS 'jbr.hiveudf.EmpExperienceUDFMap';
	CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
	SELECT emp_exp_map(ei.empid,ei.doj) from ranjith.empinfo ei, ranjith.empsalary es; -- raw data
	SELECT to_json(emp_exp_map(ei.empid,ei.doj)) from ranjith.empinfo ei, ranjith.empsalary es; --using brickhouse map udf
	SELECT to_json(named_struct("empid",m.empid,"exp",m.experience)) FROM ranjith.empinfo ei LATERAL VIEW emp_exp_map(ei.empid,ei.doj) m AS empid, experience;
	SELECT to_json(named_struct("value",emp_exp_map(empid,doj))) from ranjith.empinfo;
	

--LIST Examples
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
	CREATE TEMPORARY FUNCTION emp_exp_list AS 'jbr.hiveudf.EmpExperienceUDFList';
	SELECT emp_exp_list(firstname) from ranjith.empinfo;
	SELECT emp_exp_list(empid) FROM ranjith.empinfo;
	SELECT empid FROM empinfo explode(emp_exp_list(firstname)) WHERE empid='100';
	SELECT struct(emp_exp_list(firstname)) FROM ranjith.empinfo;
	
	
-- OTHER Examples
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.1.jar;
	CREATE TEMPORARY FUNCTION emp_bonus AS 'jbr.hiveudf.EmpBonusUDF';
	SELECT emp_bonus('100','/user/<username>/mytest/emp/empbonus/'); -- by hdfs location
	SELECT emp_bonus('700','/user/<username>/mytest/emp/empbonus/empbonus2.txt'); -- by hdfs file

-- Read File Content from Amazon S3.
	ADD JAR s3://ranjith/HiveUDFExamples-0.2.jar;
	CREATE TEMPORARY FUNCTION ReadAwsS3FileContent AS 'jbr.hiveudf.ReadAwsS3FileContent';
	SELECT ReadAwsS3FileContent('<aws access key>','<aws security key>', 'ranjith', 'ranjith/myhive/hive-data.txt') FROM mydb.mytable;

-- Read File Content from HDFS file and list of files content from a directory.
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.2.jar;
	CREATE TEMPORARY FUNCTION ReadHDFSFileContent AS 'jbr.hiveudf.ReadHDFSFileContent';
	SELECT ReadHDFSFileContent('/user/<username>/mytest/testdata/file1.txt') FROM mydb.mytable; -- hdfs file
	SELECT ReadHDFSFileContent('/user/<username>/mytest/testdata') FROM mydb.mytable; --directory

--Generic UDF Examples
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.3.jar;
	CREATE TEMPORARY FUNCTION exp AS 'jbr.hivegenericudf.EmpInfoStructObjectInspector';
	SELECT exp(empid, doj) FROM empinfo;

	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.3.jar;
	CREATE TEMPORARY FUNCTION MapObjectInspectorGenericUDF AS 'jbr.hivegenericudf.MapObjectInspectorGenericUDF';
	SELECT MapObjectInspectorGenericUDF(empid, firstname, lastname) FROM ranjith.empinfo;

	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.3.jar;
	CREATE TEMPORARY FUNCTION StructObjectInspectorGenericUDF AS 'jbr.hivegenericudf.StructObjectInspectorGenericUDF';
	SELECT StructObjectInspectorGenericUDF(empid, firstname, lastname) FROM ranjith.empinfo;
	
	ADD JAR /prod/hadoop/user/<username>/ranjith/emp/HiveUDFExamples-0.3.jar;
	CREATE TEMPORARY FUNCTION EmpExperienceInfoStructOI AS 'jbr.hivegenericudf.EmpExperienceInfoStructOI';
	SELECT EmpExperienceInfoStructOI(empid, doj) FROM empinfo;

	

---- TRAIL & ERROR
	INSERT OVERWRITE DIRECTORY '/ranjith/hive/' select doj from empinfo;
	SELECT emp_exp_string(empid,${dfs -cat ranjith/hive/000000_0}) from empinfo;
	countinfo=${hdfs dfs -cat '/user/ranjith/hive/'};
