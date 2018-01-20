--=====StructObjectInspector GenericUDF Examples=================================================================
ADD JAR /prod/hadoop/user/<username>/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.1.jar;
ADD FILE /prod/hadoop/user/<username>/ranjith/hiveudf/testdata/genericudtf/hdfs-data.txt;
CREATE TEMPORARY FUNCTION ReadFileAtConfigStructGUDTF AS 'jbr.hivegenericudtf.structOI.ReadFileAtConfigStructGUDTF';
SELECT ReadFileAtConfigStructGUDTF() FROM ranjith.empinfo;

ADD JAR /prod/hadoop/user/<username>/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.1.jar;
ADD FILE /prod/hadoop/user/<username>/ranjith/hiveudf/testdata/genericudtf/hdfs-data.txt;
CREATE TEMPORARY FUNCTION ReadFileAtConfigStructGUDTF AS 'jbr.hivegenericudtf.structOI.ReadFileAtInitStructGUDTF';
SELECT ReadFileAtConfigStructGUDTF() FROM ranjith.empinfo;

ADD JAR /prod/hadoop/user/<username>/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION EmpInfoStructGUDTF AS 'jbr.hivegenericudtf.structOI.EmpInfoStructGUDTF';
CREATE TABLE empinfotemp AS
SELECT EmpInfoStructGUDTF(empid,firstname,lastname,dob,designation) FROM ranjith.empinfo;


DROP TABLE IF EXISTS empinfotemp2;
ADD JAR /prod/hadoop/user/<username>/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.2.jar;
CREATE TEMPORARY FUNCTION EmpInfoStructGUDTF AS 'jbr.hivegenericudtf.structOI.EmpInfoStructGUDTF';
CREATE TABLE empinfotemp2 AS
SELECT EmpInfoStructGUDTF(empid,firstname,lastname,dob,designation) FROM ranjith.empinfo;
select * from empinfotemp2;


use ranjith;
drop table if exists einfo;
ADD JAR /prod/hadoop/user/themescape/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.3.jar;
CREATE TEMPORARY FUNCTION EmpInfoStructGUDTF AS 'jbr.hivegenericudtf.structOI.EmpInfoStructGUDTF';
CREATE TABLE einfo AS
SELECT EmpInfoStructGUDTF(empid,firstname,lastname,dob,designation) FROM ranjith.empinfo;
select * from einfo;

-- STRUCT DATA TYPE (for library.xml)
-- 1. create base table
USE ranjith;
DROP TABLE IF EXISTS library;
CREATE TABLE library (bookid STRING, department STRING, data STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;
--LOCATION '/user/<username>/ranjith/hiveudf/testdata/library/'; // this is hdfs path
-- 2. load data into table
 LOAD DATA LOCAL INPATH '/hadoop/user/<username>/ranjith/hiveudf/testdata/library/library1.txt' OVERWRITE INTO TABLE library;

USE ranjith;
DROP TABLE IF EXISTS library1;
CREATE TABLE library1 (bookid STRING, department STRING, data STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
LOCATION 'hdfs://Ceres/user/themescape/ranjith/hiveudf/testdata/library/';

--STRUCT
books : STRUCT <
		id : STRING,
		category : STRING,
		department : STRING,
		title : STRING,
		author : STRUCT <
				 firstname : STRING,
				 lastname : STRING,
				 address: STRUCT <
					      doorno : INT,
						  city : STRING,
						  state : STRING,
						  country : STRING
				>				
		>,
		publisher : STRING,
		publishdate : date,
		price : STRING,
		isbn : STRING,
		>
-- STRUCT QUERIES
-- create table with struct data on existing table data using UDF.
USE ranjith;
ADD JAR /prod/hadoop/user/themescape/ranjith/hiveudf/lib/HiveGenericUDTFExamples-0.4.jar;
CREATE TEMPORARY FUNCTION libstruct AS 'jbr.hivegenericudtf.structOI.LibraryStruct';
CREATE TABLE libtemp1 AS
select libstruct(data) FROM library;
select mydata FROM dalibtemp1;

-- view the struct data