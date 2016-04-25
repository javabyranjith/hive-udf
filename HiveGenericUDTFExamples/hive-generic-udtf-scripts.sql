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
