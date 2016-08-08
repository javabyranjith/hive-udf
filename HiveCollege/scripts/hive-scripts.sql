-- CREATE TABLE
use ranjith;
DROP TABLE IF EXISTS college;
CREATE EXTERNAL TABLE college(id STRING, name STRING, address STRING, admissionyear INT, admissiondata STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "/user/<username>/ranjith/college/data";

-- CHECK THE COUNT
SELECT COUNT(*) FROM college;

-- VIEW THE DATA by direct select
select name, id from college;

-- VIEW DATA by creating separate view for STRUCT fields.
use ranjith;
ADD JAR /hadoop/user/<username>/ranjith/college/jdom2-2.0.6.jar;
ADD JAR /hadoop/user/<username>/ranjith/college/jaxen-1.1.6.jar;
ADD JAR /hadoop/user/<username>/ranjith/college/HiveCollege-0.1.jar;
CREATE TEMPORARY FUNCTION college AS 'jbr.hivecollege.CollegeUDF';
DROP TABLE IF EXISTS college_admission_data;
CREATE TABLE college_admission_data AS
SELECT c.id as college_id, c.name as college_name, c.address as college_address, c.admissionyear as admission_year, ad.students, ad.staffs
FROM ranjith.college c LATERAL VIEW college(c.admissiondata) ad AS
students, staffs;

DROP VIEW IF EXISTS college_students_details;
CREATE VIEW college_students_details AS
SELECT ad.college_id, ad.college_name, ad.admission_year, stu.id as student_id, stu.firstname, stu.lastname, stu.dob, stu.gender, stu.phone, stu.email, stu.course, stu.dept 
FROM college_admission_data ad
LATERAL VIEW explode(ad.students) stuLv as stu;

SELECT * FROM college_staffs_details;

DROP VIEW IF EXISTS college_staffs_details;
CREATE VIEW college_staffs_details AS
SELECT ad.college_id, ad.college_name, ad.admission_year, stf.id as staff_id, stf.firstname, stf.lastname, stf.dob, stf.gender, stf.phone, stf.email, stf.dept, stf.dateofjoin, stf.qualification 
FROM college_admission_data ad
LATERAL VIEW explode(ad.staffs) stuLv as stf;

SELECT * FROM college_staffs_details;