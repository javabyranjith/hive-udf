--=====MapObjectInspector GenericUDF Examples=================================================================
ADD JAR /prod/hadoop/user/<ranjith>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION MapObjectInspectorGenericUDF AS 'jbr.hivegenericudf.mapOI.MapObjectInspectorGenericUDF';
SELECT MapObjectInspectorGenericUDF(empid, firstname, lastname) FROM ranjith.empinfo;

ADD JAR /prod/hadoop/user/<ranjith>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
ADD FILE /prod/hadoop/user/<ranjith>/ranjith/hiveudf/testdata/genericudf/sample-data.txt;
CREATE TEMPORARY FUNCTION ReadFileAtConfigureMethodGUDF AS 'jbr.hivegenericudf.mapOI.ReadFileAtConfigureMethodGUDF';
SELECT ReadFileAtConfigureMethodGUDF() FROM ranjith.empinfo;

ADD JAR /prod/hadoop/user/<ranjith>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
ADD FILE /prod/hadoop/user/<ranjith>/ranjith/hiveudf/testdata/genericudf/sample-data.txt;
CREATE TEMPORARY FUNCTION ReadFileAtIntializeMethodGUDF AS 'jbr.hivegenericudf.mapOI.ReadFileAtIntializeMethodGUDF';
SELECT ReadFileAtIntializeMethodGUDF() FROM ranjith.empinfo;


--====Primitive ObjectInspector GenericUDF Examples=======================================================
ADD JAR /prod/hadoop/user/<ranjith>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION TextArrayGUDF AS 'jbr.hivegenericudf.primitiveOI.TextArrayGUDF';
SELECT TextArrayGUDF('ranjith') FROM ranjith.empinfo;


--====StructObjectInspecor GenericUDF Examples======================================
ADD JAR /prod/hadoop/user/<ranjith>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION EmpExperienceInfoStructOI AS 'jbr.hivegenericudf.structOI.EmpExperienceInfoStructOI';
SELECT EmpExperienceInfoStructOI(empid, doj) FROM ranjith.empinfo;

ADD JAR /prod/hadoop/user/<themescape>/ranjith/hiveudf/lib/HiveGenericUDFExamples-0.1.jar;
CREATE TEMPORARY FUNCTION EmpInfoStructOI AS 'jbr.hivegenericudf.structOI.EmpInfoStructOI';
SELECT EmpInfoStructOI(empid,firstname,lastname) FROM ranjith.empinfo;