--STRUCT
	SELECT struct("ranjith","sekar").col1 AS one, struct("ranjith","sekar").col2 AS two FROM empinfo;
	SELECT 
	struct(ei.empid,ei.firstname,ei.lastname, ec.phone).col1 AS empid, 
	struct(ei.empid,ei.firstname,ei.lastname, ec.phone).col2 AS firstname, 
	struct(ei.empid,ei.firstname,ei.lastname, ec.phone).col3 AS lastname, 
	struct(ei.empid,ei.firstname,ei.lastname, ec.phone).col4 AS phone 
	FROM empinfo ei JOIN empcontact ec ON (ec.empid=ei.empid);
	
--SPLIT
	SELECT split('ranjith|sekar','|')[0] as one, split('ranjith|sekar','|')[1] as two FROM empinfo;
	SELECT struct(split('ranjith,sekar',',')).col1 as one, struct(split('ranjith,sekar',',')).col2 as two FROM empinfo;
	SELECT split("r,a,n,j,i,t,h",",") FROM ranjith.empinfo ei JOIN ranjith.empcontact ec ON (ei.empid=ec.empid) WHERE ei.empid='100';
	SELECT struct(split("r,a,n",",")).col1 AS one, struct(split("r,a,n",",")).col2 AS two, struct(split("r,a,n",",")).col3 AS three FROM ranjith.empinfo;
	SELECT struct("a","b","c").col1 AS one, struct("a","b","c").col2 AS two, struct("a","b","c").col3 AS three FROM ranjith.empinfo;
	


--STACK
	CREATE TEMPORARY FUNCTION EmpExperienceUDFList AS 'jbr.hiveudf.EmpExperienceUDFList';
	SELECT STACK(2,(EmpExperienceUDFList('ra'))) FROM ranjith.empinfo LIMIT 1;
	SELECT STACK(2,split('ran,jith',',')) FROM ranjith.empinfo limit 1;
	
--LATERAL VIEW
	SELECT e.empid, f.firstchar FROM ranjith.empinfo e LATERAL VIEW explode(EmpExperienceUDFList(firstname)) f AS firstchar LIMIT 16;


-- TABLE JOIN Examples
	SELECT * FROM empinfo ei JOIN (SELECT empid ecid from empcontact ec where city='Chennai') q1 ON ei.empid = q1.ecid;
	SELECT ei.firstname, ec.ci FROM empinfo ei JOIN (SELECT city ci,empid eid FROM empcontact) ec ON ei.empid = ec.eid;
	SELECT explode(EmpExperienceUDFList(ec.city)) city FROM ranjith.empinfo ei JOIN ranjith.empcontact ec ON (ei.empid=ec.empid) WHERE ei.empid='100';
	
--INSERT 
	INSERT INTO TABLE test4 SELECT struct("q","b").col1 AS firstname,struct("q","b").col2 AS lastname FROM ranjith.empinfo limit 1;
	INSERT INTO TABLE test4 SELECT struct(split('r,a',',')).col1 AS firstname,struct(split('r,a',',')).col2 AS lastname FROM ranjith.empinfo limit 1;
