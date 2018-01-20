--IF condition
SELECT IF(empid='100','id found','id not found') FROM empinfo; -- returns 'id found'


--COALESCE
SELECT COALESCE(null,2,null,5) FROM empinfo LIMIT 1; -- returns 2 since 2 is the first not null value in the sequence. 


--CASE 
SELECT
CASE 
WHEN empid ='100' THEN 'id found'
ELSE 'id not found'
END 
FROM empinfo;

SELECT
CASE 
WHEN empid ='100' THEN 'id found'
ELSE 'id not found'
END AS id 
FROM empinfo;