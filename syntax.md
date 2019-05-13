## SQSRV SQL Syntax ##

SQSRV has a limited version of SQL. This document will layout the current syntax that is allowed. 

Document Conventions:

- Each SQL command cannot be spread across multiple lines. In this text it may appear to be on multiple lines but SQSRV uses \n as the command terminator.
- Reserved Words are all uppercase e.g. SELECT
- Identifiers such as *tablename* or *col* are italicised
- Optional items are enclosed in square brackets e.g. \[NULL]
- Elipsis ... are used to indicate a repeating pattern
- Examples are highlited by a code block (shown below)

~~~
Examples are in code blocks
~~~

### Types ###

Current types for SQSRV are:

*	**int** - 64 bit signed integer
*	**string** - Variable length string
*	**bool** - Boolean with values of *true* or *false*

Note: All types may have the value of *null*

### SQL Commands ###

#### CREATE ####

CREATE TABLE *tablename* (*col1* *type* \[NOT \[NULL]], ..., *colN* *type* \[NOT \[NULL]])
  
	 CREATE TABLE people (firstname string NULL, lastname string, id int NOT NULL, active bool NOT NULL)
  
#### DROP ####

DROP TABLE *tablename*

~~~
DROP TABLE people
~~~

#### INSERT ####

##### Single Row Insert #####

INSERT INTO *tablename* (*col1*,..., *coln*) VALUES (*value1*,..., *valueN*)

~~~
INSERT INTO people (id, lastname, firstname, active) VALUES (1, "Flintstone", "Fred", true)
~~~

##### Multi Row Insert #####

INSERT INTO *tablename* (*col~1~*,..., *col~n~*) VALUES (*value~1~^1^*,..., *value~n~^1^*), ... ,(*value~1~^k^*, ..., *value~n~^k^*)

~~~
INSERT INTO people (id, active, lastname) VALUES (2, true, "Rubble"), (3, false, "Rockhead"), (4, true, "Slate")
~~~

#### UPDATE ####

UPDATE *tablename* SET *col~1~* = *value~1~*, ..., *col~n~* = *value~n~* \[WHERE [***Where clause***](#where-clause)]

~~~
UPDATE people SET active = true WHERE active = false
~~~

#### DELETE ####

DELETE FROM *tablename* \[WHERE [***Where clause***](#where-clause)]

~~~
DELETE FROM people WHERE id = 1 or id > 3
~~~

#### SELECT ####

SELECT * FROM *tablename* \[WHERE [***Where clause***](#where-clause)] 

SELECT *col1*, ..., *colN* FROM *tablename* \[WHERE [***Where clause***](#where-clause)] 

SELECT COUNT() FROM *tablename* \[WHERE [***Where clause***](#where-clause)] 

~~~
SELECT firstname, lastname FROM people WHERE active = true
~~~

### Clauses ###

#### *Where clause* ####

\[NOT] *col* ***comparison*** *value* \[AND||OR] ...

#### *Comparison* ####

One of **>**, **<**, **=**
