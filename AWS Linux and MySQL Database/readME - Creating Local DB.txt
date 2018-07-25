Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
------------------------------------------------------------------------------------------
READ ME
------------------------------------------------------------------------------------------
This document will assist you with creating a database from the ground up using
MySQL workbench and MySQL server. Using the ER feature in MySQL workbench, you can use
a GUI interface to engineer a database script which can be used to construct a database
in the cloud or on local machines running MySQL.

------------------------------------------------------------------------------------------
Schema for ER Diagram
------------------------------------------------------------------------------------------
DB Name
	Table Name
		Column (--> primary key, if primary) [datatype]
			Note(s) about column
	...
		...
			...

-------------------------------------------------------------------------------------------
Creating  Local Database Using MySQL Workbench and Local MySQL server
-------------------------------------------------------------------------------------------
Tools needed:
	- MySQL Workbench (version: 6.3.10) https://www.mysql.com/products/workbench/
	- MySQL Server (version: 8.0.11 ) https://dev.mysql.com/downloads/mysql/
	- AWS CLI https://aws.amazon.com/cli/


1. Connect to MySQL server using root level access

2. *File --> New Model

3. Add Diagram
	> named diagram: my_world
	> re-named database: my_world

4. Add table(s)
	* Best to start with the tables that are going to be linked to the main table*
	> add columns
		- if a column is a primary key, check the box for PK
		- if a column cannot have null values, check box for NN
		- ...
	> configure column datatype

5. Configure relationships between the tables (i.e. many-to-many, many-to-one)
	> decide if relationships are "identifying or non-identifying"

6. Convert the ER diagram to database code
	> *Database --> Forward Engineer
	> General
		- (optional) generate INSERT statements for each table, NEXT
7. Database should now be created the locally
	> Copy file to clipboard and save to ___.sql using notepad 

-----------------------------------------------------------------------------------------------------------
Configure AWS CLI & Security Credentials
-----------------------------------------------------------------------------------------------------------
1. Service --> EC2

2. Generate an key pair and save to somewhere safe
	> EC2
	> Key Pairs
	> Create New Pair

3. In CMD, type $ aws configure
	> enter credentials

-----------------------------------------------------------------------------------------------------------
Copy SQL File to s3 bucket (Back Up Purposes)
-----------------------------------------------------------------------------------------------------------
1. Copy the sql file to s3 bucket (for backup purposes)
	> open CMD
	> run following command,  aws s3 cp /local/path/file.sql s3://bucketname/folder/file.sql
	> AWS Console --> verify that object was transferred

2. Enable versioning on the bucket for file logging 
	> Go to bucket
	> Properties --> enable versioning
		- versioning allows you to keep different and updated copies of the database file

----------------------------------------------------------------------------------------------------------
Create the MySQL script using ER Diagram
----------------------------------------------------------------------------------------------------------
1. Log in to your local database connection

2. File --> Open Model
	- select the model (.mwb extension)

3. Once the model is open, you have to engineer the script

4. Database --> Forward Engineer

5. Follow the default prompts until you get the text of the script

6. Copy and paste the script into a text file and save the file with a .sql extension
