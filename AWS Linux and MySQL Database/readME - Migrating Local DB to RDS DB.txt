Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
-------------------------------------------------------------------------------------------
READ ME
-------------------------------------------------------------------------------------------
These are instructions to move your locally created database to your cloud database
server. Once your database is migrated to the cloud, you will have the ability to connect
to the server using MySQL Workbench IDE. 

You should already have the SQL model (.mwb) and SQL script (.sql) files saved to your
bucket before performing the steps below.

-------------------------------------------------------------------------------------------
Migrating SQL file to EC2 instance from bucket
-------------------------------------------------------------------------------------------
1. SSH to your EC2 server
	- FOR WINDOWS (use putty)
	- FOR MAC (use terminal comnand)
		> $ ssh -i /path/my-key-pair.pem ec2-user@ec2-198-51-100-1.compute-1.amazonaws.com

2. Install AWS CLI on Ubuntu server (if Amazon Linux Sever, skip this step because AWS CLI is already installed)
	> $ sudo su
	> $ apt install awscli

2. Verify that your SQL file exist in your bucket
	> $ aws s3

3. Create a folder in your Linux Server to also hold the file (extra backup)
	> $ sudo su
	> $ mkdir *nameoffolder* (i.e. mkdir sqldumpfiles)

4. Go to the folder
	> $ cd *nameoffolder (i.e. cd sqldumpfiles)

5. Transfer the file from s3 bucket to your server
	> $ aws s3 cp s3://path/to/file.sql ./name_the_file.sql 
OR you can transfer the whole contents of the folder
	> $ aws s3 cp s3://path/of/folder ~/name/of/directory/ --recursive

--------------------------------------------------------------------------------------------
Connect to your DB Server 
--------------------------------------------------------------------------------------------
1. While in an SSH session, go to the location of the .sql file that you want to move to
the cloud databse server
	> $ sudo su
	> $ cd /path/to/file.sql

2. Make sure that you have the MySQL server client installed on your linux machine
	> $ sudo yum install mysql-server mysql-client

3. Test connection to cloud database sever
	> $ mysql - h *endpoint* -P 3306 -u *username from db creation* -p
	> enter password: **************************************

4. If sucessfully in the cloud db server, you can create database and then exit OR just use mysql workbench to import the file
	mysql > create database cloud_db_name; (i.e. khalibs_world)
	mysql > exit

--------------------------------------------------------------------------------------------
Connect to the server locally using MySQL Workbench and import .sql file skeleton
--------------------------------------------------------------------------------------------
1. Open MySQL Workbench

2. Add connection (+)

3. Setup New Connection 
	- Connection Name: whatever you want to name the connection or you can use the name
	of the server
	- Connection Method: Standard (TCP/IP)
	- Hostname: rds endpoint
	- Username: rds masterusername

4. Test Connection (button), enter rds masterpassword

5. Import the .sql file to the cloud database
	- Server --> Data Import
	- Import from Self-Contained File, browse for the sql file

6. After import successful, verify that things transfered 
	- Connect to cloud database server via SSH in EC2 server (see above examples)
	mysql > use db_name; (i.e. use khalibs_world;)
	mysql > show tables;

Turn off servers, when done! 