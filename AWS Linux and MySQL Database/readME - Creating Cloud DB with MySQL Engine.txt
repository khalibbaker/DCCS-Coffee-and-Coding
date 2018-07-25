Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
--------------------------------------------------------------------------------
READ ME
--------------------------------------------------------------------------------
These are instructions to create a cloud database server with a MySQL engine using AWS. 

--------------------------------------------------------------------------------
Create a cloud DB with MySQL build
--------------------------------------------------------------------------------
1. Services --> RDS

2. Launch a DB Instance

3. Select the MySQL flavor
	- Only allow for free tier usage

4 Specificy DB Details
	- License model: general-public-license
	- DB engine version: Latest 5.6 version
	- DB instance class: db.t2.micro -- 1 vCPU, 1 GiB RAM

5. Configure settings
	- DB instance identifier: khalibs-universe *should be very unique***
	- Master username: root
	- Master password: *************
	
6. Configure Advanced Setting
	- Virtual Private Cloud: Default VPC
	- Subnet Group: default
	- Pubic accessibility: Yes, because we don't have a private cloud
	- VPC security group: Choose the same security group you made for the
	the EC2 Linux sever that you created. It should already be listed.

7. Database options (not really needed, especially if you do not want a database created
			at the initial start up)
	- Database name: *your DB name*
	- Database port: (default) 3306
	- DB parameter group: default
	- Option group: default
	- IAM DB authentication
	
8. Backup
	- Backup rentention period: 7 days
	- Backup window: 4:00 UTC, which is 12:00am EST

9. Maintenance
	- enable auto minor version upgrade to keep our database server in good
	condition
	- Maintenance window: (optional)

10. Launch DB instance
