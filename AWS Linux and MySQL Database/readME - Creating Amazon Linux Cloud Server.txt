Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
--------------------------------------------------------------------------------------------
READ ME
--------------------------------------------------------------------------------------------
These are instuctions to create a basic Amazon Linux server in the cloud. After 
creating the Amazon Linux server, you will be able to perform actions on the server using
and SSH connection

--------------------------------------------------------------------------------------------
Launch an Amazon Linux Server
--------------------------------------------------------------------------------------------
1. Services --> EC2

2. Launch Instance

3. Configure the EC2 instance settings
	- Choose and Amazon Machine Image -> Amazon Linux AMI 
	- Choose Instance Type -> t2.mirco (free tier eligible)
	- Configure Instance
	** if there is a setting that is missing, leave it at the default setting**
		> Number of instances = 1
		> Network = vpc-XXXXXXXX (default)
		> Subnet = No preference (default subnet in any Availability Zone)
		> IAM Role = *create a role with s3 admin access, instructions below)
		> Shutdown behavior = Stop
		> Check box for Protect against accidental protection 
	- Add Storage
		> Size (Gib) like SSD storage = 8
		> Volume Type (General Purpose SSD)
	- Add Tags (tags = metadata)
		Key = Name, Value = *your server name*

	- Configure Security Group
		> Assign a security group = Create a new security group
		> Security Group Name = Amazon Linux
		> Description = Linux with backup storage for SQL file
		
		-> Type = SSH, Source = My IP, Description = SSH for root
		-> Type = MYSQL/Aurora, Source = My IP, Description = MySQL for root
		
	- Review Launch 
		> Select an existing key pair or create a new pair
	- Launch



-------------------------------------------------------------------------------------------
SSH to Amazon Linux Server (WINDOWS AND MAC)
-------------------------------------------------------------------------------------------
Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html

[FOR MAC]

1. Open terminal
	> Make sure you are in same directory as the PEM file
	> $ ssh -i "keypairfile.pem" ec2-user@ec2-XX-XX-XX-XXX.compute-1.amazonaws.com

[FOR WINDOWS]

1. Open putty
	> Enter public DNS for host name (i.e. ec2-XX-XXX-XXX-XXX.compute-1.amazonaws.com)
	> Go to SHH
	> Data, enter username (i.e. ec2) 
	> Go to auth
	> Select ppk file for Private Key Authentication
	> Save settings (recommended)
	> Open






