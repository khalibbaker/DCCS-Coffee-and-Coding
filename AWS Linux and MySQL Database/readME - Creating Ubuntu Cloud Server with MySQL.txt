Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
-------------------------------------------------------------------------------------------
READ ME
-------------------------------------------------------------------------------------------
These are instructions to create a basic Ubuntu Linux Server with AWS. In addition
to creating the server, these instructions will assist with installing MySQL workbench on
the server.

-------------------------------------------------------------------------------------------
Create Linux Server (Unbuntu flavor) 
-------------------------------------------------------------------------------------------
Tools needed:
	- Putty, PuttyGen (FOR WINDOWS USERS)
	- AWS CLI 

1. Services --> EC2
	> Launch Instance
	> Ubuntu Server (free tier) 
	> Configure instance details
		- 1 instance
		- default subnet (availability zone)
		- enable termination protection
		- IAM Role: s3 access (recommended)
	> Add storage
		- x gbs
	> Add tags 
	> Configure Security Group
		- SSH --> IP (anywhere)
		- RDP --> IP (anywhere)

2. Get host name
	> Select instance
	> Connect
	> Copy the public DNS (i.e. ec2-XX-XXX-XXX-XXX.compute-1.amazonaws.com)
	> Copy username, before @ in example (i.e. ubuntu)

-------------------------------------------------------------------------------------------
SSH to Ubuntu Server (WINDOWS AND MAC)
-------------------------------------------------------------------------------------------
[FOR MAC]

1. Open terminal
	> Make sure you are in same directory as the PEM file
	> # ssh -i "MyEC2KeyPair.pem" ubuntu@ec2-54-197-191-179.compute-1.amazonaws.com

[FOR WINDOWS]

1. Open putty
	> Enter public DNS for host name (i.e. ec2-XX-XXX-XXX-XXX.compute-1.amazonaws.com)
	> Go to SHH
	> Data, enter username (i.e. ubuntu) 
	> Go to auth
	> Select ppk file for Private Key Authentication
	> Save settings (recommended)
	> Open

------------------------------------------------------------------------------------------
Run Updates and Install RDP (remote desktop) on Ubuntu Server
------------------------------------------------------------------------------------------
Begin with an SSH to the Ubuntu server. For Windows user, you will use Putty. For Mac users,
you will use the termimal.

Mac: https://www.youtube.com/watch?v=q6Hm-JIzjT8
Windows: https://www.youtube.com/watch?v=bi7ow5NGC-U

Once SSH connection has been established, and you are in the terminal of the server.
1. Go to root
	> $ sudo su
	
2. Run updates
	> $ apt-get update

3. Install Ubuntu Desktop
	> $ apt-get install kubuntu-desktop

4. Install RDP
	> $ apt-get install xrdp

5. Create root passowrd
	> $ passwd root
	> enter password: ***************
6. Run the following list of commands:
	> $ sudo apt update && sudo apt upgrade
	> $ sudo sed -i 's/^PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config
	> $ sudo /etc/init.d/ssh restart
	> $ sudo apt install xrdp xfce4 xfce4-goodies tightvncserver
	> $ echo xfce4-session> /home/ubuntu/.xsession
	> $ sudo cp /home/ubuntu/.xsession /etc/skel
	> $ sudo sed -i '0,/-1/s//ask-1/' /etc/xrdp/xrdp.ini
	> $ sudo service xrdp restart

-------------------------------------------------------------------------------------------
RDP into Ubuntu Server
-------------------------------------------------------------------------------------------
[FOR MAC]
1. Instructions: https://www.youtube.com/watch?v=x7TCvLuWlF0

[FOR WINDOWS]
1. Open Remote Desktop Connection

2. Enter public DNS in for computer (i.e. ec2-XX-XXX-XXX-XXX.compute-1.amazonaws.com), public DNS
can be found in your AWS account EC2 service
	> Enter username
	> Enter password
	> Port -1
--------------------------------------------------------------------------------------------
Install MySQL Workbench and Server on Ubuntu Server (GUI)
--------------------------------------------------------------------------------------------
1. Run the following commands in terminal in Ubuntu
	> $ sudo apt-get install mysql-workbench
	> $ sudo apt-get install mysql-server
		- enter a password for root
2. Stop Server (turn off server)
	> Services --> EC2
	> Actions
	> Instance State: Stop, then start (there will be a new public dns)
--------------------------------------------------------------------------------------------
Install AWS CLI using PIP on your own computer (local machine, not server)
--------------------------------------------------------------------------------------------
For Windows (use MSI installer): https://docs.aws.amazon.com/cli/latest/userguide/awscli-install-windows.html
For Mac (use terminal): https://docs.aws.amazon.com/cli/latest/userguide/cli-install-macos.html

Once you've installed AWS CLI, run the following command:
	> $ aws configure
	> The key and secret access key can be found on your AWS account







