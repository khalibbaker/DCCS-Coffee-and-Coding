Published by: Khalib Baker, President
Organization: Data and Cloud Computing Society
Location: University of Maryland, College Park
------------------------------------------------------------------------------------------
READ ME
------------------------------------------------------------------------------------------
The purpose of this document is to ensure that your technical environment is up-
to-date and functional. There are two sections in this document: "Running Updates" and
"Installing Libraries for Python". 

It is assumed that you have Anaconda installed on your machine and that you already have

------------------------------------------------------------------------------------------
Running Updates
------------------------------------------------------------------------------------------
First elevate your priviledges to root permissions
	$ sudo su 
1. Update pip (open Anaconda prompt)
	$ pip install --upgrade pip

2. Update Amazon Linux Server
	$ yum update

3. Update Python from python2 to python 3
	- Check the version
	$ python --version
	
	- If the version is 2.XX, you'll never to update to a python 3.XX version
	$ yum install python34
	
	- Check the version
	$ python3 --version


------------------------------------------------------------------------------------------
Installing Libaries for Python (open Anaconda prompt)
------------------------------------------------------------------------------------------
Make sure your priviledges are root permissions.

Resources:
	- If pip is giving issues: https://docs.aws.amazon.com/cli/latest/userguide/awscli-install-linux.html

1. Install numpy library
	$ pip install numpy

2. Install PyMySQL
	$ pip install PyMySQL
	$ python3 -m pip install PyMySQL





