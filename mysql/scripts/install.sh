#!/bin/bash

#https://www.digitalocean.com/community/tutorials/how-to-install-mysql-on-ubuntu-20-04
#https://stackoverflow.com/questions/24270733/automate-mysql-secure-installation-with-echo-command-via-a-shell-script

#sudo apt install python3-pip -y
#sudo pip3 install mysql-connector-python

#GRANT ALL PRIVILEGES ON *.* TO 'raktim'@'localhost';
#CREATE USER 'raktim'@'localhost' IDENTIFIED BY '***';

# Make sure that NOBODY can access the server without a password
mysql -e "UPDATE mysql.user SET Password = PASSWORD('CHANGEME') WHERE User = 'root'"
# Kill the anonymous users
mysql -e "DROP USER ''@'localhost'"
# Because our hostname varies we'll use some Bash magic here.
mysql -e "DROP USER ''@'$(hostname)'"
# Kill off the demo database
mysql -e "DROP DATABASE test"
# Make our changes take effect
mysql -e "FLUSH PRIVILEGES"
# Any subsequent tries to run queries this way will get access denied because lack of usr/pwd param
