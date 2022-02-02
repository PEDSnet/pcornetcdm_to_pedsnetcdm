#!/bin/sh

export logFile=logs/log_file.log

# Set these environmental variables
#sets variables via reading .ini file
DATABASE=$(awk -F "=" '/database/ {print $2}' database.ini)
USERNAME=$(awk -F "=" '/user/ {print $2}' database.ini)
HOSTNAME=$(awk -F "=" '/host/ {print $2}' database.ini)
data1=$(awk -F "= " '/password/ {print $2}' database.ini)
#set file to the passed in .sql transform file
file=$1

export PGPASSWORD=$data1

#for given hostname/user/database, run assignment query in file
psql -v ON_ERROR_STOP=1 -h $HOSTNAME -U $USERNAME $DATABASE -f $file
