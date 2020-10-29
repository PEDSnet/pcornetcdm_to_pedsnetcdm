# `sql_etl`

sql_etl is a python based CLI too that is used to automate the process of creating the DDL for PCORnet ETL.


# Dependancies

## Python 

`sql_etl` is a python based tool. It is built under the virtual environment. This tool uses python click library for building
CLI tool. The set up tool make it easy to install. 

# Building and Running the tool

1. Navigate to [sql_etl](https://github.com/PEDSnet/pcornetcdm_to_pedsnetcdm/tree/sql_etl) folder, and download the tool.

3. Check if postgres is installed on the system using
   
   `which psql`
   
   if not install postgres onto the system using command below depending upon operating system
   
   
     `brew install postgres`
     
     or 
     
     `apt-get install postgresql libpq-dev postgresql-client postgresql-client-common`
     
     or
     
     `yum install postgres`

2. To install the CLI Tool

	  activate the virtual environment using following command
	
	`virtualenv venv`
	
	`. venv\bin\activate`
	
   install the tool
	
	 `pip install -r requirements.txt`
	 
	 `pip install --editable .`

   
3.  Select form the following option:
	
	 `loading -u <username> -h <hostname> -d <dbname> -s <schemaname> -o <option>`
	 
	 option :
	  1. pipeline&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Run the full PCORnet pipeline
	  2. ddl&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Only the DDL or initial setup for the PEDSnet to PCORnet CDM
	  3. etl&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Runs only the ETL on the PEDSnet data.
	  4. truncate&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp;Re-run of the ETL use this option to truncate all tables and remove Foregin Key constraints
	  5. update_map -&nbsp; Adding or updating new values in the concept map table.
   
   where the dbname is the name of the database which contains the PEDSnet schema that we want to Transform.
         schemaname is the name of the schema that is to be transformed
         
   Example
        
        loading_pedsnet -u sam -h host.com -d database_v27 -s dcc_pcornet -o ddl -pv v4.0
        
   To know more information of the tool
        
        loading_pedsnet --help
        
# Known issue
This tool as of now only work for PostgreSQL database. We are planning to make it compatible and more generic.
