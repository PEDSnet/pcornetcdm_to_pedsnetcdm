# `sql_etl`

sql_etl is a python based CLI too that is used to automate the process of creating the DDL for PEDSnet from a PCORnet CDM.

Please refer to PDF for additional documentation

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
	
	`python3 -m venv venv`
	
	`source venv/bin/activate`
	
   install required packages
	
	 `pip install -r requirements.txt`
	 
	 `pip install --editable .`

   
3.  Select form the following option:
	
	 `loading -u <username> -h <hostname> -d <dbname> -s <schemaname> -o <option>`
	 
	 option :
	  1. pipeline&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Run the full PCORnet pipeline
	  2. ddl&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Only the DDL or initial setup for the PCORnet to PEDSNet CDM
	  3. etl&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;-&nbsp; Runs only the ETL on the PCORnet data
	  4. load_mapping_table -&nbsp; populate the concept map table.
   
        
# Known issue
This tool as of now only work for PostgreSQL database. We are planning to make it compatible and more generic.
