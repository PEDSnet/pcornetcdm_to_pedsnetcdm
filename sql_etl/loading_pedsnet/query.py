# region Imports
import os
import shutil
import requests
import re
import glob
import fileinput
# endregion

# region filenames
concept_map_files = "data/concept_map"
csv_concept = "data/concept_map.csv"
create_table_script = "scripts/ddl_scripts/create_table.sql"
site_col_file = "scripts/ddl_scripts/site_col.sql"
privileges = "scripts/ddl_scripts/privileges.sql"
alt_owner_file = "scripts/ddl_scripts/alter_tbl_owner.sql"
trunc = "scripts/ddl_scripts/trunc_fk_idx.sql"
etl_temp_files = "scripts/etl_scripts_temp/"
etl_files ="scripts/etl_scripts/"

# endregion

# region create valueset map
def create_table(schema):
    """
    create tables in the PostgreSQL database in specified schema
    """
    for line in fileinput.input(create_table_script, inplace=1, backup='.bak'):
        line = re.sub('EXISTS .*?\.', "EXISTS " + schema + ".", line.rstrip())
        print(line)
    with open(create_table_script, 'r') as valueset_file:
        commands = valueset_file.read()
    return commands
# endregion

# region Schema Creation
def create_schema(schema):
    """creates schema if not exists"""
    command = """CREATE SCHEMA IF NOT EXISTS """ + schema + """ AUTHORIZATION pcor_et_user;
                 GRANT USAGE ON SCHEMA """ + schema + """ TO peds_staff;
                 GRANT ALL ON SCHEMA """ + schema + """ TO dcc_owner;
                 GRANT ALL ON SCHEMA """ + schema + """ TO loading_user;
                 """
    return command
# First line of query was originally 
# "CREATE SCHEMA IF NOT EXISTS (Insert Schema) AUTHORIZATION pcornet_user"
#removed AUTHORIZATION pcornet_user as the permission was throwing errors
# endregion

# region create DDL
def dll(pedsnet_version):
    """Creates dll for PEDSnet"""
    try:
        if pedsnet_version:
            dll_url = "http://data-models-sqlalchemy.research.chop.edu/pedsnet/" + pedsnet_version.strip('v') + ".0/ddl/postgresql/tables/"
        else:
           dll_url = 'https://data-models-sqlalchemy.research.chop.edu/pedsnet/4.5.0/ddl/postgresql/tables/'
        dll_script = requests.get(dll_url).text
        return dll_script
    except (Exception, requests.ConnectionError) as e:
        print(e)
# endregion

def indexes(pedsnet_version):
    """Creates dll for PEDSnet"""
    try:
        if pedsnet_version:
            index_url = "http://data-models-sqlalchemy.research.chop.edu/pedsnet/" + pedsnet_version.strip('v') + ".0/ddl/postgresql/indexes/"
        else:
           index_url = 'https://data-models-sqlalchemy.research.chop.edu/pedsnet/4.5.0/ddl/postgresql/indexes/'
        
        #add 'IF NOT EXISTS' to index script
        index_script = requests.get(index_url).text
        lines = index_script.split('\n')
        for x in range(len(lines)):
            line = lines[x]
            i = line.find("CREATE INDEX")
            if(i >= 0):
                lines[x] = line[:i + len("CREATE INDEX")] + ' IF NOT EXISTS' + line[i + len("CREATE INDEX"):]
        index_modified_script = '\n'.join(lines)
        return index_modified_script
    except (Exception, requests.ConnectionError) as e:
        print(e)

def constraints(pedsnet_version):
    """Creates dll for PEDSnet"""
    try:
        if pedsnet_version:
            constraint_url = "http://data-models-sqlalchemy.research.chop.edu/pedsnet/" + pedsnet_version.strip('v') + ".0/ddl/postgresql/constraints/"
        else:
           constraint_url = 'https://data-models-sqlalchemy.research.chop.edu/pedsnet/4.5.0/ddl/postgresql/constraints/'
        
        #add 'IF NOT EXISTS' to constraint script
        constraint_script = requests.get(constraint_url).text
        lines = constraint_script.split('\n')
        for x in range(len(lines)):
            line = lines[x]
            i = line.find("REFERENCES concept")
            if(i >= 0):
                lines[x] = line[:i + len("REFERENCES")] + ' vocabulary.' + line[i + len("REFERENCES "):]
                constraint_modified_script = '\n'.join(lines)
        return constraint_modified_script
    except (Exception, requests.ConnectionError) as e:
        print(e)

# region Alter site column
def site_col():
    """This function alters the table and creates the site columns"""
    # Replace variables in file
    #for line in fileinput.input(site_col_file, inplace=1, backup='.bak'):
    #    line = re.sub('db_name .*?\)', db_name+ ",\'" + schema + "\'", line.rstrip())
    #    print(line)
    with open(site_col_file, 'r') as site_file:
        alter_site_col = site_file.read()
    return alter_site_col
# endregion

# region Set the privileges
def permission(schema):
    """This function sets up the permissions to the schemas"""
    for line in fileinput.input(privileges, inplace=1, backup='.bak'):
        line = re.sub('SCHEMA .*?\ TO', "SCHEMA " + schema + " TO", line.rstrip())
        print(line)
    with open(privileges, 'r') as perm_file:
        privilege = perm_file.read()
    return privilege
# endregion

# region Alter table owner
def owner(schema):
    """This function returns the sql script to change the owner of all schemas"""
    with open(alt_owner_file, 'r') as owner_file:
        alter_owner = owner_file.read()
    return alter_owner + "select alter_tbl_owner_loading('" + schema + "')"
# endregion

# region Truncate All tables in schema and remove FK's
def truncateqry(schema):
    command = """SELECT truncate_schema('""" + schema + """');"""
    return command
# endregion

# region ETL Scripts Modify
def get_etl_ready(schema):
    # remove all the temp file from the directory etl_scripts
    if os.path.exists(etl_temp_files):
        shutil.rmtree(etl_temp_files)
    shutil.copytree(etl_files, etl_temp_files)

    for file in glob.glob(os.path.join(etl_temp_files, '*.sql')):
        with open(file, 'r') as f:
            content = f.read()
            content = content.replace('SITE', schema)
            f.close()
        with open(file, 'w') as f:
            f.write(content)
# endregion

