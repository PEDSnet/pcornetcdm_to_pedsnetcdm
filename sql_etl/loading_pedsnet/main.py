# region import
import configparser
import click
import os
from loading_pedsnet import process
import shutil
import config
# endregion

configfile_name = "database.ini"
temp="scripts/temp/"

@click.command()
@click.option('--pwprompt', '-p', is_flag=True, default=False,
              help='Prompt for database password')
@click.option('--searchpath', '-s', help='Schema search path in database.ex. stlouis_pcornet')
@click.option('--user', '-u', default=False, help='Database username')
@click.option('--database', '-d', default=False,
              help='Database in wich the mapping file to be loaded ex. pedsnet_dcc_vxx')
@click.option('--host', '-h', default=False, help='The Server name ex. dev01')
@click.option('--options', '-o', default=False, help='pipeline \netl \nddl \nload_mapping_table \nindexes \nfk_constraints \ntest_script')
@click.option('--testscript', '-ts', required=False, type=click.File('rb'), help='Run single table at a time')
@click.option('--pedsnet_version', '-pv', default='v4.5', help='PEDSnet ETL version v3.0 \n v4.0 \n v4.1')

def cli(searchpath, pwprompt, user, database, host, options, testscript, pedsnet_version):
    """This tool is used to load the data"""

    option_map = {
        'pipeline': process.pipeline_full,
        'etl': process.etl_only,
        # 'truncate': process.truncate_fk,
        'ddl': process.ddl_only,
        # 'update_map': process.update_valueset,
        'load_mapping_table': process.load_maps,
        'indexes': process.indexes_only,
        'fk_constraints': process.constraints_only,
        'test_script': process.test_script
    }

    # grabs values from .ini file if already created -> less to manually input during testing
    if os.path.isfile(configfile_name):
        db_params = config.config('db')
        host = db_params['host']
        database = db_params['database']
        searchpath = config.config('schema')['schema']


    # region verify
    if not user:
        click.echo('Please provide the database username.')
        user = click.prompt('Username', hide_input=False)

    password = None
    if not pwprompt:
        password = click.prompt('Database password', hide_input=True)

    if not host:
        host = click.prompt('server name', hide_input=False)

    if not database:
        database = click.prompt('Database name', hide_input=False)

    if not searchpath:
        searchpath = click.prompt('schema name', hide_input=False)

    if not options:
        options = click.prompt('Process Options: \n\n\tpipeline \n\tetl \n\tddl \n\tload_mapping_table \n\tindexes \n\tfk_constraints \n\ttest_script\n\n')

    if not pedsnet_version:
        pedsnet_version = click.prompt('PEDSnet version', hide_input=False)

    if testscript:
        test_scripr_file = testscript
        if os.path.exists(temp):
            f = open(os.path.join(temp, 'temp.sql'),'wb')
            f.writelines(test_scripr_file)
    # endregion

    # region config file
    if os.path.isfile(configfile_name):
        os.remove(configfile_name)

    if not os.path.isfile(configfile_name):
        cfgfile = open(configfile_name, 'w')

        configini = configparser.ConfigParser()
        configini.add_section('postgresql')
        configini.set('postgresql', 'host', host)
        configini.set('postgresql', 'database', database)
        configini.set('postgresql', 'user', user)
        configini.set('postgresql', 'password', password)
        configini.add_section('schema')
        configini.set('schema', 'schema', searchpath)
        configini.add_section('pedsnet_version')
        configini.set('pedsnet_version', 'version', pedsnet_version)

        configini.write(cfgfile)
        cfgfile.close()
    # endregion

    # region Process Option
    global pipe
    option_map[options]()
    # endregion


# endregion


if __name__ == '__main__':
    cli()
