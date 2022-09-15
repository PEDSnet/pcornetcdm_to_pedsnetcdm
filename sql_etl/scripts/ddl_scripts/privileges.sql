-- Set up the privilege to the user
GRANT ALL ON SCHEMA missouri_pedsnet TO chop_etl;
--GRANT USAGE ON SCHEMA missouri_pedsnet TO insert_sas_user_group;
GRANT ALL ON ALL TABLES IN SCHEMA missouri_pedsnet TO chop_etl;
--GRANT SELECT ON ALL TABLES IN SCHEMA missouri_pedsnet TO insert_sas_user_group;
