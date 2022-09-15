-- Set up the privilege to the user
GRANT ALL ON SCHEMA SITE_pedsnet TO chop_etl;
--GRANT USAGE ON SCHEMA SITE_pedsnet TO insert_sas_user_group;
GRANT ALL ON ALL TABLES IN SCHEMA SITE_pedsnet TO chop_etl;
--GRANT SELECT ON ALL TABLES IN SCHEMA SITE_pedsnet TO insert_sas_user_group;
