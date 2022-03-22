-- Set up the privilege to the user
GRANT ALL ON SCHEMA SITE_pedsnet TO insert_user_group;
--GRANT USAGE ON SCHEMA SITE_pedsnet TO insert_sas_user_group;
GRANT ALL ON ALL TABLES IN SCHEMA SITE_pedsnet TO insert_user_group;
--GRANT SELECT ON ALL TABLES IN SCHEMA SITE_pedsnet TO insert_sas_user_group;
