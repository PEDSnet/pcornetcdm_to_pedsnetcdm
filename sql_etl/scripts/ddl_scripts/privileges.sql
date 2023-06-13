-- Set up the privilege to the user
GRANT ALL ON SCHEMA pcornet_maps TO chop_etl;
--GRANT USAGE ON SCHEMA pcornet_maps TO insert_sas_user_group;
GRANT ALL ON ALL TABLES IN SCHEMA pcornet_maps TO chop_etl;
--GRANT SELECT ON ALL TABLES IN SCHEMA pcornet_maps TO insert_sas_user_group;
