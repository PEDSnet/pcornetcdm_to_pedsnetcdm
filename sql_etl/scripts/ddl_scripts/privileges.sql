-- Set up the privilege to the user
--COMMENTED out pcor_et_user as it was throwing errors
-- GRANT ALL ON SCHEMA seattle_pedsnet TO pcor_et_user;
GRANT USAGE ON  SCHEMA seattle_pedsnet TO pcornet_sas;
-- GRANT ALL ON ALL TABLES IN SCHEMA seattle_pedsnet TO pcor_et_user;
GRANT SELECT ON ALL TABLES IN SCHEMA seattle_pedsnet TO pcornet_sas;
