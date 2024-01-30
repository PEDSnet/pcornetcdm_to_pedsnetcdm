begin;
CREATE OR REPLACE FUNCTION SITE_pedsnet.isnumeric(character varying) RETURNS BOOLEAN AS $$
DECLARE x NUMERIC;
BEGIN
        x = $1::NUMERIC;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
                RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;
commit;

begin;
create or replace function SITE_pedsnet.is_date(s varchar) returns boolean as $$
begin
          perform s::date;
          return true;
        exception when others then
                  return false;
end;
$$ language plpgsql;
commit;

CREATE OR REPLACE FUNCTION SITE_pedsnet.is_int(text) RETURNS BOOLEAN AS $$
DECLARE x INT;
BEGIN
        x = $1::INT;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
                RETURN FALSE;
END;
$$
STRICT
LANGUAGE plpgsql IMMUTABLE;

create or replace function SITE_pedsnet.is_time(s varchar) returns boolean as $$
begin
	  perform s::time;
	  return true;
	exception when others then
		  return false;
end;
$$ language plpgsql;
