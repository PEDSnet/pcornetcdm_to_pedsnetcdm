/* 
Updating the function to add the materialized view instead of regular views fro dcc_pcornet.
This function is used to create the materialized lower case view 
and the capatilized view for the merged dcc_pcornet data
*/
create or replace function add_site_column(datab text, schemanm text) returns void as $$
	declare
        tbl_array text[];
        count_tbl integer;
        sqlstr text;
        sel_stat text;
    begin
    	select array(
            			SELECT tablename as table
					   	FROM pg_tables
						WHERE schemaname = schemanm
			            and tablename not in ('version_history')
                     ) into tbl_array;
        select (select count(*) FROM pg_tables
						WHERE schemaname = schemanm
				        and tablename not in ('version_history')
			   ) into count_tbl;
    	<<table_loop>>
        for i in 1.. count_tbl  loop
			sqlstr = 'alter table '||schemanm||'.'||tbl_array[i]|| ' add column site character varying not null;';
			execute sqlstr; 
	    	sel_stat := null;
        end loop table_loop;
	end;
$$ LANGUAGE plpgsql;
