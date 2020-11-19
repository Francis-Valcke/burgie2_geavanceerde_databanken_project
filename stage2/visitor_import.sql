do $$
declare

	record record;
	now timestamp := current_timestamp;
	scd1_change integer := 0;
	scd2_change integer := 0;
	row_inserts integer := 0;
	total_rows integer := 0;
	deleted_rows integer := 0;

begin
	set search_path to tourism1;
	
-- 	delete from public.visitor;
	
	for record in (
		with temp_table as (
			select 
				visitor.firstname,
				visitor.email_address,
				(cast(visitor.birthdate as text):: timestamp) as birthdate,
				visitor.surname,
				visitor.visitor_id,
				address.zipcode,
				address.housenumber,
				address.municipality,
				address.street_name,
				now as validity_start
			from visitor
			join address on visitor.address_id=address.address_id
		)
		select
			*,
			extract(year from temp_table.birthdate) as birthdate_year,
			extract(month from temp_table.birthdate) as birthdate_month,
			extract(day from temp_table.birthdate) as birthdate_day,
			extract(hour from temp_table.birthdate) as birthdate_hour,
			extract(minute from temp_table.birthdate) as birthdate_minute,
			extract(second from temp_table.birthdate) as birthdate_second,
			extract(epoch from temp_table.birthdate) as birthdate_epoch
		from temp_table
	) 
	loop
	
		-- Update SCD1 fields of existing rows
		if exists (
			select
				visitor_id
			from
				visitor
			where
				visitor_id = record.visitor_id
		)
		then
			update 
				public.visitor
			set 
				email_address = record.email_address,
				firstname = record.firstname,
				surname = record.surname,
 				birthdate_time_id = record.birthdate_epoch
			where 
				visitor_id = record.visitor_id;
				
			scd1_change := scd1_change + 1;
		end if;
		
		
		-- Insert the new row
			-- time_dimension_entry for birthdate
 		insert into public.time_dimension(
			year,
			month,
			day,
			hour,
			minute,
			second,
			epoch
 		)
 		values (
			record.birthdate_year,
			record.birthdate_month,
			record.birthdate_day,
			record.birthdate_hour,
			record.birthdate_minute,
			record.birthdate_second,
			record.birthdate_epoch
 		) on conflict do nothing;
		
			-- visitor_entry
		insert into public.visitor(
			visitor_id,
			firstname,
			surname,
			email_address,
			address_zipcode,
			address_housenumber,
			address_municipality,
			address_street_name,
			birthdate_time_id,
			validity_start
		)
		values (
			record.visitor_id,
			record.firstname,
			record.surname,
			record.email_address,
			record.zipcode,
			record.housenumber,
			record.municipality,
			record.street_name,
			record.birthdate_epoch,
			record.validity_start
		);
		
		row_inserts := row_inserts + 1;
		
	end loop;

		
	
	-- duplicate entries will exist, check SCD II attributes if we have a new version or not
	-- keep the most recent one, in that case, the SCD I values will be updated indirectly
	with comparison_last as (
		select 
			visitor_id,
			validity_start, 
		
			address_zipcode, 
			address_housenumber, 
			address_street_name, 
			address_municipality, 
		
			lag(address_zipcode) over w as last_zipcode,
			lag(address_housenumber) over w as last_housenumber,
			lag(address_street_name) over w as last_street_name,
			lag(address_municipality) over w as last_municipality
		from 
			public.visitor
		window w as (partition by visitor_id order by validity_start)
	),
	
	-- search 'duplicate' rows considering address
	remove_versions as (
		select 
			visitor_id, 
			validity_start 
		from 
			comparison_last
		where
			address_zipcode is not distinct from last_zipcode and
			address_housenumber is not distinct from last_housenumber and
			address_municipality is not distinct from last_municipality and
			address_street_name is not distinct from last_street_name
	),		
	
	-- deletion
	deleted_rows as (
		delete from public.visitor
	 	where (
			visitor_id, 
			validity_start
		) in (select * from remove_versions) returning *
	)


	-- debug
	raise notice 'scd1_updates: %',scd1_change;
	raise notice 'row_inserts: %', row_inserts;
	select count(*) from deleted_rows into deleted_rows; 
 	raise notice 'Deleted rows: %', deleted_rows;
	select count(*) from public.visitor into total_rows;
	raise notice 'total_rows: %', total_rows;

	-- update validity_end
	-- last condition in where clause makes sure we update the correct row regarding the lead function
	update 
		public.visitor v
	set 
		validity_end = newv.validity_end 
	from (
		select 
			visitor_id, 
			validity_start, 
			lead(validity_start) over w as validity_end
		from public.visitor
		window w as (partition by visitor_id order by validity_start)
		order by visitor_id, validity_start asc
	) newv
	where 
		newv.visitor_id = v.visitor_id and newv.validity_start = v.validity_start;
		
	
	
end;
$$ language plpgsql;