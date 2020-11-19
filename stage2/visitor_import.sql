do $$
declare

	record record;
	now timestamp := current_timestamp;
	scd1_change integer := 0;
	scd2_change integer := 0;

begin
	set search_path to tourism1;
	
	delete from public.visitor;
	
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
				surname = record.surname
-- 				birthdate_time_id = record.birthdate
			where 
				visitor_id = record.visitor_id;
				
			scd1_change := scd1_change + 1;
		end if;
		
		
		
		
		
		
		-- Add new row in case a SCD2 attribute of the most recent entry has changed
		if exists (
			with temp_table as (
				select 
					visitor_id,
					validity_start, 

					lag(address_zipcode) over w as last_zipcode,
					lag(address_housenumber) over w as last_housenumber,
					lag(address_municipality) over w as last_municipality,
					lag(address_street_name) over w as last_street_name

				from 
					public.visitor
				window w as (partition by visitor_id order by validity_start)
			)
			select
				*
			from
				temp_table
			where
				visitor_id = record.visitor_id
				and (
					not (
						last_zipcode = record.zipcode
						and last_municipality = record.municipality
						and last_street_name = record.street_name
						and last_housenumber  = record.housenumber
					)
				)
		)
		then
			scd2_change := scd2_change + 1;
		end if;
		
	end loop;
	
	raise notice 'scd1_updates: %',scd2_change;
	raise notice 'scd2_updates: %',scd2_change;
	
end;
$$ language plpgsql;