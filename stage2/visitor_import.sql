do $$
declare

	record record;
	now timestamp := current_timestamp;
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
				public.visitor.visitor_id = record.visitor_id
		)
		then
			update 
				public.visitor
			set 
				email_address = record.email_address,
				firstname = record.firstname,
				surname = record.surname,
				birthdate = record.birthdate
			where 
				visitor_natural_key = visitor_row.visitor_natural_key;
		end if;
		
		-- Add new row in case a SCD2 attribute of the most recent entry has changed
		if exists (
			select 
				visitor_id,
				validity_start, 

				address_zipcode, 
				address_housenumber, 
				address_municipality, 
				address_street, 
				lag(address_zipcode) over w as previous_zipcode,
				lag(address_housenumber) over w as previous_housenumber,
				lag(address_municipality) over w as previous_municipality,
				lag(address_street) over w as previous_street_name

			from 
				public.visitor
			window w as (partition by visitor_id order by validity_start)
			where
				visitor_id = record.visitor_id
				and (
					not (
						previous_zipcode = record.zipcode
						and previous_municipality = record.municipality
						and previous_street_name = record.street_name
						and previous_housenumber  = record.housenumber
					)
				)
		)
		then
			scd2_change := scd2_change + 1
		end if;
		
		
		
		
		
		
		
		
		
		
		
		
		-- if a changed scd 2 attribute exists, then create a new entry
		if exists (
			select
				visitor_id
			from
				visitor
			where
				public.visitor.visitor_id = record.visitor_id
-- 				and (
-- 					not (
-- 						public.visitor.address_zipcode = record.zipcode
-- 						and public.visitor.address_municipality = record.municipality
-- 						and public.visitor.address_street_name =record.street_name
-- 						and public.visitor.address_housenumber  = record.housenumber
-- 					)
-- 				)
		)
		then
			-- the record does exist, check if any scd2 variables were violated
			if exists (
				select
					visitor_id
				from
					visitor
				where
					public.visitor.visitor_id = record.visitor_id
					and (
						not (
							public.visitor.address_zipcode = record.zipcode
							and public.visitor.address_municipality = record.municipality
							and public.visitor.address_street_name =record.street_name
							and public.visitor.address_housenumber  = record.housenumber
						)
					)
			)
			then
				-- there changes to scd2 attributes were detected so a new entry needs to be made
			
			else
				-- it is fine to update the current record
			
			end if;
			
			
			
		else
			-- the record does not exist yet, so just create one
			
			
		end if;
		
		
		
		-- 		time_dimension_entry
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
		
		--		visitor_entry
		insert into public.visitor(
			visitor_id,
			firstname,
			surname,
			email_address,
			address_zipcode,
			address_housenumber,
			address_municipality,
			address_street_name,
			birthdate_time_id
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
			record.birthdate_epoch
		);
	end loop;
end;
$$ language plpgsql;