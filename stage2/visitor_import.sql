do $$
declare

	record record;

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
				address.street_name
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