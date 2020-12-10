do $$
declare

	record record;
	
	count_housenumber int := 0;
	count_street_number int := 0;
	count_zipcode int := 0;
	count_municipality int := 0;

begin

	set search_path to tourism2;
	
	create temporary table visitor_full on commit drop as (
		select 
			visitor.visitor_id,
			address.address_id,
			address.municipality,
			address.zipcode,
			address.housenumber,
			address.street_name
		from visitor
		join address on visitor.address_id = address.address_id
	);

	-- Zipcode regex check
	for record in (
		select visitor_id, zipcode
		from visitor_full
		where 
			not cast(zipcode as text) ~ '^[1-9]{1}[0-9]{3}$'
			or cast(zipcode as text) like ''
			or cast(housenumber as text) ~ '^0'
			or zipcode is null
	)	
	loop
		raise notice 'visitor_id = %: Inconsistant zipcode = %', record.visitor_id, record.zipcode;
		count_zipcode=count_zipcode+1;
	end loop;
	
	-- Housenr regex check
	for record in (
		select visitor_id, housenumber
		from visitor_full
		where 
			not cast(housenumber as text) ~ '^\d+[a-zA-Z]*$'
			or cast(housenumber as text) like ''
			or cast(housenumber as text) ~ '^0'
			or zipcode is null
	)	
	loop
		raise notice 'visitor_ide = %: Inconsistant housenumber = %', record.visitor_id, record.housenumber;
		count_housenumber=count_housenumber+1;
	end loop;
	
	-- Municiplaity regex check
	for record in (
		select visitor_id, municipality
		from visitor_full
		where 
			not cast(municipality as text) ~ '^[A-Z]'
			or cast(municipality as text) like ''
			or zipcode is null
	)	
	loop
		raise notice 'visitor_id = %: Inconsistant municipality = %', record.visitor_id, record.municipality;
		count_municipality=count_municipality+1;
	end loop;
	
	-- street_name regex check
	for record in (
		select visitor_id, street_name
		from visitor_full
		where 
			cast(street_name as text) ~ '\d$'
			or cast(street_name as text) like ''
			or zipcode is null
	)	
	loop
		raise notice 'visitor_id = %: Inconsistant street_name = %', record.visitor_id, record.street_name;
		count_street_number=count_street_number+1;
	end loop;
	
	-- City with wrong zipcode
--  	for record in (
--  		select visitor_id, zipcode, municipality, belgium_zip_code.zipcode
--  		from visitor_full
--  		join belgium_zip_code on lower(visitor.municipality) = lower(belgium_zip_code.city)
--  		where not zipcode = zipcode
--  	)	
--  	loop
--  		raise notice 'visitor_id = %: Inconsistant zipcode = % for city = % should be zipcode = %', record.visitor_id, record.zipcode, record.municipality, record.zipcode;
--  	end loop;
	
	raise notice '';
	-- Print stats	
	raise notice 'count_housenumber = %', count_housenumber;
	raise notice 'count_street_number = %', count_street_number;
	raise notice 'count_zipcode = %', count_zipcode;
	raise notice 'count_municipality = %', count_municipality;
end;
$$ language plpgsql;