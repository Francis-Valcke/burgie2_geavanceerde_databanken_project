do $$
declare

	record record;

begin

	set search_path to public;

	-- Zipcode regex check
	for record in (
		select visitor_id_surrogate, address_zipcode
		from visitor
		where 
			not cast(address_zipcode as text) ~ '^[1-9]{1}[0-9]{3}$'
			or cast(address_zipcode as text) like ''
			or address_zipcode is null
	)	
	loop
		raise notice 'visitor_id_surrogate = %: Inconsistant address_zipcode = %', record.visitor_id_surrogate, record.address_zipcode;
	end loop;
	
	-- Housenr regex check
	for record in (
		select visitor_id_surrogate, address_housenumber
		from visitor
		where 
			not cast(address_housenumber as text) ~ '^\d+[a-zA-Z]*$'
			or cast(address_housenumber as text) like ''
			or address_zipcode is null
	)	
	loop
		raise notice 'visitor_id_surrogate = %: Inconsistant address_housenumber = %', record.visitor_id_surrogate, record.address_housenumber;
	end loop;
	
	-- Municiplaity regex check
	for record in (
		select visitor_id_surrogate, address_municipality
		from visitor
		where 
			not cast(address_municipality as text) ~ '^[A-Z]'
			or cast(address_municipality as text) like ''
			or address_zipcode is null
	)	
	loop
		raise notice 'visitor_id_surrogate = %: Inconsistant address_municipality = %', record.visitor_id_surrogate, record.address_municipality;
	end loop;
	
	-- address_street_name regex check
	for record in (
		select visitor_id_surrogate, address_street_name
		from visitor
		where 
			cast(address_street_name as text) ~ '\d$'
			or cast(address_street_name as text) like ''
			or address_zipcode is null
	)	
	loop
		raise notice 'visitor_id_surrogate = %: Inconsistant address_street_name = %', record.visitor_id_surrogate, record.address_street_name;
	end loop;
	
	-- City with wrong zipcode
	for record in (
		select visitor_id_surrogate, address_zipcode, address_municipality, belgium_zip_code.zipcode
		from visitor
		join belgium_zip_code on lower(visitor.address_municipality) = lower(belgium_zip_code.city)
		where not address_zipcode = zipcode
	)	
	loop
		raise notice 'visitor_id_surrogate = %: Inconsistant zipcode = % for city = % should be zipcode = %', record.visitor_id_surrogate, record.address_zipcode, record.address_municipality, record.zipcode;
	end loop;

end;
$$ language plpgsql;