do $$
declare

	record record;

begin
	set search_path to tourism1;
	
	delete from public.visitor;
	
	for record in (
		select 
			visitor.firstname,
			visitor.email_address,
			visitor.birthdate,
			visitor.surname,
			visitor.visitor_id,
			address.zipcode,
			address.housenumber,
			address.municipality,
			address.street_name
		from visitor
		join address on visitor.address_id=address.address_id
	) 
	loop
		insert into public.visitor(
			visitor_id,
			firstname,
			surname,
			email_address,
			address_zipcode,
			address_housenumber,
			address_municipality,
			address_street_name
		)
		values (
			record.visitor_id,
			record.firstname,
			record.surname,
			record.email_address,
-- 			visitor.birthdate,
			record.zipcode,
			record.housenumber,
			record.municipality,
			record.street_name
		);
	end loop;
end;
$$ language plpgsql;