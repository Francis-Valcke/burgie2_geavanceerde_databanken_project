do $$
do $$
	declare
		car_row record;
	begin
		for car_row in (
			select distinct on (
				carid, 
				extract(months from age(b.date_booking, date_inservice))
			)
			carid as car_natural_key, 
			extract(months from age(b.date_booking, date_inservice))::integer as car_age_in_months, 
			date_booking as version_start,
			weekprijs as model_price_per_week, 
			dagprijs as model_price_per_day, 
			fuel_type as model_fuel_type, 
			model.name as model_name,
			brandname as brand_name, 
			brand.producer as brand_producer, 
			brand.country as brand_country,
			cargo_volume as class_cargo_volume, 
			licensetype as class_license_type, 
			passengers as class_number_of_passengers, 
			class.name as class_name
			from booking b
			join car using(carid)
			join model on(model.modelid=car.modelid)
			join brand using(brandid)
			join class using(classid)
			order by carid, extract(months from age(b.date_booking, date_inservice)), version_start asc
		) loop


		if not exists (
			select car_natural_key 
			from public.dimension_car 
			where car_row.version_start between version_start and version_end
		)
		then
			insert into public.dimension_car(
				car_natural_key,
				car_age_in_months,
				version_start,
				model_price_per_week,
				model_price_per_day,model_fuel_type,
				model_name,
				brand_name,
				brand_producer,
				brand_country,
				class_cargo_volume,
				class_license_type,
				class_number_of_passengers,
				class_name,
				last_version
			)
		values (
			car_row.car_natural_key, 
			car_row.car_age_in_months, 
			car_row.version_start,
			car_row.model_price_per_week, 
			car_row.model_price_per_day, 
			car_row.model_fuel_type,
			car_row.model_name, 
			car_row.brand_name, 
			car_row.brand_producer, 
			car_row.brand_country,
			car_row.class_cargo_volume, 
			car_row.class_license_type, 
			car_row.class_number_of_passengers,
			car_row.class_name,
			false);
		else
		--Update the SCD I attributes for car
		-- TODO
		end if;
		end loop;
	end;
	
$$ language plpgsql;