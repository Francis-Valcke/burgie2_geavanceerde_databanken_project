-- To disable messages that a table cannot be created beacuse it exists

set search_path to public;

-- Helper function to scale leivenstein scores between 0 and 1
create or replace function rescale(score real) returns real as $$
	declare
	begin
		return 
			case 
				when score = 0 then 1.0
				when score = 1 then 0.9
				when score = 2 then 0.4
				when score = 3 then 0.1
				else 0
			end;
end;
$$ language plpgsql;

	-- Function to calculate the OWA score for 2 rows
create or replace function calc_owa_score(
		firstname_1 varchar, 
		surname_1 varchar, 
		birthdate_1 date, 
		email_address_1 varchar, 
		zipcode_1 int, 
		street_name_1 varchar,

		firstname_2 varchar, 
		surname_2 varchar, 
		birthdate_2 date, 
		email_address_2 varchar, 
		zipcode_2 int, 
		street_name_2 varchar

		) returns real as $$

	declare
		-- vars for score vector
		firstname_score real := 0;
		surname_score real := 0;
		email_address_score real := 0;
		birthdate_score real := 0;
		zipcode_score real := 0;
		street_name_score real := 0;

		-- vars for weight vector
		weight_empty bool;
		n int := 6;	
		prev_quantor real := 0;
		cur_quantor real := 0;
		cur_weight real := 0;
		counter real := 1;

		owa_score real := 0;
	begin

		-- Create and populate the score vector

		create temporary table if not exists score_vector (score real not null) on commit drop;
		delete from score_vector;

		firstname_score = case when (soundex(firstname_1) = soundex(firstname_2)) then 1.0 else 0.0 end;
		-- surname_score = case when (soundex(surname_1) = soundex(surname_2)) then 1.0 else 0.0 end;
		surname_score = rescale(levenshtein(lower(surname_1), lower(surname_2)));
		birthdate_score = rescale(levenshtein(lower(cast(birthdate_1 as varchar)), lower(cast(birthdate_2 as varchar))));
		email_address_score = rescale(levenshtein(lower(email_address_1), lower(email_address_2)));
		zipcode_score = rescale(levenshtein(lower(cast(zipcode_1 as varchar)), lower(cast(zipcode_2 as varchar))));
		street_name_score = rescale(levenshtein(lower(street_name_1), lower(street_name_2)));

		insert into score_vector (score)
		values (firstname_score), (surname_score), (birthdate_score), (email_address_score), (zipcode_score), (street_name_score);					

		-- Calculate the weight vector
		create temporary table if not exists weight (weight real not null) on commit drop;
		select case when exists (select * from weight limit 1) then false else true end into weight_empty;
		if weight_empty then
			loop
				cur_quantor = (counter/n)^n;
				insert into weight (weight) values (cur_quantor - prev_quantor);
				prev_quantor = cur_quantor;

				if counter = n then exit;
				end if;

				counter = counter + 1;

			end loop;
		end if;

		-- Calculate the owa score
		with 
			score_sorted as(
				select 
					score, 
					row_number() over (order by score desc) as index
				from score_vector
			), 
			weight_sorted as (
				select 
					weight, 
					row_number() over (order by weight) as index
				from weight
			)
		select sum(score_sorted.score*weight)
		from weight_sorted
		join score_sorted on score_sorted.index = weight_sorted.index
		into owa_score;

	return owa_score;
end;
$$ language plpgsql;





do $$
declare

	record record;
	now timestamp := current_timestamp;
	scd1_change integer := 0;
	scd2_change integer := 0;
	row_inserts integer := 0;
	total_rows_before_insert integer := 0;
	total_rows_after_insert integer := 0;
	total_rows_after_delete integer := 0;
	deleted_rows integer := 0;

begin
	
	set client_min_messages = error;
	drop table if exists public.duplicates;
	create table if not exists public.duplicates as (
		-- Actual code for selecting the duplicates
		with 
			visitor_sorted as (
				select
					*,
					row_number() over (order by visitor_1.birthdate, visitor_1.email_address) as index
				from tourism1.visitor visitor_1
				join tourism1.address on tourism1.address.address_id = visitor_1.address_id
			)
		select *
		from (
			select 
				visitor_1.visitor_id as visitor_id_1,
				visitor_2.visitor_id as visitor_id_2,

				visitor_1.firstname as firstname_1, 
				visitor_2.firstname as firstname_2, 

				visitor_1.surname as surname_1, 
				visitor_2.surname as surname_2, 

				visitor_1.birthdate as birthdate_1,
				visitor_2.birthdate as birthdate_2,

				visitor_1.email_address as email_address_1, 
				visitor_2.email_address as email_address_2, 

				visitor_1.zipcode as zipcode_1, 
				visitor_2.zipcode as zipcode_2, 

				visitor_1.street_name as street_name_1, 
				visitor_2.street_name as street_name_2,

				calc_owa_score(
					visitor_1.firstname, 
					visitor_1.surname, 
					visitor_1.birthdate, 
					visitor_1.email_address, 
					visitor_1.zipcode, 
					visitor_1.street_name,

					visitor_2.firstname, 
					visitor_2.surname, 
					visitor_2.birthdate, 
					visitor_2.email_address, 
					visitor_2.zipcode, 
					visitor_2.street_name
				) as owa_score

			from visitor_sorted visitor_1
				cross join visitor_sorted visitor_2
			where 
				visitor_1.index < visitor_2.index 
				and abs(visitor_1.index - visitor_2.index) < 8
		) as comparison
		where owa_score > 0.015
		order by visitor_id_1, visitor_id_2
	);

	set client_min_messages = notice;
	set search_path to tourism1;

	-- create temporary table if not exists translation_table on commit drop as (
	drop table if exists public.translation_table;
	create table if not exists public.translation_table as (
		with duplicates_reservation_time as (
			select distinct
				visitor_id_1,
				visitor_id_2,
				last_value(r1.booking_time) over (partition by visitor_id_1 order by r1.booking_time 
						RANGE BETWEEN
						UNBOUNDED PRECEDING AND
						UNBOUNDED FOLLOWING) as r1_most_recent_reservation,
				last_value(r2.booking_time) over (partition by visitor_id_2 order by r2.booking_time 
						RANGE BETWEEN
						UNBOUNDED PRECEDING AND
						UNBOUNDED FOLLOWING) as r2_most_recent_reservation
			from public.duplicates
			left join reservation r1 on r1.visitor_id = duplicates.visitor_id_1
			left join reservation r2 on r2.visitor_id = duplicates.visitor_id_2
			order by visitor_id_1, visitor_id_2
		)
		select	
			case
				when r1_most_recent_reservation is NULL then visitor_id_1
				when r2_most_recent_reservation is NULL then visitor_id_2
				when r1_most_recent_reservation > r2_most_recent_reservation then visitor_id_2
				else visitor_id_1
				end
				as old_visitor_id,

			case
				when r1_most_recent_reservation is NULL then visitor_id_2
				when r2_most_recent_reservation is NULL then visitor_id_1
				when r1_most_recent_reservation > r2_most_recent_reservation then visitor_id_1
				else visitor_id_2
				end
				as new_visitor_id
		from duplicates_reservation_time
	);


	select count(*) from public.visitor into total_rows_before_insert;
	
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
		
		if record.visitor_id not in (
			select old_visitor_id
			from public.translation_table
		)
		then
		
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
		
		else
			raise notice 'skipping duplicate with visitor_id: %', record.visitor_id;
		end if;
		
	end loop;
	
	raise notice 'scd1_updates: %', scd1_change;
	raise notice 'row_inserts: %', row_inserts;
	select count(*) from public.visitor into total_rows_after_insert;

	-- look for changed scd2 attributes as to decide wether to keep or delete the newly added version
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
	
	-- search for duplicate rows
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
	
	-- delete duplicate rows
	deleted_rows as (
		delete from public.visitor
	 	where (
			visitor_id, 
			validity_start
		) in (select * from remove_versions) returning *
	)

	select count(*) from deleted_rows into deleted_rows; 
  	raise notice 'Deleted rows: %', deleted_rows;
	
	-- update validity_end
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
		
	select count(*) from public.visitor into total_rows_after_delete;
	
	raise notice 'total_rows_before_insert: %', total_rows_before_insert;
	raise notice 'total_rows_after_insert: %', total_rows_after_insert;
	raise notice 'total_rows_after_delete: %', total_rows_after_delete;
	
end;
$$ language plpgsql;