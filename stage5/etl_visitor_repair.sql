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
	corrected_id int = null;
	
begin	
	set client_min_messages = error;
	create table if not exists duplicates as (
		-- Actual code for selecting the duplicates
		with 
			visitor_sorted as (
				select
					*,
					row_number() over (order by visitor_1.birthdate, visitor_1.email_address) as index
				from tourism2.visitor visitor_1
				join tourism2.address on tourism2.address.address_id = visitor_1.address_id
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

	create temporary table if not exists translation_table (
		old_visitor_id integer,
		new_visitor_id integer,
		most_recent_reservation timestamp
	) on commit drop ;


	for record in (
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
				as new_visitor_id,

			case
				when r1_most_recent_reservation is NULL then r2_most_recent_reservation
				when r2_most_recent_reservation is NULL then r1_most_recent_reservation
				when r1_most_recent_reservation > r2_most_recent_reservation then r1_most_recent_reservation
				else r2_most_recent_reservation
				end
				as most_recent_reservation
		from duplicates_reservation_time
	)
	loop
		
		if exists (
			select *
			from translation_table
			where 
				record.new_visitor_id = old_visitor_id
-- 				and record.most_recent_reservation > most_recent_reservation
		) 
		then
			-- There one that was found needs to be corrected
			raise notice 'eyoo';
			
			
		else
			insert into translation_table (
				old_visitor_id,
				new_visitor_id,
				most_recent_reservation
			) values (
				record.old_visitor_id,
				record.new_visitor_id,
				record.most_recent_reservation
			);
			
		end if;
		
-- 		raise notice 'corrected_id = %', record.new_visitor_id;
	
	end loop;
end;
$$ language plpgsql
