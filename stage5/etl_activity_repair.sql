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
	
	violation record;

begin
	set search_path to tourism2;
	
	
	drop table if exists activity_full;
	create temporary table activity_full on commit drop as (
		select 
			activity.activity_id,
			activity.activity_name,
			case when (activity.website is NULL) or (activity.website = '') then false else true end as has_website,
			case when (activity.phonenumber is NULL) or (activity.phonenumber = '') then false else true end as has_phonenumber,
			activity.description,
			activity.operational as is_operational,
			case when (activity.accessibility_info_url is NULL) or (activity.accessibility_info_url = '') then false else true end as has_accessibility_info_url,
			case when (activity.email_address is NULL) or (activity.email_address = '') then false else true end as has_email_address,
			activity.tourist_region,
			activity.activity_type,
			activity.accessibility_label,
   			activity.last_modified,
			now as validity_start,
		
			accessibility_information.motoric_accessibility,
			case when (accessibility_information.motoric_accessibility is NULL) or (accessibility_information.motoric_accessibility = 0) then false else true end as motoric_accessibility_bool,
			
			-- 	allergy_information
			allergy_information.allergy,
			allergy_information.allergy_details,
			allergy_information.food_allergy,
			
			case when (allergy_information.allergy is NULL) or (allergy_information.allergy = 0) then false else true end as allergy_bool,
			case when (allergy_information.food_allergy is NULL) or (allergy_information.food_allergy = 0) then false else true end as food_allergy_bool,
			
			-- 	auditive_disability_information
			auditive_disability_information.deaf,
			auditive_disability_information.deaf_details,
			auditive_disability_information.hearing_impaired,
			auditive_disability_information.hearing_impaired_details,
		
			case when (auditive_disability_information.deaf is NULL) or (auditive_disability_information.deaf = 0) then false else true end as deaf_bool,
			case when (auditive_disability_information.hearing_impaired is NULL) or (auditive_disability_information.hearing_impaired = 0) then false else true end as hearing_impaired_bool,
			-- 	mental_disability_information
			mental_disability_information.autism,
			mental_disability_information.autism_details,
			mental_disability_information.mental_disability,
			mental_disability_information.mental_disability_details,
			
			case when (mental_disability_information.autism is NULL) or (mental_disability_information.autism = 0) then false else true end as autism_bool,
			case when (mental_disability_information.mental_disability is NULL) or (mental_disability_information.mental_disability = 0) then false else true end as mental_disability_bool,
			-- 	visual_disability_information
			visual_disability_information.blind,
			visual_disability_information.blind_details,
			visual_disability_information.visually_impaired,
			visual_disability_information.visually_impaired_details,
			
			case when (visual_disability_information.blind is NULL) or (visual_disability_information.blind = 0) then false else true end as blind_bool,
			case when (visual_disability_information.visually_impaired is NULL) or (visual_disability_information.visually_impaired = 0) then false else true end as visually_impaired_bool,
			--  address information
			street_name as address_street_name,
			municipality as address_municipality,
			housenumber as address_housenumber,
			zipcode as address_zipcode
			
		from activity
		join accessibility_information on accessibility_information.activity_id = activity.activity_id
	  	join allergy_information on allergy_information.activity_id = activity.activity_id
 		join auditive_disability_information on auditive_disability_information.activity_id = activity.activity_id
 		join mental_disability_information on mental_disability_information.activity_id = activity.activity_id
 		join visual_disability_information on visual_disability_information.activity_id = activity.activity_id
		join address on activity.address_id=address.address_id
	);
			
		
	for violation in (
		select *
		from activity_full
		where 
			deaf_bool = true and hearing_impaired_bool = false
			or blind_bool = true and visually_impaired_bool = false
			or autism_bool = true and mental_disability_bool = false
			or food_allergy_bool = true and allergy_bool = false
	)
	loop
		-- Find the violated record in activity_full and fix it
		if violation.deaf_bool = true and violation.hearing_impaired_bool = false then
			update activity_full
			set hearing_impaired_bool = true
			where violation.activity_id = activity_full.activity_id;
			raise notice 'Activity_id: %, corrected violation!', violation.activity_id;
		end if;
		
		
		if violation.blind_bool = true and violation.visually_impaired_bool = false then
			update activity_full
			set visually_impaired_bool = true
			where violation.activity_id = activity_full.activity_id;
			raise notice 'Activity_id: %, corrected violation!', violation.activity_id;
		end if;
		
		
		if violation.autism_bool = true and violation.mental_disability_bool = false then
			update activity_full
			set mental_disability_bool = true
			where violation.activity_id = activity_full.activity_id;
			raise notice 'Activity_id: %, corrected violation!', violation.activity_id;
		end if;
		
		
		if violation.food_allergy_bool = true and violation.allergy_bool = false then
			update activity_full
			set allergy_bool = true
			where violation.activity_id = activity_full.activity_id;
			raise notice 'Activity_id: %, corrected violation!', violation.activity_id;
		end if;
	end loop;	
		
	select count(*) from public.activity into total_rows_before_insert;

	for record in (
		select * from activity_full
	)
	loop
		insert into public.activity(
			-- 	activity
			activity_id,
			activity_name,
			has_website,
			has_phonenumber,
			description,
			is_operational,
			has_accessibility_info_url,
			has_email_address,
			tourist_region,
			activity_type,
			accessibility_label,
 			validity_start,
			last_modified,
			-- 	auditive_disability_information
			hearing_impaired_details,
			hearing_impaired,
			deaf_details,
			deaf,
			-- 	visual_disability_information
			blind_details,
			blind,
			visually_impaired_details,
			visually_impaired,
			-- 	mental_disability_information
			mental_disability_details,
			mental_disability,
			autism,
			autism_details,
			-- 	allergy_information
			food_allergy,
			allergy_details,
			allergy,
			-- 	accessibility_information
			motoric_accessibility,
			--  address information
			address_street_name,
			address_municipality,
			address_housenumber,
			address_zipcode
		)
		values (
			-- 	activity
			record.activity_id,
			record.activity_name,
			record.has_website,
			record.has_phonenumber,
			record.description,
			record.is_operational,
			record.has_accessibility_info_url,
			record.has_email_address,
			record.tourist_region,
			record.activity_type,
			record.accessibility_label,
 			record.validity_start,
			record.last_modified,
			-- 	auditive_disability_information
			record.hearing_impaired_details,
			record.hearing_impaired_bool,
			record.deaf_details,
			record.deaf_bool,
			-- 	visual_disability_information
			record.blind_details,
			record.blind_bool,
			record.visually_impaired_details,
			record.visually_impaired_bool,
			-- 	mental_disability_information
			record.mental_disability_details,
			record.mental_disability_bool,
			record.autism_bool,
			record.autism_details,
			-- 	allergy_information
			record.food_allergy_bool,
			record.allergy_details,
			record.allergy_bool,
			-- 	accessibility_information
			record.motoric_accessibility_bool,
			--  address information
			record.address_street_name,
			record.address_municipality,
			record.address_housenumber,
			record.address_zipcode
		);
		
	end loop;
	
	
	raise notice 'scd1_updates: %', scd1_change;
	raise notice 'row_inserts: %', row_inserts;
	select count(*) from public.activity into total_rows_after_insert;

	-- look for changed scd2 attributes as to decide wether to keep or delete the newly added version
	with comparison_last as (
		select 
			*,
			-- 	activity
			lag(activity_name) over w as last_activity_name,
			lag(has_website) over w as last_has_website,
			lag(has_phonenumber) over w as last_has_phonenumber,
			lag(description) over w as last_description,
			lag(is_operational) over w as last_is_operational,
			lag(has_accessibility_info_url) over w as last_has_accessibility_info_url,
			lag(has_email_address) over w as last_has_email_address,
			lag(tourist_region) over w as last_tourist_region,
			lag(activity_type) over w as last_activity_type,
			lag(accessibility_label) over w as last_accessibility_label,
			lag(last_modified) over w as last_last_modified,
			-- 	auditive_disability_information
			lag(hearing_impaired_details) over w as last_hearing_impaired_details,
			lag(hearing_impaired) over w as last_hearing_impaired,
			lag(deaf_details) over w as last_deaf_details,
			lag(deaf) over w as last_deaf,
			-- 	visual_disability_information
			lag(blind_details) over w as last_blind_details,
			lag(blind) over w as last_blind,
			lag(visually_impaired_details) over w as last_visually_impaired_details,
			lag(visually_impaired) over w as last_visually_impaired,
			-- 	mental_disability_information
			lag(mental_disability_details) over w as last_mental_disability_details,
			lag(mental_disability) over w as last_mental_disability,
			lag(autism) over w as last_autism,
			lag(autism_details) over w as last_autism_details,
			-- 	allergy_information
			lag(food_allergy) over w as last_food_allergy,
			lag(allergy_details) over w as last_allergy_details,
			lag(allergy) over w as last_allergy,
			-- 	accessibility_information
			lag(motoric_accessibility) over w as last_motoric_accessibility,
			--  address information
			lag(address_street_name) over w as last_address_street_name,
			lag(address_municipality) over w as last_address_municipality,
			lag(address_housenumber) over w as last_address_housenumber,
			lag(address_zipcode) over w as last_address_zipcode
		from 
			public.activity
		window w as (partition by activity_id order by validity_start)
	),
	
	-- search for duplicate rows
	remove_versions as (
		select 
			activity_id, 
			validity_start 
		from 
			comparison_last
		where
			-- 	activity
			activity_name is not distinct from last_activity_name and
			has_website is not distinct from last_has_website and
			has_phonenumber is not distinct from last_has_phonenumber and
			description is not distinct from last_description and
			is_operational is not distinct from last_is_operational and
			has_accessibility_info_url is not distinct from last_has_accessibility_info_url and
			has_email_address is not distinct from last_has_email_address and
			tourist_region is not distinct from last_tourist_region and
			activity_type is not distinct from last_activity_type and
			accessibility_label is not distinct from last_accessibility_label and
			last_modified is not distinct from last_last_modified and
			-- 	auditive_disability_information
			hearing_impaired_details is not distinct from last_hearing_impaired_details and
			hearing_impaired is not distinct from last_hearing_impaired and
			deaf_details is not distinct from last_deaf_details and
			deaf is not distinct from last_deaf and
			-- 	visual_disability_information
			blind_details is not distinct from last_blind_details and
			blind is not distinct from last_blind and
			visually_impaired_details is not distinct from last_visually_impaired_details and
			visually_impaired is not distinct from last_visually_impaired and
			-- 	mental_disability_information
			mental_disability_details is not distinct from last_mental_disability_details and
			mental_disability is not distinct from last_mental_disability and
			autism is not distinct from last_autism and
			autism_details is not distinct from last_autism_details and
			-- 	allergy_information
			food_allergy is not distinct from last_food_allergy and
			allergy_details is not distinct from last_allergy_details and
			allergy is not distinct from last_allergy and
			-- 	accessibility_information
			motoric_accessibility is not distinct from last_motoric_accessibility and
			--  address information
			address_street_name is not distinct from last_address_street_name and
			address_municipality is not distinct from last_address_municipality and
			address_housenumber is not distinct from last_address_housenumber and
			address_zipcode is not distinct from last_address_zipcode
	),		
	
	-- delete duplicate rows
	deleted_rows as (
		delete from public.activity
	 	where (
			activity_id, 
			validity_start
		) in (select * from remove_versions) returning *
	)

	select count(*) from deleted_rows into deleted_rows; 
  	raise notice 'Deleted rows: %', deleted_rows;
	
	-- update validity_end
	update 
		public.activity v
	set 
		validity_end = newv.validity_end 
	from (
		select 
			activity_id, 
			validity_start, 
			lead(validity_start) over w as validity_end
		from public.activity
		window w as (partition by activity_id order by validity_start)
		order by activity_id, validity_start asc
	) newv
	where 
		newv.activity_id = v.activity_id and newv.validity_start = v.validity_start;
		
	select count(*) from public.activity into total_rows_after_delete;
	
	raise notice 'total_rows_before_insert: %', total_rows_before_insert;
	raise notice 'total_rows_after_insert: %', total_rows_after_insert;
	raise notice 'total_rows_after_delete: %', total_rows_after_delete;
end;
$$ language plpgsql;