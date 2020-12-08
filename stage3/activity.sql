do $$
declare

	record record;

begin
	set search_path to tourism1;
		
	--  First make a join table to perform queries on
	
	drop table if exists activity_full;
	create temporary table activity_full on commit drop as (
		select 
			activity.activity_id,
		
			activity.accessibility_info_url,
			activity.accessibility_label,
			accessibility_information.motoric_accessibility,
			
			case when (accessibility_information.motoric_accessibility is NULL) or (accessibility_information.motoric_accessibility = 0) then false else true end as motoric_accessibility_bool,
		
			allergy_information.allergy,
			allergy_information.allergy_details,
			allergy_information.food_allergy,
			
			case when (allergy_information.allergy is NULL) or (allergy_information.allergy = 0) then false else true end as allergy_bool,
			case when (allergy_information.food_allergy is NULL) or (allergy_information.food_allergy = 0) then false else true end as food_allergy_bool,
		
			auditive_disability_information.deaf,
			auditive_disability_information.deaf_details,
			auditive_disability_information.hearing_impaired,
			auditive_disability_information.hearing_impaired_details,
		
			case when (auditive_disability_information.deaf is NULL) or (auditive_disability_information.deaf = 0) then false else true end as deaf_bool,
			case when (auditive_disability_information.hearing_impaired is NULL) or (auditive_disability_information.hearing_impaired = 0) then false else true end as hearing_impaired_bool,

			mental_disability_information.autism,
			mental_disability_information.autism_details,
			mental_disability_information.mental_disability,
			mental_disability_information.mental_disability_details,
		
			case when (mental_disability_information.autism is NULL) or (mental_disability_information.autism = 0) then false else true end as autism_bool,
			case when (mental_disability_information.mental_disability is NULL) or (mental_disability_information.mental_disability = 0) then false else true end as mental_disability_bool,
		
			visual_disability_information.blind,
			visual_disability_information.blind_details,
			visual_disability_information.visually_impaired,
			visual_disability_information.visually_impaired_details,
		
			case when (visual_disability_information.blind is NULL) or (visual_disability_information.blind = 0) then false else true end as blind_bool,
			case when (visual_disability_information.visually_impaired is NULL) or (visual_disability_information.visually_impaired = 0) then false else true end as visually_impaired_bool
		
			
		from activity
		join accessibility_information on accessibility_information.activity_id = activity.activity_id
	  	join allergy_information on allergy_information.activity_id = activity.activity_id
 		join auditive_disability_information on auditive_disability_information.activity_id = activity.activity_id
 		join mental_disability_information on mental_disability_information.activity_id = activity.activity_id
 		join visual_disability_information on visual_disability_information.activity_id = activity.activity_id
	);
			
	-- Check accessibility URL validity
	for record in (
 		select activity_id, accessibility_info_url
 		from activity_full
 		where 
 			(
 				not cast(accessibility_info_url as text) ~ '(https?://(?:www.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9].[^\s]{2,}|www.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9].[^\s]{2,}|https?://(?:www.|(?!www))[a-zA-Z0-9]+.[^\s]{2,}|www.[a-zA-Z0-9]+.[^\s]{2,})'
 				or cast(accessibility_info_url as text) ~ '^(.*\s+.*)+$'
 			) 
 			and not (
 				cast(accessibility_info_url as text) like '' and accessibility_label like ''
 			)
 	)	
 	loop
		raise notice 'Activity_id = %: Inconsistant accessibility_info_url = %', record.activity_id, record.accessibility_info_url;
 	end loop;
	
	for record in (
		select *
		from activity_full
		where deaf_bool = true and hearing_impaired_bool = false
	) loop
		raise notice 'Activity_id = %: deaf = %, hearing_impared = %', record.activity_id, record.deaf_bool, record.hearing_impaired_bool;
	end loop;
	
	for record in (
		select *
		from activity_full
		where blind_bool = true and visually_impaired_bool = false
	) loop
		raise notice 'Activity_id = %: blind = %, visually_impaired = %', record.activity_id, record.blind_bool, record.visually_impaired_bool;
	end loop;
	
	for record in (
		select *
		from activity_full
		where autism_bool = true and mental_disability_bool = false
	) loop
		raise notice 'Activity_id = %: autism = %, mental_disability = %', record.activity_id, record.autism_bool, record.mental_disability_bool;
	end loop;

	for record in (
		select *
		from activity_full
		where food_allergy_bool = true and allergy_bool = false
	) loop
		raise notice 'Activity_id = %:  food_allergy = %, allergy = %', record.activity_id, record.food_allergy_bool, record.allergy_bool;
	end loop;
	
	for record in (
		select *
		from activity_full
		where not (allergy_details is NULL or allergy_details like '') and allergy_bool = false
	) loop
		raise notice 'Activity_id = %:  allergy_details = %, allergy_bool = %', record.activity_id, record.allergy_details, record.allergy_bool;
	end loop;
	
	for record in (
		select *
		from activity_full
		where not (deaf_details is NULL or deaf_details like '') and deaf_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  deaf_details = %, deaf_bool = %', record.activity_id, record.deaf_details, record.deaf_bool;
	end loop;
	
	for record in (
		select *
		from activity_full
		where not (hearing_impaired_details is NULL or hearing_impaired_details like '') and hearing_impaired_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  hearing_impaired_details = %, hearing_impaired_bool = %', record.activity_id, record.hearing_impaired_details, record.hearing_impaired_bool;
	end loop;	
	
	for record in (
		select *
		from activity_full
		where not (autism_details is NULL or autism_details like '') and autism_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  autism_details = %, autism_bool = %', record.activity_id, record.autism_details, record.autism_bool;
	end loop;	
	
	for record in (
		select *
		from activity_full
		where not (mental_disability_details is NULL or mental_disability_details like '') and mental_disability_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  mental_disability_details = %, mental_disability_bool = %', record.activity_id, record.mental_disability_details, record.mental_disability_bool;
	end loop;	
	
	for record in (
		select *
		from activity_full
		where not (blind_details is NULL or blind_details like '') and blind_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  blind_details = %, blind_bool = %', record.activity_id, record.blind_details, record.blind_bool;
	end loop;		
	
	
	for record in (
		select *
		from activity_full
		where not (visually_impaired_details is NULL or visually_impaired_details like '') and visually_impaired_bool = false	
	) 
	loop
		raise notice 'Activity_id = %:  visually_impaired_details = %, blind_bool = %', record.activity_id, record.visually_impaired_details, record.visually_impaired_bool;
	end loop;	

	
	drop table if exists activity_full;
end;
$$ language plpgsql;