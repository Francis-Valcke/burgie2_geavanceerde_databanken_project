do $$
declare

	activity_row record;

begin
	set search_path to tourism1;
	
	delete from public.activity;
	
	for activity_row in (
		select 
			-- 	activity
			activity.activity_id as activity_id,
			activity.activity_name as activity_name,
			case when (activity.website is NULL) or (activity.website = '') then false else true end as has_website,
			case when (activity.phonenumber is NULL) or (activity.phonenumber = '') then false else true end as has_phonenumber,
			activity.description as description,
			activity.operational as is_operational,
			case when (activity.accessibility_info_url is NULL) or (activity.accessibility_info_url = '') then false else true end as has_accessibility_info_url,
			case when (activity.email_address is NULL) or (activity.email_address = '') then false else true end as has_email_address,
			activity.tourist_region as tourist_region,
			activity.activity_type as activity_type,
			activity.accessibility_label as accessibility_label,
-- 			activity.last_modified as last_modified,
			-- 	auditive_disability_information
			auditive_disability_information.hearing_impaired_details,
			case when (auditive_disability_information.hearing_impaired is NULL) or (auditive_disability_information.hearing_impaired = 0) then false else true end as hearing_impaired,
			auditive_disability_information.deaf_details,
			case when (auditive_disability_information.deaf is NULL) or (auditive_disability_information.deaf = 0) then false else true end as deaf,

			-- 	visual_disability_information
			visual_disability_information.blind_details,
			case when (visual_disability_information.blind is NULL) or (visual_disability_information.blind = 0) then false else true end as blind,
			visual_disability_information.visually_impaired_details,
			case when (visual_disability_information.visually_impaired is NULL) or (visual_disability_information.visually_impaired = 0) then false else true end as visually_impaired,
			-- 	mental_disability_information
			mental_disability_information.mental_disability_details,
			case when (mental_disability_information.mental_disability is NULL) or (mental_disability_information.mental_disability = 0) then false else true end as mental_disability,
			mental_disability_information.autism_details,
			case when (mental_disability_information.autism is NULL) or (mental_disability_information.autism = 0) then false else true end as autism,
			-- 	allergy_information
			case when (allergy_information.food_allergy is NULL) or (allergy_information.food_allergy = 0) then false else true end as food_allergy,
			allergy_information.allergy_details,
			case when (allergy_information.allergy is NULL) or (allergy_information.allergy = 0) then false else true end as allergy,
			-- 	accessibility_information
			case when (accessibility_information.motoric_accessibility is NULL) or (accessibility_information.motoric_accessibility = 0) then false else true end as motoric_accessibility
		from activity
		join auditive_disability_information on activity.activity_id=auditive_disability_information.activity_id
		join visual_disability_information on activity.activity_id=visual_disability_information.activity_id
		join mental_disability_information on activity.activity_id=mental_disability_information.activity_id
		join allergy_information on activity.activity_id=allergy_information.activity_id
		join accessibility_information on activity.activity_id=accessibility_information.activity_id
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
-- 			last_modified,
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
			motoric_accessibility
		)
		values (
			-- 	activity
			activity_row.activity_id,
			activity_row.activity_name,
			activity_row.has_website,
			activity_row.has_phonenumber,
			activity_row.description,
			activity_row.is_operational,
			activity_row.has_accessibility_info_url,
			activity_row.has_email_address,
			activity_row.tourist_region,
			activity_row.activity_type,
			activity_row.accessibility_label,
-- 			activity_row.last_modified,
			-- 	auditive_disability_information
			activity_row.hearing_impaired_details,
			activity_row.hearing_impaired,
			activity_row.deaf_details,
			activity_row.deaf,
			-- 	visual_disability_information
			activity_row.blind_details,
			activity_row.blind,
			activity_row.visually_impaired_details,
			activity_row.visually_impaired,
			-- 	mental_disability_information
			activity_row.mental_disability_details,
			activity_row.mental_disability,
			activity_row.autism,
			activity_row.autism_details,
			-- 	allergy_information
			activity_row.food_allergy,
			activity_row.allergy_details,
			activity_row.allergy,
			-- 	accessibility_information
			activity_row.motoric_accessibility
		);
	end loop;
end;
$$ language plpgsql;