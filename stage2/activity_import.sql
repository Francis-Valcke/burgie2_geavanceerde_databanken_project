do $$
declare

	activity_row record;

begin
	set search_path to tourism1;

	select 
		activity.activity_id as activity_id,
		activity.activity_name as activity_name,
		case when (activity.website is NULL) or (activity.website = '') then false else true end as has_website,
		case when (activity.phonenumber is NULL) or (activity.phonenumber = '') then false else true end as has_phonenumber,
		activity.description as description,
		activity.operational as is_operational,
		case when (activity.accessibility_info_url is NULL) or (activity.accessibility_info_url = '') then false else true end as has_accessibility_info_url,
		case when (activity.email_address is NULL) or (activity.email_address = '') then false else true end as has_email_address,
		activity.tourist_region as tourist_region,
		activity.activity_type as tourist_region,
		activity.accessibility_label as accessibility_label,
		activity.last_modified as last_modified
	from activity
	join auditive_disability_information on activity.activity_id=auditive_disability_information.activity_id
	join visual_disability_information on activity.activity_id=visual_disability_information.activity_id
	join mental_disability_information on activity.activity_id=mental_disability_information.activity_id
	join allergy_information on activity.activity_id=allergy_information.activity_id
	join accessibility_information on activity.activity_id=accessibility_information.activity_id;

end;
$$ language plpgsql;