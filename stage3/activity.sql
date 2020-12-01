do $$
declare

	record record;

begin
	set search_path to tourism1;
		
	-- Check accessibility URL validity
	for record in (
		select activity_id, accessibility_info_url
		from activity
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
	
	
	
end;
$$ language plpgsql;