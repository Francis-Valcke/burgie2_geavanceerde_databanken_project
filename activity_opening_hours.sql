do $$
declare

	record record;
	sub_record record;
	iterator bigint := 0;

begin
	set search_path to tourism1;
	
	delete from public.activity_opening_hours;
	delete from public.activity_activity_opening_hours_bridge;

	for record in (
		with temp_table as(
			select 
				*,
				extract(epoch FROM duration)/60 as duration_in_minutes,
				extract(year from start_time) as start_time_year,
				extract(month from start_time) as start_time_month,
				extract(day from start_time) as start_time_day,
				extract(hour from start_time) as start_time_hour,
				extract(minute from start_time) as start_time_minute,
				extract(second from start_time) as start_time_second,
				extract(epoch from start_time) as start_time_epoch,
				start_time + duration as end_time
			from 
				activity_opening_hours
		)
		select
			*,
			extract(year from end_time) as end_time_year,
			extract(month from end_time) as end_time_month,
			extract(day from end_time) as end_time_day,
			extract(hour from end_time) as end_time_hour,
			extract(minute from end_time) as end_time_minute,
			extract(second from end_time) as end_time_second,
			extract(epoch from end_time) as end_time_epoch
		from
			temp_table
	)
	loop
		-- 		start_time
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
			record.start_time_year,
			record.start_time_month,
			record.start_time_day,
			record.start_time_hour,
			record.start_time_minute,
			record.start_time_second,
			record.start_time_epoch
 		) on conflict do nothing;
		
		-- 		end_time
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
			record.end_time_year,
			record.end_time_month,
			record.end_time_day,
			record.end_time_hour,
			record.end_time_minute,
			record.end_time_second,
			record.end_time_epoch
 		) on conflict do nothing;

		-- 		insert entry in activity_opening_hours and return new assigned id
		
		insert into public.activity_opening_hours(
			start_time_id,
			end_time_id,
			duration_in_minutes
		)
		values (
			record.start_time_epoch,
			record.end_time_epoch,
			record.duration_in_minutes
		) returning activity_opening_hours_id into sub_record;
		
		-- 		use new id to fill in the bridge table
		
		insert into public.activity_activity_opening_hours_bridge(
 			activity_id,
 			activity_opening_hours_id
 		)
 		values (
 			record.activity_id,
 			sub_record.activity_opening_hours_id
 		);

	end loop;
end;
$$ language plpgsql;