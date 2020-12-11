do $$
declare

	record record;
	record2 record;

	corrected_activity_id bigint := 0;
	corrected_visitor_id bigint := 0;
	zipcode_from int := 0;
	zipcode_to int := 0;
	activity_type VARCHAR;


begin
	set search_path to tourism1;
	
	for record in (
		select
			reservation.visitor_id,
			reservation.activity_id,
			reservation.booking_period_start,
			extract(year from reservation.booking_period_start) as booking_period_start_year,
			extract(month from reservation.booking_period_start) as booking_period_start_month,
			extract(day from reservation.booking_period_start) as booking_period_start_day,
			extract(hour from reservation.booking_period_start) as booking_period_start_hour,
			extract(minute from reservation.booking_period_start) as booking_period_start_minute,
			extract(second from reservation.booking_period_start) as booking_period_start_second,
			extract(epoch from reservation.booking_period_start) as booking_period_start_epoch,
			reservation.booking_time,
			extract(year from reservation.booking_time) as booking_time_year,
			extract(month from reservation.booking_time) as booking_time_month,
			extract(day from reservation.booking_time) as booking_time_day,
			extract(hour from reservation.booking_time) as booking_time_hour,
			extract(minute from reservation.booking_time) as booking_time_minute,
			extract(second from reservation.booking_time) as booking_time_second,
			extract(epoch from reservation.booking_time) as booking_time_epoch,
			reservation.attendees

		from reservation
	) 
	loop
		
		-- 		time_dimension_entry for booking_time
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
			record.booking_time_year,
			record.booking_time_month,
			record.booking_time_day,
			record.booking_time_hour,
			record.booking_time_minute,
			record.booking_time_second,
			record.booking_time_epoch
 		) on conflict do nothing;
		
		-- 		time_dimension_entry for booking_start_time
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
			record.booking_period_start_year,
			record.booking_period_start_month,
			record.booking_period_start_day,
			record.booking_period_start_hour,
			record.booking_period_start_minute,
			record.booking_period_start_second,
			record.booking_period_start_epoch
 		) on conflict do nothing;
		
		-- Now we need to translate the visitor and activity id into the most recent version surrogates
		select
			last_value(activity_id_surrogate) over (partition by activity_id order by validity_start 
			RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) as activity_id_surrogate,
			address_zipcode,
			activity.activity_type

		from
			public.activity
		where
			record.activity_id = activity_id
		into record2;
		
		corrected_activity_id = record2.activity_id_surrogate;
		activity_type = record2.activity_type;
		zipcode_to = record2.address_zipcode;
		

		with visitor_id_mapping as (
			select
				distinct on (visitor_id) visitor_id,
				case when t.new_visitor_id is not null then new_visitor_id else visitor_id end as selected_visitor_id
			from public.visitor
			left join public.translation_table t on public.visitor.visitor_id = t.old_visitor_id
			order by visitor_id
		)
		select
			last_value(visitor_id_surrogate) over (partition by mp.selected_visitor_id order by validity_start 
			RANGE BETWEEN
            UNBOUNDED PRECEDING AND
            UNBOUNDED FOLLOWING) as visitor_id_surrogate,
			address_zipcode
		from public.visitor
		left join visitor_id_mapping mp on public.visitor.visitor_id = mp.visitor_id
		where record.visitor_id = public.visitor.visitor_id
		into record2;
		
		corrected_visitor_id = record2.visitor_id_surrogate;
		zipcode_from = record2.address_zipcode;
		
		
		--		reservation_entry
		insert into public.reservation(
			activity_id,
			visitor_id,
			booking_start_time_id,
			booking_time_id,
			attendees,
			zipcode_from,
			zipcode_to,
			activity_type
		)
		values (
			corrected_activity_id,
			corrected_visitor_id,
			record.booking_period_start_epoch,
			record.booking_time_epoch,
			record.attendees,
			zipcode_from,
			zipcode_to,
			activity_type
		);
		
	end loop;
end;
$$ language plpgsql;