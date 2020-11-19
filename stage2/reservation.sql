do $$
declare

	record record;

begin
	set search_path to tourism1;
	
	delete from public.reservation;
	
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
		
		
		--		reservation_entry
		insert into public.reservation(
			activity_id,
			visitor_id,
			booking_start_time_id,
			booking_time_id,
			attendees
		)
		values (
			record.activity_id,
			record.visitor_id,
			record.booking_period_start_epoch,
			record.booking_time_epoch,
			record.attendees
		);
		
	end loop;
end;
$$ language plpgsql;