with sat_user as (select user_id,
						 registration_dtm,
				         email,
				         country_code,
				         first_name,
				         last_name,
				         birth_dt,
				         row_number() over (partition by user_id order by actual_dtm desc) as rnum
			      from dv.sat_user
			      where actual_dtm <= '{EXECUTION_DT}'::date),
			      
subscriptions as (select su.user_id,
						 su.registration_dtm,
						 su.email,
						 su.country_code,
		         		 su.first_name,
		         		 su.last_name,
		         		 su.birth_dt,
		         		 sus.actual_dtm as subscription_update_dtm,
						 case when sus.is_premium is not null then 'Premium' else 'Basic' end as subscription_type
				  from sat_user su
				  left join dv.sat_user_subscription sus
				  		 on su.user_id = sus.user_id
				  		and su.rnum = 1
				  		and sus.actual_dtm <= '{EXECUTION_DT}'::date),
						  						  
listenings as (select su.user_id,
					  lsssul.actual_dtm,
				      su.country_code as user_country,
				      sa.country_code as song_country,
				      lsssul.at_time_sec,
				      lsssul.action_type,
				      ss.duration_sec,
				      rg.genre_name as genre_name,
				      sa.name as artist_name,
				      ss.name as song_name,
				      rg.happiness_index
				   
			   from sat_user su
			   left join dv.link_session_song_user_listen lssul
				      on su.user_id = lssul.user_id
			   left join dv.lsat_session_song_user_listen lsssul
				      on lssul.session_song_user_listen_id = lsssul.session_song_user_listen_id
			   left join dv.link_collection_song lcs 
			          on lssul.song_id = lcs.song_id
			   left join dv.link_artist_collection lac 
			          on lcs.collection_id = lac.collection_id 
			   left join dv.sat_artist sa 
			          on lac.artist_id = sa.artist_id
			   left join dv.sat_song ss 
			   		  on ss.song_id = lssul.song_id
			   left join dv.ref_genre rg 
			   	 	  on rg.original_genre_id = ss.original_genre_id 
			
			   where su.rnum = 1
			   	 and lsssul.actual_dtm <= '{EXECUTION_DT}'::date),
			   
listenings_agg as (select user_id,
						  genre_name,
						  artist_name,
						  song_name,
						  sum(case when action_type = 'Complete' 
						  			and user_country = song_country 
						  		   then 1 else 0 end) as home_listenings_whole_cnt,
						  sum(case when action_type = 'Complete' 
						  		   then 1 else 0 end) as world_listenings_whole_cnt,
						  sum(case when action_type = 'Complete'
						  			and actual_dtm between '{EXECUTION_DT}'::date and '{EXECUTION_DT}'::date - interval '1 day'
						  		   then 1 else 0 end) as world_listenings_1d_cnt,
						  sum(case when action_type = 'Pause'
						            and 1 - ((1 - at_time_sec) / duration_sec) > 0.1
						  			and actual_dtm between '{EXECUTION_DT}'::date and '{EXECUTION_DT}'::date - interval '1 day'
						  		   then 1 else 0 end) as p10_world_listenings_1d_cnt,
						  sum(case when action_type = 'Pause'
						            and 1 - ((1 - at_time_sec) / duration_sec) < 0.1
						  			and actual_dtm between '{EXECUTION_DT}'::date and '{EXECUTION_DT}'::date - interval '1 day'
						  		   then 1 else 0 end) as turn_world_listenings_1d_cnt,
						  sum(case when action_type = 'Pause'
						            and 1 - ((1 - at_time_sec) / duration_sec) > 0.1
						  		   then 1 else 0 end) as p10_world_listenings_whole_cnt,
						  sum(case when action_type = 'Start'
						            and at_time_sec = 0
						  		   then 1 else 0 end) as turn_world_listenings_whole_cnt,
						  round(avg(case when action_type in ('Pause', 'Complete')
						            then (at_time_sec / duration_sec) * 100 end), 2) as avg_percent_listening,
						  round(avg(happiness_index), 2) as avg_happiness_index
 
						  		   
						  from listenings
						  group by
						  grouping sets ((user_id), (user_id, genre_name), (user_id, artist_name), (user_id, song_name))),					  
						  
						  
top_artists as (select user_id,
						     artist_name,
							 row_number() over (partition by user_id order by turn_world_listenings_whole_cnt desc) as top
							 from listenings_agg
							 where artist_name is not null),
							 
top_genres as (select user_id,
				      genre_name,
					  row_number() over (partition by user_id order by turn_world_listenings_whole_cnt desc) as top
					  from listenings_agg
					  where genre_name is not null),
					  
top_songs as (select user_id,
				     song_name,
					 row_number() over (partition by user_id order by turn_world_listenings_whole_cnt desc) as top
					 from listenings_agg
					 where song_name is not null),

					 
user_artist_array as (select user_id,
						   array_remove(array_agg(case when top between 1 and 5
							  		 				   then artist_name 
							  		 					end order by top), null) as top5_artists
				    from top_artists
				    group by user_id),
				    
user_song_array as (select user_id,
						   array_remove(array_agg(case when top between 1 and 5
							  		 				   then song_name 
							  		 					end order by top), null) as top5_songs
				    from top_songs
				    group by user_id),
				    
user_genre_array as (select user_id,
						   array_remove(array_agg(case when top between 1 and 3
							  		 				   then genre_name 
							  		 					end order by top), null) as top3_genres
				    from top_genres
				    group by user_id),
				    
payments as (select su.user_id,
					su.registration_dtm,
					su.country_code,
					su.subscription_type as current_subscription_type,
					rcsc_b.subscription_cost as basic_subscription_cost,
					rcsc_p.subscription_cost as premium_subscription_cost,
					ceil(extract('epoch' from '{EXECUTION_DT}'::date - su.registration_dtm)/3600/24) as basic_days,
					floor(extract('epoch' from coalesce(subscription_update_dtm, su.registration_dtm) - su.registration_dtm)/3600/24) as premium_days

					from subscriptions su
					join dv.ref_country_subscription_cost rcsc_b
					  on su.country_code = rcsc_b.country_code
					  and rcsc_b.subscription_type = 'Basic'
					join dv.ref_country_subscription_cost rcsc_p
					  on su.country_code = rcsc_p.country_code
					  and rcsc_p.subscription_type = 'Premium')
			    
				    
select s.user_id,
	   s.registration_dtm,
	   s.email,
	   s.country_code,
	   s.first_name,
	   s.last_name,
	   s.birth_dt,
	   s.subscription_update_dtm,
	   s.subscription_type,
	   uaa.top5_artists,
	   usa.top5_songs,
	   uga.top3_genres,
	   la.home_listenings_whole_cnt,
	   la.world_listenings_whole_cnt,
	   la.world_listenings_1d_cnt,
	   la.p10_world_listenings_1d_cnt,
	   la.turn_world_listenings_1d_cnt,
	   la.p10_world_listenings_whole_cnt,
	   la.turn_world_listenings_whole_cnt,
	   la.avg_percent_listening,
	   la.avg_happiness_index,
	   p.basic_subscription_cost * p.basic_days + p.premium_subscription_cost * p.premium_days as total_payment

from subscriptions s
left join listenings_agg la 
	   on s.user_id = la.user_id
	  and la.artist_name is null 
	  and la.song_name is null 
	  and la.genre_name is null 
left join user_artist_array uaa
	   on s.user_id = uaa.user_id
left join user_song_array usa
	   on s.user_id = usa.user_id
left join user_genre_array uga
	   on s.user_id = uga.user_id
left join payments as p 
	   on p.user_id = s.user_id