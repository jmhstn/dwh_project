with listening as (select lssul.song_id,
						  lsssul.actual_dtm,
						  sa.country_code as home_country,
						  su.country_code as listener_country,
						  action_type
								
								
						 from dv.lsat_session_song_user_listen lsssul
						 left join dv.link_session_song_user_listen lssul
						        on lsssul.session_song_user_listen_id = lssul.session_song_user_listen_id
						 left join dv.link_collection_song lcs 
						        on lssul.song_id = lcs.song_id
						 left join dv.link_artist_collection lac 
						        on lcs.collection_id = lac.collection_id 
						 left join dv.sat_artist sa 
						        on lac.artist_id = sa.artist_id 
						 left join dv.sat_user su 
						 		on lssul.user_id = su.user_id
						 where action_type = 'Complete' or (action_type = 'Start' and at_time_sec = 0)),
						 		
listening_agg as (select song_id,
						 sum(case when song_id is not null then 1 end) as world_listenings,
						 sum(case when home_country = listener_country then 1 else 0 end) as home_listenings,
						 sum(case when actual_dtm between '{EXECUTION_DT}'::date - interval '1 day' and '{EXECUTION_DT}'::date
						 		   and action_type = 'Complete'
						 		  then 1 else 0 end) as world_listenings_1d,
						 sum(case when home_country = listener_country
						 		   and actual_dtm between '{EXECUTION_DT}'::date - interval '1 day' and '{EXECUTION_DT}'::date
						 		   and action_type = 'Complete'
						 			then 1 else 0 end) as home_listenings_1d	
						 from listening
						 group by song_id),
						 
listening_countries as (select song_id,
								   listener_country,
								   count(song_id) as listenings_by_country
							from listening
							where action_type = 'Start'
							group by song_id, listener_country),
							
listening_countries_agg as (select song_id,
									 listener_country,
									 row_number() over (partition by song_id order by listenings_by_country desc) as rnum
							from listening_countries),
									 
listenings_country_array as (select song_id,
									array_remove(array_agg(case when rnum between 1 and 3 then listener_country end), null) as top3_country
							 from listening_countries_agg
							 group by song_id),
							 
						  likes_countries as (select lssul.song_id,
						  lsssul.actual_dtm,
						  sa.country_code as home_country,
						  su.country_code as liker_country

					from dv.link_session_song_user_like lssul
					left join dv.lsat_session_song_user_like lsssul
						   on lssul.session_song_user_like_id = lsssul.session_song_user_like_id
					left join dv.link_collection_song lcs 
					       on lssul.song_id = lcs.song_id
					left join dv.link_artist_collection lac 
					       on lcs.collection_id = lac.collection_id 
					left join dv.sat_artist sa 
					       on lac.artist_id = sa.artist_id 
					left join dv.sat_user su 
					 	   on lssul.user_id = su.user_id),
					 	   
likes_agg as (select song_id,
					 sum(case when song_id is not null then 1 end) as world_likes,
					 sum(case when home_country = liker_country then 1 else 0 end) as home_likes,
					 sum(case when actual_dtm between '{EXECUTION_DT}'::date - interval '1 day' and '{EXECUTION_DT}'::date
					 		  then 1 else 0 end) as world_likes_1d,
					 sum(case when home_country = liker_country
					 		   and actual_dtm between '{EXECUTION_DT}'::date - interval '1 day' and '{{EXECUTION_DT}}'::date
					 			then 1 else 0 end) as home_likes_1d	
					 from likes_countries
					 group by song_id)


select ss.song_id,
	   '{EXECUTION_DT}'::date as actual_dtm,
	   ss.name as song_name,
	   sc.name as collection_name,
	   sc.release_dt,
	   sa.name as artist_name,
	   rg.genre_name,
	   sa.country_code,
	   coalesce(la.world_listenings, 0) as world_listenings,
	   coalesce(la.world_listenings_1d, 0) as world_listenings_1d,
	   coalesce(la.home_listenings, 0) as home_listenings,
	   coalesce(la.home_listenings_1d, 0) as home_listenings_1d,
	   coalesce(top3_country, array[]::varchar[]) as top3_country,
	   coalesce(la2.world_likes, 0) as world_likes,
	   coalesce(la2.world_likes_1d, 0) as world_likes_1d,
	   coalesce(la2.home_likes, 0) as home_likes,
	   coalesce(la2.home_likes_1d, 0) as home_likes_1d
	   

from dv.sat_song ss
left join dv.link_collection_song lcs 
	   on ss.song_id = lcs.song_id
left join dv.sat_collection sc 
	   on lcs.collection_id = sc.collection_id
left join dv.link_artist_collection lac 
	   on sc.collection_id = lac.collection_id
left join dv.sat_artist sa
	   on lac.artist_id = sa.artist_id
left join dv.ref_genre rg 
 	   on sc.original_genre_id = rg.original_genre_id
left join listening_agg la
	   on ss.song_id = la.song_id 	  
left join listenings_country_array lca
	   on ss.song_id = lca.song_id
left join likes_agg la2
	   on ss.song_id = la2.song_id 	

where world_listenings > 0