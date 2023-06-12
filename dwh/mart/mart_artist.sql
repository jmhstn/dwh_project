with latest_release as (select lac.artist_id,
						  release_dt,
						  row_number() over (partition by lac.artist_id 
						  					 order by sc.release_dt, sc.actual_dtm desc) as rnum
				   from dv.sat_collection sc
				   left join dv.link_artist_collection lac 
	   					  on sc.collection_id = lac.collection_id
				   where sc.actual_dtm <= '{EXECUTION_DT}'),
	   						   
main_genre as (select lac.artist_id,
					  rg.genre_name,
					  count(lcs.song_id) as genre_cnt,
					  sc.release_dt 
			   from dv.sat_collection sc
			   left join dv.link_collection_song lcs 
			   		  on sc.collection_id = lcs.collection_id
			   left join dv.link_artist_collection lac 
			   		  on sc.collection_id = lac.collection_id 
			   left join dv.ref_genre rg 
			   		  on sc.original_genre_id = rg.original_genre_id

			   where sc.actual_dtm <= '{EXECUTION_DT}'

			   group by lac.artist_id, rg.genre_name, sc.release_dt),

main_genre_agg as (select artist_id, genre_name,
						  row_number() over (partition by artist_id 
						  					 order by genre_cnt desc, length(genre_name) asc) as rnum,
						  genre_cnt,
						  release_dt
				   from main_genre),				   
				   
artist_listen_country as (select lac.artist_id,
					     su.country_code,
						 sum(case when lsssul.actual_dtm between '{EXECUTION_DT}'::timestamp - interval '7 day' 
						 									 and '{EXECUTION_DT}'::timestamp 
						 		  then 1 else 0 end) as country_listen_7d,
						 sum(case when lsssul.actual_dtm between '{EXECUTION_DT}'::timestamp - interval '1 day' 
						 									 and '{EXECUTION_DT}'::timestamp 
						 		  then 1 else 0 end) as country_listen_1d,
						 sum(case when lsssul.actual_dtm between '{EXECUTION_DT}'::timestamp - interval '1 day' 
						 									 and '{EXECUTION_DT}'::timestamp 
						 		   and su.subscription_type = 'Basic'
						 		  then 1 else 0 end) as country_listen_basic_1d,
						 sum(case when lsssul.actual_dtm between '{EXECUTION_DT}'::timestamp - interval '1 day' 
						 									 and '{EXECUTION_DT}'::timestamp 
						 		   and su.subscription_type = 'Premium'
						 		  then 1 else 0 end) as country_listen_premium_1d
					
				  from dv.link_artist_collection lac
				  left join dv.link_collection_song lcs
				  		 on lac.collection_id = lcs.collection_id
				  left join dv.link_session_song_user_listen lssul
				  	     on lcs.song_id = lssul.song_id    
				  left join dv.lsat_session_song_user_listen lsssul
				  		 on lssul.session_song_user_listen_id = lsssul.session_song_user_listen_id
				  left join dv.sat_user su
				  		 on lssul.user_id = su.user_id
				  
				  where  lsssul.actual_dtm between  '{EXECUTION_DT}'::timestamp  - interval '7 day' 
				  							   and '{EXECUTION_DT}'::timestamp
				  		 
				  group by artist_id, country_code
				  order by artist_id),			  
				  
artist_royalty as (select artist_id,
						  sum(alc.country_listen_basic_1d * rcsc_basic.royalty 
						  	  + alc.country_listen_premium_1d * rcsc_premium.royalty) as royalty
						  	  
				   from artist_listen_country alc
				   join dv.ref_country_subscription_cost rcsc_basic
				     on alc.country_code = rcsc_basic.country_code
				    and rcsc_basic.subscription_type = 'Basic'
				   join dv.ref_country_subscription_cost rcsc_premium
				     on alc.country_code = rcsc_premium.country_code
				    and rcsc_premium.subscription_type = 'Premium'
				    
				    group by artist_id),			  		 
				  		 
artist_listen_country_agg as (select artist_id,
									 country_code,
									 row_number() over (partition by artist_id 
									 				    order by country_listen_7d desc) as top_7d,
									 row_number() over (partition by artist_id 
									 					order by country_listen_1d desc) as top_1d
							  from artist_listen_country
							  order by artist_id),
	    						
artist_popular_songs as (select lac.artist_id,
								ss.name as song_name,
								sum(case when lsssul.actual_dtm < '{EXECUTION_DT}'::timestamp then 1 else 0 end) as song_listen,
								sum(case when lsssul.actual_dtm < '{EXECUTION_DT}'::timestamp  
										  and action_type = 'Complete' 
										  then 1 else 0 end) as song_listen_full,
								sum(case when lsssul.actual_dtm < '{EXECUTION_DT}'::timestamp  
										  and action_type = 'Pause'
										  and  (lsssul.at_time_sec::decimal / ss.duration_sec) * 100 > 10
										 then 1 else 0 end) as song_listen_10
								 from dv.link_artist_collection lac
						  left join dv.link_collection_song lcs
						  		 on lac.collection_id = lcs.collection_id
						  left join dv.link_session_song_user_listen lssul
						  	     on lcs.song_id = lssul.song_id    
						  left join dv.lsat_session_song_user_listen lsssul
						  		 on lssul.session_song_user_listen_id = lsssul.session_song_user_listen_id
						  left join dv.sat_song ss
						  		 on lssul.song_id = ss.song_id
						  		 
						  group by lac.artist_id, ss.name),						  
						  
artist_popular_songs_agg as (select artist_id,
									 song_name,
									 row_number() over (partition by artist_id order by song_listen desc) as top,
									 song_listen_full,
									 song_listen_10
							  from artist_popular_songs
							  order by artist_id),
							  
artist_listen_country_array  as (select artist_id,
							  		 array_remove(array_agg(case when top_7d between 1 and 3 
							  		 							 then country_code 
							  		 							 end order by top_7d), null) as top3_countries_7d,
	    					  		 array_remove(array_agg(case when top_1d between 1 and 3 
	    					  		 							 then country_code 
	    					  		 							 end order by top_1d), null) as top3_countries_1d
	    					  from artist_listen_country_agg
	    					  group by artist_id),
	    					  
artist_popular_songs_array as (select artist_id,
							  		 array_remove(array_agg(case when top between 1 and 3 
							  		 							 then song_name
							  		 							 end order by top), null) as top3_songs,
							  		 sum(song_listen_full) as song_listen_full,
									 sum(song_listen_10) as song_listen_10
	    					  from artist_popular_songs_agg
	    					  group by artist_id),
	    					  
artist_followed as (select lsauf.artist_id,
						   sum(case when lssauf.actual_dtm < '{EXECUTION_DT}'::timestamp 
						 		  then 1 else 0 end) as follow_all,
						    sum(case when lssauf.actual_dtm between '{EXECUTION_DT}'::timestamp - interval '1 day' 
						 									 and '{EXECUTION_DT}'::timestamp 
						 		     then 1 else 0 end) as follow_1d

					from dv.link_session_artist_user_follow lsauf
					join dv.lsat_session_artist_user_follow lssauf
					  on lsauf.session_artist_user_follow_id = lssauf.session_artist_user_follow_id
					group by artist_id)
					
select {EXECUTION_DT} as actual_dtm,
		sa.artist_id,
		sa.name,
	    sa.country_code,
	    sa.founded_year,
	    lr.release_dt as latest_release_dt,
	    genre_name as main_genre,
	    coalesce(alca.top3_countries_7d, array[]::varchar[]) as top3_countries_7d,
	    coalesce(alca.top3_countries_1d, array[]::varchar[]) as top3_countries_1d,
	    coalesce(apsa.top3_songs, array[]::varchar[]) as top3_songs,
	    coalesce(apsa.song_listen_full, 0) as song_listen_full,
		coalesce(apsa.song_listen_10, 0) as song_listen_10p,
		coalesce(ar.royalty, 0) as royalty_1d,
		coalesce(af.follow_1d, 0) as follow_1d,
		coalesce(af.follow_all, 0) as follow_all,
		0.4 * coalesce(apsa.song_listen_full, 0) + 0.4 * coalesce(apsa.song_listen_10, 0) 
		+ 0.2 * coalesce(af.follow_all, 0) as calculated_rating			

from dv.sat_artist sa 
left join latest_release lr
	   on sa.artist_id = lr.artist_id
	  and lr.rnum = 1
left join main_genre_agg mga 
	   on sa.artist_id = mga.artist_id
	  and mga.rnum = 1
left join artist_listen_country_array alca
       on sa.artist_id = alca.artist_id
left join artist_popular_songs_array apsa
	   on sa.artist_id  = apsa.artist_id
left join artist_royalty ar
 	   on sa.artist_id = ar.artist_id
left join artist_followed af
 	   on sa.artist_id = af.artist_id
 	   
order by calculated_rating desc, song_listen_full desc