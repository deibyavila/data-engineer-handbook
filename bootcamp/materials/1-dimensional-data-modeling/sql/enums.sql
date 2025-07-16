create type vertex_type
as ENUM('player','team','game');




create  table vertices (
		identifier text,
		type vertex_type,
		properties json,
		primary key (identifier, type));


drop type edge_type

create type edge_type as
		enum('plays_against',
				'shares_team',
			'plays_in',
				'plays_on');

alter type edge_type rename value 'players_against' to 'plays_against'

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type vertex_type,
    object_identifier TEXT,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    PRIMARY KEY (subject_identifier,
                subject_type,
                object_identifier,
                object_type,
                edge_type)
);



insert into vertices
select 
game_id as identifier,
'game'::vertex_type as type,
json_build_object(
'pts_home',pts_home ,
'pts_away', pts_away ,
'wining_team' ,  case when home_team_wins =1 then home_team_id else visitor_team_id end
) as properties
from games




insert into vertices
with players_agg as (
select 
player_id as identifier,
max(player_name) as player_name,
count(1) as number_of_games,
sum(pts) as total_points,
array_agg(distinct team_id) as teams
from game_details
group by  player_id
)
select identifier,
'player'::vertex_type,
json_build_object(
	'player_name', player_name,
	'numbner_of_games', number_of_games,
	'total_points',total_points,
	'teams',teams
	) as properties
from players_agg


insert into vertices
with teams_dedupe as (
select * , row_number() over(partition by team_id) as rn
from  teams
)
select 
	team_id as identifier	,
	'team'::vertex_type,
	json_build_object(
	'abbreviation', abbreviation,
	'nickname', nickname ,
	'city',city,
	'arena', arena,
	'year_founded', yearfounded 
	) as poperties
from teams_dedupe
where rn=1



insert into edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
     filtered AS (
         SELECT * FROM deduped
         WHERE row_num = 1
     ),
     aggregated AS (
          SELECT
           f1.player_id as subject_player_id,
           max( f1.player_name) as subject_player_name,
           f2.player_id as object_player_id,
           max(f2.player_name) as object_player_name,
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            end as edge_type,
            COUNT(1) AS num_games,
            SUM(f1.pts) AS subject_points,
            SUM(f2.pts) as object_points
        FROM filtered f1
            JOIN filtered f2
            ON f1.game_id = f2.game_id
            AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id
        GROUP BY
                f1.player_id,
           f2.player_id,
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN  'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            END
     )select 
     subject_player_id  as subject_identifier,
     'player'::vertex_type as subject_type,
     object_player_id as object_type,
     'player'::vertex_type as object_type,
     edge_type as edge_type,
     json_build_object(
     'num_games', num_games,
     'subject_points' ,subject_points,
     'object_points', object_points
     )
     from aggregated
     
     
     
     select * from vertices v join edges e on v.identifier = e.subject_identifier
     and v.type= e.subject_type
     where e.object_type='player'::vertex_type