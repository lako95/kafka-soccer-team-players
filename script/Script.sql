create table MATCH(
	id int,
	match_day int,
	home_team varchar(50),
	away_team varchar(50),
	winner varchar(50),
	score varchar(10),
	primary key (id)
);

create table NO_WINNER_MATCH(
	id int,
	match_day int,
	home_team varchar(50),
	away_team varchar(50),
	winner varchar(50),
	score varchar(10),
	primary key (id)
);

select * from match;

select * from no_winner_match;



delete from match;
delete from no_winner_match;