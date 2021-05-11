-- MOVIE DATASET TEAM:
-- ISIS RAMIREZ
-- SIRI MANJUNATH
-- XIAOYE ZHANG


--LOAD_CREATE_TABLES
-- SET ENVIRONMENT TO LOAD MOVIE
set client_encoding to 'utf8';


------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------Dummy Tables -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Create Dummy Tables
DROP TABLE IF EXISTS ACTOR_JSON;
DROP TABLE IF EXISTS CREW_JSON;
DROP TABLE IF EXISTS KEYWORD_JSON;
DROP TABLE IF EXISTS RATING_DUMMY;
DROP TABLE IF EXISTS MOVIE_DUMMY;

CREATE TABLE ACTOR_JSON(
	ALLDATA JSON,
    MOVIE_ID INT);
	
CREATE TABLE CREW_JSON (
	ALLDATA JSON,
    MOVIE_ID INT);

CREATE TABLE KEYWORD_JSON (
	ALLDATA JSON, 
	MOVIE_ID INT);

CREATE TABLE RATING_DUMMY (
	USER_ID INT, 
	MOVIE_ID INT,
	RATING FLOAT,
	RATING_TIME INT);	

copy ACTOR_JSON(ALLDATA, MOVIE_ID) FROM 'ImportData/ACTORrevised.csv' DELIMITER ',' CSV HEADER;	
copy CREW_JSON(ALLDATA, MOVIE_ID) FROM 'ImportData/CREWrevised.csv' DELIMITER ',' CSV HEADER;	
copy KEYWORD_JSON(MOVIE_ID, ALLDATA) FROM 'ImportData/KEYWORDrevised.csv' DELIMITER ',' CSV HEADER;	
copy RATING_DUMMY(USER_ID, MOVIE_ID, RATING, RATING_TIME) FROM 'ImportData/RATINGsample.csv' DELIMITER ',' CSV HEADER;	

------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------Database Tables -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

-- ADD CREATE TABLES
DROP TABLE IF EXISTS PRODUCTION_COUNTRY;
DROP TABLE IF EXISTS BELONGS_TO_GENRE;
DROP TABLE IF EXISTS BELONGS_TO_COLLECTION;
DROP TABLE IF EXISTS CONTAINS_KEYWORD;
DROP TABLE IF EXISTS RATING;
DROP TABLE IF EXISTS PRODUCTION_TEAM;
DROP TABLE IF EXISTS CAST_IN_MOVIE;

DROP TABLE IF EXISTS MOVIE;
DROP TABLE IF EXISTS ACTOR;
DROP TABLE IF EXISTS CREW_MEMBER;
DROP TABLE IF EXISTS COUNTRY;
DROP TABLE IF EXISTS REVIEWER;
DROP TABLE IF EXISTS KEYWORD;
DROP TABLE IF EXISTS GENRE;
DROP TABLE IF EXISTS COLLECTION;
DROP TABLE IF EXISTS KEYWORD;

-- CREATE MAIN TABLES 
create table movie(
movie_id integer primary key,
original_title text,
popularity numeric,
budget integer,
revenue integer,
runtime numeric,
status varchar(10),
tagline text,
overview text,
vote_average numeric,
vote_count integer,
production_companies json,
original_language varchar(2),
release_date DATE
);

CREATE TABLE actor(
actor_id INTEGER,
gender VARCHAR (10),
name VARCHAR,
CONSTRAINT actor_pk PRIMARY KEY(actor_id)
);

CREATE TABLE crew_member(
crew_id INTEGER,
gender VARCHAR (10),
name VARCHAR,
CONSTRAINT crew_member_pk PRIMARY KEY(crew_id)
);

CREATE TABLE country (
iso VARCHAR (2) primary key,
name VARCHAR (30)
);

CREATE TABLE reviewer(
user_id VARCHAR (20),
CONSTRAINT reviewer_pk PRIMARY KEY(user_id)
);

CREATE TABLE keyword(
keyword VARCHAR,
keyword_id INTEGER,
CONSTRAINT keyword_pk PRIMARY KEY(keyword_id)
);

create table genre(
gid integer primary key,
name varchar(20)
);

create table collection(
cid integer primary key,
name varchar(50)
);

-- CREATE RELATION TABLES
CREATE TABLE cast_in_movie(
actor_id INTEGER,
movie_id INTEGER,
character VARCHAR,
credit_order INTEGER,
CONSTRAINT cast_mv FOREIGN KEY(movie_id) REFERENCES movie(movie_id),
CONSTRAINT cast_actor FOREIGN KEY(actor_id) REFERENCES actor(actor_id)
);

CREATE TABLE production_team(
crew_id INTEGER,
	
movie_id INTEGER,
job VARCHAR,
department VARCHAR,
CONSTRAINT production_team_mv FOREIGN KEY(movie_id) REFERENCES movie(movie_id),
CONSTRAINT production_team_crew FOREIGN KEY(crew_id) REFERENCES crew_member(crew_id)
);

CREATE TABLE contains_keyword(
keyword_id INTEGER,
movie_id INTEGER,
CONSTRAINT contains_keyword_mv FOREIGN KEY(movie_id) REFERENCES movie(movie_id),
CONSTRAINT contains_keyword_kwywd FOREIGN KEY(keyword_id) REFERENCES keyword(keyword_id)
);


create table production_country(
movie_id integer references movie(movie_id),
iso varchar(3) references country(iso)
);

CREATE TABLE rating(
user_id VARCHAR (20),	
movie_id INTEGER,
rating_score FLOAT,
rating_time INTEGER,
CONSTRAINT rating_mv FOREIGN KEY(movie_id) REFERENCES movie(movie_id),
CONSTRAINT rating_rvw FOREIGN KEY(user_id) REFERENCES reviewer(user_id)
);

create table belongs_to_genre(
movie_id integer references movie(movie_id),
gid integer references genre(gid)
);

create table belongs_to_collection(
movie_id integer references movie(movie_id),
cid integer references collection(cid)
);

------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------- Insert Data  -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

-- INSERT STATEMENTS 

-- COPY TO MOVIE
COPY movie FROM 'ImportData/movie.csv' CSV QUOTE '"' ESCAPE '''';

-- Insert into ACTOR
INSERT INTO ACTOR(ACTOR_ID, NAME, GENDER)
SELECT DISTINCT CAST(sub2.ACTOR_ID as INTEGER), sub2.NAME, sub2.GENDER  FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as ACTOR_ID, 
sub.INDIVIDUALS::json ->> 'gender' as GENDER, 
sub.INDIVIDUALS::json ->> 'name' as NAME
FROM
(SELECT json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  ACTOR_JSON) sub) sub2; 
				 
-- Insert into CAST
INSERT INTO CAST_IN_MOVIE(ACTOR_ID, MOVIE_ID, CHARACTER, CREDIT_ORDER)
SELECT DISTINCT CAST(sub2.ACTOR_ID as INTEGER), CAST(sub2.MOVIE_ID as INTEGER), sub2.CHARACTER, CAST(sub2.order as INTEGER)  FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as ACTOR_ID, 
sub.INDIVIDUALS::json ->> 'character' as CHARACTER, 
sub.INDIVIDUALS::json ->> 'order' as ORDER,
MOVIE_ID as MOVIE_ID
FROM
(SELECT MOVIE_ID, json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  ACTOR_JSON) sub) sub2; 

-- INSERT into CREW				 
INSERT INTO CREW_MEMBER(CREW_ID, NAME, GENDER)
SELECT CAST(sub3.CREW_ID as INT), sub3.NAME, sub3.GENDER FROM
(SELECT DISTINCT sub2.CREW_ID, sub2.NAME, sub2.GENDER  FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as CREW_ID, 
sub.INDIVIDUALS::json ->> 'gender' as GENDER, 
sub.INDIVIDUALS::json ->> 'name' as NAME
FROM
(SELECT json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  CREW_JSON) sub) sub2) sub3
WHERE sub3.NAME != 'None '; 

-- Insert into PRODUCTION_TEAM
INSERT INTO PRODUCTION_TEAM(CREW_ID, MOVIE_ID, JOB, DEPARTMENT)
SELECT DISTINCT CAST(sub2.CREW_ID as INT), sub2.MOVIE_ID, sub2.JOB, sub2.DEPARTMENT  FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as CREW_ID, 
sub.INDIVIDUALS::json ->> 'job' as JOB, 
sub.INDIVIDUALS::json ->> 'department' as DEPARTMENT,
MOVIE_ID as MOVIE_ID
FROM
(SELECT MOVIE_ID, json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  CREW_JSON) sub) sub2
WHERE CAST(sub2.CREW_ID as INT) IN (SELECT CREW_ID from CREW_MEMBER); 

-- INSERT into KEYWORD			
INSERT INTO KEYWORD(KEYWORD_ID, KEYWORD)
SELECT DISTINCT CAST(sub2.KEYWORD_ID as INTEGER), sub2.KEYWORD FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as KEYWORD_ID, 
sub.INDIVIDUALS::json ->> 'name' as KEYWORD
FROM
(SELECT json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  KEYWORD_JSON) sub) sub2; 
			
-- INSERT into CONTAINS_KEYWORD			
INSERT INTO CONTAINS_KEYWORD(KEYWORD_ID, MOVIE_ID)
SELECT DISTINCT CAST(sub2.KEYWORD_ID as INTEGER), CAST(sub2.MOVIE_ID as INTEGER) FROM
(SELECT sub.INDIVIDUALS::json ->> 'id' as KEYWORD_ID, 
MOVIE_ID as MOVIE_ID
FROM
(SELECT MOVIE_ID, json_array_elements(ALLDATA::json) as INDIVIDUALS
FROM  KEYWORD_JSON) sub) sub2; 
			
-- INSERT into REVIEWER
INSERT INTO REVIEWER(USER_ID)
(SELECT DISTINCT USER_ID FROM RATING_DUMMY); 

--INSERT into RATING
INSERT INTO RATING(USER_ID, MOVIE_ID, RATING_SCORE, RATING_TIME)
(SELECT * FROM RATING_DUMMY rd
WHERE rd.MOVIE_ID IN (SELECT mv.MOVIE_ID from MOVIE mv)); 

-- COPY TO COUNTRY
COPY country FROM 'ImportData/country.csv' CSV QUOTE '"' ESCAPE '''';

-- COPY TO PRODUCTION_COUNTRY
COPY production_country FROM 'ImportData/production_countries.csv' CSV QUOTE '"' ESCAPE '''';

-- COPY TO GENRE
COPY genre FROM 'ImportData/genre.csv' CSV QUOTE '"' ESCAPE '''';

-- COPY TO BELONGS_TO_GENRE
COPY belongs_to_genre FROM 'ImportData/belongs_to_genre.csv' CSV QUOTE '"' ESCAPE '''';

-- COPY TO COLLECTION
COPY collection FROM 'ImportData/collection.csv' CSV QUOTE '"' ESCAPE '''';

-- COPY TO BELONGS_TO_COLLECTION
COPY belongs_to_collection FROM 'ImportData/belongs_to_collection.csv' CSV QUOTE '"' ESCAPE '''';

					  
-- DELETE DUMMY TABLES
-- NO LONGER NEEDED
DROP TABLE IF EXISTS ACTOR_JSON;
DROP TABLE IF EXISTS CREW_JSON;
DROP TABLE IF EXISTS KEYWORD_JSON;
DROP TABLE IF EXISTS RATING_DUMMY;


-- COUNT of ROWS 
SELECT count(*) as MOVIE_COUNT FROM MOVIE;
SELECT count(*) as ACTOR_COUNT FROM ACTOR;
SELECT count(*) as CAST_COUNT FROM CAST_IN_MOVIE;
SELECT count(*) as CREW_COUNT FROM CREW_MEMBER;
SELECT count(*) as PROD_TEAM_COUNT FROM PRODUCTION_TEAM;
SELECT count(*) as REVIEWER_COUNT FROM REVIEWER;
SELECT count(*) as RATING_COUNT FROM RATING;
SELECT count(*) as COUNTRY_COUNT FROM COUNTRY;
SELECT count(*) as PROD_COUNTRY_COUNT FROM PRODUCTION_COUNTRY;
SELECT count(*) as KEYWORD_COUNT FROM KEYWORD;
SELECT count(*) as HAS_KEYWORD_COUNT FROM CONTAINS_KEYWORD;
SELECT count(*) as COLLECTION_COUNT FROM COLLECTION;
SELECT count(*) as TO_COLLECTION_COUNT FROM BELONGS_TO_COLLECTION;
SELECT count(*) as GENRE_COUNT FROM GENRE;
SELECT count(*) as TO_GENRE_COUNT FROM BELONGS_TO_GENRE;


------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------- VIEW -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

--JSON ARRAYS
-- return json objects
select production_companies from movie; 

-- Return the array length
select json_array_length(production_companies) from movie;

-- Returns the elements in the arrays
select json_array_elements(production_companies) from movie; 

-- Returns the production companies 
DROP VIEW IF EXISTS MOVIE_PRODUCTION_COMPANY;
create view MOVIE_PRODUCTION_COMPANY as 
select DISTINCT movie_id, allcomp.company_name, 
allcomp.comp_id from 

(select movie_id, 
    e ->> 'name' as company_name, 
    e ->> 'id' as comp_id from

(select movie_id, json_array_elements(production_companies) as e from movie) as elements
) allcomp; 

------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------Analysis Using View -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Top 5 Production Companies with HIGHEST AVERAGE REVENUE
select UPPER(mpc.company_name) as COMPANY_NAME, CAST(avg(m.revenue) AS INT) avgRevenue from MOVIE_PRODUCTION_COMPANY mpc 
natural join movie m
GROUP BY UPPER(mpc.company_name)
ORDER BY avgRevenue desc
LIMIT 5;

-- Top 5 Production Companies with HIGHEST AVERAGE REVENUE IN US
select UPPER(mpc.company_name) as COMPANY_NAME, CAST(avg(m.revenue) AS INT) avgRevenue from MOVIE_PRODUCTION_COMPANY mpc 
natural join movie m
natural join production_country pc
where pc.iso = 'US'										 
GROUP BY UPPER(mpc.company_name)
ORDER BY avgRevenue desc
LIMIT 5;

-- Top 5 Production Companies with HIGHEST AVERAGE REVENUE w/ top rated movie 
select UPPER(mpc.company_name) as production_company, m.original_title as top_rated_movie, m.vote_avg as top_score from MOVIE_PRODUCTION_COMPANY mpc 
natural join movie m
where UPPER(mpc.company_name) in 
( -- Match Top 5 High Revenue Production Companies
select UPPER(mpc_sub.company_name) from MOVIE_PRODUCTION_COMPANY mpc_sub 
natural join movie m_sub
GROUP BY UPPER(mpc_sub.company_name)
ORDER BY CAST(avg(m_sub.revenue) AS INT) desc
LIMIT 5) and
m.vote_avg =
( -- Match top rating 
select max(m_sub2.vote_avg) from MOVIE_PRODUCTION_COMPANY mpc_sub2
natural join movie m_sub2
where UPPER(mpc_sub2.company_name)=UPPER(mpc.company_name))
ORDER BY m.vote_avg desc;
										 
-- Top 5 Production Companies with HIGHEST AVERAGE REVENUE w/ highest grossing movie 
select UPPER(mpc.company_name) as production_company, m.original_title as highest_grossing_movie, m.revenue as highest_rev from MOVIE_PRODUCTION_COMPANY mpc 
natural join movie m
where UPPER(mpc.company_name) in 
( -- Match Top 5 High Revenue Production Companies
select UPPER(mpc_sub.company_name) from MOVIE_PRODUCTION_COMPANY mpc_sub 
natural join movie m_sub
GROUP BY UPPER(mpc_sub.company_name)
ORDER BY CAST(avg(m_sub.revenue) AS INT) desc
LIMIT 5) and
m.revenue =
( -- Match highest revenue
select max(m_sub2.revenue) from MOVIE_PRODUCTION_COMPANY mpc_sub2
natural join movie m_sub2
where UPPER(mpc_sub2.company_name)=UPPER(mpc.company_name))
ORDER BY m.revenue desc;



------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------Trigger -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

--TRIGGER
create or replace function updateMovie()
returns trigger as
$$
declare
	newcount integer;
	newavg numeric;
begin
	select vote_count+1, (vote_count*vote_average*1.0 + new.rating_score*2)/(vote_count+1)*1.0 into newcount, newavg
    from movie
    where movie_id = new.movie_id;
	
	update movie
	set vote_average = round(newavg,1), vote_count = newcount
	where movie_id = new.movie_id;
	
    raise notice 'new.movie_id:%, newcnt:%, newavg:%', new.movie_id, newcount, newavg;
    return new;
end;
$$
language plpgsql;

create trigger update_movie
after insert on rating
for each row
execute procedure updateMovie();


drop trigger update_movie;
drop function updateMovie;



VOTE_COUNT_AVG
CREATE OR REPLACE FUNCTION vote_avg(rating_score NUMERIC, movie_id INTEGER) RETURNS NUMERIC AS
$$
DECLARE
voter_average NUMERIC;
rating_score NUMERIC;
movie_id INTEGER;
BEGIN
UPDATE movie
SET voter_average = (
	SELECT AVG (rating.rating_score)
	FROM rating
	WHERE movie.movie_id = rating.movie_id
	GROUP BY rating.movie_id
	);
$$
language plpgsql;

SELECT *
FROM rating 

CREATE OR REPLACE FUNCTION vote_count() RETURNS INTEGER AS 
$$
DECLARE
voter_count NUMERIC;
rating_score NUMERIC;
movie_id INTEGER;
BEGIN
UPDATE movie
SET voter_count = (
	SELECT COUNT (rating.rating_score)
	FROM rating
	GROUP BY rating.movie_id);
END;
$$
language plpgsql;


------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------WEIGHTED AVERAGE RECOMMENDER -----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------

--WEIGHTED AVERAGE RECOMMENDER
/*
Weighted Rating = (v/(v+m)*R) + (m/(v+m)*C)
v - number of votes for the movie(vote_count)
m - minimum votes required to be listed in the chart
R - average rating of the movie(vote_average)
C - mean vote across the whole data
*/
drop function if exists WR_Recommender;
create function WR_Recommender 
(title character varying, min_vote integer, op character varying)
returns table(movie_title text, WeightedRating numeric) as
$$
declare
	C numeric;
	m integer;
begin
	select avg(vote_average) into C from movie;
	-- raise notice 'C: %', C;
		
	drop table if exists wrating;
	create table wrating as
	select movie_id, vote_count as v, vote_average as R, 
		round((vote_count*1.0/(vote_count+min_vote)*vote_average) + (min_vote*1.0/(vote_count+min_vote)*C), 4) as WR
	from movie
	where vote_count > min_vote;
	
	if (op = 'genre') then
		return query 
		select distinct original_title, wr
		from wrating natural join movie natural join belongs_to_genre natural join genre
		where name in (select name
					  from movie natural join belongs_to_genre natural join genre
					  where original_title = title) and original_title != title
		order by wr desc;
		
	elseif (op = 'collection') then
		return query
		select original_title, wr
		from wrating natural join movie natural join belongs_to_collection natural join collection
		where name = (select name
					 from movie natural join belongs_to_collection natural join collection
					 where original_title = title) and original_title != title
		order by wr desc;
		
	elseif (op = 'keyword') then
		return query
		select distinct original_title, wr
		from wrating natural join movie natural join contains_keyword natural join keyword
		where keyword in (select keyword
					   from movie natural join contains_keyword natural join keyword
					   where original_title = title) and original_title != title
		order by wr desc;

		
	elseif (op = 'cast') then
		return query
		select distinct original_title, wr
		from wrating natural join movie natural join cast_in_movie natural join actor
		where name in (select name
					   from movie natural join cast_in_movie natural join actor
					   where original_title = title and credit_order < 10) and original_title != title
		order by wr desc;
		
	elseif (op = 'crew') then
		return query
        select distinct original_title, wr
        from wrating natural join movie natural join production_team natural join crew_member
        where (job = 'Director' or job = 'Writer' or job like '%Producer') and name in (select name
                    from movie natural join production_team natural join crew_member
                    where original_title = title) and original_title != title
        order by wr desc;
	end if;
	
	raise notice 'Invalid parameter';
	return;
end;
$$
language plpgsql;

select * from WR_Recommender('Léon', 1800, 'genre') limit 10;

select * from WR_Recommender('Harry Potter and the Half-Blood Prince', 1800, 'collection');

select * from WR_Recommender('Batman', 1800, 'keyword');

select * from WR_Recommender('Star Wars', 1800, 'cast');

select * from WR_Recommender('Toy Story', 1800, 'crew');

drop table wrating;




--JACCARD SCORE RECOMMENDER

------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------JACCARD SCORE RECOMMENDER ---------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Composed of the 3 following functions: 


------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------- Jaccard Score Function ---------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Get the Jaccard Similarity Score between 2 movies
-- Similarity features looked at: 
-- KEYWORDS, GENRE, DIRECTORS, CAST
DROP FUNCTION IF EXISTS JaccardScore CASCADE;
CREATE OR REPLACE FUNCTION
JaccardScore(
	movieID INT,
    movieID2 INT)
RETURNS NUMERIC AS
$$
DECLARE
keywordUnion INT DEFAULT 0; 
keywordINT INT DEFAULT 0; 
genreUnion INT DEFAULT 0; 
genreINT INT DEFAULT 0;
dirUnion INT DEFAULT 0; 
dirINT INT DEFAULT 0;
castUnion INT DEFAULT 0; 
castINT INT DEFAULT 0;
query TEXT; 
Jindex NUMERIC(10,2) DEFAULT 0.00;
unionNum INT DEFAULT 0; 
intNUM INT DEFAULT 0; 
BEGIN

-------------------------------------------
-------------- KEYWORD ------------------
-------------------------------------------
-- query for keyword Union count 
query = 'select count(DISTINCT ck.keyword_id) from contains_keyword ck 
where ck.movie_id in (' || COALESCE(QUOTE_LITERAL(movieID) || ',' || QUOTE_LITERAL(movieID2)) || 
');';

																				 
-- run the query 
EXECUTE query INTO keywordUnion;

-------------------------------------------											  
-- query for keyword intersect count
query = 'select count(DISTINCT ck.keyword_id) from contains_keyword ck 
where ck.movie_id=' || COALESCE(QUOTE_LITERAL(movieID2)) || 
' and ck.keyword_id IN 
(select ck2.keyword_id from contains_keyword ck2 
where ck2.movie_id= ' || COALESCE(QUOTE_LITERAL(movieID))|| ');';

-- run the query 
EXECUTE query INTO keywordINT;	
											  
-------------------------------------------
-------------- GENRE ------------------
-------------------------------------------
-- query for genre  Union count
query = 'select count(DISTINCT gk.gid) from belongs_to_genre gk 
where gk.movie_id IN (' || COALESCE(QUOTE_LITERAL(movieID) || ',' || QUOTE_LITERAL(movieID2)) || 
');';

-- run the query 
EXECUTE query INTO genreUnion;

-------------------------------------------											  
-- query for genre intersect count
query = 'select count(DISTINCT gk.gid) from belongs_to_genre gk 
where gk.movie_id=' || COALESCE(QUOTE_LITERAL(movieID2)) || 
' and gk.gid IN 
(select gk2.gid from belongs_to_genre gk2 
where gk2.movie_id= ' || COALESCE(QUOTE_LITERAL(movieID))|| ');';

-- run the query 
EXECUTE query INTO genreINT;	
																  
-------------------------------------------
-------------- Director ------------------
-------------------------------------------
-- query for director Union count
query = 'select count( DISTINCT pt.crew_id) from production_team pt 
where pt.job=' || QUOTE_LITERAL('Director') || 'and pt.movie_id IN (' || 
COALESCE(QUOTE_LITERAL(movieID) || ',' || QUOTE_LITERAL(movieID2)) || 
');';

-- run the query 
EXECUTE query INTO dirUnion;

-------------------------------------------											  
-- query for director intersect count
query = 'select count(DISTINCT pt.crew_id) from production_team pt 
where pt.job=' || QUOTE_LITERAL('Director') || 'and pt.movie_id=' || COALESCE(QUOTE_LITERAL(movieID2)) || 
' and pt.crew_id IN  
(select pt2.crew_id from production_team pt2 
where pt2.job=' || QUOTE_LITERAL('Director') || 'and pt2.movie_id= ' || COALESCE(QUOTE_LITERAL(movieID))|| ');';

-- run the query 
EXECUTE query INTO dirINT;																	 
											  
-------------------------------------------
-------------- CAST ------------------
-------------------------------------------
-- query for cast Union count
query = 'select count(DISTINCT cim.actor_id) from cast_in_movie cim 
where cim.movie_id IN (' || COALESCE(QUOTE_LITERAL(movieID) || ',' || QUOTE_LITERAL(movieID2)) || 
');';

-- run the query 
EXECUTE query INTO castUnion;

-------------------------------------------											  
-- query for cast intersect count
query = 'select count(DISTINCT cim.actor_id) from cast_in_movie cim 
where cim.movie_id=' || COALESCE(QUOTE_LITERAL(movieID2)) || 
'and cim.actor_id IN 
(select cim2.actor_id from cast_in_movie cim2 
where cim2.movie_id=' || COALESCE(QUOTE_LITERAL(movieID))|| ')';

-- run the query 
EXECUTE query INTO castINT;	

-------------------------------------------
-------------- JACCARD Calculation---------
-------------------------------------------
unionNum = keywordUnion + genreUnion + dirUnion + castUnion;
intNum = keywordInt + genreInt + dirInt + castInt;
IF unionNUM > 0 THEN 
Jindex = ROUND(CAST(intNum AS NUMERIC) / CAST(unionNum AS NUMERIC), 2); 											 
END IF; 											 
RETURN Jindex;
END;
$$
LANGUAGE plpgsql;


------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------- Jaccard Table Function ---------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Get a table of Jaccard Similarity Scores for ALL movies (except the specified movie)
DROP FUNCTION IF EXISTS JaccardTable CASCADE;
CREATE OR REPLACE FUNCTION
JaccardTable(
	movieID INT)
RETURNS TABLE -- return movie id, movie title, jaccard score
(movieID2 INT,
 movieTitle TEXT,
 Jindex NUMERIC(10,2)) AS
$$
DECLARE
row_data RECORD;
BEGIN
-- Get Jaccard Score for all movies except the specified movie. 
FOR row_data IN (select m.* from movie m where m.movie_id != movieID) 
LOOP
	movieID2 := row_data.movie_id; 
    movieTitle := row_data.original_title; 
	IF (select cid from belongs_to_collection where movie_id = row_data.movie_id ORDER BY cid) = 
	   (select cid from belongs_to_collection where movie_id = movieID ORDER BY cid) THEN 
    	Jindex := 1.00; -- IF MOVIE IN SAME COLLECTION, JINDEX = 1
	ELSE		
    	Jindex := JaccardScore(movieID, row_data.movie_id);
	END IF; 
RETURN NEXT;
END LOOP; 
END;
$$
LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------- JacRecommendation Function ---------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------
-- JacRecommend the 10 movies with the highest Jaccard Similarity Score

DROP FUNCTION IF EXISTS JacRecommend CASCADE;
CREATE OR REPLACE FUNCTION
JacRecommend(
	movieID INT,
    title VARCHAR)
RETURNS TABLE
(movie TEXT) AS
$$
DECLARE
BEGIN -- get the 10 movies with the highest jaccard score
IF (movieID) != (select m.movie_id from movie m where original_title = title) THEN
	RETURN QUERY select * from QUOTE_LITERAL(-1);
ELSE
	RETURN QUERY select jt.movieTitle as JacRecommended_title from JaccardTable(movieID) jt order by Jindex desc limit 10;
END IF;
END;
$$
LANGUAGE plpgsql;
				
--------------------------------------------------------------------------- Testing -----------------------------------------------------------------------
-- Example movie used is: Toy Store, movie_id = 862

-- Test the JaccardScore Function. Get jaccard score for movies: Toy Story and Toy Story 2			
select * from JaccardScore(862, 863);	

-- Test the JaccardTable Function. It works, but takes a while. 			
select * from JaccardTable(862);	

-- Test the JacRecommender Function. It works, but takes a while. 			
select * from JacRecommend(862, 'Toy Story');	

-- GET EXECUTION TIME.
EXPLAIN ANALYZE
select * from JacRecommend(862, 'Toy Story');	
