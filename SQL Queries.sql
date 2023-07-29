USE Terrorism;

SELECT * FROM global_terrorism_initial;

SELECT *
INTO global_terrorism
FROM global_terrorism_initial;

SELECT * FROM global_terrorism_initial;

-- create dimension date table 
CREATE TABLE dim_dates
	(date date,
	year int,
	month tinyint,
	day tinyint,
	weekday_num tinyint,
	weekday_name varchar(15));

INSERT INTO dim_dates(date)
SELECT TOP 18992
	DATEADD(DAY,ROW_NUMBER() OVER(ORDER BY (SELECT NULL)), '1970-01-01')
FROM sys.objects AS a CROSS JOIN sys.objects AS b CROSS JOIN sys.objects AS c;

UPDATE dim_dates
set	year		= YEAR(date),
	month		= MONTH(date),
	day		= DAY(date),
	weekday_num	= DATEPART(WEEKDAY, date),
	weekday_name	= DATEname(WEEKDAY, date);

select * from dim_dates;

-- create date column in global_terrorism table
ALTER TABLE global_terrorism
ADD date date;

UPDATE global_terrorism 
SET month = 1
where month = 0;

UPDATE global_terrorism 
SET day = 1
where day = 0;

UPDATE global_terrorism
set date = DATEFROMPARTS(year,month,day);

-- drop duplicate rows
WITH cte AS 
	(SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY 
			date,
			country_id,
			region_id,
			state,
			city,
			latitude,
			longitude,
			summary,
			attack_type_id,
			attack_type2_id,
			attack_type3_id,
			target_type_id,
			target_subtype_id,
			group_name,
			weap_type1_id,
			weap_subtype1_id,
			weap_type2_id,
			weap_subtype2_id,
			weap_detail,
			n_kills,
			n_wounded,
			prop_dam_extent_id,
			prop_value,
			hostkid_outcome_id
					ORDER BY date) AS rn,
			1 as one
	FROM global_terrorism)
DELETE FROM cte
WHERE rn > 1;

SELECT * FROM global_terrorism;

-- drop rows without sufficient location data
UPDATE global_terrorism
SET state = NULL
WHERE state = 'Unknown';

UPDATE global_terrorism
SET city = NULL
WHERE city = 'Unknown';

DELETE FROM global_terrorism
WHERE 
	country_id IS NULL OR
	region_id IS NULL;

-- drop columns uneeded from data exploration or vague and unclear
ALTER TABLE global_terrorism
DROP COLUMN 
	country2_id, 
	country2, 
	hostkid,
	alternative_id,
	alternative

-- change negative property damage to zero
UPDATE global_terrorism
SET prop_value = 0
WHERE prop_value < 0;

-- create primary key column ordered by date
ALTER TABLE global_terrorism
ADD incident_id INT;

ALTER TABLE global_terrorism
ADD beans_id INT IDENTITY PRIMARY KEY;

WITH cte AS  
	(SELECT
		*,
		ROW_NUMBER() OVER(ORDER BY date) AS rn
	FROM global_terrorism)
UPDATE global_terrorism
SET incident_id = rn
FROM cte
WHERE global_terrorism.beans_id = cte.beans_id;

ALTER TABLE global_terrorism
DROP CONSTRAINT PK__global_t__251C78B302A83D14;
ALTER TABLE global_terrorism
DROP COLUMN beans_id;

ALTER TABLE global_terrorism
ALTER COLUMN incident_id INT NOT NULL;

ALTER TABLE global_terrorism
ADD PRIMARY KEY (incident_id);

-- create dimension country table
SELECT 
	DISTINCT country_id,
	country
INTO dim_countries
FROM global_terrorism;

ALTER TABLE dim_countries
ALTER COLUMN country_id INT NOT NULL;

ALTER TABLE dim_countries
ADD PRIMARY KEY (country_id);

SELECT * FROM dim_countries
ORDER BY country_id;

-- create rank by number of terror attacks column in country table
ALTER TABLE dim_countries
ADD rank_terror_attacks int;

WITH cte AS 
	(SELECT
		a.country_id,
		COUNT(incident_id) AS n_terror_attacks,
		RANK() OVER(ORDER BY COUNT(incident_id)DESC) AS rank_terror_attacks
	FROM dim_countries a RIGHT JOIN fact_global_terrorism b on a.country_id = b.country_id
	GROUP BY 
		a.country_id)
UPDATE dim_countries
SET dim_countries.rank_terror_attacks = cte.rank_terror_attacks
FROM cte 
WHERE cte.country_id = dim_countries.country_id;

SELECT * FROM dim_countries
ORDER BY rank_terror_attacks;

-- create dimension region table 
SELECT 
	DISTINCT region_id,
	region
INTO dim_regions
FROM global_terrorism;

ALTER TABLE dim_regions
ALTER COLUMN region_id INT NOT NULL;

ALTER TABLE dim_regions
ADD PRIMARY KEY (region_id);

SELECT * FROM dim_regions
ORDER BY region_id;

-- create dimension state table 
DROP TABLE IF EXISTS dim_states;
SELECT 
	DISTINCT state,
	RANK() OVER(ORDER BY state) AS state_id
INTO dim_states
FROM global_terrorism
WHERE state IS NOT NULL;

ALTER TABLE dim_states
ALTER COLUMN state_id INT NOT NULL;

ALTER TABLE dim_states
ADD PRIMARY KEY (state_id);

SELECT * FROM dim_states
ORDER BY state_id;

-- create state_id column in global_terrorism table
ALTER TABLE global_terrorism
ADD state_id int;

WITH cte AS  
	(SELECT *
	FROM dim_states)
UPDATE global_terrorism
SET state_id = cte.state_id
FROM cte
WHERE global_terrorism.state = cte.state;

-- create dimension city table
DROP TABLE IF EXISTS dim_cities;
SELECT 
	DISTINCT city,
	RANK() OVER(ORDER BY city) AS city_id
INTO dim_cities
FROM global_terrorism
WHERE city IS NOT NULL;

ALTER TABLE dim_cities
ALTER COLUMN city_id INT NOT NULL;

ALTER TABLE dim_cities
ADD PRIMARY KEY (city_id);

SELECT * FROM dim_cities
ORDER BY city_id;

-- create city_id column in global_terrorism table
ALTER TABLE global_terrorism
ADD city_id INT;

WITH cte AS  
	(SELECT *
	FROM dim_cities)
UPDATE global_terrorism
SET city_id = cte.city_id
FROM cte
WHERE global_terrorism.city = cte.city;

-- create dimension attack type 
WITH cte_attack_type AS 
		(SELECT
			DISTINCT attack_type_id,
			attack_type
		FROM global_terrorism
	UNION
		SELECT
			DISTINCT attack_type2_id AS attack_type_id,
			attack_type2 AS attack_type
		from global_terrorism
	UNION
		SELECT
			DISTINCT attack_type3_id AS attack_type_id,
			attack_type3 AS attack_type
		FROM global_terrorism)
SELECT * 
INTO dim_attack_types
FROM cte_attack_type
WHERE attack_type_id IS NOT NULL;

ALTER TABLE dim_attack_types
ALTER COLUMN attack_type_id INT NOT NULL;

ALTER TABLE dim_attack_types
ADD PRIMARY KEY (attack_type_id);

SELECT * FROM dim_attack_types
ORDER BY attack_type_id;

-- create dimension target type table 
SELECT 
	DISTINCT target_type_id,
	target_type
INTO dim_target_type
FROM global_terrorism;

ALTER TABLE dim_target_type
ALTER COLUMN target_type_id INT NOT NULL;

ALTER TABLE dim_target_type
ADD PRIMARY KEY (target_type_id);

SELECT * FROM dim_target_type
ORDER BY target_type_id;

-- create dimension target subtype table 
SELECT 
	DISTINCT target_subtype_id,
	target_subtype
INTO dim_target_subtype
FROM global_terrorism
WHERE target_subtype_id IS NOT NULL;

ALTER TABLE dim_target_subtype
ALTER COLUMN target_subtype_id INT NOT NULL;

ALTER TABLE dim_target_subtype
ADD PRIMARY KEY (target_subtype_id);

SELECT * FROM dim_target_subtype
ORDER BY target_subtype_id;

-- create dimension weapon type table
WITH cte_weapon_type as 
		(SELECT
			DISTINCT weap_type1_id AS weap_type_id,
			weap_type1 AS weap_type
		FROM global_terrorism
	UNION
		SELECT
			DISTINCT weap_type2_id AS weap_type_id,
			weap_type2 AS weap_type
		FROM global_terrorism)
SELECT *
INTO dim_weapon_type
FROM cte_weapon_type
WHERE weap_type_id IS NOT NULL;

ALTER TABLE dim_weapon_type
ALTER COLUMN weap_type_id INT NOT NULL;

ALTER TABLE dim_weapon_type
ADD PRIMARY KEY (weap_type_id);

SELECT * FROM dim_weapon_type
ORDER BY weap_type_id;

-- create dimension weapon subtype table
WITH cte_weapon_subtype as 
		(SELECT
			DISTINCT weap_subtype1_id AS weap_subtype_id,
			weap_subtype1 AS weap_subtype
		FROM global_terrorism
	UNION
		SELECT
			DISTINCT weap_subtype2_id AS weap_subtype_id,
			weap_subtype2 AS weap_subtype
		FROM global_terrorism)
SELECT *
INTO dim_weapon_subtype
FROM cte_weapon_subtype
WHERE weap_subtype_id IS NOT NULL;

ALTER TABLE dim_weapon_subtype
ALTER COLUMN weap_subtype_id INT NOT NULL;

ALTER TABLE dim_weapon_subtype
ADD PRIMARY KEY (weap_subtype_id);

SELECT * FROM dim_weapon_subtype
ORDER BY weap_subtype_id;

-- create dimension property damage table
UPDATE global_terrorism
SET prop_dam_extent_id = 3
WHERE prop_dam_extent = 'Minor (likely < $1 million)';

SELECT 
	DISTINCT prop_dam_extent_id,
	prop_dam_extent
INTO dim_prop_dam_extent
FROM  global_terrorism
WHERE prop_dam_extent_id IS NOT NULL;


ALTER TABLE dim_prop_dam_extent
ALTER COLUMN prop_dam_extent_id INT NOT NULL;

ALTER TABLE dim_prop_dam_extent
ADD PRIMARY KEY (prop_dam_extent_id);

SELECT * FROM dim_prop_dam_extent
ORDER BY prop_dam_extent_id;

-- create dimension host/kidnapping outcome table
SELECT 
	DISTINCT hostkid_outcome_id,
	hostkid_outcome
INTO dim_hostkid_outcome
FROM global_terrorism
WHERE hostkid_outcome_id IS NOT NULL;

ALTER TABLE dim_hostkid_outcome
ALTER COLUMN hostkid_outcome_id INT NOT NULL;

ALTER TABLE dim_hostkid_outcome
ADD PRIMARY KEY (hostkid_outcome_id);

SELECT * FROM dim_hostkid_outcome
ORDER BY hostkid_outcome_id;

-- create dimension group names table
DROP TABLE IF EXISTS dim_group_names;
SELECT	
	ROW_NUMBER() OVER(ORDER BY COUNT(incident_id)DESC) AS group_name_id,
	group_name,
	COUNT(incident_id) as n_terror_attacks,
	SUM(n_kills) as n_deaths,
	ROW_NUMBER() OVER(ORDER BY COUNT(incident_id)DESC) AS rank_n_terror_attacks,
	ROW_NUMBER() OVER(ORDER BY SUM(n_kills)DESC) AS rank_n_deaths
INTO dim_group_names
FROM global_terrorism
GROUP BY group_name;

ALTER TABLE dim_group_names
ALTER COLUMN group_name_id INT NOT NULL;

ALTER TABLE dim_group_names
ADD PRIMARY KEY (group_name_id);

SELECT * FROM dim_group_names;

-- add group name id column to global terrorism table 
ALTER TABLE global_terrorism
ADD group_name_id INT;

WITH cte AS  
	(SELECT *
	FROM dim_group_names)
UPDATE global_terrorism
SET global_terrorism.group_name_id = cte.group_name_id
FROM cte
WHERE global_terrorism.group_name = cte.group_name;

SELECT * FROM global_terrorism;

-- adding prevalent country column to group names table
ALTER TABLE dim_group_names
ADD prevalent_country VARCHAR(100);

WITH cte AS 
	(SELECT
		b.group_name_id,
		b.group_name,
		c.country as prevalent_country,
		COUNT(incident_id) AS n_terror_attacks,
		ROW_NUMBER() OVER(PARTITION BY b.group_name ORDER BY COUNT(incident_id)DESC) AS rn
	FROM
		global_terrorism a JOIN dim_group_names b ON a.group_name = b.group_name JOIN
		dim_countries c ON a.country_id = c.country_id
	WHERE b.group_name <> 'Unknown'
	GROUP BY
		b.group_name_id,
		b.group_name,
		c.country)
UPDATE dim_group_names
SET dim_group_names.prevalent_country = cte.prevalent_country
FROM CTE
WHERE 
	dim_group_names.group_name_id = cte.group_name_id AND
	rn = 1;

SELECT * FROM dim_group_names;

-- group names validation
SELECT
	DISTINCT a.group_name,
	group_name_id
FROM global_terrorism a JOIN dim_group_names b on a.group_name=b.group_name
ORDER BY group_name_id ASC;

SELECT
	group_name_id,
	group_name,
	count(incident_id),
	sum(n_kills)
FROM global_terrorism
GROUP BY
	group_name_id,
	group_name
ORDER BY 1 ASC;

-- create fact global terrorism table
DROP TABLE IF EXISTS fact_global_terrorism;
SELECT 
	incident_id,
	date,
	country_id,
	region_id,
	state_id,
	city_id,
	latitude,
	longitude,
	location_description,
	summary,
	success,
	suicide,
	attack_type_id,
	attack_type2_id,
	attack_type3_id,
	target_type_id,
	target_subtype_id,
	group_name,
	motive,
	guncertain1 as guncertain,
	individual,
	n_perps,
	n_perps_captured,
	weap_type1_id,
	weap_subtype1_id,
	weap_type2_id,
	weap_subtype2_id,
	weap_detail,
	n_kills,
	n_wounded,
	prop_dam,
	prop_dam_extent_id,
	prop_value,
	prop_dam_desc,
	hostkid_outcome_id
INTO fact_global_terrorism
FROM global_terrorism;

SELECT * FROM fact_global_terrorism
ORDER BY incident_id;

SELECT * FROM global_terrorism;
SELECT * FROM fact_global_terrorism;

-- merge attack type columns
UPDATE global_terrorism
SET attack_type2_id = NULL
WHERE attack_type2_id = attack_type_id;

UPDATE global_terrorism
SET attack_type3_id = NULL
WHERE 
	attack_type3_id = attack_type2_id OR
	attack_type3_id = attack_type_id;

DROP TABLE IF EXISTS fact_all_attack_types;
SELECT *
INTO fact_all_attack_types
FROM 
	(SELECT 
		country_id,
		city_id,
		incident_id,
		attack_type_id
	FROM global_terrorism
			UNION ALL
	SELECT 
		country_id,
		city_id,
		incident_id,
		attack_type2_id AS attack_type_id
	FROM global_terrorism
	WHERE attack_type2_id IS NOT NULL
			UNION ALL
	SELECT 
		country_id, 
		city_id,
		incident_id,
		attack_type3_id AS attack_type_id
	FROM global_terrorism
	WHERE attack_type3_id IS NOT NULL) o;

SELECT
	(SELECT COUNT(attack_type_id)
	FROM global_terrorism
	WHERE attack_type_id IS NOT NULL)+
	(SELECT COUNT(attack_type2_id)
	FROM global_terrorism
	WHERE attack_type2_id IS NOT NULL)+
	(SELECT COUNT(attack_type3_id)
	FROM global_terrorism
	WHERE attack_type3_id IS NOT NULL)
	
SELECT * FROM fact_all_attack_types;

-- merge weapon type columns
DROP TABLE IF EXISTS fact_all_weap_types;
SELECT *
INTO fact_all_weap_types
FROM 
	(SELECT 
		city_id,
		weap_type1_id AS weap_type_id,
		weap_subtype1_id AS weap_subtype_id,
		weap_detail
	FROM global_terrorism
			UNION ALL
	SELECT 
		city_id,
		weap_type2_id AS weap_type_id,
		weap_subtype2_id AS weap_subtype_id,
		weap_detail
	FROM global_terrorism
	WHERE attack_type2_id IS NOT NULL) o;

SELECT * FROM fact_all_weap_types;

-- create dealiest terror attacks by country table
DROP TABLE IF EXISTS fact_deadliest_terror_attacks;
WITH cte AS (
	SELECT
		b.country_id as country_id,
		country,
		date,
		city,
		CASE WHEN summary IS NULL THEN 'No Summary Available' ELSE summary END AS summary,
		attack_type,
		group_name,
		n_kills,
		row_number() over(partition by country order by n_kills desc) AS rank
	FROM 
		fact_global_terrorism a JOIN 
		dim_countries b ON a.country_id=b.country_id JOIN 
		dim_cities c ON a.city_id=c.city_id JOIN
		dim_attack_types d ON a.attack_type_id=d.attack_type_id JOIN
		dim_weapon_type e ON a.weap_type1_id=e.weap_type_id)
SELECT	
	country_id,
	country,
	date,
	city,
	summary,
	attack_type,
	group_name,
	n_kills
INTO fact_deadliest_terror_attacks
FROM cte
WHERE rank = 1;

UPDATE fact_deadliest_terror_attacks
SET date = NULL,
	city = 'N/A',
	summary ='N/A',
	attack_type ='N/A',
	group_name ='N/A',
	n_kills = 0
WHERE n_kills = 0;

UPDATE fact_deadliest_terror_attacks
SET n_kills = 0
WHERE n_kills IS NULL;

SELECT * FROM fact_deadliest_terror_attacks
ORDER BY n_kills DESC;

-- create dealiest terror attacks by group table
DROP TABLE IF EXISTS fact_deadliest_attacks_by_group;
WITH CTE AS (
	SELECT 
		group_name_id,
		incident_id,
		country_id,
		date,
		city_id,
		n_kills,
		summary,
		ROW_NUMBER() OVER(PARTITION BY group_name_id ORDER BY n_kills DESC) AS rank
	FROM global_terrorism
	GROUP BY 
		date,
		group_name_id,
		incident_id,
		country_id,
		city_id,
		n_kills,
		summary
	)
SELECT 
	group_name_id,
	incident_id,
	country_id,
	date,
	city_id,
	n_kills,
	summary
INTO fact_deadliest_attacks_by_group
FROM CTE WHERE rank = 1;

SELECT * FROM fact_deadliest_attacks_by_group
ORDER BY n_kills DESC


-- 
SELECT * FROM global_terrorism;
