"""
    - DROP tables if they exist in the database
"""
staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_immigration_table"
staging_demography_table_drop = "DROP TABLE IF EXISTS staging_demography_table"
immigrants_table_drop = "DROP TABLE IF EXISTS immigrants_table"
population_table_drop = "DROP TABLE IF EXISTS population_table"
flights_table_drop = "DROP TABLE IF EXISTS flights_table"
"""
    - Create staging tables to extract data from S3 to Redshift and other tables for the project
"""
staging_immigration_table_create = """
CREATE TABLE IF NOT EXISTS staging_immigration_table
(
 unnamed bigint,
 cicid float,
 i94yr float,
 i94mon float,
 i94cit float,
 i94res float,
 i94port varchar,
 arrdate float,
 i94mode float,
 i94addr varchar,
 depdate varchar,
 i94bir float,
 i94visa float,
 count float,
 dtadfile bigint,
 visapost varchar,
 occup varchar,
 entdepa varchar,
 entdepd varchar,
 entdepu float,
 matflag varchar,
 biryear float,
 dtaddto varchar,
 gender varchar,
 insnum float,
 airline varchar,
 admnum float,
 fltno varchar,
 visatype varchar 
)
 """


staging_demography_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_demography_table
(
 city varchar,
 state varchar,
 median_age float,
 male_population float8,
 female_population float8,
 total_population bigint,
 number_of_veterans float,
 foreign_born float8,
 avg_household_size float,
 state_code varchar,
 race varchar,
 count bigint
)
 """)

population_table_create = ("""
CREATE TABLE IF NOT EXISTS population_table
(state_code varchar NOT NULL PRIMARY KEY,
 male_population bigint,
 female_population bigint,
 total_population bigint
)
""")

immigrants_table_create = ("""
CREATE TABLE IF NOT EXISTS immigrants_table
(
 id float NOT NULL PRIMARY KEY,
 admission_number float,
 arrival_year float,
 arrival_month float,
 state_visited varchar,
 gender char,
 visa_type int,
 birth_year float,
 occupation varchar,
 airline varchar
) 
""")

flights_table_create = ("""
CREATE TABLE IF NOT EXISTS flights_table
(
airline varchar NOT NULL PRIMARY KEY,
flights_total varchar
)
""")

"""
    Insert data in the tables
"""

staging_immigration_table_insert = ("""
INSERT INTO staging_immigration_table
(
 unnamed,
 cicid,
 i94yr,
 i94mon,
 i94cit,
 i94res,
 i94port,
 arrdate,
 i94mode,
 i94addr,
 depdate,
 i94bir,
 i94visa,
 count,
 dtadfile,
 visapost,
 occup,
 entdepa,
 entdepd,
 entdepu,
 matflag,
 biryear,
 dtaddto,
 gender,
 insnum,
 airline,
 admnum,
 fltno,
 visatype 
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")
   

staging_demography_table_insert = ("""
INSERT INTO staging_demography_table
(
 city,
 state,
 median_age,
 male_population,
 female_population,
 total_population,
 number_of_veterans,
 foreign_born,
 avg_household_size,
 state_code,
 race,
 count
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

"""
    - FINAL tables required for the project, load data using staging tables
"""

immigrants_table_insert = ("""
INSERT INTO immigrants_table (
id,
admission_number,
arrival_year, 
arrival_month,
state_visited,
gender,
visa_type,
birth_year, 
occupation,
airline)
SELECT DISTINCT cicid, cast(admnum as float), i94yr, i94mon, i94addr, gender, i94visa, biryear, occup, fltno
FROM staging_immigration_table;                         
""")

population_table_insert = ("""
INSERT INTO population_table
(
state_code, 
male_population, 
female_population,
total_population
)
SELECT state_code, SUM(total_population), SUM(male_population), SUM(female_population) 
FROM staging_demography_table
GROUP BY state_code;
""")

flights_table_insert = ("""
INSERT INTO flights_table
(
airline,
flights_total
)
SELECT distinct airline, count(fltno) 
FROM staging_immigration_table
GROUP BY airline;
""")

"""
    - List of queries 
"""
create_staging_tables_queries = [staging_immigration_table_create, staging_demography_table_create]

create_other_tables_queries = [population_table_create, immigrants_table_create, flights_table_create]

insert_staging_immigration_table_query = [staging_immigration_table_insert]

insert_staging_demography_table_query = [staging_demography_table_insert]

insert_other_tables_queries = [immigrants_table_insert, population_table_insert, flights_table_insert]

drop_table_queries = [staging_immigration_table_drop, staging_demography_table_drop, immigrants_table_drop, population_table_drop, flights_table_drop]

