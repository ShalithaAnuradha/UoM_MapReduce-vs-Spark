DROP TABLE IF EXISTS airline_delay_detail;

CREATE EXTERNAL TABLE airline_delay_detail
(ID bigint,Year int,Month int, day_of_month int,day_of_week int,dep_time double,CRS_dep_time int,arr_time double,
CRS_arr_time int, unique_carrier string, flight_num int, tail_num String, actual_elapse_time double, CRS_elapse_time double,
air_time double, arr_delay double, dep_delay double, origin String, dest String, distance int, taxi_in double, taxi_out double,
cancelled int, cancellation_code String, diverted int, carrier_delay double, weather_delay double, NAS_delay double,
security_delay double, late_aircraft_delay double )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE
LOCATION "s3://aws-assignment-shalitha/input/";


CREATE EXTERNAL TABLE specific_delay_scenario(
Year string,
delay string)
STORED AS SEQUENCEFILE
LOCATION 's3://aws-assignment-shalitha/input/';


-- Choose year wise carrier delay for the given period (2003 - 2010)
INSERT OVERWRITE TABLE specific_delay_scenario
SELECT Year, SUM(carrier_delay) AS carrier_delay
FROM airline_delay_detail
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Choose year wise NAS delay for the given period (2003 - 2010)
INSERT OVERWRITE TABLE specific_delay_scenario
SELECT Year,SUM(NAS_delay) AS NAS_delay
FROM airline_delay_detail
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Choose year wise weather delay for the given period (2003 - 2010)
INSERT OVERWRITE TABLE specific_delay_scenario
SELECT Year, SUM(weather_delay) AS weather_delay
FROM airline_delay_detail
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Choose year wise late aircraft delay for the given period (2003 - 2010)
INSERT OVERWRITE TABLE specific_delay_scenario
SELECT Year, SUM(late_aircraft_delay) AS late_aircraft_delay
FROM airline_delay_detail
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Choose year wise late security delay for the given period (2003 - 2010)
INSERT OVERWRITE TABLE specific_delay_scenario
SELECT Year, SUM(security_delay) AS security_delay
FROM airline_delay_detail
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;