/*CREATE THE VIEW VW_DRIVER_CURATED_DETAILS*/
CREATE OR REPLACE VIEW `burner-manchoud1.bbb_hackathon_data.VW_DRIVER_CURATED_DETAILS` AS 
select *, 
(COMPOSITE_DRIVER_SCORE/10)*100 as COMPOSITE_DRIVER_SCORE_PERCENTILE,
CASE WHEN (COMPOSITE_DRIVER_SCORE >=0 AND COMPOSITE_DRIVER_SCORE <5)  THEN 0
     WHEN (COMPOSITE_DRIVER_SCORE >=5 AND COMPOSITE_DRIVER_SCORE <8)  THEN 10 
     WHEN (COMPOSITE_DRIVER_SCORE >=8 AND COMPOSITE_DRIVER_SCORE <10) THEN 20 END AS INSURANCE_PREMIUM_DISCOIUNT ,
CASE WHEN  COMPOSITE_DRIVER_SCORE <5 THEN 'RECCOMEND COURSE TO IMPROVE SCORE' ELSE NULL END AS DRIVER_COURSE_RECOMMENDATION,
CASE WHEN Weather_conditions = 'snowy' then 'RECOMMEND HEATED SEATS' 
     WHEN Weather_conditions = 'clear' then 'RECOMMEND SUNROOF/MOONROOF'  
     ELSE NULL END AS WEATHER_BASED_RECOMMENDATION,
CASE WHEN Seatbelt_usage = 1 THEN 'GOOD'
     WHEN Seatbelt_usage = 0 THEN 'POOR'
     ELSE NULL END AS SEATBELT_RATING,
CASE WHEN (Speeding_incidents >=0 AND Speeding_incidents<3) THEN 'GOOD'
     WHEN (Speeding_incidents >=3 AND Speeding_incidents<5) THEN 'MODERATE'
     WHEN Speeding_incidents >= 5  THEN 'POOR' 
     END AS SPEEDING_RATING,
CASE WHEN (Frequency_of_hard_braking >=0 AND Frequency_of_hard_braking<4) THEN 'GOOD'
     WHEN (Frequency_of_hard_braking >=4 AND Frequency_of_hard_braking<8) THEN 'MODERATE'
     WHEN Frequency_of_hard_braking >= 8  THEN 'POOR' 
     END AS HARD_BRAKING_RATING,
CASE WHEN (Frequency_of_hard_acceleration >=0 AND Frequency_of_hard_acceleration<4) THEN 'GOOD'
     WHEN (Frequency_of_hard_acceleration >=4 AND Frequency_of_hard_acceleration<8) THEN 'MODERATE'
     WHEN Frequency_of_hard_acceleration >= 8  THEN 'POOR' 
     END AS ACCELERATION_RATING,     
CASE WHEN (Frequency_of_sudden_lane_changes >=0 AND Frequency_of_sudden_lane_changes<3) THEN 'GOOD'
     WHEN (Frequency_of_sudden_lane_changes >=3 AND Frequency_of_sudden_lane_changes<5) THEN 'MODERATE'
     WHEN Frequency_of_sudden_lane_changes >= 5  THEN 'POOR' 
     END AS LANE_CHANGE_RATING,
CASE WHEN Average_time_between_vehicle_maintenance = '1-3 months' THEN 'GOOD'
     WHEN Average_time_between_vehicle_maintenance = '3-6 months' THEN 'MODERATE'
     WHEN Average_time_between_vehicle_maintenance = '>6 months'  THEN 'POOR' 
     END AS Vehicle_Maintenance_RATING,
from (SELECT Customer_ID,
SUM(SEAT_BELT_SCORE+Speeding_incident_SCORE+braking_SCORE+acceleration_SCORE+lane_changes_SCORE+vehicle_maintenance_SCORE) COMPOSITE_DRIVER_SCORE,
Weather_conditions,
Route_taken,
LAST_TRIP_DISTANCE,
LAST_TRIP_TIME,
Traffic_conditions,
Starting_location,
Ending_location,
Fuel_consumption AS Total_Fuel_consumption,
GENRE,
Average_time_between_vehicle_maintenance,
Seatbelt_usage,
Speeding_incidents,
Frequency_of_hard_braking,
Frequency_of_hard_acceleration,
Frequency_of_sudden_lane_changes FROM( SELECT 
SUBSTR(DRIVING_DATA.Customer_ID,5,6) AS Customer_ID,
DRIVING_DATA.Fuel_consumption,
SAFETY.Seatbelt_usage Seatbelt_usage,
CAST(SAFETY.Speeding_incidents AS INT64) Speeding_incidents,
CAST(SAFETY.Frequency_of_hard_braking AS INT64) Frequency_of_hard_braking,
CAST(SAFETY.Frequency_of_hard_acceleration AS INT64) Frequency_of_hard_acceleration,
CAST(SAFETY.Frequency_of_sudden_lane_changes AS INT64) Frequency_of_sudden_lane_changes,
 SAFETY.Average_time_between_vehicle_maintenance,
 SAFETY.Weather_conditions,
 ROUTES.Route_taken,
 ROUTES.Distance AS LAST_TRIP_DISTANCE,
 ROUTES.Time_taken AS LAST_TRIP_TIME,
 ROUTES.Traffic_conditions,
 ROUTES.Starting_location,
 ROUTES.Ending_location,
 MUSIC_DATA.string_field_1 AS GENRE,
CASE WHEN SAFETY.Seatbelt_usage = 1 THEN 2 ELSE 0 END AS SEAT_BELT_SCORE,
CASE WHEN (SAFETY.Speeding_incidents >=0 AND SAFETY.Speeding_incidents <3)  THEN 2 
     WHEN (SAFETY.Speeding_incidents >=3 AND SAFETY.Speeding_incidents <5)  THEN 1 
     ELSE 0 END AS Speeding_incident_SCORE  ,
CASE WHEN (SAFETY.Frequency_of_hard_braking >=0 AND SAFETY.Frequency_of_hard_braking <4)  THEN 2 
     WHEN (SAFETY.Frequency_of_hard_braking >=4 AND SAFETY.Frequency_of_hard_braking <8)  THEN 1 
     ELSE 0 END AS braking_SCORE,
CASE WHEN (SAFETY.Frequency_of_hard_acceleration >=0 AND SAFETY.Frequency_of_hard_acceleration <4)  THEN 2 
     WHEN (SAFETY.Frequency_of_hard_acceleration >=4 AND SAFETY.Frequency_of_hard_acceleration <8)  THEN 1 
     ELSE 0 END AS acceleration_SCORE, 
CASE WHEN (SAFETY.Frequency_of_sudden_lane_changes >=0 AND SAFETY.Frequency_of_sudden_lane_changes <3)  THEN 2 
     WHEN (SAFETY.Frequency_of_sudden_lane_changes >=3 AND SAFETY.Frequency_of_sudden_lane_changes <5)  THEN 1 
     ELSE 0 END AS lane_changes_SCORE  , 
CASE WHEN SAFETY.Average_time_between_vehicle_maintenance ='<1 month' THEN 2
     WHEN (SAFETY.Average_time_between_vehicle_maintenance ='1-3 months' OR SAFETY.Average_time_between_vehicle_maintenance ='1-3 months') THEN 1
     ELSE 0 END AS vehicle_maintenance_SCORE
FROM  `burner-manchoud1.bbb_hackathon_data.DRIVING_DATA`  DRIVING_DATA 
LEFT JOIN `burner-manchoud1.bbb_hackathon_data.SAFETY` SAFETY
ON DRIVING_DATA.Customer_ID = SAFETY.Customer_ID 
LEFT JOIN `burner-manchoud1.bbb_hackathon_data.PREFERED_ROUTES` as ROUTES
ON DRIVING_DATA.Customer_ID = ROUTES.Customer_ID 
LEFT JOIN `burner-manchoud1.bbb_hackathon_data.MUSIC_PREFERENCE_CAR` AS MUSIC_DATA
ON DRIVING_DATA.Customer_ID = MUSIC_DATA.string_field_0 
WHERE ROUTES.Mode_of_transportation= 'Car')
GROUP BY Customer_ID, Weather_conditions, Route_taken, LAST_TRIP_DISTANCE, LAST_TRIP_TIME, Traffic_conditions, Starting_location,Ending_location,Seatbelt_usage,Speeding_incidents,Frequency_of_hard_braking,
Frequency_of_hard_acceleration, Frequency_of_sudden_lane_changes, Fuel_consumption, GENRE ,Average_time_between_vehicle_maintenance
ORDER BY 1);