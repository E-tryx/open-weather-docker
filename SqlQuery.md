Here are comprehensive SQL queries to analyze weather conditions from January to December across multiple dimensions:

## 1. Monthly Temperature Analysis

```sql
-- Monthly average, min, and max temperatures by city
SELECT
  city,
  month,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(MIN(temperature), 2) as min_temperature,
  ROUND(MAX(temperature), 2) as max_temperature,
  COUNT(*) as readings_count
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 2. Monthly Rainfall Analysis

```sql
-- Monthly rainfall patterns and statistics
SELECT
  city,
  month,
  ROUND(SUM(rainfall_1h), 2) as total_rainfall_mm,
  ROUND(AVG(rainfall_1h), 2) as avg_daily_rainfall_mm,
  ROUND(MAX(rainfall_1h), 2) as max_daily_rainfall_mm,
  COUNT(CASE WHEN rainfall_1h > 0 THEN 1 END) as rainy_days,
  ROUND(COUNT(CASE WHEN rainfall_1h > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as rainy_days_percent
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 3. Weather Conditions Frequency Analysis

```sql
-- Most common weather conditions by month
SELECT
  city,
  month,
  weather_main,
  weather_description,
  COUNT(*) as frequency,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city, month), 1) as percentage
FROM weather_data
WHERE year = 2024
GROUP BY city, month, weather_main, weather_description
ORDER BY city, month, frequency DESC;
```

## 4. Humidity and Pressure Analysis

```sql
-- Monthly humidity and pressure patterns
SELECT
  city,
  month,
  ROUND(AVG(humidity), 1) as avg_humidity,
  ROUND(MIN(humidity), 1) as min_humidity,
  ROUND(MAX(humidity), 1) as max_humidity,
  ROUND(AVG(pressure), 1) as avg_pressure,
  ROUND(MIN(pressure), 1) as min_pressure,
  ROUND(MAX(pressure), 1) as max_pressure
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 5. Wind Speed Analysis

```sql
-- Monthly wind patterns
SELECT
  city,
  month,
  ROUND(AVG(wind_speed), 2) as avg_wind_speed,
  ROUND(MAX(wind_speed), 2) as max_wind_speed,
  ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY wind_speed), 2) as wind_speed_75th_percentile,
  COUNT(*) as readings_count
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 6. Seasonal Analysis (Kenyan Seasons)

```sql
-- Analysis by Kenyan seasons
SELECT
  city,
  CASE 
    WHEN month IN (12, 1, 2) THEN 'Dry Season 1 (Dec-Feb)'
    WHEN month IN (3, 4, 5) THEN 'Long Rains (Mar-May)'
    WHEN month IN (6, 7, 8, 9) THEN 'Dry Season 2 (Jun-Sep)'
    WHEN month IN (10, 11) THEN 'Short Rains (Oct-Nov)'
  END as season,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(SUM(rainfall_1h), 2) as total_rainfall_mm,
  ROUND(AVG(humidity), 1) as avg_humidity,
  ROUND(AVG(wind_speed), 2) as avg_wind_speed,
  COUNT(*) as readings_count
FROM weather_data
WHERE year = 2024
GROUP BY city, 
  CASE 
    WHEN month IN (12, 1, 2) THEN 'Dry Season 1 (Dec-Feb)'
    WHEN month IN (3, 4, 5) THEN 'Long Rains (Mar-May)'
    WHEN month IN (6, 7, 8, 9) THEN 'Dry Season 2 (Jun-Sep)'
    WHEN month IN (10, 11) THEN 'Short Rains (Oct-Nov)'
  END
ORDER BY city, season;
```

## 7. Temperature Extremes Analysis

```sql
-- Days with temperature extremes by month
SELECT
  city,
  month,
  COUNT(CASE WHEN temperature < 15 THEN 1 END) as cold_days_below_15c,
  COUNT(CASE WHEN temperature BETWEEN 15 AND 25 THEN 1 END) as mild_days_15_25c,
  COUNT(CASE WHEN temperature > 25 THEN 1 END) as hot_days_above_25c,
  ROUND(MIN(temperature), 1) as absolute_min_temp,
  ROUND(MAX(temperature), 1) as absolute_max_temp
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 8. Rainfall Intensity Analysis

```sql
-- Rainfall intensity categories by month
SELECT
  city,
  month,
  COUNT(CASE WHEN rainfall_1h = 0 THEN 1 END) as no_rain_days,
  COUNT(CASE WHEN rainfall_1h > 0 AND rainfall_1h <= 2.5 THEN 1 END) as light_rain_days,
  COUNT(CASE WHEN rainfall_1h > 2.5 AND rainfall_1h <= 7.5 THEN 1 END) as moderate_rain_days,
  COUNT(CASE WHEN rainfall_1h > 7.5 AND rainfall_1h <= 15 THEN 1 END) as heavy_rain_days,
  COUNT(CASE WHEN rainfall_1h > 15 THEN 1 END) as very_heavy_rain_days
FROM weather_data
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 9. Weather Conditions Correlation

```sql
-- Correlation between weather conditions and temperature/rainfall
SELECT
  weather_main,
  COUNT(*) as occurrences,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(AVG(rainfall_1h), 2) as avg_rainfall,
  ROUND(AVG(humidity), 1) as avg_humidity,
  ROUND(AVG(wind_speed), 2) as avg_wind_speed
FROM weather_data
WHERE year = 2024
GROUP BY weather_main
ORDER BY occurrences DESC;
```

## 10. Comprehensive Monthly Summary

```sql
-- Complete monthly weather summary
SELECT
  city,
  month,
  TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') as month_name,
  COUNT(*) as total_readings,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(SUM(rainfall_1h), 2) as total_rainfall_mm,
  ROUND(AVG(humidity), 1) as avg_humidity,
  ROUND(AVG(pressure), 1) as avg_pressure,
  ROUND(AVG(wind_speed), 2) as avg_wind_speed,
  -- Most common weather condition
  (SELECT weather_main 
   FROM weather_data w2 
   WHERE w2.city = w1.city AND w2.month = w1.month AND w2.year = 2024
   GROUP BY weather_main 
   ORDER BY COUNT(*) DESC 
   LIMIT 1) as dominant_weather,
  -- Rainy day percentage
  ROUND(COUNT(CASE WHEN rainfall_1h > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as rainy_days_percent
FROM weather_data w1
WHERE year = 2024
GROUP BY city, month
ORDER BY city, month;
```

## 11. City Comparison Query

```sql
-- Compare all cities' annual weather patterns
SELECT
  city,
  ROUND(AVG(temperature), 2) as annual_avg_temperature,
  ROUND(SUM(rainfall_1h), 2) as annual_total_rainfall,
  ROUND(AVG(humidity), 1) as annual_avg_humidity,
  ROUND(AVG(wind_speed), 2) as annual_avg_wind_speed,
  COUNT(CASE WHEN rainfall_1h > 0 THEN 1 END) as total_rainy_days,
  (SELECT weather_main 
   FROM weather_data w2 
   WHERE w2.city = w1.city AND w2.year = 2024
   GROUP BY weather_main 
   ORDER BY COUNT(*) DESC 
   LIMIT 1) as most_common_weather
FROM weather_data w1
WHERE year = 2024
GROUP BY city
ORDER BY annual_total_rainfall DESC;
```

## 12. Monthly Trends with Previous Month Comparison

```sql
-- Monthly trends with month-over-month changes
WITH monthly_stats AS (
  SELECT
    city,
    month,
    ROUND(AVG(temperature), 2) as avg_temp,
    ROUND(SUM(rainfall_1h), 2) as total_rain,
    LAG(ROUND(AVG(temperature), 2)) OVER (PARTITION BY city ORDER BY month) as prev_avg_temp,
    LAG(ROUND(SUM(rainfall_1h), 2)) OVER (PARTITION BY city ORDER BY month) as prev_total_rain
  FROM weather_data
  WHERE year = 2024
  GROUP BY city, month
)
SELECT
  city,
  month,
  avg_temp,
  total_rain,
  prev_avg_temp,
  prev_total_rain,
  ROUND(avg_temp - prev_avg_temp, 2) as temp_change,
  ROUND(total_rain - prev_total_rain, 2) as rain_change
FROM monthly_stats
ORDER BY city, month;
```

These queries provide comprehensive analysis of:

- **Temperature patterns** throughout the year
- **Rainfall distribution** and intensity
- **Seasonal variations** specific to Kenyan climate
- **Weather condition frequencies**
- **Humidity and pressure trends**
- **Wind patterns**
- **City-to-city comparisons**
- **Monthly trends and changes**

You can run these directly in your PostgreSQL database or adapt them for Grafana dashboards to create comprehensive weather analysis visualizations!