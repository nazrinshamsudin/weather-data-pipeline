MERGE `open-weather-api-airflow.weather.daily_data` AS target
USING `open-weather-api-airflow.weather.stg_daily_data` AS source

ON target.datetime = source.datetime  -- ← change dt to datetime

WHEN MATCHED THEN
  UPDATE SET
    target.dt = source.dt,
    target.sunrise = source.sunrise,
    target.sunset = source.sunset,
    target.temp = source.temp,
    target.feels_like = source.feels_like,
    target.pressure = source.pressure,
    target.humidity = source.humidity,
    target.dew_point = source.dew_point,
    target.clouds = source.clouds,
    target.visibility = source.visibility,
    target.wind_speed = source.wind_speed,
    target.wind_deg = source.wind_deg,
    target.weather = source.weather,
    target.datetime = source.datetime

WHEN NOT MATCHED THEN
  INSERT (
    dt, sunrise, sunset, temp, feels_like, pressure,
    humidity, dew_point, clouds, visibility,
    wind_speed, wind_deg, weather, datetime
  )
  VALUES (
    source.dt, source.sunrise, source.sunset, source.temp, source.feels_like,
    source.pressure, source.humidity, source.dew_point, source.clouds,
    source.visibility, source.wind_speed, source.wind_deg,
    source.weather, source.datetime
  );