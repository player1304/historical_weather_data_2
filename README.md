This is a rewrite of the historical weather data gathering script using OpenWeatherMap's One Call (Daily Aggregation) [API](https://openweathermap.org/api/one-call-3#history_daily_aggregation). API key needs to be provided by the user, which can be obtained by registering at [OpenWeatherMap](https://openweathermap.org/). Modify main.py to change city list and date range.

The merge.py would merge the daily weather data in .\data into a single CSV file for analysis. It would also check for duplicate rows.

The analysis.xlsx reads .\data\weather_data_aggregated.csv and generates a dashboard. Relative path shall be preserved for the Excel document to work properly.