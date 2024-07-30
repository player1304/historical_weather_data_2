# https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lon}&date={date}&units=metric&appid={API_key}

from SECRETS import API_KEY, PROXY_ADDRESS
from flatten_json import flatten
import requests
import pandas as pd
import datetime
import time
import urllib.parse
import os
import csv

USE_PROXY: bool = False

CITIES = ["Shenzhen", "Shanghai", "Guangzhou", "Beijing"]
START_DATE = datetime.datetime(2024, 7, 1)
END_DATE = datetime.datetime(2024, 7, 29)
OUTPUT_FILE = ".\\data\\weather_data.csv"


def append_to_csv(file_path, data, fieldnames):
    """Append a row to the CSV file"""
    with open(file_path, "a", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(data)


def confirm_overwrite(file_path):
    """
    Prompt user to confirm overwriting the output file.
    
    Returns True if user confirms to overwrite or no duplicate is found, False otherwise.
    """
    if os.path.exists(file_path):
        response = input(
            f"*** Warning: The file '{file_path}' already exists. Do you want to overwrite it? Type 'Y' to confirm: ***"
        )
        return response.lower() == "y"
    return True


def get_city_coordinates(city):
    """Get latitude and longitude of a city"""
    encoded_city = urllib.parse.quote(city, safe="")
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={encoded_city}&limit=1&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if data:
        print(f"{city}: {data[0]["lat"]}, {data[0]["lon"]}")
        return data[0]["lat"], data[0]["lon"]
    else:
        print(f"*** Error: no geo-code data found for {city} ***")
        return None, None


def get_weather_data(city, lat, lon, date_str):
    """Get weather data for a city on a specific date in the format YYYY-MM-DD"""
    url = f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lon}&date={date_str}&units=metric&appid={API_KEY}"

    if USE_PROXY == True:
        response = requests.get(url, proxies=PROXY_ADDRESS)
    else:
        response = requests.get(url)

    data = response.json()

    # Check if response is empty
    if data:
        flattened_data = flatten(data)
        # add columns for easier processing in Excel
        flattened_data["city_name"] = city
        return flattened_data

    else:  # data is empty
        print(f"*** Warning: No geo-coding data found for {city} on {date_str} ***")
        return None


def update_csv_with_new_column(file_path, new_fieldnames):
    """In case a key is not found in the header, update the CSV file with a new column"""
    temp_file = file_path + ".temp"
    with open(file_path, "r", newline="", encoding="utf-8-sig") as csvfile, open(
        temp_file, "w", newline="", encoding="utf-8-sig"
    ) as tempfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(tempfile, fieldnames=new_fieldnames)
        writer.writeheader()
        for row in reader:
            for field in new_fieldnames:
                if field not in row:
                    row[field] = "NA"
            writer.writerow(row)
    os.replace(temp_file, file_path)


def validate_date_range(start_date, end_date):
    """
    Validate that both start_date and end_date are valid datetime objects,
    and that end_date is the same as or later than start_date.
    
    Args:
    start_date (datetime.datetime): The start date
    end_date (datetime.datetime): The end date
    
    Returns:
    bool: True if validation passes, False otherwise
    """
    # Check if both are datetime objects
    if not (isinstance(start_date, datetime.datetime) and isinstance(end_date, datetime.datetime)):
        print("*** Error: Both dates must be datetime objects. ***")
        return False
    
    # Check if end_date is same as or later than start_date
    if end_date < start_date:
        print("*** Error: END_DATE must be the same as or later than START_DATE. Aborting. ***")
        return False
    
    return True


def write_csv_header(file_path, fieldnames):
    """Write the CSV header"""
    with open(file_path, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()


def main():
    # Check if output exists
    if not confirm_overwrite(OUTPUT_FILE):
        print("Operation cancelled.")
        return

    # Check if date range is valid
    if not validate_date_range(START_DATE, END_DATE):
        return

    city_coords = {city: get_city_coordinates(city) for city in CITIES}
    date_range = pd.date_range(start=START_DATE, end=END_DATE, freq="D")

    fieldnames = None  # header of the csv
    for date in date_range:
        for city, (lat, lon) in city_coords.items():
            if lat is not None and lon is not None:
                date_str = date.strftime("%Y-%m-%d")
                data = get_weather_data(city, lat, lon, date_str)
                if data:

                    if fieldnames is None:  # New csv, no header yet
                        fieldnames = list(data.keys())
                        write_csv_header(OUTPUT_FILE, fieldnames)

                    elif set(data.keys()) != set(
                        fieldnames
                    ):  # Data does not match the header
                        new_fields = set(data.keys()) - set(fieldnames)
                        missing_fields = set(fieldnames) - set(data.keys())

                        if new_fields:  # A new field was found, need to update header
                            fieldnames.extend(new_fields)
                            update_csv_with_new_column(OUTPUT_FILE, fieldnames)
                            print(f"New column(s) added: {', '.join(new_fields)}")

                        if missing_fields:  # Missing values, no need to worry
                            print(
                                f"Debug: some columns are missing: {', '.join(missing_fields)}. No need to worry."
                            )

                    # Ensure all fieldnames are present in data
                    for field in fieldnames:
                        if field not in data:
                            data[field] = "NA"

                    # Write the new row to csv
                    append_to_csv(OUTPUT_FILE, data, fieldnames)
                    print(f"Data for {city} on {date_str} appended to {OUTPUT_FILE}.")

                time.sleep(1)  # To avoid hitting API rate limits

    print(f"Data collection complete. All data saved to {OUTPUT_FILE}.")


if __name__ == "__main__":
    main()
