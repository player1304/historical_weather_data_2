import csv
import glob
import os
from collections import defaultdict
import re
import pandas as pd

OUTPUT_FILE = ".\\data\\weather_data_aggregated.csv"


def check_duplicates(file_path):
    """Check for duplicate rows in a CSV file. If so, delete these rows and save to a new file."""
    df = pd.read_csv(file_path)

    # Check for duplicate rows
    duplicates = df.duplicated()

    if duplicates.any():
        num_duplicates = duplicates.sum()
        print(f"*** Warning: duplicates found.\nNumber of duplicate rows: {num_duplicates} ***")
        df_no_duplicates = df.drop_duplicates()
        new_file_path = file_path + ".dedup.csv"
        df_no_duplicates.to_csv(new_file_path, index=False)

        print("Duplicates removed and saved to a new file: ")
        print(new_file_path)

    else:
        print("No duplicate rows found.")


def get_all_fieldnames(csv_files):
    """Get all unique fieldnames from all CSV files"""
    all_fieldnames = set()
    for file in csv_files:
        with open(file, "r", newline="", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            all_fieldnames.update(reader.fieldnames)
    return sorted(list(all_fieldnames))


def is_valid_date(date_str):
    """Check if the date string is in the format YYYY-MM-DD"""
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", date_str))


def process_and_check_dates(input_file, output_file):
    """Check if all dates are in the format of YYYY-MM-DD"""
    incorrect_format_found = False
    with open(input_file, "r", newline="", encoding="utf-8-sig") as infile, open(
        output_file, "w", newline="", encoding="utf-8-sig"
    ) as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            date = row["date"]
            if date:  # Only process non-empty date fields
                if not is_valid_date(date):
                    incorrect_format_found = True
            writer.writerow(row)

    if incorrect_format_found:
        print("*** Warning: Incorrect date format found. Manual checking is required. ***")

    return output_file


def merge_csv_files(output_file):
    # Get all CSV files in the data folder
    csv_files = glob.glob(".\\data\\weather_data_*.csv")

    if not csv_files:
        print("Error: No CSV files found with the pattern 'weather_data_*.csv'")
        return

    # Get all unique fieldnames
    all_fieldnames = get_all_fieldnames(csv_files)

    # Ensure 'date' (native from JSON response) and 'city_name'(added from main.py) are in the fieldnames
    if "date" not in all_fieldnames or "city_name" not in all_fieldnames:
        print("Error: 'date' and 'city_name' columns are required in all CSV files")
        return

    # Write the merged data to a temporary file
    temp_output_file = output_file + ".temp"
    with open(temp_output_file, "w", newline="", encoding="utf-8-sig") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=all_fieldnames)
        writer.writeheader()

        for file in csv_files:
            print(f"Processing {file}...")
            with open(file, "r", newline="", encoding="utf-8-sig") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    # Fill missing fields with "NA"
                    for field in all_fieldnames:
                        if field not in row:
                            row[field] = "NA"
                    writer.writerow(row)

    print(f"Total number of columns: {len(all_fieldnames)}")
    print(f"Columns: {', '.join(all_fieldnames)}")

    # Check for duplicates
    check_duplicates(temp_output_file)

    # Process and check dates
    final_output_file = process_and_check_dates(temp_output_file, output_file)

    # Remove the temporary file
    os.remove(temp_output_file)

    print(f"Final output file with processed dates: {final_output_file}")


if __name__ == "__main__":
    print(
        " --- This is a helper script to aggregate all weather_data_*.csv in .\\data into a single csv. ---"
    )
    if os.path.exists(OUTPUT_FILE):
        response = input(
            "*** Warning: the aggregated file already exists. Do you want to overwrite it? Type Y to confirm: ***"
        )
        if response.lower() == "y":
            base_name, extension = os.path.splitext(OUTPUT_FILE)
            old_file = f"{base_name}_old{extension}"
            os.rename(OUTPUT_FILE, old_file)
            merge_csv_files(OUTPUT_FILE)
            # print("Overwriting.........")
            os.remove(old_file)
        else:
            print("Abort.")
    else:
        merge_csv_files(OUTPUT_FILE)
