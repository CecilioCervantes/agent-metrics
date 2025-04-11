import pandas as pd
import os
import pathlib
import re
from datetime import datetime, timedelta
from math import floor

# avobe are the link to import from frameworks and other code



#helper function that retunrns metrics depending on the week day
def get_daily_time_goals(report_date):
    """
    Returns daily requirements based on the day of the week.
    All times returned in decimal hours.
    """
    weekday = report_date.weekday()

    if weekday in [0, 1, 2, 3]:  # Mon–Thu
        return 9.5, 2.333, 1.0
    elif weekday == 4:  # Friday
        return 7.5, 2.0, 0.75
    elif weekday == 5:  # Saturday
        return 6.0, 1.5, 0.75
    elif weekday == 6:  # Sunday
        return 5.0, 1.0, 0.75



# 🔁 Rename columns from the csv to our final presentation
COLUMN_RENAME_MAP = {
    "Login ID": "Agent",
    "Shift Start": "1st Call",
    "Shift End": "Shift End",
    "Logged Time": "Time Connected",
    "Break (t)": "Break",
    "Appointments (#)": "Sales",
    "Ready:Talk Time": "Talk Time",
    "Ready:Wrap Time": "Wrap Up"
}




# 🎯 Column order for display
DISPLAY_COLUMN_ORDER = ["1st Call", "Sales", "Agent", "Time To Goal", "Time Connected", "Break", "Talk Time", "Wrap Up", "Shift End", "Time Mismatch", "_MismatchAmount", "_TTG_Adjusted"]




# 🔧 Function to convert string like "2 hours 43 min 30 s" → 2.725 (in hours as decimal)
def time_string_to_decimal(time_str):
    if pd.isna(time_str) or str(time_str).strip() == "-":
        return None  # Keep missing values as None

    try:
        return float(time_str)
    except ValueError:
        pass  # If it's not a number, keep going with string parsing

    hours = minutes = seconds = 0
    h = re.search(r"(\d+)\s*hours?", str(time_str))
    m = re.search(r"(\d+)\s*min", str(time_str))
    s = re.search(r"(\d+)\s*s", str(time_str))
    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))
    return round(hours + minutes / 60 + seconds / 3600, 3)



# function to check if there is a mismatch in what the system gives and the actual time frames
def detect_inconsistencies(df):
    def check_mismatch(row):
        try:
            # Extract just the time portion from the strings (e.g. "Apr 3 6:45AM" → "6:45AM")
            start_str = str(row.get("1st Call")).split()[-1]
            end_str = str(row.get("Shift End")).split()[-1]

            # Convert to datetime.time objects
            start_time = datetime.strptime(start_str, "%I:%M%p")
            end_time = datetime.strptime(end_str, "%I:%M%p")

            # Use dummy date to calculate duration in seconds
            dummy_date = datetime(2000, 1, 1)
            start_dt = dummy_date.replace(hour=start_time.hour, minute=start_time.minute)
            end_dt = dummy_date.replace(hour=end_time.hour, minute=end_time.minute)

            # Handle wrap-around (e.g. shift passes midnight)
            if end_dt < start_dt:
                end_dt += timedelta(days=1)

            max_possible = (end_dt - start_dt).total_seconds() / 3600  # hours as decimal
            time_connected = row.get("Time Connected", 0)

            if pd.isna(time_connected):
                return True, "⚠️ Missing Time Connected"

            diff = time_connected - max_possible

            def format_diff_to_hms(hours):
                total_seconds = int(hours * 3600)
                h = total_seconds // 3600
                m = (total_seconds % 3600) // 60
                s = total_seconds % 60
                return f"{h:02}:{m:02}:{s:02}"

            # Mismatch logic
            if diff > 0.167:  # More than 10 min
                extra_time = format_diff_to_hms(diff)
                visible = f"⚠️ +{extra_time}"
                mismatch_amount = diff
            else:
                visible = "✅"
                mismatch_amount = 0

            internal_debug = f"{row['Agent']} | TC: {time_connected:.2f} | Max: {max_possible:.2f} | Diff: {diff:.2f}"
            return visible, internal_debug, mismatch_amount


        except Exception as e:
            return "⚠️", f"{row.get('Agent', 'Unknown')} | Error: {e}"

    # Apply and unpack results
    results = df.apply(check_mismatch, axis=1)
    df["Time Mismatch"] = results.apply(lambda x: x[0])
    df["_Debug"] = results.apply(lambda x: x[1])
    df["_MismatchAmount"] = results.apply(lambda x: x[2])

    return df




###################################################################################





def load_and_process_data(uploaded_dfs, report_date):
    """
    Loads all CSV files, renames and reorders columns, and returns them grouped by filename.
    """
    combined_data = {}
    server_number = 1  # 👈 Start counting from Server 1

    for df in uploaded_dfs:

            # ❌ Drop the last row (totals)
            df = df[:-1] if len(df) > 0 else df

            # Optional: drop last row if it's empty
            df = df.dropna(how="all")

            # 🏷️ Rename columns
            df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
            
            # ⏱️ Convert and format time-related columns
            time_columns = ["Time Connected", "Break", "Talk Time", "Wrap Up"]

            for col in time_columns:
                if col in df.columns:
                    df[col] = df[col].apply(time_string_to_decimal) # Store as decimal only


            # ⚠️ First: Flag shift time mismatches (Time Connected vs. Shift Span)
            df = detect_inconsistencies(df)

            # ✅ Then: Calculate Time To Goal — now mismatch info is available
            goal_time, break_limit, wrap_limit = get_daily_time_goals(report_date)


            def calculate_time_to_goal(row):
                tc = row.get("Time Connected", 0)
                br = row.get("Break", 0)
                wr = row.get("Wrap Up", 0)

                extra_break = max(0, br - break_limit)
                extra_wrap = max(0, wr - wrap_limit)
                total_penalty = extra_break + extra_wrap

                mismatch_penalty = row.get("_MismatchAmount", 0)

                # Add a flag for UI display if mismatch affected the time
                if mismatch_penalty > 0:
                    row["_TTG_Adjusted"] = True
                else:
                    row["_TTG_Adjusted"] = False

                return (tc - goal_time - total_penalty) - mismatch_penalty


            # 🛠️ Calculate TTG and add gear flag
            def calculate_ttgs(row):
                tc = row.get("Time Connected", 0)
                br = row.get("Break", 0)
                wr = row.get("Wrap Up", 0)

                extra_break = max(0, br - break_limit)
                extra_wrap = max(0, wr - wrap_limit)
                total_penalty = extra_break + extra_wrap
                mismatch_penalty = row.get("_MismatchAmount", 0)

                ttg = (tc - goal_time - total_penalty) - mismatch_penalty
                adjusted = mismatch_penalty > 0
                return pd.Series([ttg, adjusted])

            # 👇 Unpack into columns
            df[["Time To Goal", "_TTG_Adjusted"]] = df.apply(calculate_ttgs, axis=1)







            # 🏢 Assign Office based on agent login prefix
            def classify_office(agent_name):
                if isinstance(agent_name, str):
                    if agent_name.startswith("n "): return "Tepic"
                    if agent_name.startswith("a "): return "Army"
                    if agent_name.startswith("w "): return "West"
                    if agent_name.startswith("sp ") or agent_name.startswith("pr "): return "Sp & Prime"
                    if agent_name.startswith("e "): return "Egypt"
                    if agent_name.startswith("s "): return "Spanish"
                    if agent_name.startswith("g "): return "Nigeria"
                return "Other"

            df["Office"] = df["Agent"].apply(classify_office)



            # Ensure all expected columns are present — fill missing with empty values
            for col in DISPLAY_COLUMN_ORDER:
                if col not in df.columns:
                    df[col] = ""  # or pd.NA if you want it to be explicitly empty

            columns_to_keep = [col for col in DISPLAY_COLUMN_ORDER if col in df.columns]

            if "Office" in df.columns:
                columns_to_keep.append("Office")

            df = df[columns_to_keep]



            #sort a to z
            df = df.sort_values(by="Agent", ascending=True)

            # 🔢 Reset index to start from 1 for clean display
            df.index = range(1, len(df) + 1)



            # 💾 Store using Server 1, Server 2...
            combined_data[f"Server {server_number}"] = df
            server_number += 1  # 👈 Move to next server

    return combined_data
