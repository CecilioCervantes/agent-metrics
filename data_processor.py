import pandas as pd
import os
import pathlib
import re
from datetime import datetime, timedelta
from math import floor
from io import BytesIO
import dropbox
import plotly.graph_objects as go
import tempfile
import plotly.io as pio
#import pdfkit
#from mailersend import emails
import base64
from dotenv import load_dotenv
from xhtml2pdf import pisa
import json
import streamlit as st
from google.oauth2.service_account import Credentials
import gspread













#-------------------------------------------------------------------------------------------------------------------------------------------------------------
## === TIME FORMATTERS ===
def format_time_columns(df):
    """
    Returns:
        pd.DataFrame: Updated DataFrame with formatted time-strings
    """
    time_columns = ["Time Connected", "Break", "Talk Time", "Wrap Up", "Time To Goal"]

    for col in time_columns:
        if col in df.columns:
            # 1) Coerce blanks/invalid → NaN
            df[col] = pd.to_numeric(df[col], errors="coerce")

            if col == "Time To Goal":
                # include gear icon if _TTG_Adjusted is True
                df[col] = df.apply(
                    lambda row: (
                        decimal_to_hhmmss(row[col])
                        + (" ⚙️" if row.get("_TTG_Adjusted") else "")
                    ) if pd.notna(row[col]) else "--:--:--",
                    axis=1
                )
            else:
                # neutral format, no +/- sign
                df[col] = df[col].apply(
                    lambda x: decimal_to_hhmmss_nosign(x) if pd.notna(x) else "--:--:--"
                )

    return df




def decimal_to_hhmmss(decimal_hours):
    """
    Converts decimal hours to a signed hh:mm:ss string.

    Adds a '+' or '-' prefix based on sign of the value.

    Parameters:
        decimal_hours (float): Time in decimal hour format

    Returns:
        str: Time as '+hh:mm:ss' or '-hh:mm:ss', or ❌ if invalid
    """
    if pd.isna(decimal_hours):
        return "❌"

    total_seconds = int(decimal_hours * 3600)
    sign = "-" if total_seconds < 0 else "+"
    total_seconds = abs(total_seconds)

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{sign}{hours:02}:{minutes:02}:{seconds:02}"


def decimal_to_hhmmss_nosign(decimal_hours):
    """
    Converts decimal hours to an hh:mm:ss string with no prefix.

    Typically used for neutral display like Talk Time, Break, etc.

    Parameters:
        decimal_hours (float): Time in decimal hours

    Returns:
        str: Time as 'hh:mm:ss', or ❌ if invalid
    """
    if pd.isna(decimal_hours):
        return "❌"

    total_seconds = int(abs(decimal_hours) * 3600)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"




def time_string_to_decimal(time_str):
    """
    Converts strings like '2 hours 43 min 30 s' into decimal hours (e.g. 2.725).

    Handles:
    - Raw strings from CSVs with 'hours', 'min', 's'
    - Direct numeric strings (e.g. '1.5')
    - Missing or invalid inputs

    Parameters:
        time_str (str): Time string in verbose format

    Returns:
        float or None: Time in decimal hours, or None if unparseable
    """
    if pd.isna(time_str) or str(time_str).strip() == "-":
        return None

    try:
        # Handle standard HH:MM:SS strings
        if re.match(r'^\d{1,2}:\d{2}:\d{2}$', str(time_str)):
            h, m, s = map(int, str(time_str).split(':'))
            return round(h + m/60 + s/3600, 3)
        # Try direct float conversion (e.g., '1.5')
        return float(time_str)

    except ValueError:
        pass  # Proceed to parsing if not a float

    # Initialize values
    hours = minutes = seconds = 0

    # Extract components using regex
    h = re.search(r"(\d+)\s*hours?", str(time_str))
    m = re.search(r"(\d+)\s*min", str(time_str))
    s = re.search(r"(\d+)\s*s", str(time_str))

    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))

    # Return total time in decimal hours, rounded to 3 decimal places
    return round(hours + minutes / 60 + seconds / 3600, 3)




def convert_time_columns_for_export(df):
    """
    Converts all relevant time-related columns in a DataFrame to hh:mm:ss format with sign.

    Used during export (e.g. Supabase, Google Sheets) to show over/underperformance clearly.

    Parameters:
        df (pd.DataFrame): DataFrame with time columns in decimal format

    Returns:
        pd.DataFrame: Updated DataFrame with formatted strings
    """
    time_cols = ["Time To Goal", "Time Connected", "Break", "Talk Time", "Wrap Up"]

    for col in time_cols:
        if col in df.columns:
            df[col] = df[col].apply(decimal_to_hhmmss)

    return df








#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === EXPORT: GOOGLE SHEETS ===

def connect_to_gsheet(sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Handle both TOML (local) and cloud-parsed secrets
    raw_creds = st.secrets["GCP_SERVICE_ACCOUNT"]
    creds_dict = json.loads(raw_creds) if isinstance(raw_creds, str) else raw_creds

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)






def export_df_to_sheet(df, worksheet):
    """
    Writes a DataFrame to a given Google Sheets worksheet using gspread.

    - Clears and replaces existing content
    - Adds header row followed by data rows

    Parameters:
        df (pd.DataFrame): DataFrame to export
        worksheet (gspread.models.Worksheet): Target worksheet object

    Returns:
        None
    """
    data = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.update(data)



def create_unique_worksheet(sheet, title):
    """
    Creates a new worksheet in a Google Sheet.

    Attempts to duplicate a worksheet named 'Template'.
    If not available, falls back to creating a blank worksheet.

    Parameters:
        sheet (gspread.Spreadsheet): The Google Sheet object
        title (str): Name for the new worksheet

    Returns:
        gspread.Worksheet: The newly created or duplicated worksheet
    """
    try:
        # Try duplicating 'Template' worksheet
        template = sheet.worksheet("Template")
        new_worksheet = template.duplicate(new_sheet_name=title)

        # Unhide the duplicated sheet
        sheet.batch_update({
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": new_worksheet.id,
                        "hidden": False
                    },
                    "fields": "hidden"
                }
            }]
        })

        return new_worksheet

    except Exception:
        # If no template or error, create blank sheet
        return sheet.add_worksheet(title=title, rows=1000, cols=26)














#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === SORTING / DISPLAY HELPERS ===

def sort_dataframe(df, selected_column, sort_direction_map=None):
    """
    Sorts a DataFrame based on a given column or column group.

    Supports:
        - Regular column sorting
        - Composite column logic (e.g., Break + Wrap Up)
        - Optional custom sort direction mapping

    Parameters:
        df (pd.DataFrame): DataFrame to sort
        selected_column (str or list): Column name or list of columns to sort by
        sort_direction_map (dict): Optional direction overrides (col_name → bool)

    Returns:
        pd.DataFrame: Sorted DataFrame
    """
    if sort_direction_map is None:
        sort_direction_map = {}

    # 🔧 Ensure 'Sales' column is numeric
    if "Sales" in df.columns:
        df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce").fillna(0).astype(int)

    # Composite sort: Break + Wrap Up
    if isinstance(selected_column, list):
        if set(selected_column) == {"Break", "Wrap Up"}:
            df["_BreakWrapSum"] = df["Break"] + df["Wrap Up"]
            df = df.sort_values(by="_BreakWrapSum", ascending=True)
            df.drop(columns="_BreakWrapSum", inplace=True)
        else:
            df = df.sort_values(by=selected_column, ascending=False)

    # Single column sort
    elif selected_column:
        ascending = sort_direction_map.get(selected_column, False)
        df = df.sort_values(by=selected_column, ascending=ascending)

    # Fallback: sort by Agent name
    else:
        df = df.sort_values(by="Agent", ascending=True)

    return df




def load_chase_data(df_raw):
    # 1) Drop any “Total” rows
    df = df_raw[~df_raw["Agente"].astype(str).str.contains("Total", na=False)].copy()

    # 2) Rename exactly the Timesheet headers
    df = df.rename(columns={
        "Agente": "Agent",
        "Hora de Inicio de Sesión": "1st Call",
        "Hora de Cierre de Sesión":  "Shift End",
        "Tiempo en Sesión":           "Time Connected",
        "Duración de Conversación":   "Talk Time",
        "Duración de Receso":         "Break",
        "Tiempo de Finalización":     "Wrap Up",
        # optional, if you ever see it
        "Ventas/Potencial/Cita":      "Sales",
    })

    # 3) Strip whitespace on Agent
    df["Agent"] = df["Agent"].astype(str).str.strip()

    # 4) Normalize Sales → int  (zero if missing)
    if "Sales" in df.columns:
        df["Sales"] = (
            pd.to_numeric(df["Sales"].astype(str).str.extract(r"(\d+)")[0],
                          errors="coerce")
              .fillna(0)
              .astype(int)
        )
    else:
        df["Sales"] = 0

    # 5) Parse every duration column (including Time Connected + Shift End)
    for col in ["Time Connected", "Talk Time", "Break", "Wrap Up", "Shift End"]:
        if col in df.columns:
            df[col] = df[col].apply(time_string_to_decimal).fillna(0.0)

    # 6) Ensure 1st Call exists so downstream code can always reference it
    if "1st Call" not in df.columns:
        df["1st Call"] = ""

    # 7) Fill in the “mismatch” and debug placeholders
    df["_MismatchAmount"] = 0
    df["Time Mismatch"]   = "✅"
    df["_Debug"]          = ""

    return df









#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === DATA INGESTION: DROPBOX / MANUAL ===

def get_latest_dropbox_csv(folder_path, dbx=None):
    """
    Fetches the latest CSV files from a Dropbox folder.

    Connects using environment credentials (or a pre-injected client for testing),
    sorts by last modified date (most recent first), and returns a list of
    (filename, BytesIO) tuples.

    Parameters:
        folder_path (str): Target Dropbox folder path (e.g. '/ReadyModeReports')
        dbx (dropbox.Dropbox): Optional pre-authenticated Dropbox client

    Returns:
        List[Tuple[str, BytesIO]]: List of filenames and file content
    """


    #print("🔌 Dropbox client initialized")


    # 🔐 Load Dropbox client if not provided
    if dbx is None:
        dbx = dropbox.Dropbox(
            oauth2_access_token=os.getenv("DROPBOX_ACCESS_TOKEN"),
            oauth2_refresh_token=os.getenv("DROPBOX_REFRESH_TOKEN"),
            app_key=os.getenv("DROPBOX_APP_KEY"),
            app_secret=os.getenv("DROPBOX_APP_SECRET"),
            timeout=5  # ← add this line
        )

    #print(f"📦 Attempting to list files in: {folder_path}")
    entries = dbx.files_list_folder(folder_path).entries
   #print(f"✅ Found {len(entries)} files in Dropbox folder")

    try:
        # Fetch all entries and filter CSVs
        entries = dbx.files_list_folder(folder_path).entries
        csv_files = sorted(
            [f for f in entries if f.name.endswith(".csv")],
            key=lambda x: x.server_modified,
            reverse=True
        )
        if not csv_files:
            return []

        # Sort CSVs by modified time (newest first)
        sorted_files = sorted(csv_files, key=lambda x: x.server_modified, reverse=True)

        # Download and buffer content
        file_list = []
        for csv_file in sorted_files:
            _, res = dbx.files_download(f"{folder_path}/{csv_file.name}")
            file_list.append((csv_file.name, BytesIO(res.content)))

        return file_list

    except Exception as e:
        raise RuntimeError(f"Dropbox error: {e}")







#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === TIME RULES / GOALS ===


def get_daily_time_goals(report_date):
    """
    Returns expected performance metrics based on the day of the week.

    Used to evaluate whether an agent meets daily login, break, and wrap-up goals.

    Parameters:
        report_date (datetime or pd.Timestamp): The date of the report

    Returns:
        Tuple[float, float, float, float or None, str]:
            - goal_time (float): Required login time in decimal hours
            - break_limit (float): Maximum break time allowed
            - wrap_limit (float): Maximum wrap-up time allowed
            - talk_time_goal (float or None): Minimum expected talk time
            - shift_start_time (str): Expected first call time (HH:MM, 24-hour format)
    """
    weekday = report_date.weekday()  # Monday = 0, Sunday = 6

    if weekday in [0, 1, 2, 3]:  # Mon–Thu
        return 9.5, 2.333, 1.0, 4.5, "07:45"
    elif weekday == 4:  # Friday
        return 7, 2.0, 0.75, 3.5, "8:15"
    elif weekday == 5:  # Saturday
        return 6, 1.5, 0.75, 2.75, "08:15"
    elif weekday == 6:  # Sunday
        return 5.0, 1.0, 0.75, None, "09:00"






def get_bar_color(metric, percent):
    """
    Assigns intuitive colors for chart bars based on goal vs limit logic.
    - Green = good
    - Yellow = close to threshold
    - Orange = warning
    - Red = fail
    """
    try:
        pct = percent / 100.0

        if metric in ["Break", "Wrap Up"]:  # Limit-based (lower is better)
            if pct < 0.5:
                return "#00FF6E"   # Green
            elif pct < 0.75:
                return "#FFD700"   # Yellow
            elif pct < 0.95:
                return "#FF8C00"   # Orange
            else:
                return "#FF3B3B"   # Red

        else:  # Goal-based (higher is better)
            if pct >= 0.95:
                return "#00FF6E"   # Green
            elif pct >= 0.75:
                return "#FFD700"   # Yellow
            elif pct >= 0.5:
                return "#FF8C00"   # Orange
            else:
                return "#FF3B3B"   # Red

    except:
        return "#999999"  # Gray fallback on error

def calculate_ttg_value(tc, br, wr, mismatch_amount, report_date):
    """Calculate Time To Goal for aggregated rows."""
    goal_time, break_limit, wrap_limit, _, _ = get_daily_time_goals(report_date)

    extra_break = max(0, br - break_limit)
    extra_wrap = max(0, wr - wrap_limit)

    available_break = max(0, break_limit - br)
    available_wrap = max(0, wrap_limit - wr)

    wrap_offset = min(extra_wrap, available_break)
    break_offset = min(extra_break, available_wrap)

    extra_wrap -= wrap_offset
    extra_break -= break_offset

    total_penalty = extra_break + extra_wrap

    ttg = (tc - goal_time - total_penalty) - mismatch_amount
    adjusted = mismatch_amount > 0
    return ttg, adjusted


def format_mismatch(mismatch_amount):
    if mismatch_amount > 0:
        return f"⚠️ +{decimal_to_hhmmss_nosign(mismatch_amount)}"
    return "✅"


def insert_total_rows(df, report_date):
    """Insert aggregated total rows for agents with duplicates."""
    result = []
    numeric_cols = [
        "Time Connected", "Break", "Talk Time", "Wrap Up", "Sales", "_MismatchAmount"
    ]

    for agent, group in df.groupby("Agent", sort=False):
        for _, row in group.iterrows():
            result.append(row.to_dict())

        if len(group) > 1:
            print(f"🧪 GROUPED AGENT: {agent}")
            print(group[["Agent", "Sales", "Break", "Wrap Up", "Talk Time", "Time Connected"]])

            print(f"➡️ Creating TOTAL row for: {agent}")
            print("RAW SALES:", group["Sales"].tolist())
            print("RAW BREAK:", group["Break"].tolist())
            print("RAW WRAP:", group["Wrap Up"].tolist())
            print("RAW TALK:", group["Talk Time"].tolist())
            print("RAW TC:", group["Time Connected"].tolist())

            total_row = group.iloc[-1].copy()

            for col in numeric_cols:
                if col in group.columns:
                    clean_vals = pd.to_numeric(group[col], errors="coerce").fillna(0)
                    total_row[col] = int(clean_vals.sum()) if col == "Sales" else clean_vals.sum()


            


            if "1st Call" in group.columns:
                try:
                    total_row["1st Call"] = group["1st Call"].min()
                except Exception:
                    pass
            if "Shift End" in group.columns:
                try:
                    total_row["Shift End"] = group["Shift End"].max()
                except Exception:
                    pass

            mismatch_sum = total_row.get("_MismatchAmount", 0)
            total_row["Time Mismatch"] = format_mismatch(mismatch_sum)

            ttg, adjusted = calculate_ttg_value(
                total_row.get("Time Connected", 0),
                total_row.get("Break", 0),
                total_row.get("Wrap Up", 0),
                mismatch_sum,
                report_date,
            )


            print(f"🧪 GROUPED AGENT: {agent}")
            print(group[["Agent", "Sales", "Break", "Wrap Up", "Talk Time", "Time Connected"]])

            total_row["Time To Goal"] = ttg
            total_row["_TTG_Adjusted"] = adjusted
            total_row["is_total"] = True
            total_row["Server"] = "Total"

            result.append(total_row.to_dict())

    new_df = pd.DataFrame(result)
    new_df.index = range(1, len(new_df) + 1)
    return new_df










#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === CONSTANTS / GLOBAL SETTINGS ===


# 🔁 Maps raw CSV headers to cleaned, standardized column names
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

# 🎯 Column order used for displaying processed data (UI and exports)
DISPLAY_COLUMN_ORDER = [
    "Sales", "Server", "1st Call", "Shift End", "Agent", "Time To Goal", "Time Connected",
    "Break", "Talk Time", "Wrap Up", 
    "Time Mismatch", "_MismatchAmount", "_TTG_Adjusted"
]






#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === DATA CLEANING / VALIDATION ===

def detect_inconsistencies(df):
    """
    Flags rows where 'Time Connected' exceeds the actual shift duration by >10 minutes.

    - Uses 1st Call and Shift End as reference points.
    - Calculates max possible shift time and compares to Time Connected.
    - Adds three columns:
        - 'Time Mismatch' (✅ or ⚠️ +HH:MM:SS)
        - '_Debug' (internal trace string)
        - '_MismatchAmount' (excess time in decimal hours)

    Parameters:
        df (pd.DataFrame): DataFrame with agent time data

    Returns:
        pd.DataFrame: Updated DataFrame with mismatch flags and debug columns
    """

    def check_mismatch(row):
        try:
            # 🕒 Extract time components (drop date)
            start_str = str(row.get("1st Call")).split()[-1]
            end_str = str(row.get("Shift End")).split()[-1]

            start_time = datetime.strptime(start_str, "%I:%M%p")
            end_time = datetime.strptime(end_str, "%I:%M%p")

            # 🧠 Use fixed date to compute timedelta
            dummy_date = datetime(2000, 1, 1)
            start_dt = dummy_date.replace(hour=start_time.hour, minute=start_time.minute)
            end_dt = dummy_date.replace(hour=end_time.hour, minute=end_time.minute)

            if end_dt < start_dt:
                end_dt += timedelta(days=1)  # Handle overnight shifts

            max_possible = (end_dt - start_dt).total_seconds() / 3600
            time_connected = row.get("Time Connected", 0)

            if pd.isna(time_connected):
                return "⚠️ Missing", "Missing Time Connected", 0

            diff = time_connected - max_possible

            def format_diff_to_hms(hours):
                total_seconds = int(hours * 3600)
                h = total_seconds // 3600
                m = (total_seconds % 3600) // 60
                s = total_seconds % 60
                return f"{h:02}:{m:02}:{s:02}"

            # 🚨 Flag if difference > 10 min
            if diff > 0:
                visible = f"⚠️ +{format_diff_to_hms(diff)}"
                mismatch_amount = diff
            else:
                visible = "✅"
                mismatch_amount = 0

            debug = f"{row['Agent']} | TC: {time_connected:.2f} | Max: {max_possible:.2f} | Diff: {diff:.2f}"
            return visible, debug, mismatch_amount

        except Exception as e:
            return "⚠️", f"{row.get('Agent', 'Unknown')} | Error: {e}", 0

    # 🧪 Apply to all rows and unpack results into 3 columns
    results = df.apply(check_mismatch, axis=1)
    df["Time Mismatch"] = results.apply(lambda x: x[0])
    df["_Debug"] = results.apply(lambda x: x[1])
    df["_MismatchAmount"] = results.apply(lambda x: x[2])

    return df




def classify_office(agent_name):
    if isinstance(agent_name, str):
        if agent_name.startswith("n "): return "Tepic"
        if agent_name.startswith("a "): return "Army"
        if agent_name.startswith("w "): return "West"
        if agent_name.startswith("sp ") or agent_name.startswith("pr "): return "Sp & Prime"
        if agent_name.startswith("e "): return "Egypt"
        if agent_name.startswith("s "): return "Spanish"
        if agent_name.startswith("g "): return "Pakistan"
    return "Other"




#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === CORE DATA PROCESSING ===


def load_and_process_data(uploaded_dfs, report_date):
    """
    Processes all uploaded CSV files and returns cleaned, enriched data grouped by server.

    Each file represents a different ReadyMode server. The function:
        - Adds report date
        - Drops footer rows
        - Renames columns
        - Converts time fields to decimal hours
        - Flags time mismatches
        - Calculates Time To Goal (TTG)
        - Assigns Office based on Login ID
        - Ensures consistent column order

    Parameters:
        uploaded_dfs (List[Tuple[str, pd.DataFrame]]): List of (filename, DataFrame) tuples
        report_date (datetime): The selected report date

    Returns:
        Dict[str, pd.DataFrame]: Dictionary of DataFrames keyed by "Server 1", "Server 2", etc.
    """
    combined_data = {}
    server_number = 1  # Start count at Server 1

    for file_name, df in uploaded_dfs:
        df["Report Date"] = report_date.strftime("%Y-%m-%d")

        # Drop last row if totals or empty
        df = df[:-1] if len(df) > 0 else df
        df = df.dropna(how="all")


        # Normalize raw agent names: remove non-breaking and trailing spaces
        if "Login ID" in df.columns:
            df["Login ID"] = (
                df["Login ID"]
                .astype(str)
                .str.replace("\u00A0", " ", regex=False)  # replace non-breaking space
                .str.replace(r"\s+", " ", regex=True)     # collapse weird spacing
                .str.strip()                              # remove leading/trailing
            )


        # 🆕 Detect Chase data (column 'Agente' is unique to Chase files)
        if "Agente" in df.columns:
            # 1) Load & rename chase columns
            df = load_chase_data(df)
            df["Report Date"] = report_date.strftime("%Y-%m-%d")

            # 2) Convert all time columns into decimal hours
            for col in ["Time Connected", "Break", "Talk Time", "Wrap Up"]:
                if col in df.columns:
                    df[col] = df[col].apply(time_string_to_decimal)

            # 3) Compute Time To Goal (TTG) for Chase rows
            goal_time, break_limit, wrap_limit, _, _ = get_daily_time_goals(report_date)
            def calculate_ttgs_chase(row):
                tc = row.get("Time Connected", 0)
                br = row.get("Break", 0)
                wr = row.get("Wrap Up", 0)
                # same cross-compensation you use elsewhere
                extra_break = max(0, br - break_limit)
                extra_wrap  = max(0, wr - wrap_limit)
                available_break = max(0, break_limit - br)
                available_wrap  = max(0, wrap_limit - wr)
                wrap_offset = min(extra_wrap, available_break)
                break_offset= min(extra_break, available_wrap)
                extra_wrap  -= wrap_offset
                extra_break -= break_offset
                total_penalty   = extra_break + extra_wrap
                mismatch_penalty = 0
                ttg = (tc - goal_time - total_penalty) - mismatch_penalty
                return pd.Series([ttg, False])
            df[["Time To Goal", "_TTG_Adjusted"]] = df.apply(calculate_ttgs_chase, axis=1)

            # 4) Label & finalize
            df["Server"] = "Chase"
            df["Office"] = df["Agent"].apply(classify_office)
            for col in DISPLAY_COLUMN_ORDER:
                if col not in df.columns:
                    df[col] = ""
            df = df[[c for c in DISPLAY_COLUMN_ORDER if c in df.columns]
                    + ["Office", "Report Date", "Server"]]
            df = df.sort_values(by="Agent", ascending=True)
            df.index = range(1, len(df) + 1)

            combined_data["Chase"] = df
            continue



        # Rename columns using global mapping
        df.rename(columns=COLUMN_RENAME_MAP, inplace=True)


        # Convert time-related columns to decimal format
        for col in ["Time Connected", "Break", "Talk Time", "Wrap Up"]:
            if col in df.columns:
                df[col] = df[col].apply(time_string_to_decimal)

        # Flag time mismatches between shift and reported time
        df = detect_inconsistencies(df)

        # Load required values for Time To Goal logic
        goal_time, break_limit, wrap_limit, _, _ = get_daily_time_goals(report_date)


        # Time To Goal (TTG) calculation per row
        def calculate_ttgs(row):
            tc = row.get("Time Connected", 0)
            br = row.get("Break", 0)
            wr = row.get("Wrap Up", 0)

            extra_break = max(0, br - break_limit)
            extra_wrap = max(0, wr - wrap_limit)

            available_break = max(0, break_limit - br)
            available_wrap = max(0, wrap_limit - wr)

            # Apply cross-compensation: only once each
            wrap_offset = min(extra_wrap, available_break)
            break_offset = min(extra_break, available_wrap)

            extra_wrap -= wrap_offset
            extra_break -= break_offset



            total_penalty = extra_break + extra_wrap
            mismatch_penalty = row.get("_MismatchAmount", 0)

            ttg = (tc - goal_time - total_penalty) - mismatch_penalty
            adjusted = mismatch_penalty > 0
            return pd.Series([ttg, adjusted])

        df[["Time To Goal", "_TTG_Adjusted"]] = df.apply(calculate_ttgs, axis=1)


        df["Office"] = df["Agent"].apply(classify_office)


        # ✅ Extract actual server number from the file name
        match = re.search(r"automation(\d+)", file_name.lower())
        server_number_str = match.group(1) if match else "?"

        df["Server"] = f"Server {server_number_str}"

        # Ensure all display columns exist
        for col in DISPLAY_COLUMN_ORDER:
            if col not in df.columns:
                df[col] = ""

        # Final column list + metadata
        columns_to_keep = [col for col in DISPLAY_COLUMN_ORDER if col in df.columns]
        for meta_col in ["Office", "Report Date", "Server"]:
            if meta_col in df.columns:
                columns_to_keep.append(meta_col)

        df = df[columns_to_keep]

        # Sort and reindex
        df = df.sort_values(by="Agent", ascending=True)
        df.index = range(1, len(df) + 1)

        # Store under server name
        
        combined_data[f"Server {server_number_str}"] = df
        server_number += 1


    for df_name, df in combined_data.items():
        for col in ["Sales", "Break", "Wrap Up", "Talk Time", "Time Connected"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)


    return combined_data







#-------------------------------------------------------------------------------------------------------------------------------------------------------------
### === EXPORT: PDF / EMAIL ===



def send_email(to_email, subject, body, attachment_path=None, from_email=None):
    """
    Sends an email with optional PDF attachment using Brevo SMTP.


    Relies on SENDGRID_API_KEY stored in environment variables.

    Parameters:
        to_email (str): Recipient email address
        subject (str): Email subject line
        body (str): Email body text (plain)
        attachment_path (str, optional): Path to PDF file to attach
        from_email (str, optional): Sender address override

    Returns:
        Tuple[bool, str]: Success flag and message string
    """
    try:
        # Load .env and get the API key
        load_dotenv()
        smtp_server = os.getenv("BREVO_SMTP_SERVER", "smtp-relay.brevo.com")
        smtp_port = int(os.getenv("BREVO_SMTP_PORT", 587))
        smtp_user = os.getenv("BREVO_SMTP_USER")
        smtp_pass = os.getenv("BREVO_SMTP_PASS")

        if not smtp_user or not smtp_pass:
            return False, "❌ Missing Brevo SMTP credentials in environment variables."
        from_email = from_email or "cecilio@marketingleads.com.mx"

        


        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.application import MIMEApplication

        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        if attachment_path:
            with open(attachment_path, "rb") as f:
                part = MIMEApplication(f.read(), _subtype="pdf")
                part.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment_path))
                msg.attach(part)

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, to_email, msg.as_string())

        return True, f"✅ Email sent to {to_email}"



    except Exception as e:
        return False, f"❌ Error: {e}"




def export_html_pdf(grouped_data, output_path, chart_folder):
    from collections import Counter

    html_blocks = []

    # Add header + goal paragraph using any row
    sample_row = next(iter(grouped_data.values()))[["Report Date"]].iloc[0]
    report_date = pd.to_datetime(sample_row["Report Date"])
    goal_time, break_limit, wrap_limit, talk_goal, _ = get_daily_time_goals(report_date)
    goal_time = decimal_to_hhmmss_nosign(goal_time)
    break_limit = decimal_to_hhmmss_nosign(break_limit)
    wrap_limit = decimal_to_hhmmss_nosign(wrap_limit)
    talk_goal = decimal_to_hhmmss_nosign(talk_goal)



    goal_paragraph = f"""
    <p style="margin-bottom: 8px;">
        Today’s goal is to ensure all agents complete their Logged In Time, avoid exceeding Break or Wrap-Up time, and reach the minimum Talk Time.
        More time on the phones means more opportunities to sell.<br /><br />
        <strong>• Time Connected:</strong> {goal_time}<br />
        <strong>• Break Limit:</strong> {break_limit}<br />
        <strong>• Wrap-Up Limit:</strong> {wrap_limit}<br />
        <strong>• Talk Time Goal:</strong> {talk_goal}
    </p>
    <hr style="border: none; border-top: 2px solid #000; margin: 16px 0;" />
"""

#############---------------------------------------


    html_blocks.append(f"""
        <h1 style="color: #007acc; font-size: 30px; margin-bottom: 12px;">Daily Agent Report</h1>
        {goal_paragraph}
    """)





    for office_index, (office, office_df) in enumerate(grouped_data.items()):

        # ✅ Use only first login per agent for punctuality stats
        stats_df = (
            office_df.sort_values(by=["Agent", "1st Call"])
            .drop_duplicates(subset="Agent", keep="first")
        )


        stats_df = office_df.attrs.get("unique_summary_rows", office_df)

        total = len(stats_df)
        status_counts = Counter()
        for _, row in stats_df.iterrows():


            try:
                call_dt = pd.to_datetime(row["1st Call"] + f" {report_date.year}")
                _, _, _, _, shift_start = get_daily_time_goals(report_date)
                shift_time = datetime.strptime(shift_start, "%H:%M").time()
                shift_dt = call_dt.replace(hour=shift_time.hour, minute=shift_time.minute, second=0)
                delta = (call_dt - shift_dt).total_seconds() / 60
                if delta <= 0:
                    status_counts["on_time"] += 1
                elif delta <= 5:
                    status_counts["just_made_it"] += 1
                else:
                    status_counts["late"] += 1
            except:
                status_counts["late"] += 1

        on_pct = round((status_counts["on_time"] / total) * 100)
        just_pct = round((status_counts["just_made_it"] / total) * 100)
        late_pct = round((status_counts["late"] / total) * 100)



        break_style = "page-break-before: always;" if office_index > 0 else ""

        html_blocks.append(f"""
            <div style="{break_style}">
                <p style="font-size: 16px; line-height: 1.5; font-weight: bold;">
                    {office} — {total} agents connected<br />
                    <span style="color: green; font-weight: normal;">On Time: {on_pct}%</span><br />
                    <span style="color: #FFA500; font-weight: normal;">Just Made It: {just_pct}%</span><br />
                    <span style="color: red; font-weight: normal;">Late: {late_pct}%</span>
                </p>
                <hr style="border: none; border-top: 1px dashed #aaa; margin: 14px 0;" />
            </div>
        """)


        print(f"📤 EXPORTING {office} — {len(office_df)} agents")
        print(office_df[["Agent", "Sales", "Time Connected", "Break", "Wrap Up"]])


        for _, row in office_df.iterrows():
            ttg_val = row.get("Time To Goal", None)
            if pd.notna(ttg_val):
                ttg_str = decimal_to_hhmmss(ttg_val)
                ttg_color = "green" if ttg_val >= 0 else "red"
                ttg_str = f"<span style='color:{ttg_color}'>{ttg_str}</span>"
            else:
                ttg_str = "--:--:--"


            try:
                call_dt = pd.to_datetime(row["1st Call"] + f" {report_date.year}")
                _, _, _, _, shift_start = get_daily_time_goals(report_date)
                shift_time = datetime.strptime(shift_start, "%H:%M").time()
                shift_dt = call_dt.replace(hour=shift_time.hour, minute=shift_time.minute, second=0)
                delta = (call_dt - shift_dt).total_seconds() / 60
                mins = abs(int(delta))
                delay = f"{mins} min" if mins < 60 else f"{mins//60}h {mins%60}m"
                if delta <= 0:
                    status = f"<span style='color:green; font-weight:bold;'>On time ({delay} early)</span>"
                elif delta <= 5:
                    status = f"<span style='color:#FFA500; font-weight:bold;'>Just made it ({delay} late)</span>"
                else:
                    status = f"<span style='color:red; font-weight:bold;'>Late ({delay} late)</span>"
            except:
                status = "<span style='color:gray;'>Unknown</span>"

            sales = row.get("Sales", 0)    
            # 🔵 Label logic
            agent = row["Agent"]
            is_total = row.get("is_total") is True
            server_label = row.get("Server", "")
            server_number = ""

            if is_total:
                agent_label = f"{agent} (Total)"
            else:
                if server_label.startswith("Server") and server_label[-1].isdigit():
                    server_number = server_label[-1]
                else:
                    server_number = "?"
                agent_label = f"{agent} on Server {server_number}"

            # Chart image path
            chart_filename = f"{agent.replace(' ', '_')}_{row.name}.png"
            chart_path = os.path.abspath(os.path.join(chart_folder, chart_filename))

            # Final HTML block
            html_blocks.append(f"""
            <table style="page-break-inside: avoid; width: 100%; margin-bottom: 20px;">
                <tr>
                    <td style="font-size: 15px; line-height: 1.5;">
                        <strong style="font-size: 16px; color: #000;">{agent_label}</strong><br />
                        {status}<br />
                        <strong>Sales:</strong> {sales}<br />
                        <strong>Time To Goal:</strong> {ttg_str}<br /><br />
                        <img src="{chart_path}" style="width: 100%; margin-top: 0px;" /><br />
                        <div style="border-top: 1px solid #ddd; margin: 12px 0;"></div>
                    </td>
                </tr>
            </table>
            """)




    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: Helvetica, Arial, sans-serif;
            font-size: 15px;
            color: #111;
            background-color: #fff;
            margin: 0;
            padding: 20px 25px;
        }}
        h1 {{
            color: #007acc;
            font-size: 22px;
            margin-bottom: 10px;
        }}
        h2 {{
            font-size: 18px;
            color: #333;
            margin: 14px 0 6px 0;
        }}
        p, li {{
            font-size: 15px;
            line-height: 1.4;
            margin: 4px 0;
        }}
        ul {{
            padding-left: 20px;
            margin-bottom: 10px;
        }}
    </style>
</head>
<body>
    {''.join(html_blocks)}
</body>
</html>"""

    with open(output_path, "wb") as f:
        pisa.CreatePDF(src=full_html, dest=f)


















## === VISUALIZATION HELPERS (PDF/Charts) ===
def build_agent_html_section(row, chart_path):
    report_date = pd.to_datetime(row["Report Date"])
    goal_time, break_limit, wrap_limit, talk_time_goal, shift_start = get_daily_time_goals(report_date)

    def format_time(val):
        return decimal_to_hhmmss_nosign(val) if pd.notna(val) else "--:--:--"

    def format_time_signed(val):
        return decimal_to_hhmmss(val) if pd.notna(val) else "--:--:--"

    # === Time To Goal display (signed format always shown) ===
    time_to_goal_display = format_time_signed(row.get("Time To Goal", None))

    # === Clock-in analysis ===
    first_call_str = str(row.get("1st Call", ""))
    try:
        call_dt = pd.to_datetime(first_call_str + f" {report_date.year}")
        shift_time_obj = datetime.strptime(shift_start, "%H:%M").time()
        shift_dt = call_dt.replace(hour=shift_time_obj.hour, minute=shift_time_obj.minute, second=0)

        delta_minutes = (call_dt - shift_dt).total_seconds() / 60
        minutes_abs = abs(int(delta_minutes))
        direction = "early" if delta_minutes < 0 else "late"

        hours = minutes_abs // 60
        minutes = minutes_abs % 60
        readable_delay = f"{hours}h {minutes}m" if hours > 0 else f"{minutes} min"

        if delta_minutes <= 0:
            status_label = "On time"
            status_color = "#00aa00"
        elif delta_minutes <= 5:
            status_label = "Just made it"
            status_color = "#FFD700"
        else:
            status_label = "Late"
            status_color = "#cc0000"

        inline_status = f"<span style='color:{status_color}; font-weight:bold'>{status_label} ({readable_delay} {direction})</span>"

    except Exception:
        inline_status = "<span style='color:#000000; font-weight:bold'>Clock-in unknown</span>"

    # 🔵 Label logic: human-readable agent_label (Total / Chase / Server N / fallback)
    agent = row["Agent"]
    is_total = row.get("is_total") is True
    server_label = row.get("Server", "")
    if is_total:
        agent_label = f"{agent} (Total)"
    elif server_label == "Chase":
        agent_label = f"{agent} (Chase)"
    elif server_label.startswith("Server") and server_label[-1].isdigit():
        agent_label = f"{agent} on {server_label}"
    else:
        agent_label = f"{agent} on {server_label}"

    # === Final HTML block ===
    return f"""
    <table style="width: 100%; border-spacing: 20px 10px; margin-bottom: 20px; page-break-inside: avoid;">
        <tr>
            <td style="vertical-align: top; width: 48%;">
                <div style="font-family: Helvetica, Arial, sans-serif; font-size: 18px; font-weight: bold; color: #000000; line-height: 1.5;">
                    <h2 style="margin: 0 0 8px 0; font-size: 18px; color: #007acc;">
                        {agent_label} {inline_status}
                    </h2>
                    <p style="margin: 4px 0 12px 0;"><strong>Time To Goal:</strong> {time_to_goal_display}</p>
                    <p style="margin: 4px 0;"><strong>Sales:</strong> {row.get('Sales', 0)}</p>
                    <p style="margin: 4px 0;"><strong>- Time Connected Goal:</strong> {format_time(goal_time)}</p>
                    <p style="margin: 4px 0;"><strong>- Break Limit:</strong> {format_time(break_limit)}</p>
                    <p style="margin: 4px 0;"><strong>- Wrap-Up Limit:</strong> {format_time(wrap_limit)}</p>
                    <p style="margin: 4px 0;"><strong>- Talk Time Goal:</strong> {format_time(talk_time_goal)}</p>
                </div>
            </td>
            <td style="vertical-align: top; width: 52%;">
                <img src="{chart_path}" style="width: 100%; border: none; border-radius: 6px;" />
            </td>
        </tr>
    </table>
    <hr style="border: none; border-top: 1px solid #ccc; margin: 30px 0;" />
    """





def build_export_figure(row, color_override=None):
    # Extract time goals
    report_date = pd.to_datetime(row["Report Date"])
    goal_time, break_limit, wrap_limit, talk_time_goal, shift_start = get_daily_time_goals(report_date)

    goals = {
        "Talk Time": talk_time_goal,
        "Break": break_limit,
        "Wrap Up": wrap_limit,
        "Time Connected": goal_time
    }
    metrics = {
        "Talk Time": row.get("Talk Time", 0),
        "Break": row.get("Break", 0),
        "Wrap Up": row.get("Wrap Up", 0),
        "Time Connected": max(0, row.get("Time Connected", 0) - row.get("_MismatchAmount", 0))
    }

    def format_time(val):
        return decimal_to_hhmmss_nosign(val) if pd.notna(val) else "--:--:--"

    fig = go.Figure()

    for metric, value in metrics.items():
        try:
            percent = round((value / goals[metric]) * 100) if pd.notna(value) and pd.notna(goals[metric]) and goals[metric] != 0 else 0
        except Exception:
            percent = 0

        bar_value = min(percent, 150)
        bar_color = color_override if color_override else get_bar_color(metric, percent)
        text_display = f"{format_time(value)} / {format_time(goals[metric])}"

        # Text logic: inside if >=50%, otherwise outside
        text_position = "inside" if percent >= 50 else "outside"

        fig.add_trace(go.Bar(
            x=[bar_value],
            y=[metric],
            orientation='h',
            text=[text_display],
            textposition=text_position,
            textfont=dict(color="black", size=28),
            marker=dict(
                color=bar_color,
                line=dict(color='rgba(0,0,0,0.25)', width=1),
            ),
            hoverinfo='skip'
        ))

    # Add goal line
    fig.add_shape(
        type="line",
        x0=100,
        x1=100,
        y0=-0.5,
        y1=len(metrics) - 0.5,
        line=dict(color="black", width=2)
    )

    fig.update_layout(
        height=500,
        width=1000,
        margin=dict(l=160, r=40, t=40, b=40),
        font=dict(
            family="Helvetica, Arial, sans-serif",
            size=22,
            color="black"
        ),
        xaxis=dict(
            title="Progress (%)",
            title_font=dict(size=22),
            tickfont=dict(size=20),
            range=[0, 150],
            automargin=True
        ),
        yaxis=dict(
            tickfont=dict(size=28),
            title=None,
            automargin=True
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )

    return fig








def build_progress_figure(row, unique_key_suffix=None, color_override=None):
    """
    Builds a horizontal bar chart showing agent progress vs daily goals.
    Used in both Streamlit UI (via render_agent_block) and during PDF export.
    """

    # Extract time goals for the day
    report_date = pd.to_datetime(row["Report Date"])
    goal_time, break_limit, wrap_limit, talk_time_goal, shift_start = get_daily_time_goals(report_date)

    # Map goals and actuals
    goals = {
        "Talk Time": talk_time_goal,
        "Break": break_limit,
        "Wrap Up": wrap_limit,
        "Time Connected": goal_time
    }
    metrics = {
        "Talk Time": row.get("Talk Time", 0),
        "Break": row.get("Break", 0),
        "Wrap Up": row.get("Wrap Up", 0),
        "Time Connected": max(0, row.get("Time Connected", 0) - row.get("_MismatchAmount", 0))
    }

    def format_time(val):
        return decimal_to_hhmmss_nosign(val) if pd.notna(val) else "--:--:--"

    fig = go.Figure()
    annotations = []

    for metric, value in metrics.items():
        try:
            percent = round((value / goals[metric]) * 100) if pd.notna(value) and pd.notna(goals[metric]) and goals[metric] != 0 else 0
        except Exception:
            percent = 0

        bar_value = min(percent, 150)  # Visually cap bar but reflect overage
        bar_color = color_override if color_override else get_bar_color(metric, percent)


        text_display = (
            f"{format_time(value)} / {format_time(goals[metric])}"
            if pd.notna(value) and pd.notna(goals[metric])
            else "No data"
        )

        fig.add_trace(go.Bar(
            x=[bar_value],
            y=[metric],
            orientation='h',
            marker=dict(
                color=bar_color,
                line=dict(color='rgba(0,0,0,0.25)', width=1),
            ),
            hoverinfo='x'
        ))

        ann_x = bar_value / 2 if percent >= 50 else bar_value + 5
        ann_align = 'center' if percent >= 50 else 'left'

        annotations.append(dict(
            x=ann_x,
            y=metric,
            text=text_display,
            showarrow=False,
            font=dict(color='#444', size=13),
            xanchor=ann_align,
            yanchor='middle'
        ))

    # Add solid vertical line at 100% goal threshold
    fig.add_shape(
        type="line",
        x0=100,
        x1=100,
        y0=-0.5,
        y1=len(metrics) - 0.5,
        line=dict(color="white", width=2)
    )

    fig.update_layout(
        xaxis=dict(
            range=[0, 150],
            title="Progress (%)",
            gridcolor="rgba(200,200,200,0.25)",
            dtick=20,
            showline=False,
            zeroline=False
        ),
        yaxis=dict(
            automargin=True,
            tickfont=dict(size=14),
            title=None,
        ),
        height=280,
        margin=dict(l=100, r=20, t=20, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        annotations=annotations
    )

    return fig, goals
