# main.py

import streamlit as st
from datetime import datetime
from data_processor import load_and_process_data
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import pytz

SHEET_ID = "1fgd07CflSVeQ5SSlmHKt8-i-tXmz89ILn-uQaPeYVto"




# Dropdown label → column name
SORT_MAP = {
    "Agent Name": "Agent",
    "Talk Time": "Talk Time",
    "Break Time": "Break",
    "Wrap Up": "Wrap Up",
    "Sales": "Sales"
}

# Column name → sort order (True = ascending, False = descending)
SORT_DIRECTION = {
    "Agent": True,
    "Talk Time": False,
    "Break": True,
    "Wrap Up": True,
    "Sales": False
}



def sort_dataframe(df, selected_column):
    """
    Sorts the DataFrame by the selected column or columns.
    Handles normal columns and composite logic like Break & Wrap Up.
    """

    # 🔧 Ensure 'Sales' is numeric for proper sorting
    if "Sales" in df.columns:
        df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce").fillna(0).astype(int)

    if isinstance(selected_column, list):
        # Special case: Break & Wrap Up → sum both columns and sort ascending
        if set(selected_column) == {"Break", "Wrap Up"}:
            df["_BreakWrapSum"] = df["Break"] + df["Wrap Up"]
            df = df.sort_values(by="_BreakWrapSum", ascending=True)
            df.drop(columns="_BreakWrapSum", inplace=True)
        else:
            # Default fallback for multi-column sorts
            df = df.sort_values(by=selected_column, ascending=False)
    elif selected_column:
        ascending = SORT_DIRECTION.get(selected_column, False)
        df = df.sort_values(by=selected_column, ascending=ascending)
    else:
        df = df.sort_values(by="Agent", ascending=True)

    return df




def format_time_columns(df):
    time_columns = ["Time Connected", "Break", "Talk Time", "Wrap Up", "Time To Goal"]
    for col in time_columns:
        if col in df.columns:
            if col == "Time To Goal":
                df[col] = df.apply(
                    lambda row: decimal_to_hhmmss(row[col]) + (" ⚙️" if row.get("_TTG_Adjusted") else ""),
                    axis=1
                )
            else:
                df[col] = df[col].apply(decimal_to_hhmmss_nosign)  # remove sign
    return df



def display_aps_summary(grouped_data, grouping_mode):
    """
    Display APS summary in the sidebar for each group: server or office.
    """

    st.sidebar.markdown("### 📊 APS Summary by Group")

    total_agents = 0
    total_with_sales = 0
    total_sales = 0

    # --- Group by Office Logic ---
    if grouping_mode == "Group By Office":
        # Merge all dataframes into one
        combined_df = pd.concat(grouped_data.values(), ignore_index=True)
        combined_df["Sales"] = pd.to_numeric(combined_df["Sales"], errors="coerce").fillna(0).astype(int)

        # Group by Office name
        grouped = combined_df.groupby("Office")

        for office_name, df in grouped:
            agents = len(df)
            with_sales = df[df["Sales"] > 0].shape[0]
            sales = df["Sales"].sum()
            aps = sales / agents if agents else 0
            pct_with_sales = round((with_sales / agents) * 100) if agents else 0

            total_agents += agents
            total_with_sales += with_sales
            total_sales += sales

            st.sidebar.markdown(f"""
<div style="padding: 6px 0;">
    <div style="font-weight: 600; font-size: 14px;">🏢 {office_name} Office</div>
    <div style="font-size: 13px; margin-top: 2px;">{agents} agents logged in, {pct_with_sales}% have leads</div>
    <div style="font-size: 13px;">{sales} sales total – APS <strong>{aps:.2f}</strong></div>
</div>
<hr style="margin: 6px 0;">
""", unsafe_allow_html=True)

    # --- Group by Server Logic ---
    else:
        for group_name, df in grouped_data.items():
            df = df.copy()
            if "Sales" not in df.columns:
                continue

            df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce").fillna(0).astype(int)

            agents = len(df)
            with_sales = df[df["Sales"] > 0].shape[0]
            sales = df["Sales"].sum()
            aps = sales / agents if agents else 0
            pct_with_sales = round((with_sales / agents) * 100) if agents else 0

            total_agents += agents
            total_with_sales += with_sales
            total_sales += sales

            st.sidebar.markdown(f"""
<div style="padding: 6px 0;">
    <div style="font-weight: 600; font-size: 14px;">📂 {group_name}</div>
    <div style="font-size: 13px; margin-top: 2px;">{agents} agents logged in, {pct_with_sales}% have leads</div>
    <div style="font-size: 13px;">{sales} sales total – APS <strong>{aps:.2f}</strong></div>
</div>
<hr style="margin: 6px 0;">
""", unsafe_allow_html=True)

    # Company-wide total
    total_aps = total_sales / total_agents if total_agents else 0
    percent_with_sales = round((total_with_sales / total_agents) * 100) if total_agents else 0

    st.sidebar.markdown(f"""
<div style="padding: 8px 0;">
    <div style="font-weight: 600; font-size: 14px;">📦 Company Total</div>
    <div style="font-size: 13px; margin-top: 2px;">{total_agents} agents total, {percent_with_sales}% with sales</div>
    <div style="font-size: 13px;">{total_sales} sales overall – APS <strong>{total_aps:.2f}</strong></div>
</div>
""", unsafe_allow_html=True)






# this function converts decimal values to hh:mm:ss adding a positive or negative sign
def decimal_to_hhmmss(decimal_hours):
    if pd.isna(decimal_hours):
        return "❌"
    
    total_seconds = int(decimal_hours * 3600)
    sign = "-" if total_seconds < 0 else "+"
    total_seconds = abs(total_seconds)

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{sign}{hours:02}:{minutes:02}:{seconds:02}"


# this function also converts decimals to hh:mm:ss but no sign 
def decimal_to_hhmmss_nosign(decimal_hours):
    if pd.isna(decimal_hours):
        return "❌"
    
    total_seconds = int(abs(decimal_hours) * 3600)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"



import json
from google.oauth2.service_account import Credentials

def connect_to_gsheet(sheet_id):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_dict = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    return sheet

def create_unique_worksheet(sheet, base_name):
    existing_titles = [ws.title for ws in sheet.worksheets()]
    name = base_name
    counter = 1

    while name in existing_titles:
        name = f"{base_name} ({counter})"
        counter += 1

    worksheet = sheet.add_worksheet(title=name, rows="1000", cols="30")
    return worksheet

def export_df_to_sheet(df, worksheet):
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())








if "raw_data" not in st.session_state:
    st.session_state.raw_data = None
if "grouping_mode" not in st.session_state:
    st.session_state.grouping_mode = "Group By Server"
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = None




# Set page config (title and layout)
st.set_page_config(
    page_title="Agent Metrics Viewer",
    layout="wide",
    initial_sidebar_state="expanded"
)



# --- SIDEBAR (Left Panel) ---
with st.sidebar:
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] div[class^="css"] {
            padding-top: 0rem;
            padding-bottom: 0rem;
        }
        button[kind="secondary"] {
            margin-bottom: 4px !important;
        }
        div[data-testid="stRadio"] {
            margin-bottom: -12px;
            margin-top: -6px;
        }
        div[data-baseweb="select"] {
            margin-top: -10px;
        }
        /* 🔥 Force radio buttons to be inline */
        div[data-testid="stRadio"] > div {
            display: flex;
            gap: 8px;
            flex-direction: row !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )


    
    # Upload Box + Load Button
    uploaded_files = st.file_uploader(
        "Drop your CSVs below 👇",
        type="csv",
        accept_multiple_files=True,
        key="file_uploader"
    )

    if uploaded_files:
        st.session_state.uploaded_files = uploaded_files
        st.success("✅ Files uploaded successfully. Now click 'Load Today's Data'.")

    update = st.button("🚀 Load Today's Data", use_container_width=True)
    if update and st.session_state.uploaded_files:
        dfs = [pd.read_csv(f) for f in st.session_state.uploaded_files]
        st.session_state.raw_data = load_and_process_data(dfs)

        # Reset uploader so user can upload fresh files again
        st.session_state.uploaded_files = None
        st.rerun()



    # Grouping Toggle (now forced inline)
    grouping_mode = st.radio(
        label="",
        options=["Group By Server", "Group By Office"],
        index=0
    )

    # Sort Dropdown
    sort_criterion = st.selectbox(
        label="",
        options=["Agent Name", "Talk Time", "Break & Wrap Up", "Sales"],
        index=0
    )

    # Divider before APS
    st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True)

    # APS Summary
    if st.session_state.raw_data:
        display_aps_summary(st.session_state.raw_data, grouping_mode)

















# --- MAIN SECTION ---

# Show today's date at the top
today = datetime.today().strftime("%A, %B %d, %Y")
st.caption(f"🕒 Current Metrics: {today}")


# Title and subtitle
st.markdown("## 📊 **Daily Metrics Overview**")

SORT_MAP = {
    "Agent Name": "Agent",  # Default sort (ascending)
    "Talk Time": "Talk Time",  # Sorted descending
    "Break & Wrap Up": ["Break", "Wrap Up"],  # Composite sort
    "Sales": "Sales"  # Sorted descending
}


if st.session_state.raw_data:
    grouped_data = st.session_state.raw_data
    selected_column = SORT_MAP.get(sort_criterion)

    # $$ grouping_mode is a radio on the left menu, it can contain either Group By Server or Group By Office $$ 
    #  default value is group by server

    ############################################################## GROUP BY SERVER

    if grouping_mode == "Group By Server":

        for group_name, df in grouped_data.items():

            df = df.copy()

            # Sort by selected column (Agent by default)
            df = sort_dataframe(df, selected_column)

            # Convert decimals to hh:mm:ss for display
            df = format_time_columns(df)

             # Reset row numbers from 1
            df.index = range(1, len(df) + 1)

            # Section title
            st.markdown(f"### 📂 {group_name}")

            # Only show visible columns (hide internal ones like "_Debug")
            visible_columns = [col for col in df.columns if not col.startswith("_")]
            
            st.dataframe(df[visible_columns])



    ########################------------------------ ENDS GROUP BY SERVER

      ##                                                                                                            ##
    ##   ---------------this is the switch depending on what grouping mode is selected in the left menu ---------     ##
      ##                                                                                                            ##

    ############################################################### GROUP BY OFFICE

    elif grouping_mode == "Group By Office":

        #Filter out only the DataFrames that contain an 'Office' column
        valid_frames = [df for df in grouped_data.values() if "Office" in df.columns]

        if not valid_frames:
            st.error("❌ No valid data found with 'Office' column.")
        else:
            #Combine all the server DataFrames into one large DataFrame
            combined_df = pd.concat(valid_frames, ignore_index=True)

            # 3️⃣ Find all unique office names from the combined data
            offices = combined_df["Office"].dropna().unique()

            # 4️⃣ Loop through each office and create a table
            for office in sorted(offices):
                # Get only rows for that office
                office_df = combined_df[combined_df["Office"] == office].copy()

                # Optional: Drop the 'Office' column (title already shows it)
                office_df = office_df.drop(columns="Office")

                # 5️⃣ Sort based on selected dropdown value (Agent by default)
                office_df = sort_dataframe(office_df, selected_column)

                # 6️⃣ Convert decimal values into readable hh:mm:ss
                office_df = format_time_columns(office_df)

                # 7️⃣ Reset row numbering (1, 2, 3...)
                office_df.index = range(1, len(office_df) + 1)

                # 8️⃣ Display section title
                st.markdown(f"### 🏢 {office} Office")

                visible_columns = [col for col in office_df.columns if not col.startswith("_")]

                st.dataframe(office_df[visible_columns])



    ########################------------------------ ENDS GROUP BY OFFICE

def create_unique_worksheet(sheet, title):
    try:
        template = sheet.worksheet("Template")
        new_worksheet = template.duplicate(new_sheet_name=title)

        # ✅ Unhide the worksheet via batch_update
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
        return sheet.add_worksheet(title=title, rows=1000, cols=26)



def convert_time_columns_for_export(df):
    time_cols = ["Time To Goal", "Time Connected", "Break", "Talk Time", "Wrap Up"]
    for col in time_cols:
        if col in df.columns:
            df[col] = df[col].apply(decimal_to_hhmmss)  # this uses your existing time formatter
    return df

def decimal_to_hhmmss_string(decimal_hours):
    try:
        is_negative = decimal_hours < 0
        total_seconds = int(abs(decimal_hours) * 3600)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        formatted = f"{hours:02}:{minutes:02}:{seconds:02}"
        return f"-{formatted}" if is_negative else formatted
    except:
        return ""



if st.button("📤 Export to Google Sheets"):
    try:
        if not st.session_state.raw_data:
            st.error("❌ No data loaded. Please upload and load today's CSVs first.")
        else:
            all_data = pd.concat(st.session_state.raw_data.values(), ignore_index=True)
            all_data = all_data.fillna("")  # Fix for NaNs

            # Convert decimals to hh:mm:ss strings for key columns
            time_columns = ["Time To Goal", "Time Connected", "Break", "Talk Time", "Wrap Up"]
            for col in time_columns:
                if col in all_data.columns:
                    all_data[col] = all_data[col].apply(decimal_to_hhmmss_string)

            sheet = connect_to_gsheet(SHEET_ID)
            local_tz = pytz.timezone("America/Mexico_City")
            today_str = datetime.now(local_tz).strftime("%B %d %I:%M%p")
            worksheet = create_unique_worksheet(sheet, today_str)
            export_df_to_sheet(all_data, worksheet)
            st.success(f"✅ Exported to tab '{worksheet.title}' successfully!")
    except Exception as e:
        st.error(f"❌ Export failed: {e}")
