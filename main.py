import resource
import sys

if sys.platform == "win32":
    # Windows: raise the C-runtime max-stdio (default 512) up to 2048
    try:
        import ctypes
        crt = ctypes.CDLL("msvcrt")
        # you can pick any number up to 2048 here
        crt._setmaxstdio(2048)
    except Exception:
        pass
else:
    # macOS / Linux: raise the OS soft-limit up to the hard limit
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except Exception:
        pass






# === SYSTEM / CORE PYTHON ===
import os
import json
import base64
import shutil
import tempfile
from datetime import datetime
from io import BytesIO, StringIO
from dotenv import load_dotenv
load_dotenv()

import tempfile
import zipfile



# === STREAMLIT INTERFACE ===
import streamlit as st

# === DATA HANDLING ===
import pandas as pd
import pytz

# === PLOTTING ===
import plotly.graph_objects as go
import plotly.io as pio

# === GOOGLE SHEETS EXPORT ===
import gspread
from google.oauth2.service_account import Credentials

# === DROPBOX FILE LOADER ===
import dropbox

# === SUPABASE EXPORT ===
from supabase import create_client, Client

# === PDF EXPORT ===
#import pdfkit

# === SENDGRID EMAIL EXPORT ===
#from sendgrid import SendGridAPIClient
#from sendgrid.helpers.mail import (
#    Mail, Attachment, FileContent, FileName, FileType, Disposition
#)

# === LOCAL MODULES ===
from data_processor import (
    load_and_process_data,
    get_daily_time_goals,
    get_bar_color,
    get_latest_dropbox_csv,
    sort_dataframe,
    format_time_columns,
    build_progress_figure,
    decimal_to_hhmmss_nosign,
    export_html_pdf,
    send_email,
    decimal_to_hhmmss,
    build_export_figure,
    insert_total_rows,
    connect_to_gsheet,
    create_unique_worksheet,
    export_df_to_sheet
)


# === STREAMLIT PAGE CONFIG ===
# Sets the browser tab title, layout width, and sidebar visibility
st.set_page_config(
    page_title="Metrics Viewer",
    layout="wide",
    initial_sidebar_state="collapsed"
)


from PIL import Image

def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        encoded = base64.b64encode(img_file.read()).decode()
        return f"data:image/png;base64,{encoded}"

# Show logo at top
logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    logo_base64 = get_base64_image(logo_path)
    st.markdown(f"""
        <div style='text-align: left; padding-top: 10px; padding-bottom: 10px;'>
            <img src="{logo_base64}" style="max-width: 240px; width: 100%; height: auto;">
        </div>
    """, unsafe_allow_html=True)




# === PDFKIT CONFIGURATION ===
# Required for pdfkit to convert HTML → PDF using wkhtmltopdf
#PDFKIT_CONFIG = pdfkit.configuration(wkhtmltopdf="/usr/local/bin/wkhtmltopdf")


# === STREAMLIT SESSION STATE INITIALIZATION ===
# Ensures expected keys are present in session_state with default values

for key, default in {
    "raw_data": None,
    "uploaded_files": None,
    "pdf_paths": {},  # ← store generated PDFs here
    "pdf_ready": False,  # NEW: control when to auto-generate
    "pdf_ready_next_cycle": False  # NEW: one-cycle delay buffer
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


if st.session_state.get("pdf_ready_next_cycle"):
    st.session_state["pdf_ready"] = True
    st.session_state["pdf_ready_next_cycle"] = False


# === CONFIGURATION: EXTERNAL SERVICES ===
# Load all keys securely from environment variables

# --- Google Sheets (gspread) ---
SHEET_ID = os.getenv("GSHEET_SHEET_ID")

# --- Dropbox Access ---
DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")
DROPBOX_REFRESH_TOKEN = os.getenv("DROPBOX_REFRESH_TOKEN")
DROPBOX_APP_KEY = os.getenv("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.getenv("DROPBOX_APP_SECRET")
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/ReadyModeReports")


# --- Supabase Access ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)



# === CUSTOM UI STYLING (FONTS & CHARTS) ===
# Applies global font settings and Plotly adjustments
st.markdown("""
    <style>
    /* Global font for the entire app */
    .appview-container * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
                     Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif !important;
        font-size: 15px !important;
        line-height: 1.5;
    }

    /* Bold, clean heading styles */
    h1, h2, h3, h4 {
        font-weight: 600 !important;
    }

    /* Sidebar font tweak */
    section[data-testid="stSidebar"] {
        font-size: 14px !important;
    }

    /* Caption elements */
    .stCaption {
        font-size: 13px !important;
    }

    /* DataFrame tables */
    .stDataFrame table {
        font-size: 14px !important;
    }

    /* Plotly base font and labels */
    .js-plotly-plot .plotly {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
                     Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif !important;
        font-size: 14px !important;
    }

    .js-plotly-plot .xtick > text,
    .js-plotly-plot .ytick > text,
    .js-plotly-plot .legendtext {
        font-size: 13px !important;
    }
    </style>
""", unsafe_allow_html=True)





#-------------------------------------------------------------------------------------------------------------------------------------------------------------





# === reads from dropbox and log out status #$$$$$$$$$$$$$$$$$$$$$$$$$///////////// ----------------

# Expander for logging debug and load status (UI only)
log_expander = st.sidebar.expander(
    "📡 Attempting to load latest report from the cloud...",
    expanded=False
)
st.session_state.log_expander = log_expander  # Share across app

with log_expander:
    st.info("🧠 Trying to auto-load from Dropbox...")








# Report Date: Used to calculate correct daily goals
default_date = datetime.today()
report_date = st.sidebar.date_input("📅 Report Date", default_date)


# Visual separator
st.sidebar.markdown("---")


# === STEP 1: Try loading latest CSVs from Dropbox ===
try:
    files = get_latest_dropbox_csv(DROPBOX_FOLDER)
    with log_expander:
        if files:
            st.info(f"📄 Found files: {[name for name, _ in files]}")
        else:
            st.warning("⚠️ No CSV files found in Dropbox folder.")
except Exception as e:
    files = []
    with log_expander:
        st.error(f"❌ Dropbox error: {e}")


# === STEP 2: Parse CSVs into DataFrames + Process ===
if files:
    try:
        file_data_pairs = []
        with log_expander:
            st.info("📥 Reading CSV files into DataFrames...")

        for file_name, file_bytes in files:
            df = pd.read_csv(file_bytes)
            file_data_pairs.append((file_name, df))
            

        with log_expander:
            st.info("🔄 Processing data...")

        processed_data = load_and_process_data(file_data_pairs, report_date=report_date)

        st.session_state.raw_data = processed_data
        st.session_state["pdf_paths"] = {}
        st.session_state["pdf_ready_next_cycle"] = True
        st.session_state.dropbox_file_names = [name for name, _ in files]
        st.session_state["pdf_paths"] = {}  # Clear old PDFs
        


        with log_expander:
            st.success(f"📂 Loaded files: {st.session_state.dropbox_file_names}")

    except Exception as e:
        with log_expander:
            st.error(f"❌ Failed to process files: {e}")
else:
    with log_expander:
        st.warning("⚠️ No data loaded from Dropbox.")














# === drop box for csv uploads #$$$$$$$$$$$$$$$$$$$$$$$$$///////////// ----------------
with st.sidebar:
    # --- Inject custom CSS tweaks ---
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
            margin-top: -6px;
            margin-bottom: -12px;
        }
        div[data-baseweb="select"] {
            margin-top: -10px;
        }
        /* 🔥 Inline radio buttons */
        div[data-testid="stRadio"] > div {
            display: flex;
            gap: 8px;
            flex-direction: row !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    
    
    
    # --- File Uploader ---
    uploaded_files = st.file_uploader(
        "Drop your CSVs below 👇",
        type="csv",
        accept_multiple_files=True,
        key="file_uploader"
    )


    if uploaded_files:
        st.session_state.uploaded_files = uploaded_files
        st.success("✅ Files uploaded successfully. Now click 'Load Today's Data'.")

    # --- Trigger data load ---
    update = st.button("📈 Create Report From CSVs", use_container_width=True)

    if update and st.session_state.uploaded_files:
        try:
            file_data_pairs = [
                (f.name, pd.read_csv(f)) for f in st.session_state.uploaded_files
            ]

            # Process & store results
            st.session_state.raw_data = load_and_process_data(
                file_data_pairs, report_date=report_date
            
            )
            st.session_state["pdf_paths"] = {}  # Clear old PDFs


            st.success("✅ Data processed successfully!")
            st.session_state["pdf_paths"] = {}
            st.session_state["pdf_ready_next_cycle"] = True



        except Exception as e:
            st.error(f"❌ Failed to process uploaded files: {e}")

        # Reset uploader so user can upload fresh files again
        st.session_state.uploaded_files = None
        st.rerun()





#-------------------------------------------------------------------------------------------------------------------------------------------------------------






# === GLOBAL SORTING SETUP ===
# Allows user to choose how to sort agents in the dashboard

# Sidebar dropdown for sort selection
sort_criterion = st.sidebar.selectbox(
    label="📌 Sort agents by:",
    options=[
        "Agent Name",         # A-Z sort
        "Talk Time",          # Descending (high talk time)
        "Break & Wrap Up",    # Ascending (low total break+wrap time)
        "Sales"               # Descending (more sales = better)
    ],
    index=0  # Default is "Agent Name"
)

# Mapping from display label → actual column(s) used for sorting
SORT_MAP = {
    "Agent Name": "Agent",                     # Sort A-Z
    "Talk Time": "Talk Time",                 # Sort high to low
    "Break & Wrap Up": ["Break", "Wrap Up"],  # Composite sum, sort low to high
    "Sales": "Sales"                          # Sort high to low
}

# Get the actual column(s) to sort the DataFrame later
selected_column = SORT_MAP.get(sort_criterion)




















#-------------------------------------------------------------------------------------------------------------------------------------------------------------


SORT_DIRECTION = {
    "Agent": True,
    "Talk Time": False,
    "Break": True,
    "Wrap Up": True,
    "Sales": False
}

def render_agent_block(row, unique_key_suffix=None):
    if row.get("is_total") is True:
        fig, goals = build_progress_figure(row, unique_key_suffix, color_override="#1E88E5")
    else:
        fig, goals = build_progress_figure(row, unique_key_suffix)

    report_date = pd.to_datetime(row["Report Date"])
    goal_time, break_limit, wrap_limit, talk_time_goal, shift_start = get_daily_time_goals(report_date)

    # === Clock-in punctuality analysis ===
    first_call_str = str(row.get("1st Call", ""))
    try:
        #print(f"📞 1st Call (raw): {first_call_str}")
        call_dt = pd.to_datetime(first_call_str + f" {report_date.year}")
        #print(f"📅 Full datetime string: {call_dt}")

        #print(f"⏰ Shift start (raw): {shift_start}")
        shift_time_obj = datetime.strptime(shift_start, "%H:%M").time()
        shift_dt = call_dt.replace(hour=shift_time_obj.hour, minute=shift_time_obj.minute, second=0)

        delta_minutes = (call_dt - shift_dt).total_seconds() / 60
        minutes_abs = abs(int(delta_minutes))
        direction = "early" if delta_minutes < 0 else "late"

        hours = minutes_abs // 60
        minutes = minutes_abs % 60
        if hours > 0:
            readable_delay = f"{hours}h {minutes}m"
        else:
            readable_delay = f"{minutes} min"

        if delta_minutes <= 0:
            status_label = "On time"
            status_color = "green"
        elif 0 < delta_minutes <= 5:
            status_label = "Just made it"
            status_color = "#FFD700"
        else:
            status_label = "Late"
            status_color = "red"

        inline_status = (
            f"<span style='color:{status_color}'><strong>{status_label}</strong> "
            f"({readable_delay} {direction})</span>"
        )

    except Exception as e:
        #print(f"⚠️ Clock-in parsing failed: {e}")
        inline_status = "<span style='color:gray'><strong>Clock-in unknown</strong></span>"

    # === Helper to format decimal hours into hh:mm:ss for display ===
    def format_time(val):
        return decimal_to_hhmmss_nosign(val) if pd.notna(val) else "--:--:--"

    # === Time To Goal display, converted from decimal
    ttg_raw = row.get("Time To Goal", None)
    if pd.notna(ttg_raw):
        ttg_str = decimal_to_hhmmss(ttg_raw)
        ttg_color = "green" if ttg_raw >= 0 else "red"
        ttg_str = f"<span style='color:{ttg_color}'>{ttg_str}</span>"
    else:
        ttg_str = "--:--:--"
    ttg_line = f"⏳ <strong>Time To Goal:</strong> {ttg_str}"

    # === Build the text summary block ===
    agent = row["Agent"]
    is_total = row.get("is_total") is True
    server_label = row.get("Server", "")
    server_number = ""

    # Determine label
    if is_total:
        agent_label = f"{agent} (Total)"
    else:
        if server_label.startswith("Server") and server_label[-1].isdigit():
            server_number = server_label[-1]
        else:
            server_number = "?"
        agent_label = f"{agent} on Server {server_number}"


    text_block = f"""
    ## {agent_label} {inline_status}
    {ttg_line}

    **Sales:** {row.get('Sales', 0)}  
    🗒️ **Daily Goals:**  
    - ⏱️ Time Connected: {format_time(goals['Time Connected'])}  
    - 📝 Wrap-Up Limit: {format_time(goals['Wrap Up'])}  
    - 🛑 Break Limit: {format_time(goals['Break'])}  
    - 🎙️ Talk Time Goal: {format_time(goals['Talk Time'])}
    """

    # === UI render logic ===
    if not st.session_state.get("export_mode"):
        with st.container():
            cols = st.columns([1, 2])
            with cols[0]:
                st.markdown(text_block, unsafe_allow_html=True)
            with cols[1]:
                chart_key = f"{row['Office']}_{row['Agent']}_{row.name}_chart"
                if unique_key_suffix:
                    chart_key += f"_{unique_key_suffix}"
                st.plotly_chart(fig, use_container_width=True, key=chart_key)


    #("🔍 Keys in row:", list(row.keys()))


    return fig, text_block









# === MAIN SECTION ===

# Display selected report date
selected_date_str = report_date.strftime("%A, %B %d, %Y")
st.markdown(f"🕒 **Report Metrics: {selected_date_str}**")

# === Tabs ===
tab2, tab1 = st.tabs(["📈 Agent Progress Dashboard", "📊 Daily Metrics Overview"])


############################
# TAB 1: Daily Metrics View
############################
with tab1:
    st.markdown("### 📊 **All Office Metrics**")


    # === EXPORT TO GOOGLE SHEETS BUTTON ===
    if st.button("📤 Export to Google Sheets"):
        try:
            raw_data = st.session_state.get("raw_data")

            if raw_data is None:
                st.error("❌ No data loaded. Please upload and load today's CSVs first.")
                st.stop()

            # Combine full raw dataset across offices
            if isinstance(raw_data, dict):
                df = pd.concat(raw_data.values(), ignore_index=True)
            else:
                df = raw_data.copy()

            # Insert total rows for agents with multiple entries
            rep_date = pd.to_datetime(df["Report Date"].iloc[0])
            export_df = insert_total_rows(df, rep_date)



            if export_df.empty:
                st.error("⚠️ No data found to export.")
                st.stop()

            export_df = export_df.sort_values(by=["Office", "Agent", "Time Connected"])


            # ✅ Adjust Time Connected by removing mismatch
            if "_MismatchAmount" in export_df.columns and "Time Connected" in export_df.columns:
                export_df["Time Connected"] = (
                    export_df["Time Connected"] - export_df["_MismatchAmount"]
                ).clip(lower=0)


            # Clean time columns: replace empty strings with NaN
            for col in ["Time To Goal", "Time Connected", "Break", "Talk Time", "Wrap Up"]:
                if col in export_df.columns:
                    export_df[col] = pd.to_numeric(export_df[col], errors="coerce")
                    if col == "Time To Goal":
                        export_df[col] = export_df[col].apply(lambda x: decimal_to_hhmmss(x) if pd.notna(x) else "")
                    else:
                        export_df[col] = export_df[col].apply(lambda x: decimal_to_hhmmss_nosign(x) if pd.notna(x) else "")


            # Drop internal/debug columns
            debug_cols = ["Time Mismatch", "_MismatchAmount", "_TTG_Adjusted"]
            export_df = export_df.drop(columns=[col for col in debug_cols if col in export_df.columns])

            # Connect to sheet
            sheet = connect_to_gsheet(SHEET_ID)
            local_tz = pytz.timezone("America/Mexico_City")
            today_str = datetime.today().strftime("%B %d — %I:%M%p").lstrip("0").replace(" 0", " ")
            worksheet = create_unique_worksheet(sheet, today_str)

            # Export
            export_df = export_df.fillna("")
            export_df_to_sheet(export_df, worksheet)
            st.success(f"✅ Exported to tab '{worksheet.title}' successfully!")

        except Exception as e:
            st.error(f"❌ Export failed: {e}")





    # Load processed data from session state
    raw_data = st.session_state.get("raw_data", None)

    # Accept both dict format (merged CSVs) and flat DataFrame
    if isinstance(raw_data, dict):
        df = pd.concat(raw_data.values(), ignore_index=True)
    elif isinstance(raw_data, pd.DataFrame):
        df = raw_data
    else:
        df = pd.DataFrame()

    if not df.empty:
        # Group data by Office name
        offices = df["Office"].dropna().unique()

        for office in sorted(offices):
            office_df = df[df["Office"] == office].copy()

            # Drop 'Office' column since it's implied in header
            office_df.drop(columns="Office", inplace=True, errors="ignore")

            # Sort by the current selected column (via sidebar)
            try:
                office_df = sort_dataframe(office_df, selected_column, SORT_DIRECTION)
            except Exception as e:
                st.error(f"❌ Failed to sort {office}: {e}")
                continue

            # Format decimal time columns for display
            try:
                
                rep_date = pd.to_datetime(office_df["Report Date"].iloc[0])
                office_df = insert_total_rows(office_df, rep_date)

                office_df = format_time_columns(office_df)

            except Exception as e:
                st.error(f"❌ Failed to format {office}: {e}")
                continue

            # Reset row index for clarity
            office_df.index = range(1, len(office_df) + 1)

            # Render each office block inside an expander
            unique_agents = office_df[office_df["is_total"] != True]["Agent"].nunique() if "is_total" in office_df.columns else office_df["Agent"].nunique()

            with st.expander(f"🏢 {office} Office – Showing {unique_agents} agents"):

                st.caption(f"Currently sorted by: **{sort_criterion}**")
                

                visible_columns = [
                    "Sales", "Server", "1st Call", "Shift End", "Agent", "Time To Goal", 
                    "Time Connected", "Break", "Talk Time", "Wrap Up", "Time Mismatch"
                ]
                visible_columns = [col for col in visible_columns if col in office_df.columns]
                st.dataframe(office_df[visible_columns], use_container_width=True)
    else:
        st.warning("⚠️ No data loaded or available for display.")




###########################
# TAB 2: Agent Progress Dashboard
############################
with tab2:
    st.markdown("🎯 **Goal:** Maximize Time Connected & Talk Time ✅ Keep Breaks & Wrap-Up within limits 🚦")
    
    # === BUTTON: Download PDF ===
    if st.button("📥 Download Summary PDF"):
        st.info("📦 Generating PDFs... please wait ⏳")


        OUTPUT_DIR = "exported_pdfs"
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        df = pd.concat(st.session_state.raw_data.values(), ignore_index=True) if isinstance(st.session_state.raw_data, dict) else st.session_state.raw_data.copy()
        report_date = pd.to_datetime(df["Report Date"].iloc[0])
        date_str = report_date.strftime("%B %d, %Y")

        # Reset PDF cache in session
        st.session_state["pdf_paths"] = {}

        with tempfile.TemporaryDirectory() as tmpdir:
            st.session_state["export_mode"] = True

            # STEP 1: Deduplicate globally by earliest 1st Call for summary stats
            df_sorted = df.sort_values(by=["Agent", "1st Call"])
            unique_agents = df_sorted.drop_duplicates(subset="Agent", keep="first")

            # STEP 2: Attach unique summary DF to each office's DataFrame for PDF logic
            grouped_by_office = {}
            for office, office_df in df.groupby("Office"):
                office_agents = office_df["Agent"].unique()
                office_summary_df = unique_agents[unique_agents["Agent"].isin(office_agents)]
                office_df_sorted = office_df.sort_values(["Agent", "Time Connected"], ascending=[True, False])
                office_df_sorted = insert_total_rows(office_df_sorted, report_date)
                grouped_by_office[office] = office_df_sorted 
                
                # Hack: Store unique rows for PDF stats using special key
                office_df_sorted.attrs["unique_summary_rows"] = office_summary_df
                grouped_by_office[office] = office_df_sorted


            # Initialize progress bar
            total = sum(len(df) for df in grouped_by_office.values())
            progress = st.progress(0)
            status = st.empty()
            count = 0

            # ─── Generate all charts for the FULL report with progress ───
            for office_df in grouped_by_office.values():
                for _, row in office_df.iterrows():
                    count += 1
                    progress.progress(int(count / total * 100))
                    status.text(f"Generating charts: {count}/{total}")

                    color = "#666666" if row.get("is_total") is True else None
                    fig = build_export_figure(row, color_override=color)
                    img_path = os.path.join(
                        tmpdir,
                        f"{row['Agent'].replace(' ', '_')}_{row.name}.png"
                    )
                    pio.write_image(fig, img_path, format="png", scale=0.5)



            # === Export full report ===
            full_pdf_path = os.path.join(tmpdir, f"Agent_Report_{date_str}.pdf")
            export_html_pdf(grouped_by_office, full_pdf_path, chart_folder=tmpdir)
            final_full_path = os.path.join(OUTPUT_DIR, f"Agent_Report_{date_str}.pdf")
            shutil.copyfile(full_pdf_path, final_full_path)
            st.session_state["pdf_paths"]["full"] = final_full_path


            # === Export each office separately ===
            for office, office_df in grouped_by_office.items():
                if office_df.empty:
                    continue

                 # create a folder of copies for this office
                office_tmpdir = os.path.join(tmpdir, "per_office", office)
                os.makedirs(office_tmpdir, exist_ok=True)
                
                for _, row in office_df.iterrows():
                    # filename matches the one you already rendered above
                    filename = f"{row['Agent'].replace(' ', '_')}_{row.name}.png"
                    src = os.path.join(tmpdir, filename)
                    dst = os.path.join(office_tmpdir, filename)
                    shutil.copyfile(src, dst)

                office_pdf_path = os.path.join(
                    office_tmpdir, f"{office}_Report_{date_str}.pdf"
                )
                export_html_pdf(
                    {office: office_df},
                    office_pdf_path,
                    chart_folder=office_tmpdir
                )

                final_office_path = os.path.join(
                    OUTPUT_DIR, f"{office}_Report_{date_str}.pdf"
                )
                shutil.copyfile(office_pdf_path, final_office_path)
                st.session_state["pdf_paths"][office] = final_office_path




            # ✅ === Bundle all office PDFs into a single ZIP ===
            zip_path = os.path.join(OUTPUT_DIR, f"Office_Reports_{date_str}.zip")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for office, path in st.session_state["pdf_paths"].items():
                    if office == "full":
                        continue
                    zipf.write(path, arcname=os.path.basename(path))
            st.session_state["pdf_paths"]["offices_zip"] = zip_path  
            st.session_state["export_mode"] = False

    # === Download Buttons ===
    if st.session_state.get("pdf_paths", {}).get("full"):
        with open(st.session_state["pdf_paths"]["full"], "rb") as f:
            st.download_button(
            label="📄 All Office Report",
            data=f.read(),
            file_name=os.path.basename(st.session_state["pdf_paths"]["full"]),
            mime="application/pdf",
            use_container_width=True
        )


    if st.session_state.get("pdf_paths", {}).get("offices_zip"):
        with open(st.session_state["pdf_paths"]["offices_zip"], "rb") as f:
            st.download_button(
                label="📦 Download individual office reports",
                data=f.read(),
                file_name=os.path.basename(st.session_state["pdf_paths"]["offices_zip"]),
                mime="application/zip",
                use_container_width=True
            )





    # === BUTTON: PDF Export + Email ===
    # if st.button("📧 Send Summary PDF to My Email"):
    #     st.info("📦 Building report... please wait ⏳")

    #     # Rebuild the full dataset
    #     df = pd.concat(st.session_state.raw_data.values(), ignore_index=True) if isinstance(st.session_state.raw_data, dict) else st.session_state.raw_data.copy()
    #     report_date = pd.to_datet#ime(df["Report Date"].iloc[0])
    #     date_str = report_date.strftime("%B %d, %Y")
    #     target_email = "cecilio@marketingleads.com.mx"

    #     with tempfile.TemporaryDirectory() as tmpdir:
    #         st.session_state["export_mode"] = True  # Suppress UI render

    #         # Build PNG charts in advance (avoids bugs in some backends)
    #         for _, row in df.iterrows():
    #             fig = build_export_figure(row)
    #             img_path = os.path.join(tmpdir, f"{row['Agent'].replace(' ', '_')}.png")
    #             pio.write_image(fig, img_path, format='png', scale=2)

    #         # Sort agents inside each office and group by Office name
    #         grouped_by_office = {
    #             office: office_df.sort_values("Agent")
    #             for office, office_df in df.groupby("Office")
    #         }

    #         # Create final PDF path
    #         pdf_path = os.path.join(tmpdir, f"summary_{date_str}.pdf")

    #         # Export the HTML + PDF
    #         export_html_pdf(grouped_by_office, pdf_path, PDFKIT_CONFIG, chart_folder=tmpdir)

    #         # Email it
    #         success, msg = send_email(
    #             to_email=target_email,
    #             subject=f"Agent Summary Report – {date_str}",
    #             body="Attached is the full summary.",
    #             attachment_path=pdf_path
    #         )

    #         st.session_state["export_mode"] = False  # Restore render mode

    #         if success:
    #             st.success(f"✅ Sent to {target_email}")
    #         else:
    #             st.error(f"❌ {msg}")


#### ------------------------------------------------------------------------------------------------------------------------------------------




# === Data Preparation ===
    raw_data = st.session_state.get("raw_data", None)
    if raw_data is None:
        st.warning("⚠️ No data loaded yet. Please upload a CSV or load from Dropbox.")
        st.stop()

    # Accept raw_data as either dict or flat DataFrame
    df = pd.concat(raw_data.values(), ignore_index=True) if isinstance(raw_data, dict) else raw_data.copy()

    if "Office" not in df.columns:
        st.error("❌ 'Office' column missing. Please check your data_processor logic.")
        st.stop()

    offices = df["Office"].dropna().unique()

    # === UI Rendering: One block per office ===
    if not st.session_state.get("export_mode"):
        for office in sorted(offices):
            office_df = df[df["Office"] == office].copy()

            # Sort by Agent name + Time Connected descending to group duplicates logically
            office_df = office_df.sort_values(by=["Agent", "Time Connected"], ascending=[True, False])
            rep_date = pd.to_datetime(office_df["Report Date"].iloc[0])
            office_df = insert_total_rows(office_df, rep_date)

            if office_df.empty:
                st.warning(f"⚠️ Skipping {office} — no agents found.")
                continue

            st.markdown(f"# 🏢 {office} Office")
            st.markdown("<hr style='border: 1px solid #bbb;'>", unsafe_allow_html=True)


            # Render one block per agent
            for _, agent_row in office_df.iterrows():
                try:
                    render_agent_block(agent_row)
                except Exception as e:
                    agent_name = agent_row.get("Agent", "Unknown")
                    st.error(f"❌ Failed to render agent {agent_name}: {e}")
                st.markdown("---")





# === Always show download buttons if PDFs exist ===
if (
    "pdf_paths" in st.session_state and 
    "full" in st.session_state["pdf_paths"] and 
    os.path.exists(st.session_state["pdf_paths"]["full"])
):
    # Full PDF
    with open(st.session_state["pdf_paths"]["full"], "rb") as f:
        st.download_button(
            label="📄 Click to Download Full PDF",
            data=f.read(),
            file_name=os.path.basename(st.session_state["pdf_paths"]["full"]),
            mime="application/pdf",
            use_container_width=True
        )

    # Per-Office PDFs
    st.markdown("### 📥 Download PDF per Office")
    for office in sorted(k for k in st.session_state["pdf_paths"].keys() if k != "full"):
        path = st.session_state["pdf_paths"][office]
        if os.path.exists(path):
            with open(path, "rb") as f:
                st.download_button(
                    label=f"📄 Download {office} PDF",
                    data=f.read(),
                    file_name=os.path.basename(path),
                    mime="application/pdf",
                    use_container_width=True
                )





    






#-------------------------------------------------------------------------------------------------------------------------------------------------------------












# # === EXPORT TO SUPABASE ===
# if st.button("📥 Save to Supabase"):
#     try:
#         raw_data = st.session_state.get("raw_data")

#         if raw_data is None or raw_data.empty:
#             st.error("❌ No data loaded. Please upload and load today's CSVs first.")
#             st.stop()

#         # Select and rename columns for Supabase schema
#         export_columns = [
#             "Report Date", "Agent", "Office", "Server",
#             "1st Call", "Sales", "Time To Goal", "Time Connected",
#             "Break", "Talk Time", "Wrap Up", "Shift End", "Time Mismatch"
#         ]
#         export_df = raw_data[export_columns].rename(columns={
#             "Report Date": "report_date",
#             "Agent": "agent_name",
#             "Office": "office",
#             "Server": "server",
#             "1st Call": "first_call",
#             "Sales": "sales",
#             "Time To Goal": "time_to_goal",
#             "Time Connected": "time_connected",
#             "Break": "break_time",
#             "Talk Time": "talk_time",
#             "Wrap Up": "wrap_up_time",
#             "Shift End": "shift_end",
#             "Time Mismatch": "time_mismatch"
#         })

#         # Fill NaNs for safe JSON serialization
#         export_df = export_df.fillna("")

#         # Convert time columns to readable strings
#         time_cols = ["time_to_goal", "time_connected", "break_time", "talk_time", "wrap_up_time"]
#         for col in time_cols:
#             if col in export_df.columns:
#                 export_df[col] = export_df[col].apply(decimal_to_hhmmss_string)

#         # Force all other columns to string (except 'sales')
#         for col in export_df.columns:
#             if col != "sales":
#                 export_df[col] = export_df[col].astype(str)

#         # Optional: warn if anything slipped through
#         if export_df.isnull().any().any():
#             st.warning("⚠️ There are still nulls after cleaning. Please review.")

#         # Convert to records (JSON-friendly list of dicts)
#         records = export_df.to_dict(orient="records")

#         # Push to Supabase with upsert (merge if exists)
#         response = supabase.table("agent_metrics").upsert(records).execute()

#         # Check response payload
#         if response and hasattr(response, "data") and response.data:
#             st.success("✅ Data successfully saved/updated to Supabase!")
#             st.json(response.data)  # Optional: show inserted rows
#         else:
#             st.warning("⚠️ Supabase responded, but no data was returned.")

#     except Exception as e:
#         st.error(f"❌ Failed to insert/update Supabase: {e}")
#         st.exception(e)