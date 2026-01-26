import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from fuzzywuzzy import process, fuzz
import json
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="SVB AR Cash Automation",
    page_icon="💰",
    layout="wide"
)

# Title
st.title("💰 SVB AR Cash Automation")
st.markdown("Upload your SVB transaction CSV file to automatically process and send to Google Sheets")

# Initialize session state for customer list
if 'customer_list' not in st.session_state:
    st.session_state.customer_list = []

def parse_svb_csv(uploaded_file):
    """Parse SVB transaction CSV file"""
    try:
        # Read the CSV, skipping the first row (metadata)
        df = pd.read_csv(uploaded_file, skiprows=1)

        # Select and rename relevant columns
        df_processed = pd.DataFrame({
            'Date': pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d'),
            'Transaction Type': df['Tran Type'],
            'Memo': df['Description'].fillna(''),
            'Credit Amount': pd.to_numeric(df['Credit Amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0),
            'Debit Amount': pd.to_numeric(df['Debit Amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        })

        # Create a single Amount column (positive for credits, negative for debits)
        df_processed['Amount'] = df_processed['Credit Amount'] - df_processed['Debit Amount']
        df_processed = df_processed.drop(['Credit Amount', 'Debit Amount'], axis=1)

        return df_processed
    except Exception as e:
        st.error(f"Error parsing CSV: {str(e)}")
        return None

def fuzzy_match_customer(memo, customer_list, threshold=70):
    """Fuzzy match customer name from memo using customer list"""
    if not customer_list or not memo:
        return ""

    # Extract the best match
    match = process.extractOne(memo, customer_list, scorer=fuzz.token_set_ratio)

    if match and match[1] >= threshold:
        return match[0]
    return ""

def connect_to_google_sheets(credentials_dict, spreadsheet_url):
    """Connect to Google Sheets using service account credentials"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=scope
        )

        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_url(spreadsheet_url)

        return spreadsheet
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {str(e)}")
        return None

def append_to_sheet(spreadsheet, data, worksheet_name="Transactions"):
    """Append data to Google Sheet"""
    try:
        # Try to get existing worksheet or create new one
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
            # Add headers
            headers = ['Date', 'Transaction Type', 'Memo', 'Amount', 'Identified Customer']
            worksheet.append_row(headers)

        # Append all rows
        for _, row in data.iterrows():
            worksheet.append_row(row.tolist())

        return True
    except Exception as e:
        st.error(f"Error writing to Google Sheets: {str(e)}")
        return False

# Sidebar for configuration
with st.sidebar:
    st.header("Configuration")

    # Google Sheets credentials
    st.subheader("1. Google Sheets Credentials")
    credentials_file = st.file_uploader(
        "Upload Google Service Account JSON",
        type=['json'],
        help="Upload your Google Service Account credentials JSON file"
    )

    # Google Sheets URL
    st.subheader("2. Google Sheets URL")
    sheet_url = st.text_input(
        "Enter Google Sheets URL",
        placeholder="https://docs.google.com/spreadsheets/d/...",
        help="Paste the URL of your Google Sheet"
    )

    # Customer list
    st.subheader("3. Customer List (Optional)")
    st.markdown("For fuzzy matching customer names from transaction memos")

    customer_input = st.text_area(
        "Enter customer names (one per line)",
        height=150,
        placeholder="APPLE INC.\nAMAZON.COM\nScale AI, Inc.\n..."
    )

    if customer_input:
        st.session_state.customer_list = [line.strip() for line in customer_input.split('\n') if line.strip()]
        st.success(f"✓ {len(st.session_state.customer_list)} customers loaded")

    # Fuzzy match threshold
    match_threshold = st.slider(
        "Fuzzy Match Threshold",
        min_value=50,
        max_value=100,
        value=70,
        help="Higher = more strict matching (70 is recommended)"
    )

# Main area
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Upload SVB Transaction CSV")
    uploaded_file = st.file_uploader(
        "Drag and drop or click to upload",
        type=['csv'],
        help="Upload your SVB_Transactions_YYYYMMDD_HHMMSS.csv file"
    )

# Process the file
if uploaded_file is not None:
    # Parse CSV
    df = parse_svb_csv(uploaded_file)

    if df is not None:
        # Add fuzzy matched customer column
        if st.session_state.customer_list:
            with st.spinner("Identifying customers..."):
                df['Identified Customer'] = df['Memo'].apply(
                    lambda x: fuzzy_match_customer(x, st.session_state.customer_list, match_threshold)
                )
        else:
            df['Identified Customer'] = ""

        # Display preview
        st.subheader("Preview of Processed Data")
        st.dataframe(df, use_container_width=True)

        # Statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Transactions", len(df))
        with col2:
            total_credits = df[df['Amount'] > 0]['Amount'].sum()
            st.metric("Total Credits", f"${total_credits:,.2f}")
        with col3:
            total_debits = abs(df[df['Amount'] < 0]['Amount'].sum())
            st.metric("Total Debits", f"${total_debits:,.2f}")
        with col4:
            if st.session_state.customer_list:
                matched = df['Identified Customer'].ne('').sum()
                st.metric("Customers Matched", f"{matched}/{len(df)}")

        # Send to Google Sheets button
        st.subheader("Send to Google Sheets")

        if not credentials_file:
            st.warning("⚠️ Please upload Google Service Account credentials in the sidebar")
        elif not sheet_url:
            st.warning("⚠️ Please enter Google Sheets URL in the sidebar")
        else:
            if st.button("📤 Send to Google Sheets", type="primary", use_container_width=True):
                with st.spinner("Connecting to Google Sheets..."):
                    # Parse credentials
                    credentials_dict = json.load(credentials_file)

                    # Connect to Google Sheets
                    spreadsheet = connect_to_google_sheets(credentials_dict, sheet_url)

                    if spreadsheet:
                        st.success(f"✓ Connected to: {spreadsheet.title}")

                        with st.spinner("Uploading data..."):
                            if append_to_sheet(spreadsheet, df):
                                st.success(f"✅ Successfully uploaded {len(df)} transactions to Google Sheets!")
                                st.balloons()
                            else:
                                st.error("❌ Failed to upload data")

# Information section
with st.expander("ℹ️ How to Use This App"):
    st.markdown("""
    ### Setup Instructions

    1. **Create Google Service Account**
        - Go to [Google Cloud Console](https://console.cloud.google.com/)
        - Create a new project or select existing
        - Enable Google Sheets API
        - Create Service Account credentials
        - Download JSON key file

    2. **Prepare Google Sheet**
        - Create a new Google Sheet
        - Share it with the service account email (found in JSON file)
        - Give "Editor" permission
        - Copy the sheet URL

    3. **Upload Configuration**
        - Upload service account JSON in sidebar
        - Paste Google Sheet URL in sidebar
        - (Optional) Add customer list for fuzzy matching

    4. **Process Transactions**
        - Upload your SVB CSV file
        - Review the preview
        - Click "Send to Google Sheets"

    ### What This App Does

    - Parses SVB transaction CSV files
    - Extracts: Date, Transaction Type, Memo, Amount
    - Optionally identifies customers using fuzzy matching
    - Uploads data to your Google Sheet automatically

    ### Fuzzy Matching

    If you provide a customer list, the app will attempt to match transaction memos to customer names.
    The threshold slider controls how strict the matching is (70 = balanced, 80+ = strict, 60- = loose).
    """)

# Footer
st.markdown("---")
st.markdown("**SVB AR Cash Automation** | Built with Streamlit")
