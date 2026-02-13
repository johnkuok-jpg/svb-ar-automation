import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from rapidfuzz import process, fuzz
import json
from datetime import datetime
import re

# Page configuration
st.set_page_config(
    page_title="SVB AR Cash Automation",
    page_icon="💰",
    layout="wide"
)

# Title
st.title("💰 SVB AR Cash Automation")
st.markdown("Upload your SVB transaction CSV file or fetch the latest from Google Drive to automatically process and send to Google Sheets")

# Initialize session state for customer list
if 'customer_list' not in st.session_state:
    st.session_state.customer_list = []

# Google Drive folder ID where CSVs/Sheets are dropped
DRIVE_FOLDER_ID = "161Vh-3_rH6j_at4iXozV5kj_L1U8lxWc"


def parse_svb_csv(uploaded_file):
    """Parse SVB transaction CSV file"""
    try:
        # Read the CSV, skipping the first row (metadata)
        df = pd.read_csv(uploaded_file, skiprows=1)
        return _process_raw_df(df)
    except Exception as e:
        st.error(f"Error parsing CSV: {str(e)}")
        return None


def parse_svb_dataframe(df):
    """Parse SVB transaction data from a DataFrame (e.g., from Google Sheets)"""
    try:
        return _process_raw_df(df)
    except Exception as e:
        st.error(f"Error parsing data: {str(e)}")
        return None


def _process_raw_df(df):
    """Process raw SVB transaction DataFrame into standardized format"""
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


def get_gspread_client(credentials_dict):
    """Get authenticated gspread client"""
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=scope
    )
    return gspread.authorize(credentials)


def list_drive_transaction_files(client, folder_id=DRIVE_FOLDER_ID):
    """List SVB transaction files in the Google Drive folder.
    
    Looks for both CSV files and Google Sheets with the naming pattern
    SVB_Transactions_YYYYMMDD_HHMMSS.
    """
    try:
        # Use the Drive API through gspread to list files in the folder
        from googleapiclient.discovery import build
        
        drive_service = build('drive', 'v3', credentials=client.auth)
        
        # Query for files in the folder matching our naming pattern
        query = (
            f"'{folder_id}' in parents and trashed = false and "
            f"(mimeType = 'text/csv' or "
            f"mimeType = 'application/vnd.google-apps.spreadsheet' or "
            f"mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') and "
            f"name contains 'SVB_Transactions'"
        )
        
        results = drive_service.files().list(
            q=query,
            orderBy='createdTime desc',
            pageSize=20,
            fields='files(id, name, mimeType, createdTime, modifiedTime)'
        ).execute()
        
        return results.get('files', [])
    except Exception as e:
        st.error(f"Error listing Drive files: {str(e)}")
        return []


def read_transaction_file_from_drive(client, file_info):
    """Read a transaction file from Google Drive and return a DataFrame.
    
    Supports both CSV files and Google Sheets.
    """
    try:
        file_id = file_info['id']
        mime_type = file_info['mimeType']
        
        if mime_type == 'application/vnd.google-apps.spreadsheet':
            # It's a Google Sheet - read it directly
            spreadsheet = client.open_by_key(file_id)
            worksheet = spreadsheet.sheet1
            data = worksheet.get_all_records()
            if not data:
                # Try getting all values including header
                all_values = worksheet.get_all_values()
                if len(all_values) > 1:
                    # First row might be metadata, second row headers
                    headers = all_values[1] if len(all_values) > 1 else all_values[0]
                    data_rows = all_values[2:] if len(all_values) > 2 else []
                    df = pd.DataFrame(data_rows, columns=headers)
                else:
                    df = pd.DataFrame()
            else:
                df = pd.DataFrame(data)
            return df
        else:
            # It's a CSV or Excel file - download and read
            from googleapiclient.discovery import build
            import io
            
            drive_service = build('drive', 'v3', credentials=client.auth)
            
            if mime_type == 'text/csv':
                request = drive_service.files().get_media(fileId=file_id)
                content = request.execute()
                # Skip metadata row (first line)
                csv_text = content.decode('utf-8')
                lines = csv_text.split('\n')
                if len(lines) > 1:
                    # Skip first row (metadata)
                    csv_without_metadata = '\n'.join(lines[1:])
                    df = pd.read_csv(io.StringIO(csv_without_metadata))
                else:
                    df = pd.read_csv(io.StringIO(csv_text))
                return df
            else:
                # Excel file
                request = drive_service.files().get_media(fileId=file_id)
                content = request.execute()
                df = pd.read_excel(io.BytesIO(content), skiprows=1)
                return df
                
    except Exception as e:
        st.error(f"Error reading file from Drive: {str(e)}")
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

# Main area - Two tabs for different input methods
tab_upload, tab_drive = st.tabs(["📁 Upload CSV", "☁️ Fetch from Drive"])

with tab_upload:
    st.subheader("Upload SVB Transaction CSV")
    uploaded_file = st.file_uploader(
        "Drag and drop or click to upload",
        type=['csv'],
        help="Upload your SVB_Transactions_YYYYMMDD_HHMMSS.csv file"
    )

with tab_drive:
    st.subheader("Fetch Latest from Google Drive")
    st.markdown(f"Reads the latest `SVB_Transactions_*` file from the [configured Drive folder](https://drive.google.com/drive/u/0/folders/{DRIVE_FOLDER_ID}).")
    
    if not credentials_file:
        st.warning("⚠️ Please upload Google Service Account credentials in the sidebar first")
    else:
        if st.button("🔄 Fetch Latest Transaction File", type="primary"):
            with st.spinner("Connecting to Google Drive..."):
                credentials_dict = json.load(credentials_file)
                credentials_file.seek(0)  # Reset file pointer for later use
                client = get_gspread_client(credentials_dict)
                
                files = list_drive_transaction_files(client)
                
                if not files:
                    st.warning("No SVB transaction files found in the Drive folder.")
                else:
                    st.success(f"Found {len(files)} transaction file(s)")
                    
                    # Show file list and let user select
                    file_names = [f"{f['name']} ({f.get('modifiedTime', 'N/A')[:10]})" for f in files]
                    selected_idx = st.selectbox(
                        "Select a file to process",
                        range(len(file_names)),
                        format_func=lambda i: file_names[i]
                    )
                    
                    if st.button("📥 Load Selected File"):
                        with st.spinner(f"Reading {files[selected_idx]['name']}..."):
                            raw_df = read_transaction_file_from_drive(client, files[selected_idx])
                            if raw_df is not None and not raw_df.empty:
                                st.session_state['drive_df'] = raw_df
                                st.session_state['drive_file_name'] = files[selected_idx]['name']
                                st.success(f"✓ Loaded {len(raw_df)} rows from {files[selected_idx]['name']}")
                                st.rerun()
                            else:
                                st.error("File appears to be empty or could not be read.")

# Determine which data source to use
df = None
source_name = None

if uploaded_file is not None:
    df = parse_svb_csv(uploaded_file)
    source_name = uploaded_file.name
elif 'drive_df' in st.session_state:
    df = parse_svb_dataframe(st.session_state['drive_df'])
    source_name = st.session_state.get('drive_file_name', 'Drive file')

# Process the data (regardless of source)
if df is not None:
    # Add fuzzy matched customer column
    if st.session_state.customer_list:
        with st.spinner("Identifying customers..."):
            df['Identified Customer'] = df['Memo'].apply(
                lambda x: fuzzy_match_customer(x, st.session_state.customer_list, match_threshold)
            )
    else:
        df['Identified Customer'] = ""
    
    # Display source info
    st.info(f"📄 Source: **{source_name}**")
    
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
                credentials_file.seek(0)
                
                # Connect to Google Sheets
                spreadsheet = connect_to_google_sheets(credentials_dict, sheet_url)
                
                if spreadsheet:
                    st.success(f"✓ Connected to: {spreadsheet.title}")
                    
                    with st.spinner("Uploading data..."):
                        if append_to_sheet(spreadsheet, df):
                            st.success(f"✅ Successfully uploaded {len(df)} transactions to Google Sheets!")
                            st.balloons()
                            # Clear drive data from session state after successful upload
                            if 'drive_df' in st.session_state:
                                del st.session_state['drive_df']
                                del st.session_state['drive_file_name']
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
       - **Option A:** Upload your SVB CSV file directly
       - **Option B:** Click "Fetch from Drive" to load the latest file dropped by the automated daily job
       - Review the preview
       - Click "Send to Google Sheets"
    
    ### Automated Daily Job
    
    A daily job runs at 7am PT (Mon-Fri) that:
    1. Pulls the prior day BAI2 file from SVB's SFTP
    2. Parses transactions for account x4669
    3. Drops the data as a Google Sheet in the [Drive folder](https://drive.google.com/drive/u/0/folders/161Vh-3_rH6j_at4iXozV5kj_L1U8lxWc)
    4. Sends a summary to Slack
    
    Use the "Fetch from Drive" tab to load the latest automated file.
    
    ### Fuzzy Matching
    
    If you provide a customer list, the app will attempt to match transaction memos to customer names.
    The threshold slider controls how strict the matching is (70 = balanced, 80+ = strict, 60- = loose).
    """)

# Footer
st.markdown("---")
st.markdown("**SVB AR Cash Automation** | Built with Streamlit")
