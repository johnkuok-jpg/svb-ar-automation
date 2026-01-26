# SVB AR Cash Automation

A Streamlit web application that automates processing of SVB (Silicon Valley Bank) transaction CSV files and exports them to Google Sheets with optional customer identification via fuzzy matching.

## Features

- 📤 Drag-and-drop CSV file upload
- 📊 Automatic parsing of SVB transaction format
- 📝 Extracts: Date, Transaction Type, Memo, and Amount
- 🔍 Fuzzy matching to identify customers from transaction memos
- 📈 Real-time preview of processed data
- 📋 Automatic export to Google Sheets
- 💡 Transaction statistics (total credits, debits, matched customers)

## Installation

### Prerequisites

- Python 3.8 or higher
- Google Cloud Service Account with Sheets API enabled

### Setup Steps

1. **Clone or download this repository**

```bash
cd /path/to/AR Cash Automation
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Set up Google Sheets API**

   a. Go to [Google Cloud Console](https://console.cloud.google.com/)

   b. Create a new project (or select an existing one)

   c. Enable Google Sheets API:
      - Search for "Google Sheets API" in the search bar
      - Click "Enable"

   d. Create Service Account credentials:
      - Go to "Credentials" → "Create Credentials" → "Service Account"
      - Fill in service account details
      - Click "Create and Continue"
      - Skip optional steps
      - Click "Done"

   e. Create and download JSON key:
      - Click on the created service account
      - Go to "Keys" tab
      - Click "Add Key" → "Create New Key"
      - Choose "JSON"
      - Download the JSON file (keep it secure!)

4. **Prepare your Google Sheet**

   a. Create a new Google Sheet or open an existing one

   b. Share the sheet with your service account:
      - Open the JSON credentials file
      - Copy the `client_email` value (looks like `xxx@xxx.iam.gserviceaccount.com`)
      - In Google Sheets, click "Share"
      - Paste the service account email
      - Give "Editor" permission
      - Click "Send"

   c. Copy the Google Sheet URL from your browser

## Usage

### Running the App Locally

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

### Using the App

1. **Configure in Sidebar:**
   - Upload your Google Service Account JSON file
   - Paste your Google Sheets URL
   - (Optional) Add customer names for fuzzy matching (one per line)
   - Adjust fuzzy match threshold if needed (70 is default)

2. **Upload CSV:**
   - Drag and drop your SVB CSV file (format: `SVB_Transactions_YYYYMMDD_HHMMSS.csv`)
   - Or click to browse and select the file

3. **Review Preview:**
   - Check the processed data preview
   - Review transaction statistics
   - Verify customer matches (if enabled)

4. **Send to Google Sheets:**
   - Click "Send to Google Sheets" button
   - Wait for confirmation
   - Check your Google Sheet for the new data

## CSV Format

The app expects SVB transaction CSV files with the following format:

- Row 1: Metadata (date range, report info) - automatically skipped
- Row 2: Column headers
- Row 3+: Transaction data

Required columns:
- `Date`: Transaction date
- `Tran Type`: Transaction type (ACH CREDIT, WIRE TRANSFER, etc.)
- `Description`: Transaction memo/description
- `Credit Amount`: Credit amount (if applicable)
- `Debit Amount`: Debit amount (if applicable)

## Fuzzy Matching

The fuzzy matching feature helps identify customers from transaction memos:

- Add customer names in the sidebar (one per line)
- Adjust the threshold slider (50-100):
  - **60-70**: Loose matching (may have false positives)
  - **70-80**: Balanced (recommended)
  - **80+**: Strict matching (may miss some matches)
- The app uses token set ratio for flexible matching

### Example Customer List

```
APPLE INC.
AMAZON.COM SERVICES
Scale AI, Inc.
BRIDGEWATER ASSOCIATES
Perplexity Suppl SHOPIFY
```

## Deployment Options

### Streamlit Cloud (Free)

1. Push your code to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub repository
4. Deploy!

Note: You'll need to add secrets for credentials (see [Streamlit Secrets Management](https://docs.streamlit.io/streamlit-community-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management))

### Other Options

- **Heroku**: Deploy as a web app
- **AWS/GCP/Azure**: Deploy on cloud VM
- **Docker**: Containerize and deploy anywhere

## Troubleshooting

### "Error parsing CSV"
- Ensure you're uploading an SVB transaction CSV file
- Check that the file format matches the expected structure

### "Error connecting to Google Sheets"
- Verify your service account JSON is valid
- Ensure the Google Sheets API is enabled in your GCP project
- Check that the Google Sheet URL is correct

### "Error writing to Google Sheets"
- Confirm the service account email has Editor permissions on the sheet
- Check if there are any quota limits on your Google Cloud project

### Poor fuzzy matching results
- Adjust the threshold slider
- Ensure customer names are formatted consistently
- Add more variations of customer names to the list

## File Structure

```
AR Cash Automation/
├── app.py                                    # Main Streamlit application
├── requirements.txt                          # Python dependencies
├── README.md                                 # This file
└── SVB_Transactions_20260126_152356.csv     # Sample CSV file
```

## Technologies Used

- **Streamlit**: Web application framework
- **Pandas**: Data processing
- **gspread**: Google Sheets integration
- **google-auth**: Authentication
- **fuzzywuzzy**: Fuzzy string matching
- **python-Levenshtein**: String similarity calculations

## Security Notes

- Never commit your Google Service Account JSON file to version control
- Add `*.json` to `.gitignore` if pushing to GitHub
- Keep your credentials secure and rotate them periodically
- Only share Google Sheets with the service account email, not publicly

## License

This project is provided as-is for internal use.

## Support

For issues or questions, please refer to the documentation or create an issue in the repository.
