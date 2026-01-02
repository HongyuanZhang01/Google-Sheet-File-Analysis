# Research Paper Meta-Analysis Tool

This tool automates the analysis of research papers using the **Google Gemini Batch API**. It reads a list of citations from a Google Sheet, finds the corresponding PDF on your local drive, and extracts the research methodology using AI into a ".csv" code block file.

## Features
- **Smart PDF Matching:** Uses fuzzy matching with year guardrails to find the correct local PDF file for a given citation.
- **Batch Processing:** Utilizes Gemini's Batch API for high-volume, cost-effective processing.
- **Flexible Execution:** Analyze a continuous range of rows or specific selected rows.

## Setup

1. **Install Dependencies:**
  pip install -r requirements.txt
2. **Google Cloud Credentials:**
  To access your Google Sheet, you need a Service Account:
  1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
  2. Create a new project (or select an existing one).
  3. Enable the **Google Sheets API** and **Google Drive API**.
  4. Create a **Service Account** and download the JSON key.
  5. Rename the key file to `credentials.json` and place it in this folder.
  6. **Important:** Open your Google Sheet and "Share" it with the email address found inside your `credentials.json` file.
3. **Configuration:**
  Open `analysis.py` and update the **CONFIGURATION** section at the top:
  * `GEMINI_API_KEY`: Your Google AI Studio API key (Get one [here](https://aistudio.google.com/)).
  * `SHEET_NAME`: The exact name of your Google Sheet.
  * `COLUMN_HEADER`: The header name of the column containing the citations.
  * `LOCAL_PDF_FOLDER`: The absolute path to the folder on your computer containing your PDFs.


## Usage

You can run the script in two modes:

**1. Process a range of rows:**
(e.g., analyze rows 5 through 10): python3 analysis.py row-range 5 10
**2. Process specific rows:**
(e.g., analyze only rows 12, 34, and 55): python3 analysis.py row 12 34 55

Output
The results will be saved to batch_results.csv in the same directory.
