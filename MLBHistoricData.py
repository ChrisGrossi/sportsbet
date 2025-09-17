import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os
import io
import json
import time
import datetime
import requests
from dotenv import load_dotenv
import logging

# --- Configuration and Constants ---
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Credentials & URLs from .env file
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
GOOGLE_SHEET_KEY = os.getenv('GOOGLE_SHEET_KEY')
SBRI_URL = os.getenv('SBRI_NFL_URL')
DRATINGS_URL = os.getenv('DRATINGS_MLB_URL')

# Google API Scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Worksheet names
DRATE_SHEET_NAME = 'DRateMLBHistoric'

# --- Helper Functions ---

def get_google_sheet_client(service_account_path, scopes):
    """Authenticates with Google and returns a gspread client."""
    try:
        creds = Credentials.from_service_account_file(service_account_path, scopes=scopes)
        gc = gspread.authorize(creds)
        return gc
    except FileNotFoundError:
        logging.error(f"Service account file not found at: {service_account_path}")
        return None
    except Exception as e:
        logging.error(f"Failed to authorize with Google: {e}")
        return None

def convert_to_american_odds(decimal_odds):
    """Converts decimal odds to American odds format."""
    if pd.isna(decimal_odds) or not isinstance(decimal_odds, (int, float)):
        return None
    if decimal_odds >= 2.0:
        return (decimal_odds * 100) - 100
    else:
        return -100 / (decimal_odds - 1)

def write_df_to_sheet(gs_client, sheet_key, sheet_name, dataframe):
    """Clears a worksheet and writes a DataFrame to it."""
    try:
        gs = gs_client.open_by_key(sheet_key)
        worksheet = gs.worksheet(sheet_name)
        worksheet.clear()
        set_with_dataframe(worksheet=worksheet, dataframe=dataframe, include_index=True, resize=True)
        logging.info(f"Successfully wrote data to worksheet: {sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"Worksheet '{sheet_name}' not found in the Google Sheet.")
    except Exception as e:
        logging.error(f"Failed to write to Google Sheet '{sheet_name}': {e}")

# --- Scraper Functions ---

def scrape_dratings_data(num_pages_to_scrape):
    """
    Scrapes a specified number of "completed" pages from DRatings,
    intelligently finds the 'Completed Games' table, and returns the combined data.
    """
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    all_dfs = []

    with requests.Session() as session:
        session.headers.update(headers)
        # We start at page 2 as per the URL format example
        for page_num in range(2, num_pages_to_scrape + 2):
            # The part of the URL after '#' is for the browser and not needed for scraping
            url = f"{DRATINGS_URL}/completed/{page_num}"
            logging.info(f"Scraping page {page_num}: {url}")

            try:
                response = session.get(url, timeout=15)
                response.raise_for_status() # Will raise an error for bad status codes (404, 500, etc.)
                
                # pd.read_html returns a LIST of all tables on the page
                page_tables = pd.read_html(io.StringIO(response.text))

                # --- Logic to find the correct "Completed Games" table ---
                found_table = False
                for table in page_tables:
                    # We identify the correct table by checking for essential columns
                    if 'Final Runs' in table.columns:
                        all_dfs.append(table)
                        logging.info(f"Found 'Final Runs' table on page {page_num}.")
                        found_table = True
                        break # Stop searching once we've found our table
                
                if not found_table:
                    logging.warning(f"Could not find a 'Final Runs' table on page {page_num}.")

            except requests.exceptions.RequestException as e:
                logging.warning(f"Could not scrape DRatings page {page_num}: {e}")
            
            # Be a polite scraper and wait 5 seconds between requests
            time.sleep(5)

    if not all_dfs:
        logging.error("Failed to scrape any data from DRatings.")
        return None

    # Combine all the DataFrames from all the pages into one
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Clean up the final combined DataFrame
    df = df.drop_duplicates(subset='Teams')
    df = df.drop(columns=['Quarterbacks', 'Best ML', 'Best Spread', 'Best O/U'], errors='ignore')
    return df.sort_values(by=['Time', 'Teams'])


# --- Main Execution ---

def main():
    """Main function to run the scraper and update Google Sheets."""
    logging.info("Starting the MLB Historic data scraper script.")

    # --- User Input Prompt ---
    try:
        pages_to_scrape = int(input("How many 'completed' pages would you like to scrape? (e.g., 10): "))
        if pages_to_scrape <= 0:
            logging.error("Please enter a positive number.")
            return
    except ValueError:
        logging.error("Invalid input. Please enter a number.")
        return
    
    gc = get_google_sheet_client(SERVICE_ACCOUNT_FILE, SCOPES)
    if not gc:
        logging.critical("Could not get Google client. Exiting.")
        return

    # Process DRatings
    dratings_df = scrape_dratings_data(pages_to_scrape) # Pass the user's number here
    if dratings_df is not None and not dratings_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, DRATE_SHEET_NAME, dratings_df)
    else:
        logging.error("Skipping DRatings sheet update due to scraping failure or no data.")
        
    logging.info("Script finished.")


if __name__ == "__main__":
    main()

exit()