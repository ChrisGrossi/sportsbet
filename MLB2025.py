import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
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
SBRI_URL = os.getenv('SBRI_MLB_URL')
DRATINGS_URL = os.getenv('DRATINGS_MLB_URL')

# Google API Scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Worksheet names
SBRI_SHEET_NAME = 'SBRI_MLB'
DRATE_SHEET_NAME = 'DRate_MLB'

# --- Helper Functions ---

def get_google_sheet_client(service_account_path, scopes):
    """Authenticates with Google and returns a gspread client."""
    try:
        # This is the new, recommended method
        gc = gspread.service_account(filename=service_account_path, scopes=scopes)
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

def scrape_sbri_data():
    """Scrapes, processes, and returns NFL data from SportsBet RI."""
    # NOTE: Headers should be managed better, ideally not hardcoded.
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(SBRI_URL, timeout=15)
            response.raise_for_status()
            data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from SportsBet RI: {e}")
        return None
    except json.JSONDecodeError:
        logging.error("Error decoding JSON response from SportsBet RI.")
        return None

    processed_rows = []
    for event in data.get('events', []):
        game_data = {
            'Sport': event.get('sportname'),
            'GameStart': pd.to_datetime(event.get('tsstart')),
            'Game': event.get('externaldescription'),
            'AwayTeam': event.get('shortnameaway'),
            'HomeTeam': event.get('shortnamehome'),
            'Away MLOdds': None,
            'Home MLOdds': None,
            'HomeSpread': None,
            'AwaySpreadOdds': None,
            'HomeSpreadOdds': None,
            'UnderOdds': None,
            'OverOdds': None,
            'Handicap': None
        }
        # More robustly parse markets without assuming order
        for market in event.get('markets', []):
            if market.get('name') == 'Money Line':
                for selection in market.get('selections', []):
                    if selection.get('name') == game_data['AwayTeam']:
                        game_data['Away MLOdds'] = selection.get('price')
                    elif selection.get('name') == game_data['HomeTeam']:
                        game_data['Home MLOdds'] = selection.get('price')                     
            if market.get('name') == 'Run Line':
                for selection in market.get('selections', []):
                    if selection.get('name') == game_data['AwayTeam']:
                        game_data['AwaySpreadOdds'] = selection.get('price')
                    elif selection.get('name') == game_data['HomeTeam']:
                        game_data['HomeSpread'] = selection.get('currenthandicap')
                        game_data['HomeSpreadOdds'] = selection.get('price')      
            if market.get('name') == 'Total Runs':
                for selection in market.get('selections', []):
                    if selection.get('name') == 'Over':
                        game_data['OverOdds'] = selection.get('price')
                        game_data['Handicap'] = selection.get('currentmatchhandicap')
                    elif selection.get('name') == 'Under':
                        game_data['UnderOdds'] = selection.get('price')            
                        
        processed_rows.append(game_data)
        
    if not processed_rows:
        logging.warning("No event data processed from SBRI.")
        return pd.DataFrame()
        
    df = pd.DataFrame(processed_rows)
 
    # Convert odds
    odds_cols = ['Away MLOdds', 'Home MLOdds', 'HomeSpreadOdds', 'AwaySpreadOdds', 'OverOdds', 'UnderOdds'] # Add other odds columns here
    for col in odds_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').apply(convert_to_american_odds)
            
    return df.sort_values(by=['GameStart', 'AwayTeam'])

# new attempt
def scrape_dratings_data():
    """Scrapes, processes, and returns NFL data from DRatings."""
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    all_dfs = []

    with requests.Session() as session:
        session.headers.update(headers)
        for page_num in range(3): # This is a "magic number", could be a constant
            url = f"{DRATINGS_URL}upcoming/{page_num}" if page_num > 0 else DRATINGS_URL
            logging.info(f"Scraping page {page_num}: {url}")

            try:
                response = session.get(url, timeout=15)
                response.raise_for_status() # Will raise an error for bad status codes (404, 500, etc.)
                
                # pd.read_html returns a LIST of all tables on the page
                page_tables = pd.read_html(io.StringIO(response.text))
                # --- Logic to find the correct "Pitchers" table ---
                found_table = False
                for table in page_tables:
                    # We identify the correct table by checking for essential columns
                    if 'Pitchers' in table.columns:
                        all_dfs.append(table)
                        logging.info(f"Found 'Pitchers' table on page {page_num}.")
                        found_table = True
                        break # Stop searching once we've found our table
                
                if not found_table:
                    logging.warning(f"Could not find a 'Pitchers' table on page {page_num}.")

            except requests.exceptions.RequestException as e:
                logging.warning(f"Could not scrape DRatings page {page_num}: {e}")
            
            # Be a polite scraper and wait 5 seconds between requests
            time.sleep(2)

    if not all_dfs:
        logging.error("Failed to scrape any data from DRatings.")
        return None

    # Combine all the DataFrames from all the pages into one
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Clean up the final combined DataFrame
    df = df.drop_duplicates(subset='Teams')
    df = df.drop(columns=['Pitchers', 'Best ML', 'Best Spread', 'Best O/U'], errors='ignore')
    df['Teams'] = df['Teams'].str.replace('Oakland Athletics', 'Athletics')
    # 1. Ensure the 'Time' column is a datetime object and drop bad text
    df['Time'] = pd.to_datetime(df['Time'], format='mixed', utc=True, errors='coerce')
    df.dropna(subset=['Time'], inplace=True)
    # 2. Convert to your local timezone (handles DST automatically!)
    df['Time'] = df['Time'].dt.tz_convert("America/New_York")       
    return df.sort_values(by=['Time', 'Teams'])
  
  # --- Main Execution ---

def main():
    """Main function to run the scraper and update Google Sheets."""
    logging.info("Starting the MLB data scraper script.")
    
    gc = get_google_sheet_client(SERVICE_ACCOUNT_FILE, SCOPES)
    if not gc:
        logging.critical("Could not get Google client. Exiting.")
        return

    # Process Sportsbet RI
    sbri_df = scrape_sbri_data()
    if sbri_df is not None and not sbri_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, SBRI_SHEET_NAME, sbri_df)
    else:
        logging.error("Skipping SBRI sheet update due to scraping failure or no data.")

    # Process DRatings
    dratings_df = scrape_dratings_data()
    if dratings_df is not None and not dratings_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, DRATE_SHEET_NAME, dratings_df)
    else:
        logging.error("Skipping DRatings sheet update due to scraping failure or no data.")
        
    logging.info("Script finished.")

if __name__ == "__main__":
    main()

exit()