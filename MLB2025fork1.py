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
CALC_SHEET_NAME = 'Calc_MLB'

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

def convert_american_to_decimal(american_odds):
    """Converts American odds to decimal odds."""
    if pd.isna(american_odds) or american_odds == '':
        return np.nan
    try:
        american_odds = float(american_odds)
        if american_odds > 0:
            return (american_odds / 100) + 1
        else:
            return (100 / abs(american_odds)) + 1
    except (ValueError, TypeError):
        return np.nan



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
  
def perform_baseball_analysis(sbri_df, dratings_df):
    """
    Merges baseball odds and predictions, calculates implied probabilities,
    and identifies value bets.
    """
    print("--- Starting Baseball Betting Analysis ---")

    # 1. Pre-processing: Standardize data formats for a robust merge.
    print("Standardizing data in both dataframes...")
    try:
        # --- Clean DRatings Data ---
        # A. Extract team names, ignoring win-loss records using regex.
        team_names = dratings_df['Teams'].str.extract(r'^(.*?)\s*\(\d+-\d+\)\s*(.*?)\s*\(\d+-\d+\)$', expand=True)
        dratings_df['AwayTeam'] = team_names[0].str.strip()
        dratings_df['HomeTeam'] = team_names[1].str.strip()
        # B. Split the 'Win' column and convert probabilities to decimal form.
        win_probs = dratings_df['Win'].str.split(' ', expand=True)
        dratings_df['AwayWinProb_pred'] = pd.to_numeric(win_probs[0].str.strip('%'), errors='coerce') / 100
        dratings_df['HomeWinProb_pred'] = pd.to_numeric(win_probs[1].str.strip('%'), errors='coerce') / 100
        # C. Standardize date/time for merging. Convert to 'America/New_York' and rename for the merge.
        dratings_df['Time'] = pd.to_datetime(dratings_df['Time'], errors='coerce').dt.tz_convert('America/New_York')
        dratings_df.rename(columns={'Time': 'Timestamp'}, inplace=True)

        # --- Clean SBRI Data ---
        # Standardize date/time for merging. Localize to 'America/New_York' and rename for the merge.
        sbri_df['GameStart'] = pd.to_datetime(sbri_df['GameStart'], errors='coerce').dt.tz_localize('America/New_York', ambiguous='infer')
        sbri_df.rename(columns={'GameStart': 'Timestamp'}, inplace=True)

    except Exception as e:
        print(f"Error during pre-processing. Check column formats. Error: {e}")
        return pd.DataFrame()

    # 2. Perform an OUTER merge on the specific timestamp to handle doubleheaders
    print("Merging SBRI odds and DRatings predictions...")
    merged_df = pd.merge(
        sbri_df, 
        dratings_df, 
        on=['Timestamp', 'HomeTeam', 'AwayTeam'], 
        how='outer'
    )

    if merged_df.empty:
        print("Warning: Merge resulted in an empty dataframe.")
        return pd.DataFrame()
        
    # 3. Create a 'Status' column to identify missing data
    conditions = [
        (merged_df['Home MLOdds'].notna()) & (merged_df['HomeWinProb_pred'].notna()),
        (merged_df['Home MLOdds'].isna()),
        (merged_df['HomeWinProb_pred'].isna())
    ]
    choices = ['Ready for Analysis', 'Missing Odds', 'Missing Prediction']
    merged_df['Status'] = np.select(conditions, choices, default='Unknown')

    # Fill in missing game names and sort chronologically
    merged_df['Game'] = merged_df['Game'].fillna(merged_df['Teams'])
    merged_df.sort_values(by='Timestamp', inplace=True)


    # 4. Perform calculations only on rows that have enough data
    analysis_ready_df = merged_df[merged_df['Status'] == 'Ready for Analysis'].copy()
    
    if not analysis_ready_df.empty:
        print("Calculating values for matched bets...")
        # Convert American odds to Decimal
        analysis_ready_df['HomeDecimalOdds'] = analysis_ready_df['Home MLOdds'].apply(convert_american_to_decimal)
        analysis_ready_df['AwayDecimalOdds'] = analysis_ready_df['Away MLOdds'].apply(convert_american_to_decimal)

        # Calculate Implied Probabilities
        analysis_ready_df['HomeImpliedProb'] = 1 / analysis_ready_df['HomeDecimalOdds']
        analysis_ready_df['AwayImpliedProb'] = 1 / analysis_ready_df['AwayDecimalOdds']

        # Calculate the "Value" or "Edge"
        analysis_ready_df['HomeValue'] = (analysis_ready_df['HomeWinProb_pred'] * analysis_ready_df['HomeDecimalOdds']) - 1
        analysis_ready_df['AwayValue'] = (analysis_ready_df['AwayWinProb_pred'] * analysis_ready_df['AwayDecimalOdds']) - 1
        
        # Identify the best bet for each event
        analysis_ready_df['BestBetTeam'] = np.where(analysis_ready_df['HomeValue'] > analysis_ready_df['AwayValue'], analysis_ready_df['HomeTeam'], analysis_ready_df['AwayTeam'])
        analysis_ready_df['BestBetValue'] = np.maximum(analysis_ready_df['HomeValue'], analysis_ready_df['AwayValue'])
        
        # Filter for only positive value bets
        positive_value_bets = analysis_ready_df[analysis_ready_df['BestBetValue'] > 0]
        
        # Merge the calculated data back into the main dataframe
        final_df = merged_df.merge(positive_value_bets[['Timestamp', 'HomeTeam', 'AwayTeam', 'BestBetTeam', 'BestBetValue']], on=['Timestamp', 'HomeTeam', 'AwayTeam'], how='left')
    else:
        print("No rows were ready for analysis (missing odds or predictions).")
        final_df = merged_df
        final_df['BestBetTeam'] = np.nan
        final_df['BestBetValue'] = np.nan

    print("--- Analysis Complete ---")
    return final_df.sort_values(by='Timestamp') 
    
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

    # Process DRatings
    dratings_df = scrape_dratings_data()

    # --- Run the Analysis ---
    final_analysis_df = perform_baseball_analysis(sbri_df, dratings_df)

    # --- Display Results ---
    if not final_analysis_df.empty:
        print("\n\n--- Full Game List (Sorted Chronologically) ---")
        display_columns = [
            'Timestamp',
            'Game',
            'Status',
            'BestBetTeam',
            'BestBetValue'
        ]
        
        # Format the 'Edge' for readability, handling non-numeric values
        if 'BestBetValue' in final_analysis_df.columns:
             final_analysis_df['Edge'] = final_analysis_df['BestBetValue'].apply(
                lambda x: f"{x*100:.2f}%" if pd.notna(x) else ""
            )
             display_columns[-1] = 'Edge'


        print(final_analysis_df[display_columns].to_string(index=False))

    if final_analysis_df is not None and not final_analysis_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, CALC_SHEET_NAME, final_analysis_df)
    else:
        logging.error("Skipping Calc sheet update due to scraping failure or no data.")
        
    logging.info("Script finished.")

if __name__ == "__main__":
    main()

exit()