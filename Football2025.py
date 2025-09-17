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
from bs4 import BeautifulSoup


# --- Configuration and Constants ---
load_dotenv()

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Credentials & URLs from .env file
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
GOOGLE_SHEET_KEY = os.getenv('GOOGLE_SHEET_KEY')
SBRI_URL = os.getenv('SBRI_NFL_URL')
DRATINGS_URL = os.getenv('DRATINGS_NFL_URL')
TPT_URL = os.getenv('TPT_NFL_URL')
FFWIN_URL = os.getenv('FFWIN_NFL_URL')


# Google API Scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Worksheet names
SBRI_SHEET_NAME = 'SBRI_NFL'
DRATE_SHEET_NAME = 'DRate_NFL'
TPT_SHEET_NAME = 'TPT_NFL'
FFWIN_SHEET_NAME = 'FFWin_NFL'

# --- Helper Functions ---

def get_google_sheet_client(service_account_path, scopes):
    """Authenticates with Google and returns a gspread client."""
    try:
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

# ---Data Cleanup for TPT Data ---

TEAM_NAME_MAP = {
    'Arizona': 'Arizona Cardinals',
    'Atlanta': 'Atlanta Falcons',
    'Baltimore': 'Baltimore Ravens',
    'Buffalo': 'Buffalo Bills',
    'Carolina': 'Carolina Panthers',
    'Chicago': 'Chicago Bears',
    'Cincinnati': 'Cincinnati Bengals',
    'Cleveland': 'Cleveland Browns',
    'Dallas': 'Dallas Cowboys',
    'Denver': 'Denver Broncos',
    'Detroit': 'Detroit Lions',
    'Green Bay': 'Green Bay Packers',
    'Houston': 'Houston Texans',
    'Indianapolis': 'Indianapolis Colts',
    'Jacksonville': 'Jacksonville Jaguars',
    'Kansas City': 'Kansas City Chiefs',
    'Las Vegas': 'Las Vegas Raiders',
    'LA Chargers': 'Los Angeles Chargers',
    'LA Rams': 'Los Angeles Rams',
    'Miami': 'Miami Dolphins',
    'Minnesota': 'Minnesota Vikings',
    'New England': 'New England Patriots',
    'New Orleans': 'New Orleans Saints',
    'N.Y. Giants': 'New York Giants',
    'N.Y. Jets': 'New York Jets',
    'Philadelphia': 'Philadelphia Eagles',
    'Pittsburgh': 'Pittsburgh Steelers',
    'San Francisco': 'San Francisco 49ers',
    'Seattle': 'Seattle Seahawks',
    'Tampa Bay': 'Tampa Bay Buccaneers',
    'Tennessee': 'Tennessee Titans',
    'Washington': 'Washington Commanders',
    'Eagles': 'Philadelphia Eagles',
    'Cowboys': 'Dallas Cowboys',
    'Chargers': 'Los Angeles Chargers',
    'Chiefs': 'Kansas City Chiefs',
    'Colts': 'Indianapolis Colts',
    'Dolphins': 'Miami Dolphins',
    'Jets': 'New York Jets',
    'Steelers': 'Pittsburgh Steelers',
    'Giants': 'New York Giants',
    'Falcons': 'Atlanta Falcons',
    'Buccaneers': 'Tampa Bay Buccaneers',
    'Saints': 'New Orleans Saints',
    'Cardinals': 'Arizona Cardinals',
    'Browns': 'Cleveland Browns',
    'Bengals': 'Cincinnati Bengals',
    'Jaguars': 'Jacksonville Jaguars',
    'Panthers': 'Carolina Panthers',
    'Patriots': 'New England Patriots',
    'Raiders': 'Las Vegas Raiders',
    'Broncos': 'Denver Broncos',
    'Titans': 'Tennessee Titans',
    'Seahawks': 'Seattle Seahawks',
    'Niners': 'San Francisco 49ers',
    'Rams': 'Los Angeles Rams',
    'Texans': 'Houston Texans',
    'Packers': 'Green Bay Packers',
    'Lions': 'Detroit Lions',
    'Bills': 'Buffalo Bills',
    'Ravens': 'Baltimore Ravens',
    'Bears': 'Chicago Bears',
    'Vikings': 'Minnesota Vikings'
    }

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
            if market.get('name') == 'Spread':
                for selection in market.get('selections', []):
                    if selection.get('name') == game_data['AwayTeam']:
                        game_data['AwaySpreadOdds'] = selection.get('price')
                    elif selection.get('name') == game_data['HomeTeam']:
                        game_data['HomeSpread'] = selection.get('currenthandicap')
                        game_data['HomeSpreadOdds'] = selection.get('price')      
            if market.get('name') == 'Total Points':
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

def scrape_dratings_data():
    """Scrapes, processes, and returns NFL data from DRatings."""
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    all_dfs = []
    
    with requests.Session() as session:
        session.headers.update(headers)
        for page_num in range(9): # This '9' is a "magic number", could be a constant
            url = f"{DRATINGS_URL}upcoming/{page_num}" if page_num > 0 else DRATINGS_URL
            try:
                response = session.get(url, timeout=15)
                response.raise_for_status()
                page_tables = pd.read_html(io.StringIO(response.text))
                if page_tables and len(page_tables[0].columns) >= 10:
                    all_dfs.append(page_tables[0])
            except requests.exceptions.RequestException as e:
                logging.warning(f"Could not scrape DRatings page {page_num}: {e}")
                continue # Try the next page
                
    if not all_dfs:
        logging.error("Failed to scrape any data from DRatings.")
        return None
        
    df = pd.concat(all_dfs, ignore_index=True)
    df = df.drop_duplicates(subset='Teams')
    df = df.drop(columns=['Quarterbacks', 'Best ML', 'Best Spread', 'Best O/U'], errors='ignore')
    # 1. Ensure the 'Time' column is a datetime object and drop bad text
    df['Time'] = pd.to_datetime(df['Time'], format='mixed', utc=True, errors='coerce')
    df.dropna(subset=['Time'], inplace=True)
    # 2. Convert to your local timezone (handles DST automatically!)
    df['Time'] = df['Time'].dt.tz_convert("America/New_York")       
    return df.sort_values(by=['Time', 'Teams'])
    
def scrape_tpt_data():
    """Scrapes, processes, and returns NFL data from a target site."""
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    all_dfs = []
        
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(TPT_URL, timeout=15)
            response.raise_for_status()

            # 1. Parse HTML and find the first <pre> tag
            soup = BeautifulSoup(response.text, 'html.parser')
            pre_tag = soup.find('pre')

            if not pre_tag:
                logging.error("Could not find a <pre> tag on the page.")
                return None

            full_text = pre_tag.get_text()

            # 2. Isolate the specific data table within the text
            # We find the header to know where the table starts
            header_start = 'Home                Visitor'
            table_start_index = full_text.find(header_start)
            
            if table_start_index == -1:
                logging.error("Could not find the data table header in the <pre> tag.")
                return None
            
            # The actual data starts two lines after the header line itself
            table_text = '\n'.join(full_text[table_start_index:].splitlines()[2:])

            # Find the separator line and keep only the text before it
            separator = '__________'
            if separator in table_text:
                table_text = table_text.split(separator, 1)[0]

            # 3. Define the column names and their character positions (colspecs)
            # These positions are based on the format in your HTML file
            col_specs = [
                (0, 19),   # Home
                (19, 39),  # Visitor
                (39, 48),  # Opening line
                (48, 57),  # Updated line
                (57, 66),  # Midweek line
                (66, 78),  # Prediction Avg.
                (78, 89),  # Prediction Median
                (89, 108), # Prediction Standard Deviation
                (108, 117),# Prediction Min
                (117, 124),# Prediction Max
                (124, 136),# Probability Wins
                (136, 146) # Probability Covers
            ]
            
            col_names = [
                'Home', 'Visitor', 'OpeningLine', 'UpdatedLine', 'MidweekLine', 
                'PredictionAvg', 'PredictionMedian', 'PredictionStdDev', 
                'PredictionMin', 'PredictionMax', 'ProbabilityWins', 'ProbabilityCovers'
            ]

            # 4. Use pd.read_fwf to parse the fixed-width text
            df = pd.read_fwf(io.StringIO(table_text), colspecs=col_specs, names=col_names)
            
            # Drop any rows that are completely empty, which can happen at the end
            df.dropna(how='all', inplace=True)
            
            # Strip leading/trailing whitespace from team names
            df['Home'] = df['Home'].str.strip()
            df['Visitor'] = df['Visitor'].str.strip()
            # Replace short names with full names in both columns
            df['Home'] = df['Home'].replace(TEAM_NAME_MAP)
            df['Visitor'] = df['Visitor'].replace(TEAM_NAME_MAP)
            df['Matchup'] = df['Visitor'] + ' at ' + df['Home']
            logging.info(f"Successfully parsed {len(df)} rows of data.")
            return df

    except requests.exceptions.RequestException as e:
        logging.warning(f"Could not scrape TPT page: {e}")
        return None
    except Exception as e:
        logging.error(f"An error occurred while parsing the data: {e}")
        return None    
    
def scrape_ffwin_data():
    """Scrapes, processes, and returns NFL data from FFWin."""
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/', # Mimics a search engine referral
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}
    all_dfs = []
    url = FFWIN_URL
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        page_tables = pd.read_html(io.StringIO(response.text))
        if page_tables and len(page_tables[0].columns) >= 3:
            all_dfs.append(page_tables[0])
    except requests.exceptions.RequestException as e:
        logging.warning(f"Error: {e}")
    if not all_dfs:
        logging.error("Failed to scrape any data from FFWin.")
        return None
    df = pd.concat(all_dfs, ignore_index=True)   
    df['HOME'] = df['HOME'].str.strip()
    df['AWAY'] = df['AWAY'].str.strip()
    # Replace short names with full names in both columns
    df['HOME'] = df['HOME'].replace(TEAM_NAME_MAP)
    df['AWAY'] = df['AWAY'].replace(TEAM_NAME_MAP)
    df['Matchup'] = df['AWAY'] + ' at ' + df['HOME']
    return df

# --- Main Execution ---

def main():
    """Main function to run the scraper and update Google Sheets."""
    logging.info("Starting the NFL data scraper script.")
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
    
    # Process TPT
    tpt_df = scrape_tpt_data()
    if tpt_df is not None and not tpt_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, TPT_SHEET_NAME, tpt_df)
    else:
        logging.error("Skipping TPT sheet update due to scraping failure or no data.")
    
    # Process FFWin
    ffwin_df = scrape_ffwin_data()
    if ffwin_df is not None and not ffwin_df.empty:
        write_df_to_sheet(gc, GOOGLE_SHEET_KEY, FFWIN_SHEET_NAME, ffwin_df)
    else:
        logging.error("Skipping FFWin sheet update due to scraping failure or no data.")

    logging.info("Script finished.")

if __name__ == "__main__":
    main()

exit()