import csv, json, logging, math
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import requests

# API Key and URL
base_url = ''
api_key = ''
api_url = f'{base_url}?api_key={api_key}'

# API Parameters - API key is param0
api_params = {
  'api_key' : api_key,
  'param1' : 'value1',
  'param2' : 'value2',
}

# 2023 MLB league stats
MLB_2023_league_data = pd.DataFrame({
  'Stat': [
    'BA', 'OBP', 'SLG', 'RBI', 'R', 'hits', 'AB', 'PA', 'singles', 'doubles', 'triples',
    'HR', 'BB', 'iBB', 'HBP', 'SF', 'SH', 'wOBA', 'wOBA_scale', 'wRC', 'wRC_plus', 'RAR',
    'WAR', 'SO', 'GDP', 'SB', 'CS', 'BB_percentage', 'K_percentage', 'BB_K_ratio',
    'OPS_2023', 'ISO', 'BA_BIP', 'BsR', 'Off', 'Def', 'Spd', 'UBR', 'wGDP', 'wSB', 'wRAA',
    'GB_FB_ratio', 'GB_percent', 'FB_percent', 'LD_percent', 'IFFB_percent', 'HR_FB_ratio',
    'IFH', 'IFH_percent', 'BUH', 'BUH_percent', 'Pull_percent', 'Cent_percent', 'Oppo_percent',
    'Soft_percent', 'Med_percent', 'Hard_percent', 'WPA', '-WPA+', '+WPA', 'RE24', 'REW',
    'pLI', 'phLi', 'PH', 'WPA_LI', 'Clutch', 'Batting', 'Base_Running', 'Fielding', 'Positional',
    'Offense', 'Defense', 'League', 'Replacement', 'Events', 'EV', 'maxEV', 'LA', 'Barrels',
    'Barrel_percent', 'HardHit', 'HardHit_percent'
  ],
  'Value': [
    0.248, 0.320, 0.414, 21512, 22432, 40839, 164418, 184014, 26031, 8228, 712, 5868, 15819,
    474, 2112, 1230, 429, 0.318, 1.204, 22433, 100, 5716, 570, 41843, 3466, 3503, 866,
    8.6, 22.7, 0.38, 0.734, 0.166, 0.297, -1.6, 39.5, -586.3, 5.0, -0.2, -1.4, 0.0, 1.13,
    42.5, 37.5, 20.0, 9.8, 12.7, 3460, 6.6, 345, 31.2, 41.1, 34.6, 24.3, 15.5, 51.8, 32.6,
    -56.60, -3281.97, 3225.36, -195.69, -19.10, 0.99, 1.54, 3743, -29.88, -27.24, 41.1, -1.6,
    3.3, -589.5, 39.5, -586.3, 546.6, 5716.2, 124330, 89.0, 121.2, 12.2, 10021, 8.1, 48737, 39.2
    ]
})

# Player Class
class Player:
  def __init__(self, data=None, name=None, team=None, age=None):
    self.player_attributes = pd.Series(data)
    self.player_attributes.fillna(0, inplace=True)
    self.name = name
    self.team = team
    self.age = age

  def calculate_statistics(self, league_data):
    self.BA = self.hits / self.AB
    self.OBP = (self.hits + self.BB + self.HBP) / (self.AB + self.BB + self.HBP + self.SF)
    self.SLG = (self.singles + 2 * self.doubles + 3 * self.triples + 4 * self.HR) / self.AB
    self.wOBA = (0.697 * self.BB + 0.727 * self.HBP + 0.855 * self.singles + 1.248 * self.doubles +
                 1.575 * self.triples + 2.014 * self.HR) / (self.AB + self.BB - self.iBB + self.SF + self.HBP)
    self.wRC = (self.wOBA - league_data['wOBA']) / league_data['wOBA_scale'] * self.PA
    self.wRC_plus = (self.wRC / self.PA) * 100
    self.RAR = self.wRC - league_data['wRC']
    self.BB_percentage = (self.BB / self.PA) * 100
    self.K_percentage = (self.SO / self.PA) * 100
    self.BB_K_ratio = self.BB / self.SO
    self.OPS_2023 = self.OBP + self.SLG
    self.ISO = self.SLG - self.BA
    self.BA_BIP = self.BA / (1.0 - (self.BB + self.SO + self.SF) / self.AB)
    self.BsR = self.UBR + self.wGDP + self.wSB + self.wRAA
    self.Off = ((self.wOBA - league_data['wOBA']) / league_data['wOBA_scale']) * self.PA
    self.Def = league_data['Def']
    self.Spd = league_data['Spd']
    self.UBR = league_data['UBR']
    self.wGDP = league_data['wGDP']
    self.wSB = league_data['wSB']
    self.wRAA = league_data['wRAA']
    self.GB_FB_ratio = self.GB_percent / self.FB_percent if self.FB_percent > 0 else 0.0
    self.LD_percent = 100.0 - self.GB_percent - self.FB_percent
    self.IFFB_percent = league_data['IFFB_percent']
    self.HR_FB_ratio = league_data['HR_FB_ratio']
    self.IFH = league_data['IFH']
    self.IFH_percent = league_data['IFH_percent']
    self.BUH = league_data['BUH']
    self.BUH_percent = league_data['BUH_percent']
    self.Pull_percent = league_data['Pull_percent']
    self.Cent_percent = league_data['Cent_percent']
    self.Oppo_percent = league_data['Oppo_percent']
    self.Soft_percent = league_data['Soft_percent']
    self.Med_percent = league_data['Med_percent']
    self.Hard_percent = league_data['Hard_percent']


def fetch_player_data(api_url, api_params, cache, player, name=None):
  # Create a unique cache key based on the API URL and parameters
  cache_key = (api_url, frozenset(api_params.items()))
  if cache_key in cache:
    return cache[cache_key]

  try:
    response = requests.get(api_url, params=api_params)

    if response.status_code == 200:
      accumulated_data = ''
      for chunk in response.iter_content(chunk_size=8192):
        chunk_decoded = chunk.decode('utf-8')
        accumulated_data += chunk_decoded

      records = accumulated_data.split('\n')

      for record in records[:-1]:
        try:
          data = json.loads(record)
          if not validate_api_data(data):
            handle_missing_data(player)
          # Store the fetched data in the cache with expiration
          cache[cache_key] = data
          player.calculate_statistics(MLB_2023_league_data)
          return data
        except json.JSONDecodeError:
          handle_missing_data(player)
          return None
      else:
        logging.error(f'Failed to fetch data from the API. Status code: {response.status_code}')
        return None
    else:
      logging.error(f'Failed to fetch data from the API. Status code: {response.status_code}')
      return None
  except Exception as e:
      logging.error(f'An error occurred while fetching data: {str(e)}')
      return None

# Validate CSV File using Pandas
def validate_csv_file(csv_file_path, logger):
  try:
    df = pd.read_csv(csv_file_path)
    required_columns = ['column1', 'column2', 'column3']  # Replace with your actual column names
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
      raise ValueError(f"CSV file is missing required columns: {', '.join(missing_columns)}")
  except FileNotFoundError:
      logger.error(f"CSV file '{csv_file_path}' not found.")
  except Exception as e:
      logger.error(f"An error occurred during CSV file validation: {str(e)}")

def is_valid_numeric(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

# Handling Missing Data
def handle_missing_data(player):
  player.player_attributes.fillna(0, inplace=True)

def validate_api_data(data):
  if data is None:
    raise ValueError("API data is None")
  # Check if the data contains the expected fields and data format
  return 'expected_field' in data  # Replace 'expected_field' with your criteria

def generate_report(player_data, report_filename):
  # Create report file for writing
  report_data = []

  # Iterate through player data and build the report
  for player in player_data:
    report_data.append([player.name, player.team, player.age, player.BA, player.OBP, player.SLG, player.wOBA, player.wRC])

  # Convert the report data to a Pandas DataFrame
  report_df = pd.DataFrame(report_data, columns=['Player Name', 'Team', 'Age', 'Batting Average', 'OBP', 'SLG', 'wOBA', 'wRC'])

  # Write the report DataFrame to a CSV file
  report_df.to_csv(report_filename, index=False)

def main():
  # Logging config
  logging.basicConfig(filename='script.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  logger = logging.getLogger()

  # File path for CSV data
  csv_file_path = 'player_stats.csv'

  # Cache with max size of 1000 entries and 12-minute expiration
  cache = TTLCache(maxsize=1000, ttl=720)

  player_data = []  # Create an empty list to collect player data
  try:
    # Open the CSV file for reading using Pandas
    df = pd.read_csv(csv_file_path)

    # Iterate through rows of the DataFrame
    for _, row in df.iterrows():
      data = {k: float(row[k]) if k in row else 0.0 for k in MLB_2023_league_data['Stat']}
      player = Player(data)
      player_data.append(player)
      # Submit a task for fetching and processing player data in parallel
      futures.append(executor.submit(fetch_player_data, api_url, api_params, cache, player, name=player.name))

    # Wait for all tasks to complete
    for future in futures:
      future.result()

    generate_report(player_data, 'player_report.csv')

  except FileNotFoundError:
    logger.error(f"CSV file '{csv_file_path}' not found.")
  except Exception as e:
    logger.error(f'An error occurred: {str(e)}')

if __name__ == "__main__":
  futures = []
  with ThreadPoolExecutor(max_workers=5) as executor:
    main()