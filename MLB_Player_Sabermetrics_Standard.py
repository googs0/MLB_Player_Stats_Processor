import csv, json, logging, math
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor
import requests

# Initiate logging
logging.basicConfig(filename='script.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

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
MLB_2023_league_data = {
  'league_BA' : 0.248,
  'league_OBP' : 0.320,
  'league_SLG' : 0.414,
  'league_RBI' : 21512,
  'league_R' : 22432,
  'league_hits' : 40839,
  'league_AB' : 164418,
  'league_PA' : 184014,
  'league_singles' : 26031,
  'league_doubles' : 8228,
  'league_triples' : 712,
  'league_HR' : 5868,
  'league_BB' : 15819,
  'league_iBB' : 474,
  'league_HBP' : 2112,
  'league_SF' : 1230,
  'league_SH' : 429,
  'league_wOBA' : 0.318,
  'league_wOBA_scale' : 1.204,
  'league_wRC' : 22433,
  'league_wRC_plus' : 100,
  'league_RAR' : 5716,
  'league_WAR' : 570,
  'league_Dollars' : 4559.9,
  'league_SO' : 41843,
  'league_GDP' : 3466,
  'league_SB' : 3503,
  'league_CS' : 866,
  'league_BB_percentage' : 8.6,
  'league_K_percentage' : 22.7,
  'league_BB/K_ratio' : 0.38,
  'league_OPS_2023' : 0.734,
  'league_ISO' : 0.166,
  'league_BA_BIP' : 0.297,
  'league_BsR' : -1.6,
  'league_Off' : 39.5,
  'league_Def' : -586.3,
  'league_Spd' : 5.0,
  'league_UBR' : -0.2,
  'league_wGDP' : -1.4,
  'league_wSB' : 0.0,
  'league_wRAA' : 0.9,
  'league_GB/FB_ratio' : 1.13,
  'league_GB_percent' : 42.5,
  'league_FB_percent' : 37.5,
  'league_LD_percent' : 20.0,
  'league_IFFB_percent' : 9.8,
  'league_HR/FB' : 12.7,
  'league_IFH' : 3460,
  'league_IFH_percent' : 6.6,
  'league_BUH' : 345,
  'league_BUH%' : 31.2,
  'league_Pull%' : 41.1,
  'league_Cent%' : 34.6,
  'league_Oppo%' : 24.3,
  'league_Soft_percent' : 15.5,
  'league_Med_percent' : 51.8,
  'league_Hard_percent' : 32.6,
  'league_WPA' : -56.60,
  'league_-WPA+' : -3281.97,
  'league_+WPA' : 3225.36,
  'league_RE24' : -195.69,
  'league_REW' : -19.10,
  'league_pLI' : 0.99,
  'league_phLi' : 1.54,
  'league_PH' : 3743,
  'league_WPA/LI' : -29.88,
  'league_Clutch' : -27.24,
  'league_Batting' : 41.1,
  'league_Base_Running' : -1.6,
  'league_Fielding' : 3.3,
  'league_Positional' : -589.5,
  'league_Offense' : 39.5,
  'league_Defense' : -586.3,
  'league_League' : 546.6,
  'league_Replacement' : 5716.2,
  'league_Events' : 124330,
  'league_EV' : 89.0,
  'league_maxEV' : 121.2,
  'league_LA' : 12.2,
  'league_Barrels' : 10021,
  'league_Barrel_percent' : 8.1,
  'league_HardHit' : 48737, 
  'league_HardHit_percent' : 39.2,
}

# Player Class
class Player:
  # Initialize Player attributes with default values
  def __init__(self, data=None, name=None, team=None, position=None, age=None):
    self.name = name
    self.team = team
    self.position = position
    self.age = age
    
    self.player_attributes = {
      'height': 0.0,
      'weight': 0.0,
      'bat_type': '',
      'bat_order': 0,
      'BA': 0.0,
      'OBP': 0.0,
      'SLG': 0.0,
      'RBI': 0,
      'R': 0,
      'hits': 0,
      'AB': 0,
      'PA': 0,
      'singles': 0,
      'doubles': 0,
      'triples': 0,
      'HR': 0,
      'BB': 0,
      'iBB': 0,
      'HBP': 0,
      'SF': 0,
      'SH': 0,
      'wOBA': 0.0,
      'wOBA_scale': 0.0,
      'wRC': 0,
      'wRC_plus': 0,
      'RAR': 0,
      'WAR': 0,
      'SO': 0,
      'GDP': 0,
      'SB': 0,
      'CS': 0,
      'BB_percent': 0.0,
      'K_percent': 0.0,
      'BB_K_ratio': 0.0,
      'OPS_2023': 0.0,
      'ISO': 0.0,
      'BA_BIP': 0.0,
      'BsR': 0.0,
      'Off': 0.0,
      'Def': 0.0,
      'Spd': 0.0,
      'UBR': 0.0,
      'wGDP': 0.0,
      'wSB': 0.0,
      'wRAA': 0.0,
      'GB_FB_ratio': 0.0,
      'GB_percent': 0.0,
      'FB_percent': 0.0,
      'LD_percent': 0.0,
      'IFFB_percent': 0.0,
      'HR_FB_ratio': 0.0,
      'IFH': 0,
      'IFH_percent': 0.0,
      'BUH': 0,
      'BUH_percent': 0.0,
      'Pull_percent': 0.0,
      'Cent_percent': 0.0,
      'Oppo_percent': 0.0,
      'Soft_percent': 0.0,
      'Med_percent': 0.0,
      'Hard_percent': 0.0,
    }
    
    # Update player attributes with default values
    self.__dict__.update(self.player_attributes)
    # If data is provided, update player attributes with data
    if data is not None and isinstance(data, dict):
      for attr, value in data.items():
        if attr in self.player_attributes:
          setattr(self, attr, value)

# Function to fetch player data
# Function to fetch player data
def fetch_player_data(api_url, api_params, cache, player, name=None):
  # Create a unique cache key based on the API URL and parameters
  cache_key = (api_url, frozenset(api_params.items()))

  if cache_key in cache:
    return cache[cache_key]

  response = requests.get(api_url, params=api_params)
  try:
    with response as resp:
      if resp.status_code == 200:
        accumulated_data = ''
        for chunk in resp.iter_content(chunk_size=8192):
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
            process_player_data(player, MLB_2023_league_data)
            return data

          except json.JSONDecodeError:
            handle_missing_data(player)
            return None

        else:
          logging.error(f'Failed to fetch data from the API. Status code: {response.status_code}')
          return None
  except Exception as e:
    logging.error(f'An error occurred while fetching data: {str(e)}')
    return None
      
def process_player_data(player, league_data, name=None):
  if player.AB and player.OBP > 0.0:
    batting_average = player.hits / player.AB
    on_base_percentage = (player.hits + player.BB + player.HBP) / (player.AB + player.BB + player.HBP + player.SF)
    slugging_percentage = (player.singles + 2 * player.doubles + 3 * player.triples + 4 * player.HR) / player.AB
    wOBA = (0.697 * player.BB + 0.727 * player.HBP + 0.855 * player.singles + 1.248 * player.doubles +
            1.575 * player.triples + 2.014 * player.HR) / (player.AB + player.BB - player.iBB + player.SF + player.HBP)
    wRC = (wOBA - league_data['league_wOBA']) / league_data['league_wOBA_scale'] * player.PA
    wRC_plus = (wRC / player.PA) * 100

  else:
    batting_average = on_base_percentage = slugging_percentage = wOBA = wRC = wRC_plus = 0.0
    logging.warning('No active data found.')

  # Process player statistics
  ops = on_base_percentage + slugging_percentage
  iso = slugging_percentage - batting_average
  ops_plus = (ops / league_data['league_OPS_2023']) * 100
  war = player.WAR
  RAR = wRC - league_data['league_wRC']
  SO = player.PA - player.BB - player.HBP
  GDP = player.AB - player.R + SO + player.SH
  BB_percent = (player.BB / player.PA) * 100
  K_percent = (SO / player.PA) * 100
  BB_K_ratio = player.BB / SO
  BA_BIP = batting_average / (1.0 - (player.BB + SO + player.SF) / player.AB)
  BsR = player.UBR + player.wGDP + player.wSB + player.wRAA
  Off = ((wOBA - league_data['league_wOBA']) / league_data['league_wOBA_scale']) * player.PA
  Def = -586.3
  Spd = 5.0
  UBR = -0.2
  wGDP = -1.4
  wSB = 0.0
  wRAA = 0.9
  GB_FB_ratio = (player.GB_percent / player.FB_percent) if player.FB_percent > 0 else 0.0
  LD_percent = 100.0 - player.GB_percent - player.FB_percent
  IFFB_percent = 9.8
  HR_FB_ratio = 0.0
  IFH = 0
  IFH_percent = 0.0
  BUH = 0
  BUH_percent = 0.0
  Pull_percent = 0.0
  Cent_percent = 0.0
  Oppo_percent = 0.0
  Soft_percent = 0.0
  Med_percent = 0.0
  Hard_percent = 0.0

  logging.info(f'Slash Line  AVG: {batting_average:.3f} | OBP: {on_base_percentage:.3f} | SLG: {slugging_percentage:.3f}')
  logging.info(f'OPS: {ops:.3f} | OPS+: {ops_plus:.1f} | WAR: {war:.2f}')
  logging.info(f'wRC: {wRC:.2f} | wRC+: {wRC_plus:.1f} | ISO: {iso:.3f}')
  logging.info(f'RAR: {RAR:.2f} | SO: {SO} | GDP: {GDP}')
  logging.info(f'BB%: {BB_percent:.1f} | K%: {K_percent:.1f} | BB/K: {BB_K_ratio:.2f}')
  logging.info(f'OPS_2023: {ops:.3f} | ISO: {iso:.3f} | BA_BIP: {BA_BIP:.3f}')
  logging.info(f'BsR: {BsR:.1f} | Off: {Off:.1f} | Def: {Def:.1f}')
  logging.info(f'Spd: {Spd:.1f} | UBR: {UBR:.1f} | wGDP: {wGDP:.1f}')
  logging.info(f'wSB: {wSB:.1f} | wRAA: {wRAA:.1f} | GB/FB: {GB_FB_ratio:.2f}')
  logging.info(f'LD%: {LD_percent:.1f} | IFFB%: {IFFB_percent:.1f}')
  logging.info(f'HR/FB: {HR_FB_ratio:.1f} | IFH: {IFH} | IFH%: {IFH_percent:.1f}')
  logging.info(f'BUH: {BUH} | BUH%: {BUH_percent:.1f}')
  logging.info(f'Pull%: {Pull_percent:.1f} | Cent%: {Cent_percent:.1f} | Oppo%: {Oppo_percent:.1f}')
  logging.info(f'Soft%: {Soft_percent:.1f} | Med%: {Med_percent:.1f} | Hard%: {Hard_percent:.1f}')

# Data Validation for CSV File
def validate_csv_file(csv_file_path):
  try:
    with open(csv_file_path, 'r', buffering=8192) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        required_columns = ['name', 'team', 'position', 'bat_type', 'bat_order', 'column1', 'column2', 'column3']
        header = csv_reader.fieldnames
        if not all(col in header for col in required_columns):
            raise ValueError("CSV file is missing required columns.")
        for row in csv_reader:
          # Validate data format in each row
          if not validate_csv_row(row):
            raise ValueError("Invalid data in the CSV file")
  except FileNotFoundError:
      logger.error(f"CSV file '{csv_file_path}' not found.")

# Data Validation for CSV Rows
def validate_csv_row(row):
  # Replace with your numeric columns
  numeric_columns = ['column1', 'column2', 'column3']
  return all(is_valid_numeric(row[column]) for column in numeric_columns)

def is_valid_numeric(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

# Handling Missing Data
def handle_missing_data(player):
  missing_data_attributes = [key for key, value in player.player_attributes.items() if value is None or math.isnan(value)]
  for key in missing_data_attributes:
    setattr(player, key, player.player_attributes[key])

def validate_api_data(data):
  if data is None:
    raise ValueError("API data is None.")
  # Check if the data contains the expected fields and data format
  return 'expected_field' in data  # Replace 'expected_field' with your criteria

def generate_report(player_data, report_filename):
  with open(report_filename, 'w', newline='') as report_file:
    writer = csv.writer(report_file)
    # Header Row
    writer.writerow(['Name', 'Team', 'Position', 'Bat Type', 'Bat Order', 'Batting Average', 'OPS', 'WAR'])

    # Iterate through player data and write the report, make sure the parameters match the Header!
    for player in player_data:
      writer.writerow([player.name, player.team, player.position, player.bat_type, player.bat_order, player.BA, player.OPS, player.WAR])

def main():
  # File path for CSV data
  csv_file_path = 'player_stats.csv'
  # Cache with a maximum size of 1000 entries and 12-minute expiration
  cache = TTLCache(maxsize=1000, ttl=720)
  player_data = []
  try:
    with open(csv_file_path, 'r', buffering=8192) as csv_file:
      csv_reader = csv.DictReader(csv_file)

          # ThreadPoolExecutor with 4 threads
      with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        header = csv_reader.fieldnames
        required_columns = ['name', 'team', 'position', 'bat_type', 'bat_order', 'column1', 'column2', 'column3']

        if not all(col in header for col in required_columns):
          raise ValueError("CSV file is missing required columns.")

        for row in csv_reader:
          data = {k: float(v) if v else 0.0 for k, v in row.items()}
          player = Player(data)
          player_data.append(player)
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
  main()