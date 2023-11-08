from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import logging
import math
import pandas as pd
import requests

# Translated to pandas

# 2023 MLB league stats
MLB_2023_league_data = {
  'league_BA': 0.248,
  'league_OBP': 0.320,
  'league_SLG': 0.414,
  'league_RBI': 21512,
  'league_R': 22432,
  'league_hits': 40839,
  'league_AB': 164418,
  'league_PA': 184014,
  'league_singles': 26031,
  'league_doubles': 8228,
  'league_triples': 712,
  'league_HR': 5868,
  'league_BB': 15819,
  'league_iBB': 474,
  'league_HBP': 2112,
  'league_SF': 1230,
  'league_SH': 429,
  'league_wOBA': 0.318,
  'league_wOBA_scale': 1.204,
  'league_wRC': 22433,
  'league_wRC_plus': 100,
  'league_RAR': 5716,
  'league_WAR': 570,
  'league_Dollars': 4559.9,
  'league_SO': 41843,
  'league_GDP': 3466,
  'league_SB': 3503,
  'league_CS': 866,
  'league_BB_percentage': 8.6,
  'league_K_percentage': 22.7,
  'league_BB_K_ratio': 0.38,
  'league_OPS_2023': 0.734,
  'league_ISO': 0.166,
  'league_BA_BIP': 0.297,
  'league_BsR': -1.6,
  'league_Off': 39.5,
  'league_Def': -586.3,
  'league_Spd': 5.0,
  'league_UBR': -0.2,
  'league_wGDP': -1.4,
  'league_wSB': 0.0,
  'league_wRAA': 0.9,
  'league_GB_FB_ratio': 1.13,
  'league_GB_percent': 42.5,
  'league_FB_percent': 37.5,
  'league_LD_percent': 20.0,
  'league_IFFB_percent': 9.8,
  'league_HR_FB_ratio': 12.7,
  'league_IFH': 3460,
  'league_IFH_percent': 6.6,
  'league_BUH': 345,
  'league_BUH_percent': 31.2,
  'league_Pull_percent': 41.1,
  'league_Cent_percent': 34.6,
  'league_Oppo_percent': 24.3,
  'league_Soft_percent': 15.5,
  'league_Med_percent': 51.8,
  'league_Hard_percent': 32.6,
  'league_WPA': -56.60,
  'league_-WPA+': -3281.97,
  'league_+WPA': 3225.36,
  'league_RE24': -195.69,
  'league_REW': -19.10,
  'league_pLI': 0.99,
  'league_phLi': 1.54,
  'league_PH': 3743,
  'league_WPA_LI': -29.88,
  'league_Clutch': -27.24,
  'league_Batting': 41.1,
  'league_Base_Running': -1.6,
  'league_Fielding': 3.3,
  'league_Positional': -589.5,
  'league_Offense': 39.5,
  'league_Defense': -586.3,
  'league_League': 546.6,
  'league_Replacement': 5716.2,
  'league_Events': 124330,
  'league_EV': 89.0,
  'league_maxEV': 121.2,
  'league_LA': 12.2,
  'league_Barrels': 10021,
  'league_Barrel_percent': 8.1,
  'league_HardHit': 48737,
  'league_HardHit_percent': 39.2,
}

# Player Class
class Player:
  def __init__(self, data=None):
    self.player_attributes = {
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
      'BB_percentage': 0.0,
      'K_percentage': 0.0,
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
  
    if data is not None:
      self.player_attributes.update(data)

  def calculate_statistics(self, league_data):
    self.BA = self.hits / self.AB
    self.OBP = (self.hits + self.BB + self.HBP) / (self.AB + self.BB + self.HBP + self.SF)
    self.SLG = (self.singles + 2 * self.doubles + 3 * self.triples + 4 * self.HR) / self.AB
    self.wOBA = (0.697 * self.BB + 0.727 * self.HBP + 0.855 * self.singles + 1.248 * self.doubles +
                 1.575 * self.triples + 2.014 * self.HR) / (self.AB + self.BB - self.iBB + self.SF + self.HBP)
    self.wRC = (self.wOBA - league_data['league_wOBA']) / league_data['league_wOBA_scale'] * self.PA
    self.wRC_plus = (self.wRC / self.PA) * 100
    self.RAR = self.wRC - league_data['league_wRC']
    self.BB_percentage = (self.BB / self.PA) * 100
    self.K_percentage = (self.SO / self.PA) * 100
    self.BB_K_ratio = self.BB / self.SO
    self.OPS_2023 = self.OBP + self.SLG
    self.ISO = self.SLG - self.BA
    self.BA_BIP = self.BA / (1.0 - (self.BB + self.SO + self.SF) / self.AB)
    self.BsR = self.UBR + self.wGDP + self.wSB + self.wRAA
    self.Off = ((self.wOBA - league_data['league_wOBA']) / league_data['league_wOBA_scale']) * self.PA
    self.Def = league_data['league_Def']
    self.Spd = league_data['league_Spd']
    self.UBR = league_data['league_UBR']
    self.wGDP = league_data['league_wGDP']
    self.wSB = league_data['league_wSB']
    self.wRAA = league_data['league_wRAA']
    self.GB_FB_ratio = self.GB_percent / self.FB_percent if self.FB_percent > 0 else 0.0
    self.LD_percent = 100.0 - self.GB_percent - self.FB_percent
    self.IFFB_percent = league_data['league_IFFB_percent']
    self.HR_FB_ratio = league_data['league_HR_FB_ratio']
    self.IFH = league_data['league_IFH']
    self.IFH_percent = league_data['league_IFH_percent']
    self.BUH = league_data['league_BUH']
    self.BUH_percent = league_data['league_BUH_percent']
    self.Pull_percent = league_data['league_Pull_percent']
    self.Cent_percent = league_data['league_Cent_percent']
    self.Oppo_percent = league_data['league_Oppo_percent']
    self.Soft_percent = league_data['league_Soft_percent']
    self.Med_percent = league_data['league_Med_percent']
    self.Hard_percent = league_data['league_Hard_percent']

# Data Validation for CSV File
def validate_csv_file(csv_file_path):
  try:
    with open(csv_file_path, 'r', buffering=8192) as csv_file:
      csv_reader = csv.DictReader(csv_file)
      # Replace with my required columns
      required_columns = ['column1', 'column2', 'column3']  
      header = csv_reader.fieldnames
      if not all(col in header for col in required_columns):
        raise ValueError("CSV file is missing required columns.")
      for row in csv_reader:
        # Validate data format in each row
        if not validate_csv_row(row):
          raise ValueError("Invalid data in the CSV file.")
  except FileNotFoundError:
    raise FileNotFoundError(f"CSV file '{csv_file_path}' not found.")

# Data Validation for CSV Rows
def validate_csv_row(row):
  # Replace with my numeric columns
  numeric_columns = ['numeric_column1', 'numeric_column2']  
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
  # Implement validation for the received API data
  # Check if the data contains the expected fields and data format
  return 'expected_field' in data  # Replace 'expected_field' with your criteria

def generate_report(player_data, report_filename):
  # Create or open a report file for writing
  with open(report_filename, 'w', newline='') as report_file:
    writer = csv.writer(report_file)
    # Header Row - Add more params
    writer.writerow(['Player Name', 'Batting Average', 'OPS', 'WAR'])

    # Iterate through player data and write report -- make sure params match Header!
    for player in player_data:
      writer.writerow([player.name, player.BA, player.OPS, player.WAR])

# Main function
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
    # Open the CSV file for reading
    with open(csv_file_path, 'r', buffering=8192) as csv_file:
      csv_reader = csv.DictReader(csv_file)

      # Create a ThreadPoolExecutor with a specified number of threads
      with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # Iterate through rows in the CSV file
        for row in csv_reader:
          data = {k: float(v) if v else 0.0 for k, v in row.items()}
          player = Player(data)
          player_data.append(player)  # Append player data to the list
          # Submit a task for fetching and processing player data in parallel
          futures.append(executor.submit(fetch_player_data, api_url, api_params, cache, player))

        # Wait for all tasks to complete
        for future in futures:
            future.result()

        generate_report(player_data, 'player_report.csv') 

  except FileNotFoundError:
      logger.error(f"CSV file '{csv_file_path}' not found.")
  except Exception as e:
      logger.error(f'An error occurred: {str(e)}')

# Check if the script is being run as the main program
if __name__ == "__main__":
  # Call the main function to start the script
  main()