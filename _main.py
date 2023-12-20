import pandas as pd
import plotly.express as px
from tabulate import tabulate
import numpy as np

# Console display settings
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 15)

# Dictionary of hitting stats in csv file
hitting_stats_dict = {
    'last_name, first_name': '', 'player_id': '', 'year': 0, 'player_age': 0, 'ab': 0, 'pa': 0, 'hit': 0, 'single': 0, 'double': 0,
    'triple': 0, 'home_run': 0, 'strikeout': 0, 'walk': 0, 'k_percent': 0, 'bb_percent': 0, 'batting_avg': 0,
    'slg_percent': 0, 'on_base_percent': 0, 'on_base_plus_slg': 0, 'isolated_power': 0, 'babip': 0, 'b_rbi': 0,
    'b_lob': 0, 'b_total_bases': 0, 'r_total_caught_stealing': 0, 'r_total_stolen_base': 0, 'b_ab_scoring': 0,
    'b_ball': 0, 'b_called_strike': 0, 'b_catcher_interf': 0, 'b_foul': 0, 'b_foul_tip': 0, 'b_game': 0,
    'b_gnd_into_dp': 0, 'b_gnd_into_tp': 0, 'b_gnd_rule_double': 0, 'b_hit_by_pitch': 0, 'b_hit_ground': 0,
    'b_hit_fly': 0, 'b_hit_into_play': 0, 'b_hit_line_drive': 0, 'b_hit_popup': 0, 'b_out_fly': 0, 'b_out_ground': 0,
    'b_out_line_drive': 0, 'b_out_popup': 0, 'b_intent_ball': 0, 'b_intent_walk': 0, 'b_interference': 0,
    'b_pinch_hit': 0, 'b_pinch_run': 0, 'b_pitchout': 0, 'b_played_dh': 0, 'b_sac_bunt': 0, 'b_sac_fly': 0,
    'b_total_sacrifices': 0, 'r_defensive_indiff': 0, 'r_run': 0, 'b_total_ball': 0, 'b_total_strike': 0,
    'b_swinging_strike': 0, 'r_caught_stealing_2b': 0, 'r_caught_stealing_3b': 0, 'r_caught_stealing_home': 0,
    'r_interference': 0, 'r_pickoff_1b': 0, 'r_pickoff_2b': 0, 'r_pickoff_3b': 0, 'r_stolen_base_2b': 0,
    'r_stolen_base_3b': 0, 'r_stolen_base_home': 0, 'b_total_swinging_strike': 0, 'b_total_pitches': 0,
    'r_stolen_base_pct': 0, 'r_total_pickoff': 0, 'b_reached_on_error': 0, 'b_walkoff': 0, 'b_reached_on_int': 0,
    'xba': 0, 'xslg': 0, 'woba': 0, 'xwoba': 0, 'xobp': 0, 'xiso': 0, 'wobacon': 0, 'xwobacon': 0, 'bacon': 0,
    'xbacon': 0, 'xbadiff': 0, 'xslgdiff': 0, 'wobadiff': 0, 'exit_velocity_avg': 0, 'launch_angle_avg': 0,
    'sweet_spot_percent': 0, 'barrel': 0, 'barrel_batted_rate': 0, 'solidcontact_percent': 0,
    'flareburner_percent': 0, 'poorlyunder_percent': 0, 'poorlytopped_percent': 0, 'poorlyweak_percent': 0,
    'hard_hit_percent': 0, 'avg_best_speed': 0, 'avg_hyper_speed': 0, 'z_swing_percent': 0,
    'z_swing_miss_percent': 0, 'oz_swing_percent': 0, 'oz_swing_miss_percent': 0, 'oz_contact_percent': 0,
    'out_zone_swing_miss': 0, 'out_zone_swing': 0, 'out_zone_percent': 0, 'out_zone': 0,
    'meatball_swing_percent': 0, 'meatball_percent': 0, 'pitch_count_offspeed': 0, 'pitch_count_fastball': 0,
    'pitch_count_breaking': 0, 'pitch_count': 0, 'iz_contact_percent': 0, 'in_zone_swing_miss': 0,
    'in_zone_swing': 0, 'in_zone_percent': 0, 'in_zone': 0, 'edge_percent': 0, 'edge': 0, 'whiff_percent': 0,
    'swing_percent': 0, 'pull_percent': 0, 'straightaway_percent': 0, 'opposite_percent': 0, 'batted_ball': 0,
    'f_strike_percent': 0, 'groundballs_percent': 0, 'groundballs': 0, 'flyballs_percent': 0, 'flyballs': 0,
    'linedrives_percent': 0, 'linedrives': 0, 'popups_percent': 0, 'popups': 0, 'pop_2b_sba_count': 0,
    'pop_2b_sba': 0, 'pop_2b_sb': 0, 'pop_2b_cs': 0, 'pop_3b_sba_count': 0, 'pop_3b_sba': 0, 'pop_3b_sb': 0,
    'pop_3b_cs': 0, 'exchange_2b_3b_sba': 0, 'maxeff_arm_2b_3b_sba': 0, 'n_outs_above_average': 0
}


class Player:
    def __init__(self, full_name):
        df = pd.read_csv('2015-2023-baseball-hitting-stats.csv')

        if 'last_name, first_name' in df.columns:
            # Split the 'last_name, first_name' column into 2 columns, add full_name column
            df[['last_name', 'first_name']] = df['last_name, first_name'].str.split(', ', n=1, expand=True)
            df['full_name'] = df['first_name'] + ' ' + df['last_name']

            # Find player
            player_data = df[df['full_name'] == full_name].to_dict(orient='records')
            if player_data:
                player_data = player_data[0]

                # Iterate through hitting_stats_dict and set attributes
                for key, default_value in hitting_stats_dict.items():
                    setattr(self, key, player_data.get(key, default_value))

                # Set the full_name attribute
                self.full_name = player_data['full_name']
            else:
                print(f"No player found with the full name: {full_name}")
        else:
            print("Error: 'last_name, first_name' column not found.")

    def validate_numeric_attributes(self):
        # Get all attributes
        attributes = vars(self).keys()

        for attr in attributes:
            value = getattr(self, attr, None)

            if value is not None and isinstance(value, (int, float)):
                # Convert to int if the attribute is numeric
                setattr(self, attr, int(value))
            else:
                print(f"Invalid value for {attr}: {value}. Using default value 0.")
                setattr(self, attr, 0)

    @staticmethod
    def compare_players(*player_objects):
        player_names = [p.full_name for p in player_objects]
        # Get all attributes
        stats = vars(player_objects[0]).keys()

        # Prep for tabulation
        data = []
        for stat in stats:
            values = [getattr(p, stat) for p in player_objects]
            data.append([stat] + values)

        # Formatted table
        table = tabulate(data, headers=['Attribute'] + player_names, tablefmt='pretty')
        print(table)


def find_stat_leaders(df, stats_of_interest, top_n=1, years_range=None):
    """
    Find the top leaders for specified stats in each year or range of years.

    Parameters:
    1:param df (pd.DataFrame): The DataFrame containing the data.
    2:param stats_of_interest (list): List of stats for which leaders need to be found.
    3:param top_n (int): Number of top leaders to find for each year. Default is 1.
    4:param years_range (list or None): List of years. If None, consider all years. Default is None. Ex. [2015, 2016]
    _:return: pd.DataFrame: DataFrame containing the top leader(s) for specified stats in each year or range of years.
    """

    if years_range:
        df = df[df['year'].isin(years_range)]

    # Check if specified columns exist in the DataFrame
    missing_columns = set(stats_of_interest) - set(df.columns)
    if missing_columns:
        print(f"Warning: Columns {missing_columns} not found in the DataFrame. Skipping those columns.")

    # Drop rows with NaN values for the specified columns
    df_filtered = df.dropna(subset=stats_of_interest, how='all')

    # Group by 'year' and find the top leaders for each stat
    grouped_data = df_filtered.groupby('year')
    leaders_in_years = grouped_data.apply(lambda x: x.nlargest(top_n, columns=stats_of_interest))

    return leaders_in_years


def polar_pull_straight_oppo(player_name):
    """
    Create a polar bar chart showing the pull, straightaway, and opposite percentages for a given player

    1:param player_name: str [full_name of the player (example: 'Gunnar Henderson')]
    _:return: Polar chart
    """
    player = Player(player_name)

    # Check if the player instance was successfully created
    if player.full_name is not None:
        # Extract relevant metrics for the polar bar chart
        metrics = ['pull_percent', 'straightaway_percent', 'opposite_percent']
        values = [getattr(player, metric) for metric in metrics]

        # DataFrame for the polar
        df = pd.DataFrame({
            'Metric': metrics,
            'Value': values
        })

        # Create a polar bar chart using Plotly Express
        fig = px.bar_polar(df, r='Value', theta='Metric', color='Metric',
                           range_theta=[0, 360],
                           color_discrete_sequence=px.colors.qualitative.Vivid,
                           labels={'Value': 'Percentage'},
                           title=f'''{player.full_name}
                           Pull / Straightaway / Opposite Percentages''',
                           width=1000, height=900)

        fig.update_layout(
            polar=dict(
                radialaxis=dict(tickvals=[], ticktext=['0%', '25%', '50%', '75%', '100%']),
            ),
            showlegend=True
        )

        fig.show()
    else:
        print(f"No player found with the full name: {player_name}")


def radar_chart(player_name):
    """
    Create a radar chart showing the hitting performance percentages for a given player. Percentages include:
    groundball, flyball, linedrive, popup, sweet spot, hard hit, solid contact,
    flareburner, poorly under, and poorly topped

    1:param player_name: str [full_name of the player (example: 'Gunnar Henderson')]
    _:return: radar chart
    """
    player = Player(player_name)

    if player.full_name is not None:
        # Metrics to extract
        metrics = ['groundballs_percent', 'flyballs_percent', 'linedrives_percent', 'popups_percent',
                   'sweet_spot_percent', 'hard_hit_percent', 'solidcontact_percent', 'flareburner_percent',
                   'poorlyunder_percent', 'poorlytopped_percent', 'poorlyweak_percent']

        # Extract values for the specified metrics
        values = [getattr(player, metric) for metric in metrics]

        df = pd.DataFrame({
            'Metric': metrics,
            'Value': values
        })

        # Radar chart
        fig = px.line_polar(df, r='Value', theta='Metric', line_close=True,
                            range_theta=[0, 360],
                            labels={'Value': 'Percentage'},
                            title=f'{player.full_name}',
                            width=1000, height=900,
                            color_discrete_sequence=px.colors.qualitative.G10)

        fig.update_layout(
            polar=dict(
                radialaxis=dict(tickvals=[], ticktext=['0%', '25%', '50%', '75%', '100%']),
            ),
            showlegend=True,
            annotations=[
                dict(
                    text='Hitting Performance Percentages',
                    xref='paper',
                    yref='paper',
                    x=-0.06, y=1.05,
                    showarrow=False,
                    font=dict(size=14, color='#444'))]
        )

        fig.show()
    else:
        print(f"No player found with the full name: {player_name}")


def bubble_chart(*player_names):
    """
    Create a bubble chart showing the relationship between Average Exit Velocity, Average Launch Angle,
    Iso Power, and HRs

    1:param player_names: str [full_name of the players (example: 'Gunnar Henderson', 'Adley Rutschman', 'Shohei Ohtani')]
    _:return: bubble chart
    """
    df = pd.DataFrame()

    # Iterate through each player name
    for player_name in player_names:
        player = Player(player_name)

        if player.full_name is not None:
            # Specify the numeric attributes for the bubble chart
            numeric_attributes = ['exit_velocity_avg', 'launch_angle_avg', 'home_run', 'triple', 'double', 'isolated_power']

            # Create a dictionary with attribute names as keys and lists of attribute values as values
            data = {attr: [getattr(player, attr)] for attr in numeric_attributes}

            df = pd.concat([df, pd.DataFrame(data)])

        else:
            print(f"No player found with the full name: {player_name}")

    # Bubble chart
    fig = px.scatter(df, x='exit_velocity_avg', y='launch_angle_avg', size='home_run',
                     color='isolated_power', text=player_names,
                     labels={'exit_velocity_avg': 'Average Exit Velocity',
                             'launch_angle_avg': 'Average Launch Angle',
                             'home_run': 'Home Runs',
                             'isolated_power': 'Isolated Power'},
                     title='Average Exit Velocity vs. Average Launch Angle in correlation to Iso Power and Home Runs',
                     width=1000, height=900)

    fig.update_traces(textposition='top center')
    fig.show()


def scatter_plot(*player_names):
    """
    Create a scatter plot showing the relationship between Grounded Into Extra Out Total and Hits

    1:param player_names: str [full_name of the player (example: 'Gunnar Henderson',
    'Adley Rutschman', 'Shohei Ohtani')]
    _:return: scatter plot
    """
    df = pd.DataFrame()

    for player_name in player_names:
        player = Player(player_name)

        if player.full_name is not None:
            # Metrics to extract for scatter plot
            metrics = ['b_gnd_into_dp', 'b_gnd_into_tp', 'b_hit_into_play', 'player_age', 'hit', 'ab', 'pa']

            # Extract metrics
            data = {attr: [getattr(player, attr)] for attr in metrics}

            # Calculate grounded_into_extra_out_total and add it to the data
            data['grounded_into_extra_out_total'] = [data['b_gnd_into_dp'][0] + data['b_gnd_into_tp'][0]]

            df = pd.concat([df, pd.DataFrame(data)])
        else:
            print(f"No data available for the player: {player_name}")

    # Scatter plot
    fig = px.scatter(df, x='grounded_into_extra_out_total', y='hit', size='grounded_into_extra_out_total',
                     color='hit', text=player_names,
                     labels={'grounded_into_extra_out_total': 'Grounded Into Extra Out Total',
                             'player_age': 'Player Age',
                             'hit': 'Hits',
                             'ab': 'At Bats',
                             'pa': 'Plate Appearances'},
                     title='Grounded Into Extra Out Total vs. Hits',
                     width=1000, height=900,
                     hover_data={'grounded_into_extra_out_total': True,
                                 'player_age': True,
                                 'hit': True,
                                 'ab': True,
                                 'pa': True})

    fig.update_traces(marker=dict(sizemode='diameter', sizeref=0.2), textposition='top center')
    fig.show()


def main():
    # Read the CSV file
    df = pd.read_csv('2015-2023-baseball-hitting-stats.csv')

    # Compare players
    gunnar_henderson = Player('Gunnar Henderson')
    adley_rutschman = Player('Adley Rutschman')
    shohei_ohtani = Player('Shohei Ohtani')
    Player.compare_players(gunnar_henderson, adley_rutschman, shohei_ohtani)

    # Stat leaders
    stats_of_interest = ['woba']
    top_stat_leaders = find_stat_leaders(df, stats_of_interest, top_n=1, years_range=[2021, 2022, 2023])
    print(f"Top Stat Leaders for {stats_of_interest}\n{top_stat_leaders}")

    # Plots
    polar_pull_straight_oppo('Gunnar Henderson')
    radar_chart('Gunnar Henderson')
    bubble_chart('Gunnar Henderson', 'Adley Rutschman', 'Shohei Ohtani', 'Adolis Garcia',
                 'Juan Soto', 'Anthony Santander', 'Matt Olson', 'Corey Seager')
    scatter_plot('Gunnar Henderson', 'Shohei Ohtani', 'Adley Rutschman', 'Anthony Santander',
                 'Aaron Judge', 'Adolis Garcia')


if __name__ == "__main__":
    main()
