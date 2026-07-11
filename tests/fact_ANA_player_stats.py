import pandas as pd
import json
from nhlpy import NHLClient

client = NHLClient()

# definng columns to keeo
columns_to_keep = [
    'playerId',
    'franchise_id',
    'seasonId',
    'assists',
    'goals',
    'evGoals',
    'evPoints',
    'gameWinningGoals',
    'gamesPlayed',
    'faceoffWinPct',
    'points',
    'penaltyMinutes',
    'otGoals',
    'plusMinus',
    'pointsPerGame',
    'positionCode',
    'ppGoals',
    'ppPoints',
    'shGoals',
    'shPoints',
    'shootingPct',
    'shootsCatches',
    'shots',
    'timeOnIcePerGame'
]


# building dictionary 
team_data = client.teams.teams()
team_lookup = {team['abbr']: team['franchise_id'] for team in team_data}

print(f"Team lookup built - {len(team_lookup)} teams")

# pull all skaters 
raw_stats = client.stats.skater_stats_summary(
    start_season="20242025",
    end_season="20242025",  
    limit = -1
)

# filter for ANA players
ana_stats = [p for p in raw_stats if p['teamAbbrevs'] == 'ANA']

print(f"ANA skaters found - {len(ana_stats)}")

# add franchuse id to each player row
for player in ana_stats:
    player['franchise_id'] = team_lookup.get(player['teamAbbrevs'], None)

# convert into df 
df = pd.DataFrame(ana_stats)
# for col in df.columns:
#     print(col)
df = df[columns_to_keep]

# reorder columns 
key_cols = ['playerId', 'franchise_id', 'seasonId', 'teamAbbrevs']
other_cols = [c for c in df.columns if c not in key_cols]

# validate
print(df.shape)
print(df.dtypes)
print(df.head())

# same into .csv
df.to_csv("tests\\fact_ANA_player_stats.csv", index=False)



