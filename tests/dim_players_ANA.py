# %% ----
# 0. config
# ---- 

import json
import pandas as pd
from nhlpy import NHLClient

# main client object provide access to all NHL API endpoints
client = NHLClient()

# %% ----
# 1. build team lookup dictionary
#-----

# returns a flat list of all team dictionaries
team_data = client.teams.teams()
# finds franchise_id by passing in a team abbreviation
team_lookup = {team['abbr']: team['franchise_id'] for team in team_data}

print(f"Team lookup built - {len(team_lookup)} teams")


# %% ----
# 2. define seasons to pull
# ----

# list of season to pull, each will be looped through to collect all unique players
seasons = [
    "20202021",
    "20212022",
    "20222023",
    "20232024",
    "20242025"
]

# team abbreviation to pull players for
TEAM_ABBR = "ANA"

# Look up franchise_id from our lookup dictionary - to be stamp on every player row
TEAM_FRANCHISE_ID = team_lookup.get(TEAM_ABBR, None)
print(f"Franshise ID for {TEAM_ABBR}: {TEAM_FRANCHISE_ID}")


# %% ----
# 3. collect unique player IDs accross all sessions
# ----

# use set as it automatically deduplicates
all_player_ids = set()

# loop through each season at a time
for season in seasons:
    print(f"Fethcing {TEAM_ABBR} roster for {season}...")
    #returns roster fot this specific season in a form of dictionary with 3 keys: forwards, defensemen, goalies
    roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=season)
    #combine all three positions into a single flat list and loop through it to return ID 
    for p in roster['forwards'] + roster['defensemen'] + roster['goalies']:
        all_player_ids.add(p['id'])
        # .add() automatically deduplicates values

print(f"\nUnique players found accross all seassons: {len(all_player_ids)}")


# %% ----
# 4. define which fields to keep
# ----

# single value fields mapped dirrectly to the fina df
single_value_fields = [
    'playerId',
    'isActive',
    'currentTeamId',
    'currentTeamAbbrev',
    'sweaterNumber',
    'position',
    'heightInCentimeters',
    'weightInKilograms',
    'birthDate',
    'birthCountry',
    'shootsCatches',
    'headshot',
    'heroImage'
]

# nested DICT field to flatten 
# key = field name in API response
# value = which sub-key to extract 
#   default - grabs only default value
#   None - grabs all sub-keys, prefixed with field name

dict_fields = {
    'firstName': 'default',
    'lastName': 'default',
    'birthCity': 'default',
    'draftDetails': None
}


# %% ----
# 5. define flatten function
# ----

# reusable function that takes one raw player dictionary from API and returns a clean flat dictionary ready for pandas
def flatten_player(data, team_lookup):
    # empty dictionary that gets populated 
    player_flat = {}

    # loop through single value fields 
    for field in single_value_fields:
        player_flat[field] = data.get(field, None)

    # loop through nested DICT fields 
    for field, subkey in dict_fields.items():
        # skips field entirely if it does not exist on a player 
        if field not in data:
            continue
        if subkey:
            # extracts one specific sub-key value
            player_flat[field] = data[field].get(subkey, None)
        else:
            # extracts all sub-keys onto separate columns prefixed with parent field name
            for k, v in data[field].items():
                player_flat[f"{field}_{k}"] = v

    # add franchise_id by mapping to currentTeamAbbrev
    player_flat['franchise_id'] = team_lookup.get(
        data.get('currentTeamAbbrev', None), None
    )

    # returns the colmpleted flat player dictionary
    return player_flat


# %% ----
# 6. fetch career stats for each player
# ----

# empty list to collect one flat dict per player - each dict will become one row in the final df
all_player_flat = []

# loop through all unique player ID collected accross the seasons 
for player_id in all_player_ids:
    print(f"Fetching player {player_id}...")
    # fetch full career stats for a player in a form of large nested dict. str() converts int into string as API expects a string
    raw = client.stats.player_career_stats(player_id=str(player_id))
    # passes the raw response through the function to return a clean flat dict for a player 
    flat = flatten_player(raw, team_lookup)
    # override franchise_id with the one from previous loop using TEAM_FRANCHISE_ID constant defined in step 2 
    flat['franchise_id'] = TEAM_FRANCHISE_ID
    # adds player's flat dict to a collection list 
    all_player_flat.append(flat)


# %% ----
# 7. build a df
# ----

# converts list of flat player dicts into a pandas df - each dict becomes one row, each key becomes a column 
df = pd.DataFrame(all_player_flat)

# reorders key columns
key_cols = ['playerId', 'franchise_id', 'currentTeamId', 'currentTeamAbbrev']
# collects all remaining columns that are not defined in key_cols
other_cols = [c for c in df.columns if c not in key_cols]
# rewrites the df with new columns location
df = df[key_cols + other_cols]


# %% ----
# 8. validate and save
# ----

# print shape to confirm expected number of rows and columns
print(f"\nDone - {df.shape[0]} players, {df.shape[1]} columns")
# print data types for each column
print(df.dtypes)
# print firts 5 rows
print(df.head())

# save to CSV, index=False prevents pandas from writing its own index column
df.to_csv("dim_player_ANA.csv", index=False)
print("Saved dim_player_ANA.csv")