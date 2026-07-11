import json
import pandas as pd
from nhlpy import NHLClient
client = NHLClient()

data = client.teams.team_roster(team_abbr="ANA", season="20242025")

forwards = data['forwards']
defensemen = data['defensemen']
goalies = data['goalies']

for p in forwards:      p['rosterPosition'] = 'F'
for p in defensemen:    p['rosterPosition'] = 'D'
for p in goalies:       p['rosterPosition'] = 'G'

all_players = forwards + defensemen + goalies

def flatten_player(p):
    return {
        k: (v['default']) if isinstance(v, dict) and 'default' in v else v
        for k, v in p.items()
    }

df = pd.DataFrame([flatten_player(p) for p in all_players])

print(df.shape)
print(df.head())
print(df.dtypes)

# df.to_csv("anaheim_ducks_roster_2024-25.csv", index=False)
# print("Saved!")