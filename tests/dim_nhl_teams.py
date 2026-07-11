import pandas as pd
from nhlpy import NHLClient
import json

client = NHLClient()

data = client.teams.teams()


single_value_fields = [
    'franchise_id',
    'name',
    'common_name',
    'abbr',
    'logo'
]

dict_fields = {
    'conference': None,
    'division': None,    
}

def flatten_team(data):
    team_flat = {}

    for field in single_value_fields:
        team_flat[field] = data.get(field, None)

    for field, subkey in dict_fields.items():
        if field not in data:
            continue
        if subkey:
            team_flat[field] = data[field].get(subkey, None)
        else:
            for k, v in data[field].items():
                team_flat[f"{field}_{k}"] = v

    return team_flat

df = pd.DataFrame([flatten_team(team) for team in data])

# print(df.shape)
# print(df.dtypes)
# print(df.head())

df.to_csv("tests\\dim_nhl_teams.csv", index=False)
print("Saved dim_nhl_teams.csv!") 
