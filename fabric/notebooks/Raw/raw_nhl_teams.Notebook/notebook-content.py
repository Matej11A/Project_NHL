# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7fa7e213-3f48-43a9-9854-240338af99f8",
# META       "default_lakehouse_name": "lh_Main",
# META       "default_lakehouse_workspace_id": "4dde6d65-494a-490e-895a-613e38da7758",
# META       "known_lakehouses": [
# META         {
# META           "id": "7fa7e213-3f48-43a9-9854-240338af99f8"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1c9c4c3b-612e-8da1-4689-71c8adda755d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

ingestion_date = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from nhlpy import NHLClient
import json
import os
from datetime import date
import notebookutils.mssparkutils as mssparkutils 

client = NHLClient()
data = client.teams.teams()

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

raw_path = f"/lakehouse/default/Files/raw/nhl_teams/{ingestion_date}/teams.json"

os.makedirs(os.path.dirname(raw_path), exist_ok=True)

with open (raw_path, "w") as f:
    json.dump(data, f)

print(f"Wrote raw teams JSON to {raw_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
