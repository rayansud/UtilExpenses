import pandas as pd
from thefuzz import process
from thefuzz import fuzz
from tqdm import tqdm 

fercs = pd.read_csv("utility_id_pudl.csv")
eias = pd.read_csv("utilities_entity_eia.csv")

ferc_matches = []
eia_matches = []

for idx,ferc_row in tqdm(fercs.iterrows()):
    print(ferc_row['utility_name_ferc1'])
    try:
        best_match = process.extractOne(ferc_row['utility_name_ferc1'], eias['utility_name_eia'])[0]
        ferc_matches.append(ferc_row['utility_name_ferc1'])
        eia_matches.append(best_match)
    except:
        print('')

pd.DataFrame(data = {'ferc':ferc_matches,
                     'eia':eia_matches}).to_csv('eia ferc fuzzy matched.csv')