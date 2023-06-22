import pandas as pd
from thefuzz import process
from thefuzz import fuzz

ferc = pd.read_excel('NERC_merge.xlsx',sheet_name='ferc_unmerged')
eia = pd.read_excel('NERC_merge.xlsx',sheet_name='eia_unmerged')

for idx,ferc_row in ferc.iterrows():
    print(ferc_row['utility_name_ferc1'])
    try:
        best_match = process.extract(ferc_row['utility_name_ferc1'], eia['utility_name_eia'], scorer=fuzz.ratio)
        print('matched')
        print(best_match)
    except TypeError:
        print('typeerror')
        best_match = ''
    except:
        print('other error')
        best_match = ''