import wget
from pathlib import Path

COUNTY_DATA_SOURCE_FILENAME = "cbp16co.zip"
COUNTY_DATA_SOURCE = "https://www2.census.gov/programs-surveys/cbp/datasets/2016/" + COUNTY_DATA_SOURCE_FILENAME + "?#"
COUNTY_DATA_INFO_SOURCE = "https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2015_record_layouts/county_layout_2015.txt?#"

COUNTY_DISTANCES_SOURCE_FILENAME = "sf12010countydistance100miles.csv.zip"
COUNTY_DISTANCES_SOURCE = "https://www.nber.org/distance/2010/sf1/county/" + COUNTY_DISTANCES_SOURCE_FILENAME

COUNTY_NAMES_SOURCE = "https://www.nber.org/distance/2010/sf1/county/sf12010countyname.csv"

assets_dir = Path('./assets')
assets_dir.mkdir(exist_ok=True)

# you can do this with the paths library!
for filename, source in [
        (COUNTY_DATA_SOURCE_FILENAME, COUNTY_DATA_SOURCE),
        (COUNTY_DISTANCES_SOURCE_FILENAME, COUNTY_DISTANCES_SOURCE)
    ]:
    if not (assets_dir/filename).exists():
        wget.download(source, out=str(assets_dir))

# we have our assets!
print("Asset downloading completed")

# parse information file
# table of protected, competitive, and monopolistic sectors
text_data = None
with open("sectoral_data.txt", 'r') as sectoral_data_file:
    text_data = sectoral_data_file.read()

import re

# prefix matcher
# if we're making something more sophisticated with most specific first back to least specific
# we could do this by set matching based on most specific first, then less specific, etc
# for now, lets just do iterative comparison for EXPENSIVE time
industrial_classifications = {}

LABOR_CODE_MATCHER = re.compile(r'[0-9]+ [CMT]')
for match in re.finditer(LABOR_CODE_MATCHER, text_data):
    result = match.group(0)
    space_idx = result.index(' ')
    industrial_classifications[result[:space_idx]] = result[space_idx + 1]

print("Sectoral classification parsing completed")
print(industrial_classifications)

import pandas as pd
import numpy as np

full_data = pd.read_csv(str(assets_dir/COUNTY_DATA_SOURCE_FILENAME))

# data cleaning
# only use if current results are no good
# full_data.drop(full_data[(full_data['empflag'] == 'S') | (full_data['emp_nf'] == 'D')], inplace=True)

# only concerned about our industries and total employment
TOTAL_EMPLOYMENT_NAICS_FLAG = '------'

industrial_prefixes = np.empty(len(full_data['naics']), dtype=bool)

i = 0
for naic in full_data['naics']:
    found = False
    for key in industrial_classifications.keys():
        if naic.startswith(key):
            found = True
            break
    industrial_prefixes[i] = found
    i += 1

industrial_prefixes = pd.Series(industrial_prefixes)

print(industrial_prefixes)
print('-----------------------------------------------------------')
print(full_data['naics'] == TOTAL_EMPLOYMENT_NAICS_FLAG)

full_data.drop(full_data[(full_data['naics'] != TOTAL_EMPLOYMENT_NAICS_FLAG) & ~industrial_prefixes].index, inplace=True, axis=0)

print(full_data)

print("cleaning done!")
# for now, looking for closest to balanced between these sectors as possible
# only going per-county right now
