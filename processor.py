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
# full_data.drop(full_data[(full_data['empflag'] == 'S') | (full_data['emp_nf'] == 'D')], inplace=True, axis=0)

# only concerned about our industries and total employment
TOTAL_EMPLOYMENT_NAICS_FLAG = '------'

industrial_prefixes = np.empty(len(full_data['naics']), dtype=bool)
industrial_type = np.empty(len(full_data['naics']), dtype=str)

i = 0
for naic in full_data['naics']:
    found = False
    for key, value in industrial_classifications.items():
        if naic.startswith(key):
            found = True
            industrial_type[i] = value
            break
    industrial_prefixes[i] = found
    i += 1

industrial_prefixes = pd.Series(industrial_prefixes)

full_data['exposure'] = industrial_type
full_data.drop(full_data[(full_data['naics'] != TOTAL_EMPLOYMENT_NAICS_FLAG) & ~industrial_prefixes].index, inplace=True, axis=0)

def estimate_employment_if_nonexistant(row):
    row_elem = row['empflag']
    estimated_employment = row['emp']
    if row_elem == np.nan:
        return estimated_employment
    if row_elem == 'A':
        return 10
    elif row_elem == 'B':
        return 50
    elif row_elem == 'C':
        return 175
    elif row_elem == 'D':
        print("There shouldn't be a D")
        return 250
    elif row_elem == 'E':
        return 375
    elif row_elem == 'F':
        return 750
    elif row_elem == 'G':
        return 1750
    elif row_elem == 'H':
        return 3750
    elif row_elem == 'I':
        return 7500
    elif row_elem == 'J':
        return 17500
    elif row_elem == 'K':
        return 37500
    elif row_elem == 'L':
        return 75000
    elif row_elem == 'M':
        # no upper bound, just double 100k
        return 200000
    else:
        pass
        # the data is withheld and stuff
    return estimated_employment


# now we want to guess at the class size for a given field if not given in employment numbers
full_data['emp'] = full_data.apply(estimate_employment_if_nonexistant, axis=1)

print("cleaning done!")
# for now, looking for closest to balanced between these sectors as possible
# essentially, this means that we want the county to have an even split with the majority of its workforce between
# C, M, and T totals
# only going per-county right now

exposure_data = pd.DataFrame()
exposure_data = full_data.groupby(['exposure', 'fipstate', 'fipscty'], as_index=False).sum()

tariffed_data = exposure_data.loc[exposure_data['exposure'] == 'T']
competitive_data = exposure_data.loc[exposure_data['exposure'] == 'C']
monopoly_data = exposure_data.loc[exposure_data['exposure'] == 'M']
total_data = exposure_data.loc[exposure_data['exposure'] == '']

# now compose data into large frame
simplified_exposure_data = total_data.merge(tariffed_data, 'outer', on=["fipstate", "fipscty"], suffixes=("_total", "_tariffed"), validate='1:1')
simplified_exposure_data = simplified_exposure_data.merge(competitive_data, 'outer', on=["fipstate", "fipscty"], suffixes=(None, "_competitive"), copy=False, validate='1:1')
simplified_exposure_data = simplified_exposure_data.merge(monopoly_data, 'outer', on=["fipstate", "fipscty"], suffixes=(None, "_monopoly"), copy=False, validate='1:1')
print(simplified_exposure_data)

# great, we now have exposure data
# we now have to figure out hwo close each one of these counties comes to aggregate numbers (balanced)


