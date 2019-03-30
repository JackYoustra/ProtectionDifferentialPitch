import wget
from pathlib import Path
import pandas as pd
import numpy as np

# lets write our own pandas caching utility
# rip it doeesn't work
def cache_dataframe(original_function):
    def make_cache_dataframe_op():
        cache_dir = Path('./cache')
        cache_dir.mkdir(exist_ok=True)
        cached_location = cache_dir/(original_function.__name__ + ".zip")
        if cached_location.exists():
            print("Using cached " + original_function.__name__)
            return pd.read_csv(str(cached_location))
        print(original_function.__name__ + " not found, recreating")
        retVal = original_function()
        retVal.to_csv(str(cached_location))
        return retVal
    return make_cache_dataframe_op

def collect_data(data_source, data_source_filename):
    COUNTY_DATA_INFO_SOURCE = "https://www2.census.gov/programs-surveys/rhfs/cbp/technical%20documentation/2015_record_layouts/county_layout_2015.txt?#"
    COUNTY_DISTANCES_SOURCE_FILENAME = "sf12010countydistance100miles.csv.zip"
    COUNTY_DISTANCES_SOURCE = "https://www.nber.org/distance/2010/sf1/county/" + COUNTY_DISTANCES_SOURCE_FILENAME
    COUNTY_NAMES_SOURCE = "https://www2.census.gov/programs-surveys/cbp/technical-documentation/reference/state-county-geography-reference/georef12.txt"
    COUNTY_NAMES_FILENAME = "georef12.txt"
    assets_dir = Path('./assets')
    assets_dir.mkdir(exist_ok=True)
    # you can do this with the paths library!
    for filename, source in [
        (data_source_filename, data_source),
        (COUNTY_DISTANCES_SOURCE_FILENAME, COUNTY_DISTANCES_SOURCE),
        (COUNTY_NAMES_FILENAME, COUNTY_NAMES_SOURCE)
    ]:
        if not (assets_dir / filename).exists():
            wget.download(source, out=str(assets_dir))
    # we have our assets!
    print("Asset downloading completed")
    # parse information file
    # table of protected, competitive, and monopolistic sectors
    text_data = None
    with open("sectoral_data_16.txt", 'r') as sectoral_data_file:
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
    full_data = pd.read_csv(str(assets_dir / data_source_filename))
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
    full_data.drop(full_data[(full_data['naics'] != TOTAL_EMPLOYMENT_NAICS_FLAG) & ~industrial_prefixes].index,
                   inplace=True, axis=0)

    # now actually make sane county names
    # strategy: make county UUID, then merge with info data
    names = pd.read_csv(assets_dir/COUNTY_NAMES_FILENAME)

    # we now don't need uuids
    #state_formatter = "{:02d}".format
    #county_formatter = "{:03d}".format
    #full_data['county_uuid'] = [state_formatter(int(state)) + county_formatter(int(county)) for state, county in zip(full_data['fipstate'].values, full_data['fipscty'].values)]
    #full_data['county_uuid'] = full_data['county_uuid'].astype(np.int64)

    # change to left merge if you want aggregate statistics (state, national)
    full_data = full_data.merge(names, how='inner', left_on=['fipstate', 'fipscty'], right_on=['fipstate', 'fipscty'], suffixes=(False, False), copy=False, validate='m:1')

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
    return full_data

from contextlib import redirect_stdout
def cache_results(f):
    def wrapped(*args, **kw):
        assets_dir = Path('./results')
        assets_dir.mkdir(exist_ok=True)
        target_dir = assets_dir/(str(args[1]) + ".txt")
        result = None
        if target_dir.exists():
            with target_dir.open() as target_file:
                result = target_file.read()
        with target_dir.open(mode='w') as target_file:
            with redirect_stdout(target_file):
                f(*args, **kw)
        with target_dir.open() as target_file:
            result = target_file.read()
        print(result)
        return
    return wrapped

def make_summary(data_source, data_source_filename):
    full_data = collect_data(data_source, data_source_filename)

    # for now, looking for closest to balanced between these sectors as possible
    # essentially, this means that we want the county to have an even split with the majority of its workforce between
    # C, M, and T totals
    # only going per-county right now

    # keys that are just going to uniquely follow us around
    common_merge_keys = ['fipstate', 'fipscty', 'ctyname']

    exposure_data = pd.DataFrame()
    exposure_data = full_data.groupby(['exposure'] + common_merge_keys, as_index=False).sum()

    tariffed_data = exposure_data.loc[exposure_data['exposure'] == 'T']
    competitive_data = exposure_data.loc[exposure_data['exposure'] == 'C']
    monopoly_data = exposure_data.loc[exposure_data['exposure'] == 'M']
    total_data = exposure_data.loc[exposure_data['exposure'] == '']

    # now compose data into large frame
    simplified_exposure_data = total_data.merge(tariffed_data, 'outer', on=common_merge_keys, suffixes=("_total", "_tariffed"), validate='1:1')
    simplified_exposure_data = simplified_exposure_data.merge(competitive_data, 'outer', on=common_merge_keys, suffixes=(None, "_competitive"), copy=False, validate='1:1')
    simplified_exposure_data = simplified_exposure_data.merge(monopoly_data, 'outer', on=common_merge_keys, suffixes=("_competitive", "_monopoly"), copy=False, validate='1:1')

    # great, we now have exposure data
    # we now have to figure out hwo close each one of these counties comes to aggregate numbers (balanced)
    exposure_employment_data = simplified_exposure_data[["emp_total", "emp_tariffed", "emp_competitive", "emp_monopoly"] + common_merge_keys]

    # defined as closest fitness to the model I have in mind
    # "biggest employers in town" curve probably looks something like x^2
    # the rest is just based on a relatively even split - probably just MSE from mean
    # TODO: ML on past stuff?
    def county_loss_func(row):
        total = row["emp_total"]
        tariffed = row["emp_tariffed"]
        if np.isnan(tariffed):
            tariffed = 0
        competitive = row["emp_competitive"]
        if np.isnan(competitive):
            competitive = 0
        monopoly = row["emp_monopoly"]
        if np.isnan(monopoly):
            monopoly = 0

        relevant_categories = [tariffed, competitive, monopoly]
        relevant_employment = sum(relevant_categories)

        # penalty for not being an ideal town with only these three types of employment
        size_penalty = relevant_employment / total
        if tariffed == 0 or competitive == 0 or monopoly == 0:
            size_penalty = np.infty

        # penalty for not being evenly initially distributed between these three things. Right now, MSE
        distribution_penalty = sum( [(x - (total / 3))**2 for x in relevant_categories])

        # Add - don't want one to cancel out the other
        # TODO: Test coefficients?
        penalty = (1.0 + size_penalty) * distribution_penalty
        return penalty


    exposure_employment_data["loss"] = exposure_employment_data.apply(county_loss_func, axis=1)
    # exclude all with a fitness of zero
    exposure_employment_data.sort_values(by=['loss'], inplace=True, axis=0)

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.set_option.html
    with pd.option_context('display.max_columns', None, 'display.expand_frame_repr', False, 'display.max_rows', 200):
        print(exposure_employment_data)


# Make a summary of an industry given a CBP datasets thingy
COUNTY_DATA_SOURCE_2016_FILENAME = "cbp16co.zip"
COUNTY_DATA_SOURCE_2016 = "https://www2.census.gov/programs-surveys/cbp/datasets/2016/" + COUNTY_DATA_SOURCE_2016_FILENAME + "?#"

COUNTY_DATA_SOURCE_2002_FILENAME = "cbp02co.zip"
COUNTY_DATA_SOURCE_2002 = "https://www2.census.gov/programs-surveys/cbp/datasets/2002/" + COUNTY_DATA_SOURCE_2002_FILENAME + "?#"

COUNTY_DATA_SOURCE_2005_FILENAME = "cbp05co.zip"
COUNTY_DATA_SOURCE_2005 = "https://www2.census.gov/programs-surveys/cbp/datasets/2005/" + COUNTY_DATA_SOURCE_2005_FILENAME + "?#"


make_summary(COUNTY_DATA_SOURCE_2016, COUNTY_DATA_SOURCE_2016_FILENAME)
#make_summary(COUNTY_DATA_SOURCE_2002, COUNTY_DATA_SOURCE_2002_FILENAME)
#make_summary(COUNTY_DATA_SOURCE_2005, COUNTY_DATA_SOURCE_2005_FILENAME)
