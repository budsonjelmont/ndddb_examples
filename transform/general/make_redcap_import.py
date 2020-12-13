import argparse as ap
import re
import numpy as np
import pandas as pd
import sys

# Mimic unix tee function to print to log & stdout together
class Tee:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)
    def flush(self, *args, **kwargs):
        pass
    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2

sys.stdout = Tee(open('/tmp/redcap_transform_log.txt', 'w'), sys.stdout)

parser = ap.ArgumentParser(description='Transforms a spreadsheet into a REDCap import-ready .csv file by translating fields into REDCap dictionary values based on a user supplied REDCap data dictionary.')
parser.add_argument('datfile', metavar='datfile', type=str, nargs=1, help='Path to .tsv/.csv/.txt/.xls/.xlsx file containing the data to be transformed')
parser.add_argument('datfile_id', metavar='datfile_id', type=str, nargs=1, help='ID field in datfile that will serve as the record ID in REDCap (and should not be transformed)')

parser.add_argument('ddfile', metavar='ddfile', type=str, nargs=1, help='Path to data dictionary to use for translating values')

args = parser.parse_args()

# File parser
FNAMEPATTERN = '(.*)(\.xlsx|\.xls|\.csv|\.tab|\.tsv)$'
def parseFile(fpath):
    fname = fpath.split('/').pop()
    result = re.match(FNAMEPATTERN, fname)
    fname = result.group(1)
    ext = result.group(2)
    if ext == '.xlsx' or ext == '.xls':
        df = pd.read_excel(fpath,dtype='str')
    elif ext == '.csv':
        df = pd.read_csv(fpath,dtype='str')
    elif ext == '.tab' or ext == '.tsv':
        df = pd.read_csv(fpath, sep='\t',dtype='str')
    return (df, fname)

# Read & pre-process data & data dictionary
datfile=args.datfile[0]

(dat, datname) = parseFile(datfile)
idfield = args.datfile_id[0].lower()

# Convert spaces to '_' and lcase column names
dat.columns = dat.columns.str.replace(' ','_')
dat.rename(columns=str.lower,inplace=True)

# REDCap data dictionaries contain non-UTF8 chars, so:
dd = pd.read_csv(args.ddfile[0],encoding='latin-1')

dd.set_index('Variable / Field Name',inplace=True,drop=False,append=False,verify_integrity=True)

CHOICEPATTERN = re.compile('^\s*([0-9]+),\s*(.*)\s*$')
ALTPATTERN = re.compile('^\s*(.*),\s*(.*)\s*$')
def parseChoiceMap(optstr, pattern, altpattern):
    try:
        choicesmap = {}
        optlist = optstr.split('|')
    except AttributeError:
        return choicesmap
    for opt in optlist:
        #Some options in the Epi25 data dictionary have HTML decoration or tags, so split out any text that falls between [] or <>
        opt = re.sub('\[.*\]|<(br|div).*>','', opt)
        opt = opt.rstrip()
        parsed = re.search(pattern, opt)
        try:
            if parsed.groups().__len__()!=2:
                print('WARNING: Selection choices parsing returned > or < than 2 matches')
                break
            else:
                choicesmap[parsed.group(2)] = parsed.group(1)
        except AttributeError:
            parsed = re.search(altpattern, opt)
            if parsed is None:
                continue
            choicesmap[parsed.group(2)] = parsed.group(1)
    return choicesmap

optmaps = dd.apply(lambda x: parseChoiceMap(x['Choices, Calculations, OR Slider Labels'],CHOICEPATTERN,ALTPATTERN),axis=1)

# Define exceptions to be thrown when cleaning data
class FieldNotFoundError (Exception):
    def __init__(self, fieldname, message="Column in metadata not found in input data."):
        self.fieldname = fieldname
        self.message = message
        super().__init__(self.message)

def report_missing(field):
    print(field + ' not found')
    keepme.remove(field)

def report_unmapped(field):
    print(field + ' not found')
    keepme.remove(field)

# Initialize list of columns to keep from data dictionary, & add checkbox columns as you go
keepme = list(optmaps.index)
keepme.append(idfield)

# Loop over data dictionary and translate values
for field,meta in dd.iterrows():
    type = meta['Field Type']
    if type != 'checkbox':
        if field not in dat.columns:
            #raise FieldNotFoundError(field)
            report_missing(field)
            continue
    # Values: 'text', 'dropdown', 'radio', 'checkbox', 'notes'
    if type in ['text','notes']:
        if meta['Text Validation Type OR Show Slider Number']=='date_mdy':
            dat[field] = dat[field].str.replace('[0-9]{0,3}:[0-9]{0,3}:[0-9]{0,3}','',regex=True)
        continue # No further handling required
    elif type == 'yesno':
        dat[field] = dat[field].str.lower()
        dat[field].replace({
          't':'1','true':'1','y':'1','yes':'1',
          'f':'0','false':'0','n':'0','no':'0'
        },inplace=True)
        # Check to see if any values could not be translated
        populated = dat[field][dat[field].notna()]
        if ~populated.isin(['0','1']).all():
            print('The following rows contain values that cannot be mapped to booleans and have been set to NA:')
            print(populated.loc[~populated.isin(['0','1'])])
            dat.loc[~dat[field].isin(['0','1']),field]=''
            pass # TODO error handling for unmapped values
    elif type in ['dropdown','radio']:
        # Need to check if all values are successfully translated!
        try:
            #dat[f].isin([x for x in optmaps.loc[f].keys()]).
            dat[field].replace(optmaps.loc[field],inplace=True)
            keepme.remove(field)
        except KeyError as FieldNotFoundError:
            report_missing(field)
    elif type in ['checkbox']:
        # Need to check if all values are successfully translated!
        try:
            valsplit = dat[field].str.split(';',expand=True).applymap(lambda x: np.nan if pd.isna(x) else x.strip())
            allvalsinmap = valsplit.apply(lambda x: pd.Series([y in optmaps.loc[field].keys() or pd.isna(y) for y in x]),axis=1).all(axis=1)
            # Instead check if valnotinmap=TRUE
            if ~allvalsinmap.all():
                print(field + ' contains unmappable values!')
            # From gene translation script: genes = genes.apply(lambda x: [lookup[1].get(y,y) for y in x.loc[~pd.isna(x)]],axis=1)
            notna = ~pd.isna(valsplit)
            valsplit[notna]
            # Loop over labels. For each label, check if it exists in each valsplit() row
            coded = pd.Series([x for x in optmaps.loc[field].keys()]).apply(
                lambda x:
                    valsplit.apply(lambda y: (y==x).any(),axis=1).replace({True:1,False:0})
            ).transpose()
            cb_cols = [field + '___' + z for z in optmaps.loc[field].values()]
            coded.columns = cb_cols
            dat = dat.join(coded)
            # Add newly derived checkbox columns to the list of columns to keep & drop original column name
            keepme.remove(field)
            keepme.extend(cb_cols)
        except KeyError:
            report_missing(field)
    else:
        report_missing(field)

# Drop all columns not found in keepme list
transformed_dat = dat[keepme]

# Write out transformed data frame
transformed_dat.to_csv(datfile[:-4] + '_redcap_import.csv',header=True,index=False)# TODO:
print('------Done!--------')
