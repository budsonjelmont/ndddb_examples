# Read in Epic data dump from EDW report & look for the tages present in the Epilepsy Follow Up Smart Phrase.
# If found, regex parse the available tags & map them to REDCap fields. Then, pull all the REDCap records for this instrument
# to check if an identical note already exists (keyed on subject/date/provider combination), and if it doesn't, add the new
# note to the next available REDCap instance ID. Then push the new data frame back to REDCap.

import argparse as ap
import pandas as pd

import datetime as dt

import sys
sys.path.insert(0, '../../conf/')
import pyserver_etl_config as etlconfig
from config import config
import NDDdb_py_modules as NDDdb

#Data cleaning functions#####################################################

# Reformat Epic-supplied diagnosis date column and add sync date columnn
def coerceDateStr(datestr,informat):
  ds = dt.datetime.strptime(datestr,informat)
  return ds.strftime('%Y-%m-%d')

############################################################################

parser = ap.ArgumentParser(description='Read in Epic data dump from EDW report & look for the tages present in the Epilepsy Follow Up Smart Phrase. If found, regex parse the available tags & map them to REDCap fields. Then, pull all the REDCap records for this instrument to check if an identical note already exists (keyed on subject/date/provider combination), and if it does not, add the new note to the next available REDCap instance ID. Then push the new data frame back to REDCap.'')
parser.add_argument('epic_xls', metavar='epic_xls', type=str, nargs=1, help='Path to .xls/.xlsx file containing the data report from the EDW')

args = parser.parse_args()

epic_xls=args.epic_xls[0]

notes_epic = pd.read_excel(epic_xls) # e.g. '/Users/judsonbelmont/Documents/EPIC/Epic data dumps/Data V4_with notes_Line.xlsx'

notes_epic.sort_values(['Y_MRN','LOG_TIMESTAMP','LINE'],inplace=True)

notes_epic_concat = notes_epic.groupby(['Y_MRN','LOG_TIMESTAMP','Employee Name','DEPARTMENT_NAME']).NOTE_TEXT.apply(lambda x: ''.join(x))
notes_epic = notes_epic_concat.reset_index()

#### Regex matching to get EPIFU fields ####
vars = ['epifu_inthist','epifu_meds','epifu_magnet','epifu_longeps','epifu_detect','epifu_stim','epifu_nseizmo','epifu_neventsincelast','epifu_seizriskfact','epifu_mrrev','epifu_previmg','epifu_labdata','epifu_schhomissue','epifu_pastmedhx','epifu_lastrecseiz','epifu_semiol','epifu_semiolchange','epifu_etiol','epifu_assocdis']
tags = ['Interval History','Medications','Magnet','Long episodes','Detection','Stimulation','Number of seizures per month','Number of Events since last visit','Seizure Risk factors','Medical Record Review','Previous Imaging','Laboratory Data','School/Home issues','Past Medical History','Past Medical History Diagnosis','Last recorded seizure','Semiology','Change in Semiology','Etiology','Associated Disorders']
tags = [t+':' for t in tags]
nexttag = '|'.join(tags)
patterns = [t + '(.*?)(' + nexttag + '|$)' for t in tags]

queries = dict(zip(vars,patterns))
for var, pattern in queries.items():
    notes_epic[var] = notes_epic['NOTE_TEXT'].str.extract(pattern)[[0]]

# l & rstrip all rows to clean hanging characters
textcols = list(notes_epic.filter(like='epifu', axis=1))
notes_epic[textcols] = notes_epic[textcols].apply(lambda x: x.str.strip())

# Pull REDCap records
rc = NDDdb.pullRCRecords(etlconfig.rcparams['rc_api_url'], etlconfig.rcparams['ndd_rc_data_apikey'],'family_enrollment,demographics,enrollment,epifu','','')

# Extract only rows representing FAMILIES
rcfam = rc.loc[rc['redcap_event_name']=='family_data_arm_1']
rcfam.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
# Extract only rows representing SUBJECTS & drop rows where MRN is blank
rcsubj = rc.loc[rc['redcap_event_name']=='family_member_arm_1']
rcsubj = rcsubj.loc[rcsubj['demo_mrn']!='']
#rcsubj.set_index(keys='demo_mrn', drop = False, inplace = True, verify_integrity = True)
# Extract only rows representing LONGITUDINAL DATA
rcclindat = rc.loc[rc['redcap_event_name']=='clinical_data_coll_arm_1']
# Get the rows representing diagnosis codes
rcepifu = rcclindat.loc[rcclindat['redcap_repeat_instrument']=='epifu']

# Get MRN for each diagnosis code already in REDCap, first by getting the redcap ID for each row and then using the repeat instance to get the MRN
rcepifu.set_index(['redcap_id','epifu_subj'],inplace=True,drop=False, append=False, verify_integrity=False)
rcsubj.set_index(['redcap_id','redcap_repeat_instance'],inplace=True,drop=False, append=False, verify_integrity=True)
rcepifu['mrn'] = rcsubj['demo_mrn']

# Re-cast MRN column to string
notes_epic['Y_MRN'] = notes_epic['Y_MRN'].astype(str)

# Set Data Collection ID and notes Subject repeat instance for incoming notes
notes_epic.set_index(keys='Y_MRN', drop = False, inplace = True, verify_integrity = False)
rcsubj.set_index(keys='demo_mrn', drop = False, inplace = True, verify_integrity = True)
notes_epic['redcap_id'] = rcsubj['redcap_id']
notes_epic['epifu_subj'] = rcsubj['redcap_repeat_instance']

# Drop rows that didn't match to a redcap ID
notes_epic.dropna(how='all',inplace=True, subset=['redcap_id'])

# Reformat timestamp date string
notes_epic['LOG_TIMESTAMP'] = notes_epic['LOG_TIMESTAMP'].astype(str).apply(coerceDateStr,args=('%Y-%m-%d',))

# Set Redcap repeat instance for incoming notes that are already in Redcap
notes_epic.set_index(['redcap_id','LOG_TIMESTAMP','Employee Name'],inplace=True,drop=False, append=False, verify_integrity=False)
rcepifu.set_index(['redcap_id','epifu_notedate','epifu_provider'],inplace=True,drop=False, append=False, verify_integrity=False)
notes_epic['redcap_repeat_instance'] = pd.to_numeric(rcepifu['redcap_repeat_instance'])

# For incoming notes that aren't in Redcap yet, assign them the next consecutive Redcap repeat instances
notes_epic.reset_index(drop=True,inplace=True)

mrngroups = notes_epic.groupby('Y_MRN')
for mrn,rows in mrngroups:
  maxrep = notes_epic.loc[rows.index,'redcap_repeat_instance'].max()
  if pd.isna(maxrep):
    maxrep = 0
  nextrep = maxrep + 1
  for i in rows.index:
    if pd.isna(notes_epic.loc[i,'redcap_repeat_instance']):
      notes_epic.loc[i,'redcap_repeat_instance'] = nextrep
      nextrep = nextrep + 1

# Reformat/rename columns and import
notes_epic['epifu_syncdate'] = dt.date.today().strftime('%Y-%m-%d')

notes_epic.rename(columns={'LOG_TIMESTAMP':'epifu_notedate','Employee Name':'epifu_provider','DEPARTMENT_NAME':'epifu_dept','NOTE_TEXT':'epifu_fullnote'}, inplace=True)

# Drop MRN number before importing to Redcap
notes_epic.drop('Y_MRN',inplace=True,axis=1)
# Add event name
notes_epic['redcap_event_name'] = 'clinical_data_coll_arm_1'
notes_epic['redcap_repeat_instrument'] = 'epifu'

# Convert repeat instance from numeric to string
notes_epic['redcap_repeat_instance'] = notes_epic['redcap_repeat_instance'].astype(int).astype(str)

# Clean out any invalid characters
notes_epic = notes_epic.apply(lambda x: x.str.encode('utf-8', 'ignore').str.decode('utf-8'))
notes_epic = notes_epic.apply(lambda x: x.str.replace('\n',''))
# TODO add step str.replace(u'\xa0', ' ').encode('utf-8')

# Change all NAs to empty string
notes_epic.fillna('',inplace=True)

# Write to .csv & push to REDCap
notes_epic.to_csv('epifuclinnotes_import.csv', columns=['redcap_id'] + [x for x in list(notes_epic) if x!='redcap_id'],index=False)
NDDdb.pushRCRecord(etlconfig.rcparams['rc_api_url'], etlconfig.rcparams['ndd_rc_data_apikey'],'',notes_epic.drop(columns='redcap_repeat_instrument'))
