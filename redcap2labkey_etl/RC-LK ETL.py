#!/usr/bin/env python

# ETL procedure for sync'ing records in LabKey with phenotypic data collected on REDCap
# and assigning database identifiers.
# Overview:
# 1. Pull all records and metadata (the data dictionary) from NDD REDCap
# 2. Iterate through each field in the data dictionary, convert its coded
#    value to text if necessary, and write each REDCap instrument's fields
#    to a new data frame.
# 3. Pull Enrollment records from LabKey to get the new available integer ID
#    and F#s to assign
# 4. Assign new IDs to any REDCap subjects who don't have them yet and write
#    the newly assigned IDs back to REDCap.
# 5. Push REDCap data to LabKey

import sys
sys.path.insert(0,'../conf')
sys.path.insert(0,'../lib')

from pyserver_etl_config import rcparams, lkparams, importStudyArchivePath
from rc2lk_table_config import rcinstr2lkconfig,rfreferralinstr,lktableconfig,dontimport,ynmap
from functools import reduce
import NDDdb_py_modules as NDDdb
import pycurl
import io
import json
import pandas as pd
import numpy as np
import datetime as dt
import re

import os
import shutil

import labkey
from labkey.utils import create_server_context
from labkey.query import select_rows, insert_rows, update_rows, execute_sql

#######
# REDCap choices regexes
FFORMAT = re.compile('^F[0-9]{6}$')
FINDFORMAT = re.compile('^F[0-9]{6}-[0-9]{2}$')
CHOICEPATTERN = re.compile('^\s*([0-9]+),\s*(.*)\s*$')
ALTPATTERN = re.compile('^\s*(.*),\s*(.*)\s*$')

# Take list of checked values & convert to ';'-delimited string--now deprecated
def concatCheckboxVals__DEPR(r):
    r.replace('', np.nan, inplace=True)
    r.dropna(inplace=True)
    if len(r) != 0:
        return ';'.join(r)
    else:
        return ''

# Find the Subject ID of an individual who was specified in a Redcap dropdown from piped-in vars
RCDDPATTERN = '\[family_member_arm_1\]\[demo_firstname\]\[([0-9])\]'
def getSubjIDof(rcstr,rcid,selectfampattern):
    if rcstr=='' or rcstr.startswith('Unknown'):
        return ''
    else:
        parsed = re.search(selectfampattern, rcstr)
        rep = parsed.group(1)
        subjid = rcsubjdat.loc[((rcsubjdat['redcap_id']==rcid) & (rcsubjdat['redcap_repeat_instance']==rep)),'labkey_subjid']
        if len(subjid) > 0:
            return subjid.iloc[0]
        else:
            return ''

# For clinical genetics results, match the report
RCCGPATTERN = '\[clinical_data_coll_arm_1\]\[clingen_reportsubj\]\[([0-9])\]'
def getFromReport(rcstr,rcid,selectpattern,matchinstr):
    if rcstr=='' or rcstr.startswith('Unknown'):
        return ''
    else:
        parsed = re.search(selectpattern, rcstr)
        rep = parsed.group(1)
        matchrow = rcclindat.loc[((rcclindat['redcap_repeat_instrument']==matchinstr) & ((rcclindat['redcap_id']==rcid) & (rcclindat['redcap_repeat_instance']==rep))),]
        subjid = getSubjIDof(matchrow['clingen_reportsubj'].iloc[0],rcid,RCDDPATTERN)
        date = matchrow['clingen_reportdate'].iloc[0]
        # lsid = rcforms[matchinstr].loc[((rcforms[f]['redcap_repeat_instrument']==matchinstr) & ((rcforms[f]['redcap_id']==rcid) & (rcforms[f]['redcap_repeat_instance']==rep))),'lsid'].iloc[0]
        reportlsid = rcforms[matchinstr].loc[(rcforms[matchinstr]['redcap_repeat_instance']==rep) & (rcforms[matchinstr]['SubjectID']==subjid),'lsid'].iloc[0]
        if len(subjid) > 0:
            return {'SubjectID':subjid,'date':date,'reportLSID':reportlsid}
        else:
            return ''

# Combine multiple fields into a single one
def concatFields(fields):
    fields.replace('',np.nan,inplace=True)
    fields.dropna(inplace=True)
    return ';'.join(fields)

# Combine multiple fields into a single one where one field is an explanation of 'Other' values
OTHERVALPATTERN = re.compile('Other')  # TODO fix pattern to avoid incorrect matches (^|;)Other(;|$)
def concatOtherFields(fields,otherdict):
    datfield = fields.loc[otherdict['data']]
    otherfield = fields.loc[otherdict['other']]
    othermerge = re.sub('Other','Other:' + otherfield,datfield)
    return(othermerge)

#################
# Make zip file to import to LabKey
def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

#################################################

# Get command line arguments
debugMode = sys.argv[1] # {True,False}

if debugMode == '':
    debugMode = True

# Get LabKey params & establish server context
labkey_server = lkparams['labkey_server']
project_name = lkparams['project_name']
project_name_enrollment = lkparams['project_name_enrollment']
context_path = lkparams['context_path']
use_ssl = lkparams['use_ssl']

server_context = create_server_context(labkey_server, project_name, context_path, use_ssl)

# Pull all REDCap records
# encoding = "ISO-8859-1" fixes import, but....
refs = NDDdb.pullRCRecords(rcparams['rc_api_url'], rcparams['ndd_rc_refer_apikey'],'','','')

# Pull Redcap instruments in chunks of n instruments at a time, then combine into single data frame
def chunks(lst, n):
  for i in range(0, len(lst), n):
    yield lst[i:i + n]

chunksize = 10
instr = list(set(rcinstr2lkconfig.keys()) - set(rfreferralinstr))
instr_subsets = chunks(instr,chunksize)

rc_list = []
for i in instr_subsets:
  rc_list.append(NDDdb.pullRCRecords(rcparams['rc_api_url'], rcparams['ndd_rc_data_apikey'],','.join(i),'','redcap_id,redcap_event_name,redcap_repeat_instance,redcap_repeat_instrument'))

rc = reduce(lambda left,right: pd.merge(left,right,left_on=['redcap_id','redcap_event_name','redcap_repeat_instance','redcap_repeat_instrument'],right_on=['redcap_id','redcap_event_name','redcap_repeat_instance','redcap_repeat_instrument'],how='outer'), rc_list)
rc.reset_index(inplace=True,drop=True)

# Separate rows by event
rcfamdat = rc.loc[rc['redcap_event_name']=='family_data_arm_1']
rcsubjdat = rc.loc[rc['redcap_event_name']=='family_member_arm_1']
rcclindat = rc.loc[rc['redcap_event_name']=='clinical_data_coll_arm_1']

refs = refs.loc[refs['redcap_repeat_instrument']=='']

# Drop referrals that don't have an F-individual #, then set it as the index
# NOTE: This is for later, when we'll need to match referrals to their LabKey SubjectIDs
refs = refs.loc[refs['f_idnum'].str.startswith('F')]
refs.set_index('f_idnum',drop=False,append=False,inplace=True,verify_integrity=True)

# Rename redcap_id to referral_id---don't think this is necessary now
refs = refs.rename(columns={'redcap_id': 'referral_id'})

# Drop Subject rows where relationship has not been specified
rcsubjdat = rcsubjdat.loc[(rcsubjdat['demo_relation']!='')]

rcdict = {
    'referral':refs,
    'family_data_arm_1':rcfamdat,
    'family_member_arm_1':rcsubjdat,
    'clinical_data_coll_arm_1':rcclindat
}

# Make separate dataframe of the non-redundant (non-repeating) rows only
# plus select columns which we'll use when it comes time to assign IDs

rcnr = rcfamdat

# Set indices on the REDCap data to the individuals' integer IDs
rc.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = False) # TODO I don't think this line is necessary
rcnr.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)

# Pull the REDCap projects' data dictionaries to get all the unique form names in REDCap
dd_dc = NDDdb.getMetaData(rcparams['rc_api_url'], rcparams['ndd_rc_data_apikey'],'')
dd_dc.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)

dd_refer = NDDdb.getMetaData(rcparams['rc_api_url'], rcparams['ndd_rc_refer_apikey'],'')
dd_refer.loc[dd_refer['field_name']=='redcap_id','field_name']='referral_id'
dd_refer = dd_refer.loc[dd_refer['form_name']=='physician_referral_form'] # TODO this line can be deprecated once the leftover instruments are deleted from the Referral project
dd_refer.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)

# Initialize dictionary of data frames that will hold the data frame for each REDCap instrument
# Exclude family_members from forms, since we need to handle them separately
# Also get the 'form complete' and 'redcap_repeat_instance' fields and add them to the new data frame
rcforms = {}
forms = list(rcinstr2lkconfig.keys())
for f in forms:
    rcdat = rcdict[rcinstr2lkconfig[f]['event']]
    rcforms[f] = pd.DataFrame()
    completefield = f + '_complete'
    rcforms[f][completefield] =  rcdat[completefield]
    rcforms[f]['redcap_repeat_instance'] = rcdat['redcap_repeat_instance']

# Iterate over each field in the REDCap data dictionary
# If it's in the data dictionary OR it's a checkbox field:
#   If it is NOT an identifier:
#     Check if it's a dropdown or radio button field
#     If NO:
#         Take the column and transfer it to the appropriate new data frame
#     If YES:
#         Parse the 'select_choices_or_calculations' field in the data dict and make a map of integers -> values
#         Apply the transformation over every row
#         Move the result to the the appropriate new data frame
for dd in [dd_refer,dd_dc]:
  for field in dd.index:
      if field not in dontimport:
          if dd['form_name'].loc[field] in forms:
              isCheckbox = dd['field_type'].loc[field] == 'checkbox'
              if field in rc or field in refs or isCheckbox:
                  if dd['identifier'].loc[field] == 'y' and field != 'redcap_id':
                    continue
                  # Use event name to determine which dataframe to pull rows from
                  rcdat = rcdict[rcinstr2lkconfig[dd['form_name'].loc[field]]['event']]
                  if dd['field_type'].loc[field] in ['dropdown','radio','checkbox']:
                      if dd['select_choices_or_calculations'].loc[field] != '': # handle edge cases like 'topecdate' where input type may be wrong
                          optmap = NDDdb.parseChoiceMap(dd['select_choices_or_calculations'].loc[field],CHOICEPATTERN,ALTPATTERN)
                          # If field is a checkbox:
                          #   Iterate through every integer value in the choices map
                          #   Find the corresponding checkbox field
                          if isCheckbox:
                              checkedlistcols = []
                              #f = filter(lambda x:'icd_epi' in x, list(rc)) #Better way to select columns
                              for (o,v) in optmap.items():
                                  FIELDPATTERN = re.compile('^' + field + '_*' + str(o).lower()+'$')
                                  cbfieldmatch = list(filter(FIELDPATTERN.match, list(rcdat)))
                                  if len(cbfieldmatch) != 1:
                                      print('FATAL ERROR: No match for regex pattern ' + str(FIELDPATTERN) + ' for REDCap field ' + field)
                                      sys.exit()
                                  else:
                                      cbfield = cbfieldmatch[0]
                                  # Replace '0' (unchecked) with null to mark it for removal, and replace '1' (checked) with the option's value
                                  rcdat[cbfield].replace(to_replace = ['0','1'], value = [np.nan,v], inplace=True)
                                  checkedlistcols.append(cbfield)
                              #concatcbs = rcdat[checkedlistcols].apply(concatCheckboxVals, axis=1) # No longer works after update to latest pandas on 4/1/2020
                              concatcbs = rcdat[checkedlistcols].apply(lambda x: ';'.join(x[~x.isna()]),axis=1)
                              rcforms[dd['form_name'].loc[field]][field] = concatcbs
                          # If it isn't a checkbox, convert all codes to their values
                          else:
                              rcdat[field].replace(optmap, inplace=True)
                  if dd['field_type'].loc[field] == 'yesno':
                      rcforms[dd['form_name'].loc[field]][field] = rcdat[field].map(ynmap)
                  elif not isCheckbox:
                      rcforms[dd['form_name'].loc[field]][field] = rcdat[field]
              else:
                  # print('that field isn\'t in the data dump')
                  pass

### Assign LabKey SubjectIDs
# Get the next available SubjectID
nextIDqresult = select_rows(server_context, 'study','GetNextSubjectID')
nextSubjID = nextIDqresult['rows'][0]['nextSubjID']
if not nextSubjID:
    nextSubjID = 0

# Initialize fields that will be used in ID assignment
rcsubjdat.loc[:,'labkey_subjid'].replace('', np.nan, inplace=True)

# For each REDCap ID, match it to its existing Subject ID & F number
# If there isn't a corresponding Subject ID, assign one
# Then check if this individual should have an F#, and assign one
# if they don't have one already. F# is determined using the referral
# date when available, but falls back to the current date if it isn't.
anybodynew = False
for rcid in rcsubjdat.index:
    if pd.notnull(rcsubjdat.loc[rcid,'labkey_subjid']):
        pass
    else:
        anybodynew = True
        rcsubjdat.loc[rcid,'labkey_subjid'] = nextSubjID    # Must cast to string here or process entire column (see below)
        nextSubjID += 1
    # Copy F_ID number to physician_referral_form for import to LabKey
    # rcforms[forms[0]].loc[rcid,'fnum'] = rcnr.loc[rcid,'fnum']

# If anyone new's been added to LabKey, write their ID back to REDCap
if anybodynew:
    rcsubjdat['labkey_subjid'] = rcsubjdat['labkey_subjid'].astype(int).astype(str) # Convert 'floating str' -> int -> str
    # Write newly assigned proband identifiers back to REDCap
    idcols = ['redcap_id','redcap_event_name','redcap_repeat_instance', 'labkey_subjid']
    rcupdate = rcsubjdat[idcols]
    rcupdate = rcupdate.applymap(str)
    rcupdatereq = NDDdb.pushRCRecord(rcparams['rc_api_url'], rcparams['ndd_rc_data_apikey'], '', rcupdate)
    if rcupdatereq == 'Error communicating with REDCap database.':
        print('FATAL ERROR: Could not import newly assigned identifers to REDCap')
        sys.exit()

# Add LabKey SubjectID column to referrals data frame
refs['labkey_subjid']=rcsubjdat.loc[rcsubjdat['f_idnum'].str.startswith('F')].set_index('f_idnum',verify_integrity=True)['labkey_subjid']

# Clean out old import TSVs from minimal study archive folder before making new ones
listPath = importStudyArchivePath + '/lists/'
datasetPath = importStudyArchivePath + '/study/datasets/'
for location in [listPath, datasetPath]:
  for folder, subfolders, files in os.walk(location):
    for file in files:
        if file.endswith('.tsv'):
          os.remove(os.path.join(folder, file))

# Remove rows in each form that are not marked as 'Complete', then get identifers & write processed data to the study archive folder
for table, LKconfig in lktableconfig.items():
  importdf = pd.DataFrame()
  for f in LKconfig['instr']:
    # print(f)
    completefield = f + '_complete'
    # Drop rows where 'complete' is empty
    rcforms[f] = rcforms[f].loc[rcforms[f][completefield]!='']
    # Convert '' to nan, then drop rows where everything BUT 'complete' is nan, then convert nans back to ''
    rcforms[f].replace('',np.nan, inplace=True)
    rcforms[f].dropna(axis=0, subset=list(set(list(rcforms[f])) - set([completefield,'redcap_repeat_instance'])), how='all', inplace=True)
    #rcforms[f][list(set(list(rcforms[f])) - set([completefield,'redcap_repeat_instance']))].dropna(axis=0, how='all', inplace=True) # In my local copy of this script this line was replace by the one above
    rcforms[f].replace(np.nan,'', inplace=True)
    # Get LabKey config params to prepare query
    RCconfig = rcinstr2lkconfig[f]
    schema = LKconfig['schema']
    query = table
    # Check to see if a log file exists for the current query, and make one if it doesn't
    if debugMode:
        pass
    # Process freetext fields that are explanations of 'Other' checkbox/multichoice selections
    if 'otherConfig' in RCconfig:
      others = RCconfig['otherConfig']
      for key, fielddict in others.items():
          fieldlist = list(fielddict.values())
          rcforms[f][key] = rcforms[f][fieldlist].apply(concatOtherFields,axis=1,args=(fielddict,))
      rcforms[f].drop(columns=[x['other'] for x in others.values()],inplace=True)
    # Concatenate any fields that should be combined in LabKey
    if 'concatConfig' in RCconfig:
      concat = RCconfig['concatConfig']
      for key, fieldlist in concat.items():
          #rcforms[f][key] = rcforms[f][fieldlist].apply(concatFields,axis=1) # No longer works after update to latest pandas on 4/1/2020
          rcforms[f][key] = rcforms[f][fieldlist].fillna('').sum(axis=1)
    # If a dict of name mappings is supplied, apply it here
    if 'renamer' in RCconfig:
      rcforms[f].rename(RCconfig['renamer'],axis=1,inplace=True)
    # Drop any remaining fields not mapped to LabKey
    if 'droplist' in RCconfig:
      rcforms[f].drop(columns=RCconfig['droplist'],inplace=True,errors='ignore')
    # Drop Redcap repeat instance column unless this dataset is collected longitudinally
    if RCconfig['event'] != 'clinical_data_coll_arm_1':
      rcforms[f].drop(columns=['redcap_repeat_instance'],inplace=True,errors='ignore')
    # Drop '*_complete' field if this table doesn't require the QC column
    if not RCconfig['reqQC']:
      rcforms[f].drop(columns=[completefield],inplace=True,errors='ignore')
    # else: # Map '*_complete' field integer to string
    #     rcforms[f][completefield] = rcforms[f][completefield].rename({'0':'Incomplete','1':'Unverified','2':'Complete'},inplace=True)
    # If target schema is a study dataset, do additional processing here
    datasetID = LKconfig['dsID']
    if schema == 'study':
      # Add SubjectIDs column
      if RCconfig['event']=='referral':
          rcforms[f]['SubjectID'] = refs['labkey_subjid']
      elif RCconfig['event']=='family_member_arm_1':
          rcforms[f]['SubjectID'] = rcsubjdat['labkey_subjid'] # Quicker way to do the same thing as line below
          #rcforms[f]['SubjectID'] = rcforms[f].index.map(lambda x: str(rcsubjdat.loc[x,'labkey_subjid']))
      elif RCconfig['event']=='clinical_data_coll_arm_1':
          rcforms[f]['familyid'] = rcclindat['redcap_id'] # Think this line is a quicker way to do the same thing as line below
          if f=='clinical_genetics_finding':
              cgreport = rcforms[f].apply(lambda x: getFromReport(x['clingen_report_id'],x['familyid'],RCCGPATTERN,'clinical_genetics_report'), axis=1, result_type='expand')
              rcforms[f]['SubjectID'] = cgreport['SubjectID']
              rcforms[f]['clingen_reportdate'] = cgreport['date']
              rcforms[f]['reportLSID'] = cgreport['reportLSID']
          else:
              rcforms[f]['SubjectID'] = rcforms[f].apply(lambda x: getSubjIDof(x[RCconfig['subjcol']],x['familyid'],RCDDPATTERN), axis=1)
          rcforms[f].drop(columns=['familyid',RCconfig['subjcol']],inplace=True) # drop family id & REDCap coded-subject column before import #TODO could also just rename subject col to SubjectID first--then I'd be able to handle these fields the same way I do other subject dropdowns
      # Additional handling req'd for demographics form:
      #  1. Add family ids & consent date to demographics table
      #  2. Translate 'demo_twinsib', 'demo_mother', and 'demo_father' fields to SubjectIDs
      #  Note: if twin/mother/father don't have a subjectID (because not consented, etc.) then getSubjIDof returns '' TODO
      if f=='demographics':
          rcforms[f]['familyid'] = rcforms[f].index.map(lambda x: str(rcsubjdat.loc[x,'redcap_id']))
          rcforms[f][RCconfig['datecol']] = rcforms[f].index.map(lambda x: str(rcsubjdat.loc[x,RCconfig['datecol']]))
          rcforms[f]['demo_twinsib'] = rcforms[f].apply(lambda x: getSubjIDof(x['demo_twinsib'], x['familyid'],RCDDPATTERN), axis=1)
          rcforms[f]['demo_mother'] = rcforms[f].apply(lambda x: getSubjIDof(x['demo_mother'], x['familyid'],RCDDPATTERN), axis=1)
          rcforms[f]['demo_father'] = rcforms[f].apply(lambda x: getSubjIDof(x['demo_father'], x['familyid'],RCDDPATTERN), axis=1)
      # Additional handling req'd for enrollment form:
      #  1. Get demo_dateadded field from demographics table
      elif f in ['enrollment','phenotype','chart_review','appointment_questions_subject','rett_questions'] :
          rcforms[f][RCconfig['datecol']] = rcforms[f].index.map(lambda x: str(rcsubjdat.loc[x,RCconfig['datecol']]))
      # Specify which column will include the date and then drop that column
      rcforms[f]['date'] = rcforms[f][RCconfig['datecol']]
      rcforms[f].drop(columns=[RCconfig['datecol']],inplace=True)
      # Drop rows where date is empty, since they won't import to Labkey
      rcforms[f].drop(rcforms[f].loc[rcforms[f]['date']==''].index,axis=0,inplace=True)
      # Create unique LSID for this item
      if not RCconfig['isDemo']:
          rcforms[f]['redcap_repeat_instance'].replace('',0, inplace=True)
          rcforms[f]['lsid'] = 'urn:lsid:labkey.com:Study.Data-5:'+datasetID+'.'+rcforms[f]['SubjectID']+'.'+pd.to_datetime(rcforms[f]['date']).apply(lambda date: date.strftime('%Y%m%d.%H%M'))+'.'+ rcforms[f]['redcap_repeat_instance'].apply(str)
    elif schema == 'lists':
     if f == 'appointment_questions_family':
       rcforms[f]['id'] = rcforms[f].index
    # DEPRECATED: Iterate over each row and attempt to update existing
    # LabKey record. If update fails, then insert as a new record instead.
    # rcforms[f].apply(LKinsertupdateRow, axis=1, args=(query,schema))
    importdf = pd.concat([importdf,rcforms[f]],axis=1)
  importdf = importdf.loc[:,~importdf.columns.duplicated()]
  if schema == 'study':
    importdf['QCStateLabel'] = ''
    fname = datasetPath + '/dataset' + datasetID + '.tsv'
  elif schema == 'lists':
    fname = listPath + '/' + query + '.tsv'
    # Output TSV of all data to update
    # fname = outPath + query +'_forLKimport.tsv'
  importdf.to_csv(fname,sep='\t')

# For import to labkey, need a zip file containing:
#   a study.xml file (see structure below)
#   a folder named 'datasets' containing the dataset TSVs + datasets_metadata.xml + datasets_manifest.xml

# study.xml:
# <?xml version="1.0" encoding="UTF-8"?>
# <!--Exported from NDDdb Dev Server at http://ndddbdev:8080/labkey/NDDdb/project-begin.view by judson belmont on Tue Feb 19 10:14:23 EST 2019-->
# <study archiveVersion="18.2" label="Welcome to the NeuroDev Disorders Database!" timepointType="CONTINUOUS" subjectNounSingular="Subject" subjectNounPlural="Subjects" subjectColumnName="SubjectID" investigator="" grant="" species="" alternateIdPrefix="" alternateIdDigits="6" defaultTimepointDuration="1" startDate="2018-04-10-04:00" securityType="ADVANCED_WRITE" xmlns="http://labkey.org/study/xml">
#   <assaySchedule dir="assaySchedule"/>
#   <datasets dir="datasets" file="datasets_manifest.xml">
#     <definition file="Welcome.dataset"/>
#   </datasets>
#   <comments/>
#   <properties dir="properties"/>
# </study>

zipfname = 'studyArchiveToImport'
zipPath = importStudyArchivePath + '/../' + zipfname
try:
  os.remove(zipPath + '.zip')
except FileNotFoundError:
  pass

shutil.make_archive(zipPath, 'zip', importStudyArchivePath)
