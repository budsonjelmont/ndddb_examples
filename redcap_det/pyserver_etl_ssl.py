# Flask server for handling database transactions requested from REDCap.
# To run:
#>  export FLASK_APP=pyserver_lk.py
#>  flask run
# To run in debug mode, uncomment last line and then:
#>  python3 pyserver_lk.py

import sys # sys.stdout.flush() req'd to view stdout
sys.path.insert(0,'../conf')
sys.path.insert(0,'../lib')

import pyserver_etl_config as config
import NDDdb_py_modules as NDDdb
import NDDdb_field_dict as NDDdict

from flask import Flask, request, redirect
from flask_cors import CORS, cross_origin

from OpenSSL import SSL

import requests

import json

import pandas as pd
import numpy as np

import re

import datetime as dt

from labkey.utils import create_server_context

##########
# REDCap choices regexes
CHOICEPATTERN = re.compile('^\s*([0-9]+),\s(.*)\s*$')
ALTPATTERN = re.compile('^\s*(.*),\s(.*)\s*$')
#########

# Initialize Flask properties from config file
originspermitted = config.flaskparams['originspermitted']
methodspermitted = config.flaskparams['methodspermitted']
cer = config.flaskparams['sslcer']
key = config.flaskparams['sslkey']
context = SSL.Context(SSL.SSLv23_METHOD)
# Get REDCap params from config file
rc_api_url = config.rcparams['rc_api_url']
rc_base_url = config.rcparams['rc_base_url']
rc_refer_pid = config.rcparams['ndd_rc_refer_pid']
rc_refer_apikey = config.rcparams['ndd_rc_refer_apikey']
rc_data_pid = config.rcparams['ndd_rc_data_pid']
rc_data_apikey = config.rcparams['ndd_rc_data_apikey']
rc_sample_pid = config.rcparams['ndd_rc_sample_pid']
rc_sample_apikey = config.rcparams['ndd_rc_sample_apikey']
# Initialize LK properties from config file and create server context where queries will be executed
labkey_server = config.lkparams['labkey_server']
project_name = config.lkparams['project_name']
context_path = config.lkparams['context_path']
use_ssl = config.lkparams['use_ssl']
server_context = create_server_context(labkey_server, project_name, context_path, use_ssl)

app = Flask(__name__)
app.config['SERVER_NAME']=config.flaskparams['servname']
app.config['PREFERRRED_URL_SCHEME']='https'
cors = CORS(app, resources={r'/*': {'origins': originspermitted}})

# Pull RedCap metadata that will be used in various routes
dd_refer = NDDdb.getMetaData(rc_api_url, rc_refer_apikey, 'family_members')
dd_data = NDDdb.getMetaData(rc_api_url, rc_data_apikey, 'family_members')
dd_refer.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)
dd_data.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)

# Relation map for LabKey updates
reln_map = NDDdb.parseChoiceMap(dd_data.loc['demo_relation','select_choices_or_calculations'], CHOICEPATTERN, ALTPATTERN)

### ETL/Data manipulation ###
@app.route('/dupe_check', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def dupe_check():
  checkAll = True
  if 'record' in request.args:
    checkAll = False
    refid = request.args.get('record') # Pull only current referral if an id was passed
  else:
    refid = '' # Pull all referrals and make lookup column
  instr = 'physician_referral_form'
  # Pull referral(s)
  referral = NDDdb.pullRCRecords(rc_api_url, rc_refer_apikey, instr, refid, '')
  referral = referral.loc[referral['redcap_repeat_instrument']==''] # TODO deprecate this temporary code
  referral.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
  referral['uqid'] = referral['firstname'].str.lower() + referral['lastname'].str.lower() + referral['dob'] + referral['sex']
  referral = referral.loc[referral['verifiedunique']!='1']
  if len(referral) > 0:
    # Pull all data collection records and make lookup column
    projdat = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'demographics', '', 'redcap_id')
    projdat.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = False)
    if len(projdat) > 0:
      subjs = projdat.loc[projdat['redcap_event_name']=='family_member_arm_1']
    else:
      subjs = projdat
    subjs['uqid'] = subjs['demo_firstname'].str.lower() +subjs['demo_lastname'].str.lower() +subjs['demo_dob']+subjs['demo_sex']
    # Check uniqueness of referral lookup against the list of subjects already enrolled
    referral['duplicated'] = referral['uqid'].isin(subjs['uqid'].values)
    # Write pass/fail booleans back to referral project
    update = referral[['redcap_id']].copy()
    update['verifiedunique'] = referral['duplicated'].map({True:'0',False:'1'})
#    update['referral_validation_complete']='2'
    updateres = NDDdb.pushRCRecord(rc_api_url, rc_refer_apikey, '', update)
  # Redirect user back to referral project
  if checkAll:
    dest = rc_base_url + rc_refer_pid + '&arm=1'
  else:
    dest = rc_base_url + rc_refer_pid + '&arm=1&id=' + refid
  return redirect(dest, code=302)

# Overwrite proband's contact data w/ contact info recorded in corresponding referral
@app.route('/copyContactInfo', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def copyContactInfo():
  dcid = request.args.get('record') # data collection ID if it was passed
  # Pull Data Collection record to get corresponding Referral ID
  dc = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, '', dcid,'redcap_id,referralid')
  dc.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = False)
  dcfam = dc.loc[dc['redcap_event_name']=='family_data_arm_1']
  refid = dcfam.loc[dcid,'referralid']
  # Pull Referral record
  referral = NDDdb.pullRCRecords(rc_api_url, rc_refer_apikey, 'progress_notes', refid,'redcap_id')
  referral = referral.loc[referral['redcap_repeat_instrument']==''] # TODO deprecate this temporary code
  referral.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
  proband = dc.loc[(dc['redcap_event_name']=='family_member_arm_1') & (dc['redcap_repeat_instance']=='1')]
  proband.drop(columns=['referralid','redcap_repeat_instrument'], inplace=True)
  # Iterate over Progress Notes contact info fields and copy them to the dataframe that we'll import to Data Collection
  for key,val in NDDdict.ProgNotes2Demo.items():
    proband.loc[dcid,val] = referral.loc[refid,key]
  NDDdb.pushRCRecord(rc_api_url, rc_data_apikey, '', proband)
  dest = rc_base_url + rc_data_pid + '&arm=1&id=' + dcid
  print(dest)
  return redirect(dest, code=302)

# Overwrite proband's contact data w/ contact info recorded in corresponding referral
@app.route('/copyParentsInfoFromDC', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def copyParentsInfoFromDC():
  refid = request.args.get('record') # data collection ID if it was passed
  # Pull Referral record & get DC ID
  referral = NDDdb.pullRCRecords(rc_api_url, rc_refer_apikey, 'physician_referral_form', refid,'')
  referral = referral.loc[referral['redcap_repeat_instrument']==''] # TODO deprecate this temporary code
  referral.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
  dcid = referral.loc[refid,'dataproj_id']
  # Pull Data Collection record to get corresponding Referral ID
  dc = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'demographics', dcid,'')
  print(dc)
  sys.stdout.flush()
  dc.set_index(keys='demo_relation', drop = False, inplace = True, verify_integrity = False)
  # Build data frame to import based on which relations are present
  famcontact = pd.DataFrame({'redcap_id':[refid]})
  try:
    famcontact['mom_firstname'] = dc.loc['2','demo_firstname']
    famcontact['mom_lastname'] = dc.loc['2','demo_lastname']
    famcontact['mom_phone'] = dc.loc['2','demo_phone']
    famcontact['mom_email'] = dc.loc['2','demo_email']
  except (KeyError,IndexError):
    pass
  try:
    famcontact['dad_firstname'] = dc.loc['3','demo_firstname']
    famcontact['dad_lastname'] = dc.loc['3','demo_lastname']
    famcontact['dad_phone'] = dc.loc['3','demo_phone']
    famcontact['dad_email'] = dc.loc['3','demo_email']
  except (KeyError,IndexError):
    pass
  try:
    famcontact['sib_firstname'] = dc.loc['4','demo_firstname']
    famcontact['sib_lastname'] = dc.loc['4','demo_lastname']
    famcontact['sib_phone'] = dc.loc['4','demo_phone']
    famcontact['sib_email'] = dc.loc['4','demo_email']
  except (KeyError,IndexError):
    pass
  except (ValueError):
    famcontact['sib_firstname'] = dc.loc['4','demo_firstname'].iloc[0]
    famcontact['sib_lastname'] = dc.loc['4','demo_lastname'].iloc[0]
    famcontact['sib_phone'] = dc.loc['4','demo_phone'].iloc[0]
    famcontact['sib_email'] = dc.loc['4','demo_email'].iloc[0]
  NDDdb.pushRCRecord(rc_api_url, rc_refer_apikey, '', famcontact)
  dest = rc_base_url + rc_refer_pid + '&arm=1&id=' + refid
  return redirect(dest, code=302)

####### Routes called by DET
# Triggered on changes to the referral project
@app.route('/referral_pipeline', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def referral_pipeline():
  reqdat = dict(request.form)
  # Check the format returned sent by the REDCap server to determine if the values in the dict are lists or strings
  if type(reqdat['record']) is str:
    recordid = reqdat['record']
    instr = reqdat['instrument']
  elif type(reqdat['record']) is list:
    recordid = reqdat['record'][0]
    instr = reqdat['instrument'][0]
  # Pull record
  referral = NDDdb.pullRCRecords(rc_api_url, rc_refer_apikey, instr, recordid,'pintoreferral_stat,apptdate,idnum,f_idnum,dataproj_id,physician_referral_form_complete')
  referral = referral.loc[referral['redcap_repeat_instrument']==''] # TODO deprecate this temporary code
  referral.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
  # Fill in provider email if it hasn't been set already
  if instr == 'physician_referral_form':
    if referral.loc[recordid,'provider_email']=='':
      if referral.loc[recordid,'sinaistatus']!='':
        if referral.loc[recordid,'sinaistatus']=='1':
          provider_email = NDDdict.mshprovider2email[referral.loc[recordid,'sinaiprovider']]
        elif referral.loc[recordid,'sinaistatus']=='2':
          provider_email = NDDdict.nyuprovider2email[referral.loc[recordid,'nyuprovider']]
        else:
          provider_email = ''
      update = pd.DataFrame(data={'redcap_id':[recordid], 'provider_email':[provider_email]})
      updatereferreq = NDDdb.pushRCRecord(rc_api_url, rc_refer_apikey, '', update)
  # Get data collection id
  dcid = referral.loc[recordid,'dataproj_id']
  # If the data collection id is blank, query RedCap for the next one and set the 'I'm new here' flag
  imnewhere = False
  if dcid == '':
    imnewhere = True
    dcid = str(NDDdb.getNextDCID(rc_api_url, rc_data_apikey))
  # Get individual #
  idnum = referral.loc[recordid,'idnum']
  # then check value of 'referral_triggerenroll' and 'referral_id'
  # finally write new project id back to 'referral_id'
  if referral.loc[recordid,'referral_triggerenroll'] == '1' and referral.loc[recordid,'verifiedunique']=='1':
    idnum = NDDdb.pushToDC(rc_api_url, rc_refer_apikey, rc_data_apikey, referral, recordid, dcid, idnum, imnewhere)
    if imnewhere:
      lkupdate = pd.DataFrame(data={'id':[dcid], 'referralid':[recordid]})
      NDDdb.LKinsertRow(lkupdate.iloc[0], 'Families', 'lists', server_context)
      lkid = str(NDDdb.getNextLabKeyID(server_context))
      NDDdb.assignLabKeyID(rc_api_url, rc_data_apikey, server_context, dcid, '1', lkid, 'Proband', dt.datetime.today().strftime('%Y-%m-%d'))
  # Check if this subject was referred to Pinto study and has an appointment date
  # how to check for fam's record here?
  apptdate = referral.loc[recordid,'apptdate']
  if referral.loc[recordid,'pintoreferral_stat']=='1' and apptdate != '' and idnum == '01':
    # Assign F# if this subject does not have one already
    dc = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'family_enrollment', '', '')
    if len(dc) > 0:
      dc = dc.loc[dc['redcap_event_name']=='family_data_arm_1']
    dc.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
    fnum = dc.loc[str(dcid),'fnum']
    # Assign F# if this subject does not have one already
    if fnum == '':
      myfam = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'enrollment', dcid, '')
      myfam.set_index(keys='idnum', drop = False, inplace = True, verify_integrity = False)
      lkid = myfam.loc[idnum,'labkey_subjid']
      fnum = NDDdb.getNextFnum(apptdate,dc)
      NDDdb.assignFID(rc_api_url, rc_data_apikey, server_context, dcid, fnum)
      NDDdb.assignFindivID(rc_api_url, rc_refer_apikey, rc_data_apikey, server_context, recordid, dcid, '1', lkid, fnum, idnum)
  sys.stdout.flush()
  return ''

# Triggered on changes to the data collection project
@app.route('/data_pipeline', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def data_pipeline():
    reqdat = dict(request.form)
    print(reqdat)
    print(reqdat['record'])
    # Check the format returned sent by the REDCap server to determine if the values in the dict are lists or strings
    if type(reqdat['record']) is str:
      recordid = reqdat['record']
      instr = reqdat['instrument']
    elif type(reqdat['record']) is list:
      recordid = reqdat['record'][0]
      instr = reqdat['instrument'][0]
    if instr == 'demographics' or instr == 'enrollment':
      # Check to see if a new family member has been added by pulling the Enrollment table from the Data Collection project
      rep = reqdat['redcap_repeat_instance'][0]
      enroll = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'enrollment', recordid, 'redcap_id,demo_dateadded,demo_relation')
      enroll.set_index(keys='redcap_repeat_instance', drop = False, inplace = True, verify_integrity = True)
      lkid = enroll.loc[rep,'labkey_subjid']
      reln = enroll.loc[rep,'demo_relation']
      if reln!='':
        if lkid=='': # Assign LabKey ID by querying LabKey for next available integer
          lkid = str(NDDdb.getNextLabKeyID(server_context))
          NDDdb.assignLabKeyID(rc_api_url, rc_data_apikey, server_context, recordid, rep, lkid, reln_map[reln], enroll.loc[rep,'demo_dateadded'])
        idnum = enroll.loc[rep,'idnum']
        if idnum=='': # Assign individual number by pulling family record and checking for next available value--or, if relation is one of {Proband, Mother, Father}, assign designated numbers
          famdat = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'family_enrollment', recordid, '')
          fnrow = famdat.loc[famdat['redcap_event_name']=='family_data_arm_1',].index
          fnum = famdat.loc[fnrow,'fnum'].values[0]
          if fnum != '':
            print('Checking for next Ind num')
            idnum = NDDdb.getNextIndNum(reln,enroll['idnum'])
            print('Assigninf F-idnum')
            NDDdb.assignFindivID(rc_api_url, rc_refer_apikey, rc_data_apikey, server_context, None, recordid, rep, lkid, fnum, idnum)
    # Check to see if 'copy contact from' field has been set
    if instr == 'demographics':
      demo = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'demographics', recordid,'redcap_id')
      reptocopy = demo.loc[demo['redcap_repeat_instance']==rep]['demo_copycontactfrom'].values[0]
      if reptocopy != '':
        copyfrom = demo.loc[demo['redcap_repeat_instance']==reptocopy]
        pasteto = copyfrom[['redcap_id','demo_address1','demo_address2','demo_apt','demo_city','demo_state','demo_zip','demo_email','demo_email2','demo_phone','demo_phone2']]
        pasteto['demo_copycontactfrom'] = ''
        pasteto['redcap_event_name'] = 'family_member_arm_1'
        pasteto['redcap_repeat_instance'] = rep
        updateres = NDDdb.pushRCRecord(rc_api_url, rc_data_apikey, '', pasteto)
      # If the saved record is the proband's, copy his/her email to the family contact email
      if rep == '1':
        NDDdb.setFamEmail(rc_api_url, rc_data_apikey, demo.loc[demo['redcap_repeat_instance']=='1','demo_email'].values[0], recordid)
    sys.stdout.flush()
    return

# Triggered on changes to the sample submission project
@app.route('/sample_pipeline', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def sample_pipeline():
  reqdat = dict(request.form)
  print(reqdat)
  instr = reqdat['instrument'][0]
  #instr = 'physician_referral_form'
  recordid = reqdat['record'][0]
  print('record: ' + recordid)
  # Pull record
  sample = NDDdb.pullRCRecords(rc_api_url, rc_sample_apikey, instr, recordid,'')
  print(sample)
  sample.set_index(keys='submission_id', drop = False, inplace = True, verify_integrity = True)
  return

#################
### Redirects ###

# Redirect from referral record to corresponding family record in data collection
@app.route('/gotoDataCollectRC', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def goto_ndd_datacollect_rc():
    refid = request.args.get('record')
    dataprojid = NDDdb.pullRCRecords(rc_api_url, rc_refer_apikey, 'physician_referral_form', refid, 'dataproj_id').loc[1,'dataproj_id']
    dest = rc_base_url + rc_data_pid + '&arm=1&id=' + dataprojid
    return redirect(dest, code=302)

# Redirect from data collection record to referral
@app.route('/gotoReferralRC', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def goto_ndd_referral_rc():
    dcid = request.args.get('record')
    refid = NDDdb.pullRCRecords(rc_api_url, rc_data_apikey, 'family_enrollment', dcid, 'referralid').loc[1,'referralid']
    dest = rc_base_url + rc_refer_pid + '&arm=1&id=' + refid
    return redirect(dest, code=302)

# Get request from REDCap and redirect to Family page
@app.route('/viewSubjInLabKey', methods=methodspermitted)
@cross_origin(origin=originspermitted)
def view_subj_in_LabKey():
    rcid = request.args.get('record')
    Fnum = lkpy.goto_LK_fampage(server_context, rcid)
    return redirect('https://neurodevdb.mssm.edu:8443/labkey/NDDdb/wiki-page.view?name=Family%20Details&Fnum=' + Fnum, code=302)

#################

#app.run(debug=True) # Uncomment to run in debug mode. This line must come after all routes.

context=(cer,key)
if __name__ == '__main__':
  app.run(host='0.0.0.0',ssl_context=context)
