import pycurl
import io
import sys
import csv
import pandas as pd
import numpy as np
import datetime as dt
import json
import re
import NDDdb_field_maps as NDDmap

import labkey
from labkey.query import select_rows, insert_rows, update_rows

# Select row from REDCap & populate data frame
def pullRCRecords(api_url, api_token, forms, records, fields):

	buf = io.BytesIO()

	fields = [
		('token', api_token),
		('content', 'record'),
		('format', 'csv'),
		('type', 'flat'),
		('forms', forms),
		('records', records),
		('fields', fields)
	]

	ch = pycurl.Curl()
	ch.setopt(ch.URL, api_url)
	ch.setopt(ch.HTTPPOST, fields)
	ch.setopt(ch.WRITEFUNCTION, buf.write)
	#ch.setopt(pycurl.VERBOSE, 1) #for debugging
	ch.setopt(pycurl.SSL_VERIFYPEER, 1) # 1 = Curl verifies whether the certificate is authentic
	ch.setopt(pycurl.SSL_VERIFYHOST, 2) # 2 = Curl verifies that the server you're communicating with is the same as the one on the cert
	ch.perform()
	response_code = ch.getinfo(pycurl.HTTP_CODE)
	ch.close()

	#If the request was not successful, terminate
	if response_code!=200:
		sys.exit('Error communicating with REDCap database: could not pull participant records from REDCap.')

	#convert bytestream to bytes...
	bytereport = buf.getvalue()
	buf.close()
	#...to string...
	try:
		stringreport = bytereport.decode('utf8')
	except UnicodeDecodeError:
		stringreport = bytereport.decode('latin-1')
		stringreport.encode('utf8')
	#...to list of strings by matching things that are either in quotes or things that aren't newlines
	linesplit = re.findall('(?:"[^"]*"|.)+', stringreport)
	# make dataframe to hold results
	df = pd.DataFrame(list(list(rec) for rec in csv.reader(linesplit, delimiter=',')))
	# If dataframe isn't empty, make first row the header. If it is, pull the form's metadata to get column names
	if not df.empty:
		header = df.iloc[0]
		df = df[1:]
		df.columns = header
	else:
		df = pd.DataFrame(columns=getMetaData(api_url,api_token,forms)['field_name'])
	return(df)

# Insert/update REDCap with data frame
def pushRCRecord(api_url, api_token, form, df):
	to_update = df.to_dict('records')
	to_update_json = json.dumps(to_update, separators=(',',':'))
	payload = [
	    ('token', api_token),
	    ('format', 'json'),
	    ('content','record'),
	    ('data',to_update_json)
	]
	print(payload)
	buf = io.BytesIO()
	ch = pycurl.Curl()
	ch.setopt(ch.URL, api_url)
	ch.setopt(ch.HTTPPOST, payload)
	ch.setopt(ch.WRITEFUNCTION, buf.write)
	ch.setopt(pycurl.VERBOSE, 1) #for debugging
	ch.setopt(pycurl.SSL_VERIFYPEER, 1) # 1 = Curl verifies whether the certificate is authentic
	ch.setopt(pycurl.SSL_VERIFYHOST, 2) # 2 = Curl verifies that the server you're communicating with is the same as the one on the cert
	try:
		ch.perform()
	except pycurl.error:
		print('BIG TROUBLE')
	response_code = ch.getinfo(pycurl.HTTP_CODE)
	ch.close()
	# If the request was not successful, terminate
	if response_code!=200:
		return('Error communicating with REDCap database.')
	# Get the number of records updated in REDCap
	bytereport = buf.getvalue()
	buf.close()
	stringreport = bytereport.decode('utf8')
	countPattern = re.compile('{\"count\":\s*([0-9]+)')
	c = countPattern.match(stringreport)
	recordsUpdated = c.group(1)
	return(str(recordsUpdated))

# Make dictionary from options list in REDCap
def populateFieldDict(s, fieldmap):
	dictionary = {}
	for key, value in fieldmap.items():
		dictionary[value] = s.loc[key]
	return dictionary

# Get the data dictionary for the specified project & convert to data frame
def getMetaData(api_url, api_token, forms):
	buf = io.BytesIO()
	fields = [
		('token', api_token),
		('content', 'metadata'),
		('format', 'csv'),
		('type', 'flat'),
		('forms', forms)
	]
	ch = pycurl.Curl()
	ch.setopt(ch.URL, api_url)
	ch.setopt(ch.HTTPPOST, fields)
	ch.setopt(ch.WRITEFUNCTION, buf.write)
	#ch.setopt(pycurl.VERBOSE, 1) #for debugging
	ch.setopt(pycurl.SSL_VERIFYPEER, 1) # 1 = Curl verifies whether the certificate is authentic
	ch.setopt(pycurl.SSL_VERIFYHOST, 2) # 2 = Curl verifies that the server you're communicating with is the same as the one on the cert
	ch.perform()
	response_code = ch.getinfo(pycurl.HTTP_CODE)
	ch.close()
	#If the request was not successful, terminate
	if response_code!=200:
		sys.exit('Error communicating with REDCap database: could not pull participant records from REDCap.')
	#convert bytestream to bytes...
	bytereport = buf.getvalue()
	buf.close()
	#...to string...
	try:
		stringreport = bytereport.decode('utf8')
	except UnicodeDecodeError:
		stringreport = bytereport.decode('latin-1')
		stringreport.encode('utf8')
	#...to list of strings.
	linesplit = re.findall('(?:"[^"]*"|.)+', stringreport)
	# make dataframe to hold results
	df = pd.DataFrame(list(list(rec) for rec in csv.reader(linesplit, delimiter=',')))
	header = df.iloc[0]
	df = df[1:]
	df.columns = header
	return(df)

# Take the value(s) of multiple source checkbox fields & translate into a single target checkbox
def collapseFields(i, dat, sources, target, dd_refer, dd_data):
  for f in sources:
    if dat.loc[i,f] != '':
      optmap_refer = parseChoiceMap(dd_refer['select_choices_or_calculations'].loc[f],CHOICEPATTERN,ALTPATTERN)
      optmap_data = parseChoiceMap(dd_data['select_choices_or_calculations'].loc[target],CHOICEPATTERN,ALTPATTERN)
      optmap_data = {v:k for k,v in optmap_data.items()}
      sourceval = optmap_refer[dat.loc[i,f]]
      targetval = optmap_data[sourceval]
      return targetval
  return ''

# # Parse map of choices from REDCap data dictionary -> Python dictionary
CHOICEPATTERN = re.compile('^\s*([0-9]+),\s(.*)\s*$')
ALTPATTERN = re.compile('^\s*(.*),\s(.*)\s*$')
def parseChoiceMap(optstr, pattern, altpattern):
  optlist = optstr.split('|')
  choicesmap = {}
  for opt in optlist:
    opt = opt.rstrip()
    parsed = re.search(pattern, opt)
    try:
      if parsed.groups().__len__()!=2:
        print('WARNING: Selection choices parsing returned > or < than 2 matches')
        break
      else:
        choicesmap[parsed.group(1)] = parsed.group(2)
    except AttributeError:
      parsed = re.search(altpattern, opt)
      choicesmap[parsed.group(1)] = parsed.group(2)
  return choicesmap

# Push new record to LabKey.
def LKinsertRow(rcrd, query, schema, sc):
  try:
    insertres = insert_rows(sc, schema, query, [rcrd.to_dict()])
  except labkey.exceptions.RequestError:
    print('Insert failed for record: ' + rcrd.index)
  return

# Attempt to update LabKey record, and if failed, insert new
def LKupdateinsertRow(rcrd, query, schema, sc):
  try:
    updateres = update_rows(sc, schema, query, [rcrd.to_dict()])
  except labkey.exceptions.QueryNotFoundError:
    print('Update failed for record: ' + rcrd.index)
    try:
      insertres = insert_rows(sc, schema, query, [rcrd.to_dict()])
    except labkey.exceptions.RequestError:
      print('ERROR: Could not insert/update ' + query + ' data for subject ' + str(rcrd['SubjectID']))
    return

# Change invalid dates to empty strings
def tryCoerceDate(date):
  try:
      result=pd.to_datetime(date)
  except ValueError:
    return ''
  return result

# Return the integer component of an F# (if the F# is not NULL)
def parseFs(r):
  fnum = r
  if fnum == '' or fnum == 'None':
    return ''
  else:
    return fnum[1:]

# Copy proband's email to Family Enrollment instrument
def setFamEmail(api_url, dc_api_key, email, dcid):
  update = pd.DataFrame()
  update['fam_email'] = [email]
  update['redcap_event_name'] = ['family_data_arm_1']
  update['redcap_id'] = [dcid]
  pushRCRecord(api_url, dc_api_key, '', update)
  return

#### Functions for getting IDs ###

# Retrieve the next sequential data collection ID
def getNextDCID(api_url, api_key):
  allreferids = pullRCRecords(api_url, api_key, 'family_enrollment', '', 'redcap_id')
  if len(allreferids) > 0:
    return allreferids['redcap_id'].astype(int).max() + 1
  else:
    return 1

# Retrieve the next sequential F#
def getNextFnum(apptdate,fnumdf):
  refyr = tryCoerceDate(apptdate).year
  yy = str(refyr)[-2:]
  fnumdf['Fxx'] = fnumdf['fnum'].str[1:]
  fnumdf['Fyrs'] = fnumdf['Fxx'].str[:2]
  fnumdf['Fcount'] = fnumdf.Fxx.str[-4:]
  fnumdf = fnumdf.loc[fnumdf['Fyrs']==yy]
  if len(fnumdf) > 0:
    Fcount = pd.to_numeric(fnumdf['Fcount']).max()
    nextInt = str(Fcount+1)
  else:
    nextInt = '001'
  nextF = 'F' + yy + ('0' * (4-len(nextInt))) + nextInt
  return nextF

# Using the specified relation of an individual & list of claimed individual #s in the family, assign the next available individual #
def getNextIndNum(reln, idnums): #TODO add error handling for duplicate moms/dads
  idnums = pd.to_numeric(idnums).dropna()
  # Drop non-bio parents so that their individual numbers don't throw off ID assignment
  idnums = idnums[~idnums.isin([88,99])]
  if reln == '1': # Proband
    return '01'
  elif reln == '2': # Mother
    return '02'
  elif reln == '3': # Father
    return '03'
  elif reln == '10': # Non-bio mother
    return '88'
  elif reln == '11': # Non-bio father
    return '99'
  elif idnums.size > 0:
      nextInd = idnums.max() + 1
      if nextInd <= 3:
        nextInd = 4
      nextInd = str(nextInd.astype(int))
      return('0' * (2-len(nextInd)) + nextInd)
  else:
    return '01'

# Inter-project data pushing functions
def pushToReferral(api_url, ref_api_key, dc_api_key, referral, submitid):
  # Move everything starting here to function pushToDC
  imnewhere = False
  if dcid == '':
    imnewhere = True
    # Get next available data collection ID
    dcid = getNextDCID(api_url, dc_api_key)
  # Get project metadata
  dd_refer = getMetaData(api_url, ref_api_key, 'family_members')
  dd_data = getMetaData(api_url, dc_api_key, 'family_members')
  dd_refer.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)
  dd_data.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)
  # Construct data frame for import to data collection project
  newfam = pd.DataFrame()
  newfam['referralid'] = referral['redcap_id']
  newfam['redcap_event_name'] = 'family_data_arm_1'
  newfam['labkey_famid'] = dcid
  if imnewhere:
    newfam['dataprojdate'] = dt.datetime.today().strftime('%Y-%m-%d')
  # Add proband as first Family Member
  proband = {
    'redcap_event_name':'family_member_arm_1',
    'demo_firstname':referral.loc[refid,'firstname'],
    'demo_lastname':referral.loc[refid,'lastname'],
    'demo_mrn':referral.loc[refid,'epic'],
    'demo_dob':referral.loc[refid,'dob'],
    'demo_sex':referral.loc[refid,'sex']
  }
  # Handle proband's startdate and other fields that should only need to be captured once
  if imnewhere:
    idnum = '01'
    rep = '1'
    proband['demo_relation'] = '1' # 1 = 'Proband'
    proband['demo_dateadded'] = dt.datetime.today().strftime('%Y-%m-%d')
    proband['referraldate'] = referral.loc[refid, 'referraldate']
    proband['hasndd'] = '1'
  # If individual isn't new, check his/her individual # and determine which repeat instance he/she is
  else:
    if idnum == '01':
      rep = '1'
    else:
      myfam = pullRCRecords(api_url, dc_api_key, '', dcid, 'redcap_id,idnum')
      rep = myfam.loc[myfam['idnum']==idnum,'redcap_repeat_instance'].values[0]
  proband['idnum'] = idnum
  proband['redcap_repeat_instance'] = rep
  #proband['referraldate'] = referral.loc[refid,'referraldate']
  # Handle referral form fields that should be collapsed into a single field prior to entry
  proband['physician'] = collapseFields(refid, referral, ['sinaiprovider','nyuprovider','barnabasprovider'], 'physician', dd_refer, dd_data)
  proband['physician_other'] = referral.loc[refid,'providerother']
  proband['hospitalcenter'] = collapseFields(refid, referral, ['sinaicenter','sinaistatus'], 'hospitalcenter', dd_refer, dd_data)
  proband['hospitalcenter_other'] = referral.loc[refid, 'hospitalcenter_other']
  newfam = newfam.append(proband, ignore_index=True, verify_integrity=False, sort=None)
  newfam['redcap_id'] = dcid
  newfam.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = False)
  newfam.replace(np.nan,'', inplace=True)
  newfam.to_csv('newfam.csv')
  # Enroll new subject in data collection project
  newfamreq = pushRCRecord(api_url, dc_api_key, '', newfam)
  # Finally write new project id back to 'dataproj_id'
  if imnewhere:
    update = pd.DataFrame(data={'redcap_id':[refid], 'dataproj_id':[dcid], 'idnum': idnum})
    updatereferreq = pushRCRecord(api_url, ref_api_key, '', update)
  return ''

def pushToDC(api_url, ref_api_key, dc_api_key, referral, refid, dcid, idnum, imnewhere):
  # Get project metadata
  dd_refer = getMetaData(api_url, ref_api_key, 'family_members')
  dd_data = getMetaData(api_url, dc_api_key, 'family_members')
  dd_refer.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)
  dd_data.set_index(keys='field_name', drop = False, inplace = True, verify_integrity = True)
  # Construct data frame for import to data collection project
  newfam = pd.DataFrame()
  newfam['referralid'] = referral['redcap_id']
  newfam['redcap_event_name'] = 'family_data_arm_1'
  newfam['labkey_famid'] = dcid
  if imnewhere:
    newfam['dataprojdate'] = dt.datetime.today().strftime('%Y-%m-%d')
  # Add proband as first Family Member
  proband = {
    'redcap_event_name':'family_member_arm_1',
    'demo_firstname':referral.loc[refid,'firstname'],
    'demo_lastname':referral.loc[refid,'lastname'],
    'demo_mrn':referral.loc[refid,'epic'],
    'demo_dob':referral.loc[refid,'dob'],
    'demo_sex':referral.loc[refid,'sex']
  }
  # Handle proband's startdate and other fields that should only need to be captured once
  if imnewhere:
    idnum = '01'
    rep = '1'
    proband['demo_relation'] = '1' # 1 = 'Proband'
    proband['demo_dateadded'] = dt.datetime.today().strftime('%Y-%m-%d')
    proband['referraldate'] = referral.loc[refid, 'referraldate']
    proband['hasndd'] = '1'
  # If individual isn't new, check his/her individual # and determine which repeat instance he/she is
  else:
    if idnum == '01':
      rep = '1'
    else:
      myfam = pullRCRecords(api_url, dc_api_key, '', dcid, 'redcap_id,idnum')
      rep = myfam.loc[myfam['idnum']==idnum,'redcap_repeat_instance'].values[0]
  proband['idnum'] = idnum
  proband['redcap_repeat_instance'] = rep
  #proband['referraldate'] = referral.loc[refid,'referraldate']
  # Handle referral form fields that should be collapsed into a single field prior to entry
  proband['physician'] = collapseFields(refid, referral, ['sinaiprovider','nyuprovider','barnabasprovider'], 'physician', dd_refer, dd_data)
  proband['physician_other'] = referral.loc[refid,'providerother']
  proband['hospitalcenter'] = collapseFields(refid, referral, ['sinaicenter','sinaistatus'], 'hospitalcenter', dd_refer, dd_data)
  proband['hospitalcenter_other'] = referral.loc[refid, 'hospitalcenter_other']
  #newfam = newfam.append(proband, ignore_index=True, verify_integrity=False, sort=None) #'Sort' argument throwing errors 04172019
  newfam = newfam.append(proband, ignore_index=True, verify_integrity=False)
  newfam['redcap_id'] = dcid
  newfam.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = False)
  newfam.replace(np.nan,'', inplace=True)
  #newfam.to_csv('newfam.csv')
  # Enroll new subject in data collection project
  newfamreq = pushRCRecord(api_url, dc_api_key, '', newfam)
  # Finally write new project id back to 'dataproj_id'
  if imnewhere:
    update = pd.DataFrame(data={'redcap_id':[refid], 'dataproj_id':[dcid], 'idnum': idnum})
    updatereferreq = pushRCRecord(api_url, ref_api_key, '', update)
  return '01'

# Assign F# for a newly enrolled subject
def assignFID(api_url, dc_api_key, server_context, dcid, fnum):
  # Write to data collection project
  update = pd.DataFrame(data={'redcap_id':[dcid], 'fnum':[fnum]})
  updateDCreq = pushRCRecord(api_url, dc_api_key, '', update)
  # Write to LabKey
  lkupdate = pd.DataFrame(data={'id':[dcid], 'fnum':[fnum]})
  LKupdateinsertRow(lkupdate.iloc[0], 'Families', 'lists', server_context)
  return

# Assign F-individual# for a newly enrolled subject
# TODO probably need to pass relation in func instead of getting it from data, since I want to recycle this func
# Recycling also means I need to disable referral project push when assigning F-ind to family member
def assignFindivID(api_url, ref_api_key, dc_api_key, server_context, refid, dcid, rep, subjid, fnum, idnum):
  f_idnum = fnum + '-' + idnum
  # Write to data collection project
  update = pd.DataFrame(data={'redcap_id':[dcid],'redcap_event_name':['family_member_arm_1'],'redcap_repeat_instance':[rep], 'f_idnum':[f_idnum], 'idnum':[idnum]})
  updateDCreq = pushRCRecord(api_url, dc_api_key, '', update)
  # If a referral ID was passed, write to referral project
  if refid:
    print('update referral project')
    update = pd.DataFrame(data={'redcap_id':[refid], 'f_idnum':[f_idnum]})
    updatereferreq = pushRCRecord(api_url, ref_api_key, '', update)
  # Write to LabKey
  #NDDdb.assignLabKeyID(rc_api_url, rc_data_apikey, server_context, enroll, rep, lkid)
  lkupdate = pd.DataFrame(data={'SubjectID':[subjid],'idnum':[idnum], 'f_idnum':[f_idnum]})
  LKupdateinsertRow(lkupdate.iloc[0], 'Enrollment', 'study', server_context)
  return

def assignFID___DEPR(api_url, ref_api_key, dc_api_key, server_context, apptdate, refid, dcid, idnum):
  # Assign F# if this subject does not have one already
  dc = pullRCRecords(api_url, dc_api_key, 'family_enrollment', '', '')
  if len(dc) > 0:
    dc = dc.loc[dc['redcap_event_name']=='family_data_arm_1']
  dc.set_index(keys='redcap_id', drop = False, inplace = True, verify_integrity = True)
  fnum = dc.loc[str(dcid),'fnum']
  # Assign F# if this subject does not have one already
  if fnum == '':
    print('I need an F#')
    nextF = getNextFnum(apptdate,dc)
    f_idnum = nextF + '-' + idnum
    # Write new F# to data collection project
    update = pd.DataFrame(data={'redcap_id':[dcid], 'fnum':[nextF]})
    updateDCreq = pushRCRecord(api_url, dc_api_key, '', update)
    # Write F-ID# to data collection project
    update = pd.DataFrame(data={'redcap_id':[dcid],'redcap_event_name':['family_member_arm_1'],'redcap_repeat_instance':['1'], 'f_idnum':[f_idnum]})
    updateDCreq = pushRCRecord(api_url, dc_api_key, '', update)
    # Write F-ID# to referral project
    update = pd.DataFrame(data={'redcap_id':[refid], 'f_idnum':[f_idnum]})
    updatereferreq = pushRCRecord(api_url, ref_api_key, '', update)
    # Write F-ID# to LabKey
    #lkid = NDDdb.getNextLabKeyID(server_context)
    #reln_map = parseChoiceMap(dd_data.loc['demo_relation','select_choices_or_calculations'], CHOICEPATTERN, ALTPATTERN)
    #enroll['demo_relation'] = enroll['demo_relation'].map(reln_map)
    #NDDdb.assignLabKeyID(rc_api_url, rc_data_apikey, server_context, enroll, rep, lkid)
    #lkupdate = pd.DataFrame(data={'id':[refid], 'fnum':[nextF]})
    #LKinsertRow(lkupdate.iloc[0], 'Demographics', 'study', server_context)
    #LKinsertRow(lkupdate.iloc[0], 'Enrollment', 'study', server_context)
    # Write F# to LabKey
    lkupdate = pd.DataFrame(data={'id':[refid], 'fnum':[nextF]})
    LKinsertRow(lkupdate.iloc[0], 'Families', 'lists', server_context)
  return f_idnum

# Get next LabKey subject ID
def getNextLabKeyID(server_context):
  nextIDqresult = select_rows(server_context, 'study', 'GetNextSubjectID')
  nextSubjID = nextIDqresult['rows'][0]['nextSubjID']
  if not nextSubjID:
    nextSubjID = 0
  return nextSubjID

# Assign LabKey ID to newly enrolled subject  NOTE: can write relation here, NOT F-indiv#
def assignLabKeyID(api_url, dc_api_key, server_context, dcid, rep, lkid, reln, dateadded):
  dcupdate = pd.DataFrame(data={'redcap_id':[dcid], 'redcap_event_name':['family_member_arm_1'], 'redcap_repeat_instance':[rep], 'labkey_subjid':[lkid]})
  #update = enroll.loc[[rep],['redcap_id','redcap_event_name','redcap_repeat_instance','labkey_subjid']]
  updateDCreq = pushRCRecord(api_url, dc_api_key, '', dcupdate)
  #lkupdate = enroll.loc[[rep],['redcap_id','labkey_subjid','demo_dateadded','demo_relation']].rename(columns={'labkey_subjid':'SubjectID','redcap_id':'familyid','demo_dateadded':'date'})
  lkupdate = pd.DataFrame(data={'SubjectID':[lkid], 'demo_relation':[reln], 'date':[dateadded], 'familyid':[dcid]})
  LKinsertRow(lkupdate.iloc[0], 'Demographics', 'study', server_context)
  return
