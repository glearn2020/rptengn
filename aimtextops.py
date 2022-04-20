# ************************************************
# Name   : aimtextops.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define class for extracting data to flatfile
# Usage  : import aimtextops
# ***********************************************

# import libraries
import aimtfileops as osio
import aimtdbio as db
import aimtgenlib as log
import os
import ast
import aimtemailops
import sys
import aimts3ops

# global constants
curr_script_name = os.path.basename(__file__)
blocklog = 'blocklog'
scriptbegin = 'scriptBegin'
opfilenames = {}
rptlogdate = log.Aimtdate()
valid_ext_config = ['reportName', 'sqlFiles', 'extFileNames', 'opFileFrmt', 'dbConnect', 'extActions', 'outBoundActions', 'emailfromAddress',
                    'emailtoAddress',
                    'emailCcAddress', 'emailSubject', 'emailBody']
valid_s3_upload = ['s3ExpPath', 's3ArcPath']
valid_s3_samesrvr = ['s3srcbkt', 's3tgtbkt', 's3srcpath', 's3srcFilePrfx', 's3tgtpath']
valid_s3_conn = ['aimtAWSService', 'aimtProfile', 'aimtBucket']

# place holders for stdout and stderr
extlogsys_stderr = ''
extlogsys_stdout = ''


def ext_to_fltfl(dbcondict, sqlstorun, opfilenames, opfiledelim, reportname):
    log.Aimtlog.aimtprintbeginlog('blocklog', 'Begin extraction block for ' + reportname, curr_script_name)
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Executing SQLs and extracting data...')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Total Number of SQLs:' + len(sqlstorun).__str__())
    for num in sqlstorun:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'SQL-' + num + ': ' + os.path.basename(sqlstorun[num]))
        sql = osio.Aimtfileops.readfile(sqlstorun[num])
        dbconndetails = dbcondict[num]
        try:
            ext_rsp = db.Aimtdbops.rdshftget_sql(dbconndetails, sql)
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'SQL Execution successful. Number of rows: ' +
                                         len(ext_rsp.index).__str__())
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Writing current file: ' + opfilenames[num])
            db.Aimtdbops.writedftofile(ext_rsp, opfilenames[num], opfiledelim[num])
        except Exception as extexp:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Data extraction from DB and writing file failed..')
            log.Aimtlog.aimtprinterrorlog(curr_script_name, extexp.__str__())
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Stopping Script..')
            return False
    log.Aimtlog.aimtprintbeginlog('blocklog', 'Data Extraction successful for ' + reportname, curr_script_name)
    return True


def extract_prep(configcls, mainconfsec, avlopfrmtlst, setuppath, extpath):
    # Creating variables
    sqlstorun = {}
    arcfilepats = {}
    global opfilenames
    opfilenames = {}
    opfiledelim = {}
    dbcondict = {}

    # Checking to see if files mentioned in the config files are present & setting arc files and tgt files
    sqlfilelst = ast.literal_eval(mainconfsec['sqlFiles'])
    extfilelst = ast.literal_eval(mainconfsec['extFileNames'])
    opfilefrmt = ast.literal_eval(mainconfsec['opFileFrmt'])
    dbcondict = ast.literal_eval(mainconfsec['dbConnect'])

    # Checking if count of sql file list and output file list are matching
    if len(extfilelst) != len(sqlfilelst) and len(extfilelst) != len(opfilefrmt):
        log.Aimtlog.aimtprinterrorlog(curr_script_name,
                                      'Number of sql files provided does not match with number of extract file names..')
        return False

    for sql in sqlfilelst:
        sqlstorun[sql] = os.path.join(setuppath, sqlfilelst[sql])
        resp = osio.Aimtfilevalid.fileorpathexists(sqlstorun[sql], 'file')
        # assigning database connections
        if dbcondict[sql] == 'dbRdshft':
            dbcondict[sql] = configcls.dbRdshft
        elif dbcondict[sql] == 'aimtAnltProd':
            dbcondict[sql] = configcls.aimtAnltProd

        if not resp:
            return False
        opfilespec = ast.literal_eval(avlopfrmtlst[opfilefrmt[sql]])
        arcfilepats[sql] = osio.Aimtfilevalid.crtfileptrn(extfilelst[sql], opfilespec['ext'])
        opfilenames[sql] = osio.Aimtfilevalid.crtopfilenm(extpath, extfilelst[sql], opfilespec['ext'])
        opfiledelim[sql] = opfilespec['delimiter']

    # Calling extract module
    ext_resp = ext_to_fltfl(dbcondict, sqlstorun, opfilenames, opfiledelim, mainconfsec['reportName'])
    if not ext_resp:
        return False
    else:
        return True


def s3_upload(s3concls, rptsection):
    # getting output file list from global
    global opfilenames
    # validating s3 connections and config entries
    s3conresp = log.Aimtgenvalid.aimtpklstcntchk(valid_s3_conn, s3concls, 'S3 connection for file upload')
    # validating s3 configuration for file upload
    s3confresp = log.Aimtgenvalid.aimtpklstcntchk(valid_s3_upload, rptsection, 'S3 config for file upload')
    if not s3conresp or not s3confresp:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 's3 config details insufficient...')
        return False
    # creating s3 connection
    s3ops = aimts3ops.Aimts3ops(s3concls['aimtProfile'], s3concls['aimtAWSService'])

    # remove previous files with same prefix
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Checking to remove files from previous run...')
    rmv_lst = s3ops.lists3objects(s3concls['aimtBucket'], rptsection['s3ExpPath'] + rptsection['s3FilePrfx'])
    s3upldrmvresp = True
    if len(rmv_lst) == 0:
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'No files to remove...')
    else:
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Files to remove : ' + rmv_lst.__str__())
        s3upldrmvresp = s3ops.removefiles3(s3concls['aimtBucket'], rmv_lst)
    if not s3upldrmvresp:
        return False

    # upload current day files to extract path
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Upload files to extract path..')
    extupldresp = s3ops.upldtos3fromlocal(s3concls['aimtBucket'], opfilenames, rptsection['s3ExpPath'])
    # upload current day files to archive path
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Upload files to archive path..')
    arcupldresp = s3ops.upldtos3fromlocal(s3concls['aimtBucket'], opfilenames, rptsection['s3ArcPath'])

    # if any issues with upload
    if not extupldresp or not arcupldresp:
        return False
    # if all goes well
    return True


def s3tos3_same_srvr(s3concls, rptsection):
    # validating s3 connections and config entries
    s3tos3conresp = log.Aimtgenvalid.aimtpklstcntchk(valid_s3_conn, s3concls, 'S3 connection for file upload')
    # validating s3 configuration for file upload
    s3confresp = log.Aimtgenvalid.aimtpklstcntchk(valid_s3_samesrvr, rptsection, 'S3 config for file upload')
    if not s3confresp or not s3tos3conresp:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 's3 config details insufficient...')
        return False
    # creating s3 connection
    s3ops = aimts3ops.Aimts3ops(s3concls['aimtProfile'], s3concls['aimtAWSService'])

    # get list of objects from src path
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Getting files to copy..')
    srcfl_list = s3ops.lists3objects(rptsection['s3srcbkt'], rptsection['s3srcpath'] + rptsection['s3srcFilePrfx'])
    if len(srcfl_list) == 0:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Source S3 path has no files..')
        return False

    # remove previous files with same prefix
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Checking to remove files from previous run...')
    rmv_lst = s3ops.lists3objects(rptsection['s3tgtbkt'], rptsection['s3tgtpath'] + rptsection['s3srcFilePrfx'])
    s3copyrmvresp = True
    if len(rmv_lst) == 0:
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'No files to remove...')
    else:
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Files to remove : ' + rmv_lst.__str__())
        s3copyrmvresp = s3ops.removefiles3(rptsection['s3tgtbkt'], rmv_lst)
    if not s3copyrmvresp:
        return False

    # Copying files from src to tgt path
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Copying files..' + srcfl_list.__str__())
    s3copy_rsp = s3ops.copys3tos3(rptsection['s3srcbkt'], rptsection['s3tgtbkt'], srcfl_list, rptsection['s3tgtpath'])
    if not s3copy_rsp:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 'S3 copy failed..')
        return False
    # if all goes well
    return True


def reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr):
    # Closing log file
    sys.stdout.close()
    sys.stderr.close()
    # reinstating stdout and stderr
    sys.stdout = extlogsys_stdout
    sys.stderr = extlogsys_stderr


# defining main function
def extops_main(rptsection, concnfgcls, setuppath, logpath, extpath):
    """This module is the main function in extract operations:
    This module will read through the main configuration section and perform actions outlined in :
    extActions : All extract actions: database export, etc
    outBoundActions: All outbound actions: file sharing in s3, email with attachments, etc.
    Each section in this module is commented
    """
    # config section validation
    resp = log.Aimtgenvalid.aimtpklstcntchk(valid_ext_config, rptsection, 'Data extract section')
    if not resp:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Issue with data extraction config section..')
        return False
    extlogfile = rptsection['reportName'].replace(' ', '_') + '_' + rptlogdate.getdatefilename() + '.log'
    extlogfile = os.path.join(logpath, extlogfile)
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Log File Name:' + extlogfile)

    # back up of stdout and stderr
    extlogsys_stdout = sys.stdout
    extlogsys_stderr = sys.stderr
    # redirecting stdout and stderr to log file
    sys.stdout = open(extlogfile, 'w')
    sys.stderr = sys.stdout

    # extract ops begin
    log.Aimtlog.aimtprintbeginlog('scriptbegin', 'Begin: ' + rptsection['reportName'], curr_script_name)
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Checking extract actions and outbound actions..')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Extract Actions: ' + rptsection['extActions'])
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Extract Actions: ' + rptsection['outBoundActions'])
    if 'dbexport' in rptsection['extActions']:
        # preparing for extracts
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Preparing for Extract...')
        resp = extract_prep(concnfgcls, rptsection, concnfgcls.avlopfrmt, setuppath, extpath)
        if not resp:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Preparation for extract failed for ' + rptsection['reportName'])
            # transferring log back to master log
            reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
            return False
    else:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Extract actions other than database export is not configured..')
        # transferring log back to master log
        reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
        return False

    # **************************
    # Starting outbound file share
    # **************************
    log.Aimtlog.aimtprintbeginlog('blocklog', 'Outbound Actions...', curr_script_name)
    obactions = ast.literal_eval(rptsection['outBoundActions'])

    # listing all outbound actions for s3
    fileshare = [ele for ele in obactions if 's3' in ele]
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Requested Actions: ' + fileshare.__str__())

    # *** Checking and executing S3 upload from local ***
    for action in fileshare:
        s3_actions = True
        if action == 's3upload':
            print('\n')
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Executing "***{}***"'.format(action))
            s3_actions = s3_upload(concnfgcls.aimtS3, rptsection)
            if not s3_actions:
                # transferring log back to master log
                reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
                return False
        elif action == 's3tos3samesrvr':
            print('\n')
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Executing "***{}***"'.format(action))
            s3_actions = s3tos3_same_srvr(concnfgcls.aimtS3, rptsection)
            if not s3_actions:
                # transferring log back to master log
                reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
                return False

    # ************
    # email module
    # ************
    eops = aimtemailops.Aimtemailops()
    log.Aimtlog.aimtprintbeginlog('blocklog', 'Email Actions...', curr_script_name)
    # listing all outbound actions for s3
    emailoptions = [ele for ele in obactions if 'email' in ele]
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Requested Actions: ' + emailoptions.__str__())
    for emailaction in emailoptions:
        emailresp = True
        if emailaction == 'emailattach':
            print('\n')
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Emailoption "***{}***"'.format(emailaction))
            emailresp = eops.sendmail(concnfgcls, rptsection, opfilenames, setuppath)
            if not emailresp:
                # transferring log back to master log
                reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
                return False
        elif emailaction == 'emailinfo':
            print('\n')
            dummyattach = []
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Emailoption "***{}***"'.format(emailaction))
            emailresp = eops.sendmail(concnfgcls, rptsection, dummyattach, setuppath)
            if not emailresp:
                # transferring log back to master log
                reinstate_stdout_err(extlogsys_stdout, extlogsys_stderr)
                return False

    # if all ends well
    print('\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Script call success..')
    log.Aimtlog.aimtprintbeginlog('scriptbegin', 'End script', curr_script_name)
    # Closing log file
    sys.stdout.close()
    sys.stderr.close()
    # reinstating stdout and stderr
    sys.stdout = extlogsys_stdout
    sys.stderr = extlogsys_stderr

    return True


if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')
