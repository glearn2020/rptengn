# ************************************************
# Name   : aimtreportmaster.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script created for report generation and distribution purposes
# Usage  : python aimtreportmaster.py --connconfig
# ***********************************************

import argparse
import aimtgenlib as log
import os
import sys
import ast
import aimtuserreports as aimtur
import aimtfileops as flops
import aimtemailops as chkemlconn
import aimtemailops

# global vars
curr_script_name = os.path.basename(__file__)
masterlogdate = log.Aimtdate()
valid_master_section = ['reportName', 'emailfromAddress', 'emailtoAddress', 'emailCcAddress', 'emailSubject',
                        'emailBody', 'masterLogpath', 'configPath', 'dailyConfigFile', 'dailyRptList',
                        'weeklyConfigFile', 'weeklyRptList', 'monthlyConfigFile', 'monthlyRptList']

# get options from command line
getArgs = argparse.ArgumentParser()
getArgs.add_argument('-c', '--config', required=True, dest="configPath", type=str, help='Mandatory Argument. '
                                                                                        'Provide full path of the file'
                                                                                        'Script '
                                                                                        'needs config file with db '
                                                                                        'details; report master; etc')
getArgs.add_argument('-e', '--emproster', required=False, dest="manualRoster", type=bool, help='Optional argument. '
                                                                                               'Expected values: True or '
                                                                                               'False. When set to True, '
                                                                                               'manual roster reports '
                                                                                               'will be sent to consumers')
getArgs.add_argument('-r', '--restart', required=False, dest="restartFlag", type=bool,
                     help='Flag to indicate restart of any particular section')
getArgs.add_argument('-rl', '--restartList', required=False, dest='restartList', type=str, help='Required if restart flag is TRUE. '
                                                                                                'Expects dictionary of key value pairs in a file, '
                                                                                                'with frequency (daily/weekly/monthly) as key and '
                                                                                                'list of section that needs to be restarted'
                                                                                                'to run as values')

# parsing config file path
parseArgs = getArgs.parse_args()
# Check if configfile provided exists
print('\n')
resp = flops.Aimtfilevalid.fileorpathexists(parseArgs.configPath, 'file')
if not resp:
    log.Aimtlog.aimtprinterrorlog(curr_script_name, "Config file provided does not exist")
    log.Aimtlog.aimtprinterrorlog(curr_script_name, "Provide full path")
    exit(101)


# ***
# Defining functions
# ***
def scheduled_programs():
    thisday = log.Aimtdate()
    dayofwk = thisday.getdayofweek()
    qtrstart = thisday.getqtrstart()

    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Running scheduled reports...\n')
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Calling user reports module...')
    # executing daily scripts
    if dayofwk in thisday.businessdays:
        log.Aimtlog.aimtprintbeginlog('blocklog', "Daily Reports...")
        dlyconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['dailyConfigFile'])
        aimtur.usrrpt_main(dlyconfg_fl_full_path, config, reportdist['dailyRptList'])
    else:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, ' # *** Not Business day *** #')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Skipping Daily reports...')

    # executing Monday weekly scripts
    if dayofwk == 'Monday':
        log.Aimtlog.aimtprintbeginlog('blocklog', "Monday Weekly Reports...")
        wklyconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['monWeeklyConfigFile'])
        aimtur.usrrpt_main(wklyconfg_fl_full_path, config, reportdist['monWeeklyRptList'])
    else:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, ' # *** Not Monday *** #')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Skipping Monday Weekly reports...')

    # executing Tuesday weekly scripts
    if dayofwk == 'Tuesday':
        log.Aimtlog.aimtprintbeginlog('blocklog', "Weekly Reports...")
        wklyconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['weeklyConfigFile'])
        aimtur.usrrpt_main(wklyconfg_fl_full_path, config, reportdist['weeklyRptList'])
    else:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, ' # *** Not Tuesday *** #')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Skipping Weekly reports...')

    # executing monthly scripts
    if thisday.currentday.day == 1:
        log.Aimtlog.aimtprintbeginlog('blocklog', "Monthly Reports...")
        mnthconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['monthlyConfigFile'])
        aimtur.usrrpt_main(mnthconfg_fl_full_path, config, reportdist['monthlyRptList'])
    else:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, ' # *** Not First of Month *** #')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Skipping Monthly reports...')

    # executing quarterly scripts
    if thisday.currentday == qtrstart:
        log.Aimtlog.aimtprintbeginlog('blocklog', "Quarterly Reports...")
        qtrconfig_fl_full_path = os.path.join(reportdist['configPath'], reportdist['qtrlyConfigFile'])
        aimtur.usrrpt_main(qtrconfig_fl_full_path, config, reportdist['qtrlyRptList'])
    else:
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, ' # *** Not First of Quarter *** #')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Skipping Quarterly reports...')


# ***
# End defining functions
# ***

# ***
# Main Script
# ***


# reading config file
log.Aimtlog.aimtprintinfolog(curr_script_name, "Reading config file...")
config = log.Aimtreadconnconfig(parseArgs.configPath).readconnconfigfile()
reportdist = config.autoReportsMaster
rosterexecuteflag = parseArgs.manualRoster

# Checking if master section is valid
resp = log.Aimtgenvalid.aimtpklstcntchk(valid_master_section, reportdist, 'Auto reports master section ')
if not resp:
    log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Master section does not have valid items..')
    log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Exiting script..')
    exit(101)

# Backing up stdout and stderr
mastersys_stdout = sys.stdout
mastersys_stderr = sys.stderr

# preparing logfile name and redirecting to stderr and stdout
masterlogfilename = reportdist['reportName'].replace(' ', '_') + '_' + masterlogdate.getdatefilename() + '.log'
masterlogfilename = os.path.join(reportdist['masterLogpath'], masterlogfilename)
sys.stdout = open(masterlogfilename, 'w')
sys.stderr = sys.stdout

# begin
log.Aimtlog.aimtprintbeginlog('scriptbegin', "Begin: ## Auto Reports Master ##", curr_script_name)

# validating other config files
log.Aimtlog.aimtprintinfolog(curr_script_name, 'Checking if config file exists..')
daily_resp = flops.Aimtfilevalid.fileorpathexists(os.path.join(reportdist['configPath'], reportdist['dailyConfigFile']), 'file')
weekly_resp = flops.Aimtfilevalid.fileorpathexists(os.path.join(reportdist['configPath'], reportdist['weeklyConfigFile']), 'file')
monthly_resp = flops.Aimtfilevalid.fileorpathexists(os.path.join(reportdist['configPath'], reportdist['monthlyConfigFile']), 'file')
if not daily_resp or not weekly_resp or not monthly_resp:
    log.Aimtlog.aimtprinterrorlog(curr_script_name, "Config files provided does not exist..")
    log.Aimtlog.aimtprinterrorlog(curr_script_name, "Check daily or weekly or monthly config files..")
    exit(101)

# getting cred for email
log.Aimtlog.aimtprintinfolog(curr_script_name, 'Getting cred for email..')
log.initpass()

# validating email credentials before proceeding with execution
emlrsp = chkemlconn.checkconn(config.smtpDetails)
if not emlrsp:
    log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Incorrect Email credentials provided... ')
    log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Exiting script execution...')
    # Closing stdout and stderr
    sys.stdout.close()
    sys.stderr.close()
    # reinstating stdout and stderr
    sys.stdout = mastersys_stdout
    sys.stderr = mastersys_stderr
    exit(101)

# Checking for restart vs scheduled programs
if parseArgs.restartFlag:
    flops.Aimtfilevalid.fileorpathexists(parseArgs.restartList, 'file')
    restartdict = flops.Aimtfileops.readfile(parseArgs.restartList)
    restartdict = ast.literal_eval(restartdict)
    print(restartdict)
    print(type(restartdict))
    if not restartdict or not type(restartdict) is dict:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, "restartList parameter should be a non empty dictionary. Stopping script...")
        exit(101)
    else:
        for key in restartdict:
            if key == 'daily':
                dailyrestartlist = restartdict[key]
                log.Aimtlog.aimtprintbeginlog('blocklog', 'Restarting {} : Sections {} ..'.format(key, dailyrestartlist.__str__()), curr_script_name)
                dlyconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['dailyConfigFile'])
                aimtur.usrrpt_main(dlyconfg_fl_full_path, config, dailyrestartlist)
            elif key == 'weekly':
                wklyrestartlist = restartdict[key]
                log.Aimtlog.aimtprintbeginlog('blocklog', 'Restarting {} : Sections {} ..'.format(key, wklyrestartlist.__str__()), curr_script_name)
                wkconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['weeklyConfigFile'])
                aimtur.usrrpt_main(wkconfg_fl_full_path, config, wklyrestartlist)
            elif key == 'monthly':
                monthlyrestartlist = restartdict[key]
                log.Aimtlog.aimtprintbeginlog('blocklog', 'Restarting {} : Sections {} ..'.format(key, monthlyrestartlist.__str__()),
                                              curr_script_name)
                mnthconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['monthlyConfigFile'])
                aimtur.usrrpt_main(mnthconfg_fl_full_path, config, monthlyrestartlist)
            elif key == 'quarterly':
                qtrlyrestartlist = restartdict[key]
                log.Aimtlog.aimtprintbeginlog('blocklog', 'Restarting {} : Sections {} ..'.format(key, qtrlyrestartlist.__str__()),
                                              curr_script_name)
                qtrconfg_fl_full_path = os.path.join(reportdist['configPath'], reportdist['qtrlyConfigFile'])
                aimtur.usrrpt_main(qtrconfg_fl_full_path, config, qtrlyrestartlist)
            elif key == 'roster':
                rosterresetartlist = restartdict[key]
                log.Aimtlog.aimtprintbeginlog('blocklog', 'Restarting {} : Sections {} ..'.format(key, rosterresetartlist.__str__()),
                                              curr_script_name)
                rstrconfig_fl_full_path = os.path.join(reportdist['configPath'], reportdist['rosterConfigFile'])
                aimtur.usrrpt_main(rstrconfig_fl_full_path, config, rosterresetartlist)
            else:
                log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Dictionary key provided "{}" is not recognized...'.format(key))
                log.Aimtlog.aimtprinterrorlog(curr_script_name, 'exiting script...')
                exit(101)
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Restart Complete. Back to master...')
        log.Aimtlog.aimtprintinfolog(curr_script_name, 'Closing Master log files for restart...')
        # end
        log.Aimtlog.aimtprintbeginlog('scriptbegin', "End: ## Auto Reports Master ##", curr_script_name)
        # Closing stdout and stderr
        sys.stdout.close()
        sys.stderr.close()
        # reinstating stdout and stderr
        sys.stdout = mastersys_stdout
        sys.stderr = mastersys_stderr
elif parseArgs.manualRoster:
    log.Aimtlog.aimtprintbeginlog('blocklog', "Roster Reports...")
    qtrconfig_fl_full_path = os.path.join(reportdist['configPath'], reportdist['rosterConfigFile'])
    aimtur.usrrpt_main(qtrconfig_fl_full_path, config, reportdist['rosterRptList'])
else:
    scheduled_programs()

print('\n')
log.Aimtlog.aimtprintinfolog(curr_script_name, 'Back to master...')
log.Aimtlog.aimtprintinfolog(curr_script_name, 'Closing Master log files...')
log.Aimtlog.aimtprintinfolog(curr_script_name, 'Testing one more line...')
# end
log.Aimtlog.aimtprintbeginlog('scriptbegin', "End: ## Auto Reports Master ##", curr_script_name)
# Closing stdout and stderr
sys.stdout.close()
sys.stderr.close()
# reinstating stdout and stderr
sys.stdout = mastersys_stdout
sys.stderr = mastersys_stderr

# Emailing the master log to internal users
masteremail = aimtemailops.Aimtemailops()
emailbodypath = os.path.split(reportdist['masterLogpath'])
masterlogfiledict = {'1': masterlogfilename}
masteremail.sendmail(config, reportdist, masterlogfiledict, emailbodypath[0])
