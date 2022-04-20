# ************************************************
# Name   : aimtuserreports.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define modules for extracting and distributing user reports
# Usage  : import aimtuserreports
# ***********************************************

import aimtgenlib as log
import aimtextops
import aimtfileops as fl
import os

if __name__ == '__main__':
    print('''
# ***********************************************
# Script executed from console. 
# Script will begin only after validating mandatory parameters
# ***********************************************
''')

curr_script_name = os.path.basename(__file__)
valid_config_global = ['setupPath', 'extPath', 'logPath']
logdate = log.Aimtdate()


def usrrpt_main(configfile, connconfigcls, rptlist=None):
    # Checking for two arguments
    if not configfile or not connconfigcls:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, "Method call needs two mandatory inputs. Config file (Daily/Weekly/Monthly) "
                                                        "and connection config class")
        return False
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Reading config file :' + configfile)
    usrrpt_config = log.Aimtreadconfig(configfile)
    log.Aimtlog.aimtprintinfolog(curr_script_name, "##******* Validating inputs... *******##\n")
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Checking config section global...')

    # reading global section
    ur_cnfg_global = usrrpt_config.readconfigsection('Global')

    # Checking global section if all required items are present
    glbresp = log.Aimtgenvalid.aimtpklstcntchk(valid_config_global, ur_cnfg_global, 'Global section valid')
    if not glbresp:
        return False

    # Validating paths
    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Validating setup path, ext path, log path...')
    stuprsp = fl.Aimtfilevalid.fileorpathexists(ur_cnfg_global['setupPath'], 'path')
    extrsp = fl.Aimtfilevalid.fileorpathexists(ur_cnfg_global['extPath'], 'path')
    logrsp = fl.Aimtfilevalid.fileorpathexists(ur_cnfg_global['logPath'], 'path')
    if not stuprsp or not extrsp or not logrsp:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, "One or more paths does not exist. Script cannot proceed.. Exiting..")
        return False

    # reading full section list from config file and removing global section
    usrrpt_allsec = usrrpt_config.fullsections
    usrrpt_allsec.remove('Global')

    # Preparing log path and extract path by creating current day's directory
    globallogpath = fl.Aimtfileops.createdir(ur_cnfg_global['logPath'], logdate.getdatedirname())
    globalextpath = fl.Aimtfileops.createdir(ur_cnfg_global['extPath'], logdate.getdatedirname())

    # looping through sections in config file
    for sec in usrrpt_config.fullsections:
        if sec in rptlist:
            print('\n')
            log.Aimtlog.aimtprintinfolog(curr_script_name, '#****Running {} section.. ****#'.format(sec))
            resp = aimtextops.extops_main(usrrpt_config.readconfigsection(sec), connconfigcls, ur_cnfg_global['setupPath'], globallogpath,
                                          globalextpath)
            # resp = True
            if not resp:
                log.Aimtlog.aimtprinterrorlog(curr_script_name, '# **** Section {} failed... **** #\n'.format(sec))
            else:
                log.Aimtlog.aimtprintinfolog(curr_script_name, '# **** Section {} success... **** #\n'.format(sec))

    return True
