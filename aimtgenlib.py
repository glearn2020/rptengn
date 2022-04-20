# ************************************************
# Name   : aimtgenlib.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define class for logging purposes
# Usage  : import aimtgenlib
# ***********************************************

from datetime import datetime
import configparser
import os
# import getpass
import keyring as kr

# Global constants
valid_logbegin = ['scriptbegin', 'blocklog']
valid_connconfig = ['dbRdshft', 'aimtAnltProd', 'smtpDetails', 'aimtS3', 'aimtopflfrmt']
valid_tfchk = [True, False]
valid_processing = ['dbinput', 's3input']
valid_outbound = ['emailattach', 'emails3noattach', 'emails3attach']
curr_script_name = os.path.basename(__file__)


if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')


def initpass():
    global emailusnm
    global emailpass
    # emailcred = getpass.getpass()
    emailcred = kr.get_password('aimtrpt', 'aimtrptsnd')
    emailcred = emailcred.split("||")
    emailusnm = emailcred[0]
    emailpass = emailcred[1]

# ***********************************************
# defining Class for getting date information
# This class does not have any state. All methods are static methods
# Name of each method is self evident of the purpose
# ***********************************************
class Aimtdate:
    """Aimtdate: Class created for common date operations.
    getdate: Get current date,
    getdatefilename: Get current date in format used for file name
    getdatetime: Get current date time """

    # instantiating object state
    def __init__(self):
        self.weekdays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
        self.weekends = ("Saturday", "Sunday")
        self.businessdays = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")
        self.currentday = datetime.today()

    def getdate(self):
        return self.currentday.strftime('%m/%d/%Y')

    def getdatedirname(self):
        return self.currentday.strftime('%Y%m%d')

    def getdatefilename(self):
        return self.currentday.strftime('%Y%m%d%H%M%S')

    @staticmethod
    def getdatetime():
        return datetime.today().strftime('%m/%d/%Y %H:%M:%S')

    def getdayofweek(self):
        return self.weekdays[self.currentday.weekday()]

    def getdayofmonth(self):
        return self.currentday.day

    def getqtrstart(self):
        return datetime(self.currentday.year, 3 * ((self.currentday.month - 1) // 3) + 1, 1)


# ***********************************************
# defining Class for logging
# This class does not have any state. All methods are static methods
# Name of each method is self evident of the purpose
# ***********************************************
class Aimtlog:
    """Aimtlog: Class created for common log operations.
    aimtprintinfolog: To print information in log,
    aimtprintwarnlog: To print warnings in log,
    aimtprinterrorlog: To print errors in log,
    aimtprintbeginlog: To print banner style message to segregate log information
    """

    # instantiating object state
    def __init__(self):
        pass

    # defining static method for printing information messages
    @staticmethod
    def aimtprintinfolog(this_fl_nm, message):
        print(': '.join([this_fl_nm, Aimtdate.getdatetime(), 'INFO', message]))

    # defining static method for printing warning messages
    @staticmethod
    def aimtprintwarnlog(this_fl_nm, message):
        print(': '.join([this_fl_nm, Aimtdate.getdatetime(), '%warning%', message]))

    # defining static method for printing error messages
    @staticmethod
    def aimtprinterrorlog(this_fl_nm, message):
        print('\n')
        print(': '.join(['**ERROR**', this_fl_nm, Aimtdate.getdatetime(), message]))
        # print('\n')
        # print(': '.join(['Traceback: ', this_fl_nm, Aimtdate.getdatetime(), str(traceback.print_stack())]))

    # defining static method for printing begin messages
    @staticmethod
    def aimtprintbeginlog(begintype, message, this_fl_nm='default'):
        # validating begintype
        Aimtgenvalid.aimtpklstchk(valid_logbegin, begintype, 'aimtprintbeginlog')
        if begintype not in valid_logbegin:
            Aimtlog.aimtprintwarnlog(curr_script_name, "Invalid Value for argument 'begintype'. Skipping print begin")
            Aimtlog.aimtprintwarnlog(curr_script_name, 'Expected Values: ' + valid_logbegin.__str__())
        else:
            if begintype == 'scriptbegin':
                print('#********************************************************#')
                print('# ' + message)
                print('# Date: ' + Aimtdate.getdatetime())
                print('# Script Name: ' + this_fl_nm)
                print('#********************************************************#')
                print('\n')
            elif begintype == 'blocklog':
                print('\n')
                print('#\t**********************')
                print('#\t' + message)
                print('#\t**********************')
                print('\n')
            else:
                print('\n')
                print('###@@@@$$$$%%% Whoops.. Wrong turn... :/ %%%$$$@@@####')
                print('\n')


# ***********************************************
# defining Class for generic validation methods
# ***********************************************
class Aimtgenvalid:
    """Aimtgenvalid: Class created for common log operations.
    aimtfileexists: To check if a file exists,
    aimttfcheck: To check if value is true or false,
    aimtpklstchk: To check if an item is part of list,
    """

    # instantiating object state
    def __init__(self):
        pass

    '''# defining static method to check if a file or path exists
    @staticmethod
    def aimtfileexists(chk_file):
        if not os.path.exists(chk_file):
            Aimtlog.aimtprinterrorlog(curr_script_name, 'File or Path does NOT exist: ' + chk_file)
            return False
        else:
            Aimtlog.aimtprintinfolog(curr_script_name, 'File or Path exist: ' + chk_file)
            return True'''

    @staticmethod
    def aimttfcheck(chkval, purpose, message, alert_state=""):
        if chkval not in valid_tfchk:
            Aimtlog.aimtprinterrorlog(curr_script_name,
                                      'aimttfcheck function requires True or False in chkval parameter')
        else:
            if chkval:
                Aimtlog.aimtprintinfolog(curr_script_name, message + ' check passed for ' + purpose)
                return True
            elif chkval is False and alert_state == 'FATAL':
                Aimtlog.aimtprinterrorlog(curr_script_name, message + ' check FAILED for ' + purpose)
                Aimtlog.aimtprinterrorlog(curr_script_name, 'FATAL ERROR')
                exit(101)
            else:
                Aimtlog.aimtprinterrorlog(curr_script_name, message + ' check FAILED for ' + purpose)
                return False

    @staticmethod
    def aimtpklstchk(pklst: list, value_to_check, fncall, typ='def'):
        if value_to_check not in pklst:
            Aimtlog.aimtprinterrorlog(curr_script_name, "Invalid Value supplied for function call:" + fncall)
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Expected Values: ' + pklst.__str__())
            return False

    @staticmethod
    def aimtpklstcntchk(pklst: list, input_lst: list, message):
        try:
            itmcnt = 0
            for itm in input_lst:
                if itm in pklst:
                    itmcnt += 1
            if itmcnt != len(pklst):
                raise Exception
            else:
                Aimtlog.aimtprintinfolog(curr_script_name, message + ' has all values...')
                return True
        except Exception as e:
            Aimtlog.aimtprinterrorlog(curr_script_name, message + ' does not have required items')
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Required items: ' + pklst.__str__())
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Stopping script....' + e.__str__())
            return False


# ***********************************************
# defining Class for reading generic config files and returning connection values
# ***********************************************
class Aimtconn:
    """ AIMTCONN: Class to store common params.
    dbRdshft: Connection details for Amazon redshift db,
    smtpDetails: SMTP Server details for email,
    aimtS3: S3 connection details,
    avlopfrmt: List of availabe output format
    autoReportsMaster: Master config section for executing automated reports
    """

    def __init__(self):
        self.dbRdshft = {}
        self.aimtAnltProd = {}
        self.smtpDetails = {}
        self.aimtS3 = {}
        self.autoReportsMaster = {}
        self.avlopfrmt = {}


class Aimtreadconnconfig:
    """
    Aimtreadconnconfig: class created to read connection configuration file
    """

    # instantiating object state with config file
    def __init__(self, cnfgflnm):
        self.conconfig = configparser.ConfigParser()
        self.conconfig.optionxform = str
        self.conncnfgflnm = cnfgflnm
        self.conconfig.read(self.conncnfgflnm)
        self.conncfsections = self.conconfig.sections()

    # defining method to read generic connection config file
    def readconnconfigfile(self):
        """
        readconnconfigfile: module in class Aimtreadconnconfig created to read connection configuration file
        """
        # Validating the sections in connection configuration file
        try:
            valcnt = 0
            for chk in self.conncfsections:
                if chk in valid_connconfig:
                    valcnt += 1
            if valcnt != len(valid_connconfig):
                raise Exception
        except Exception as e:
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Connection configuration file does not have required sections')
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Required sections: ' + valid_connconfig.__str__())
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Stopping script....' + e.__str__())
            return False
        # Reading and returning configuration sections
        for connconfsec in self.conncfsections:
            if connconfsec == 'dbRdshft':
                Aimtconn.dbRdshft = dict(self.conconfig.items(connconfsec))
            elif connconfsec == 'aimtAnltProd':
                Aimtconn.aimtAnltProd = dict(self.conconfig.items(connconfsec))
            elif connconfsec == 'smtpDetails':
                Aimtconn.smtpDetails = dict(self.conconfig.items(connconfsec))
            elif connconfsec == 'aimtS3':
                Aimtconn.aimtS3 = dict(self.conconfig.items(connconfsec))
            elif connconfsec == 'aimtopflfrmt':
                Aimtconn.avlopfrmt = dict(self.conconfig.items(connconfsec))
            elif connconfsec == 'autoReportsMaster':
                Aimtconn.autoReportsMaster = dict(self.conconfig.items(connconfsec))
            else:
                Aimtlog.aimtprintwarnlog(curr_script_name,
                                         'New section in config detected. Update readconnconfigfile method. Section: '
                                         '' + connconfsec)
        return Aimtconn


# ***********************************************
# defining Class for reading specific config files and returning config values
# ***********************************************
class Aimtreadconfig:
    """Aimtreadconfig: Class defined to read configuration files
    fullsections : will contain the list of all sections in the config file,
    readconfigsection : this method can be called to return config items of a specific section"""

    # instantiating object state with config file
    def __init__(self, cnfgflnm):
        self.config = configparser.ConfigParser()
        self.config.optionxform = str
        self.cnfgflnm = cnfgflnm
        self.config.read(self.cnfgflnm)
        self.fullsections = self.config.sections()
        self.retsec = {}

    # defining method to read specific section in a config file. Section provided by user
    def readconfigsection(self, cnfsec):
        if Aimtgenvalid.aimttfcheck(self.config.has_section(cnfsec), 'config section: ' + cnfsec, 'EXISTS'):
            self.retsec = dict(self.config.items(cnfsec))
            return self.retsec
        else:
            Aimtlog.aimtprinterrorlog(curr_script_name, 'Section {} does not exist. Config file read Failed'.format(cnfsec))
            return False
