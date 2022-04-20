# ************************************************
# Name   : aimtfileops.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define classes for all file operations
# Usage  : import aimtfileops
# ***********************************************

import os
import shutil
import aimtgenlib as log
import re

# global constants
valid_flchktypes = ['path', 'file']
valid_flpattype = ['date']
curr_script_name = os.path.basename(__file__)

if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')

fllogdate = log.Aimtdate()


# ***********************************************
# defining Class for file or path validation
# This class does not have any state. All methods are static methods
# Name of each method is self evident of the purpose
# ***********************************************
class Aimtfilevalid:
    # instantiating object state
    def __init__(self):
        pass

    @staticmethod
    def crtfileptrn(fileprfx, fileextn, pattype='date'):
        if pattype == 'date':
            return '^(' + fileprfx + ')(\\d{8})(\\' + fileextn + ')'
        else:
            return None

    @staticmethod
    def crtopfilenm(filepath, fileprfx, fileextn):
        return os.path.join(filepath, fileprfx + fllogdate.getdatefilename() + fileextn)

    @staticmethod
    def fileorpathexists(chkfilepath, inputtype):
        # Validating inputtype
        log.Aimtgenvalid.aimtpklstchk(valid_flchktypes, inputtype, 'fileorpathexists')
        # Check if path exists
        if inputtype == 'path':
            if os.path.exists(chkfilepath):
                log.Aimtlog.aimtprintinfolog(curr_script_name, 'The path exists mancha... ' + chkfilepath)
                return True
            else:
                log.Aimtlog.aimtprintwarnlog(curr_script_name, 'path does NOT exist: ' + chkfilepath)
                return False
        elif inputtype == 'file':
            if os.path.exists(chkfilepath) and os.path.getsize(chkfilepath) > 0:
                log.Aimtlog.aimtprintinfolog(curr_script_name, 'file exists and is not empty, just like a good chouta ' + chkfilepath)
                return True
            elif os.path.exists(chkfilepath) and os.path.getsize(chkfilepath) == 0:
                log.Aimtlog.aimtprintwarnlog(curr_script_name, 'Ha! the file exists. But is empty gancho: ' + chkfilepath)
                return False
            else:
                log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Ha! Hey, Chilinko, the file does NOT exist: ' + chkfilepath)
                return False
        else:
            print('\n')
            print('###@@@@$$$$%%% Yo Airsick lowlander... You need to let me know if you are checking file or path.. :/ :/ :/ %%%$$$@@@####')
            print('\n')


# ***********************************************
# defining Class for file operations. Listing, Archive, etc
# This class does not have any state. All methods are static methods
# Name of each method is self evident of the purpose
# ***********************************************
class Aimtfileops:
    def __init__(self):
        pass

    @staticmethod
    def filelistindir(dirpath, file_pat):
        retfilelst = []
        dirlist = os.listdir(dirpath)
        for files in dirlist:
            if re.match(file_pat, files):
                retfilelst.append(files)
        # Checking if retfilelst is empty and returning the list
        if not retfilelst:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Storms.. There are no file available in path: ' + dirpath + file_pat)
            return False
        else:
            return retfilelst

    @staticmethod
    def archivefiles(frompath, topath, file_pat):
        arcfilelist = Aimtfileops.filelistindir(frompath, file_pat)
        if not arcfilelist:
            log.Aimtlog.aimtprintinfolog(curr_script_name, 'Gon... There are no file to Archive')
            return True
        else:
            for file in arcfilelist:
                log.Aimtlog.aimtprintinfolog(curr_script_name, 'Archiving:' + file + '...')
                shutil.move(os.path.join(frompath, file), os.path.join(topath, file))
            return False

    @staticmethod
    def createdir(path, dirname):
        dirpath = os.path.join(path, dirname)
        if Aimtfilevalid.fileorpathexists(dirpath, 'path'):
            return dirpath
        os.mkdir(dirpath)
        log.Aimtlog.aimtprintinfolog(curr_script_name, '"The Lopen" created directory {} in path {}'.format(dirname, path))
        return dirpath

    @staticmethod
    def readfile(filename):
        with open(filename, 'r') as dat:
            filedat = dat.read()
            return filedat
