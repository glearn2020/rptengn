# ************************************************
# Name   : aimts3ops.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define classes for all s3 operations
# Usage  : import aimts3ops
# ***********************************************

import boto3
import os
import aimtgenlib as log

# global constants
curr_script_name = os.path.basename(__file__)

if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')


class Aimts3ops:
    # instantiating object state
    def __init__(self, prflname, srvcname):
        self.prflname = prflname
        self.srvcname = srvcname
        self.s3session = boto3.session.Session(profile_name=self.prflname)
        self.s3client = self.s3session.client(service_name=self.srvcname)
        self.tests3conn()

    def tests3conn(self):
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Checking S3 connection..")
        bucket_list = self.s3client.list_buckets()
        for nbkt in bucket_list['Buckets']:
            log.Aimtlog.aimtprintinfolog(curr_script_name, str(nbkt))
        return True

    def lists3objects(self, buktname, prefix):
        s3list = self.s3client.list_objects_v2(Bucket=buktname, Prefix=prefix)
        listfile = []
        for objt in s3list['Contents']:
            listfile.append(objt["Key"])
        return listfile

    def upldtos3fromlocal(self, buktname, uploadlist, uploadpath):
        """copytos3fromlocal:
        Class method. Requires three params:
            buktname = Name of the bucket
            uploadlist = dictionary of files to upload with full path
            uploadpath = full S3 path to upload the files with ending '/'
        """
        try:
            for dictitem in uploadlist:
                file = uploadlist[dictitem]
                basefile = os.path.basename(file)
                log.Aimtlog.aimtprintinfolog(curr_script_name, "uploading " + uploadpath + basefile + "...")
                self.s3client.upload_file(file, buktname, uploadpath + basefile)
        except Exception as exptn:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, "Upload file failed..")
            log.Aimtlog.aimtprinterrorlog(curr_script_name, exptn.__str__())
            return False
        # if all goes well
        return True

    def copys3tos3(self, srcbukt, tgtbukt, srcfilelist, tgtpath):
        """copys3tos3:
        Class method. Requires three params:
            srcbukt = Name of the source bucket;
            tgtbukt = Name of the target bucket;
            srcfilelist = list of files from source bucket with full path;
            tgtpath = Target S3 path with ending; '/'
        """
        try:
            for objs in srcfilelist:
                log.Aimtlog.aimtprintinfolog(curr_script_name, "Copying {} to {} ...".format(objs, tgtpath))
                basefile = os.path.basename(objs)
                self.s3client.copy_object(CopySource=srcbukt + '/' + objs, Bucket=tgtbukt, Key=tgtpath + basefile)
        except Exception as exptn:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, "S3 internal file copy failed..")
            log.Aimtlog.aimtprinterrorlog(curr_script_name, exptn.__str__())
            return False
        # if all goes well
        return True

    def removefiles3(self, buktname, rmvlist):
        """removefiles3:
        Class method. Requires three params:
            buktname = Name of the bucket
            rmvlist = list of files to remove with full path
        """
        try:
            for objs in rmvlist:
                log.Aimtlog.aimtprintinfolog(curr_script_name, "removing " + objs + "...")
                self.s3client.delete_object(Bucket=buktname, Key=objs)
        except Exception as exptn:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, "Remove file failed..")
            log.Aimtlog.aimtprinterrorlog(curr_script_name, exptn.__str__())
            return False
        # if all goes well
        return True
