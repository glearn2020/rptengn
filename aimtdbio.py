# ************************************************
# Name   : aimtdbio.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define classes for database in and out operations
# Usage  : import aimtdbio
# ***********************************************

import psycopg2
import pandas as pd
import aimtgenlib as log
import os

# global constants
curr_script_name = os.path.basename(__file__)

if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')


# ***********************************************
# defining Class for database operations
# This class does not have any state. All methods are static methods
# Name of each method is self evident of the purpose
# ***********************************************
class Aimtdbops:
    # instantiating object state
    def __init__(self):
        pass

    @staticmethod
    def writedftofile(datafrm, filenm, separator, indexcol=False):
        if separator == 'excel':
            options = {'strings_to_formulas': False, 'strings_to_urls': False}
            writer = pd.ExcelWriter(filenm, engine='xlsxwriter', options=options)
            datafrm.to_excel(writer, sheet_name='Sheet1', index=False)
            writer.save()
            writer.close()
        else:
            datafrm.to_csv(filenm, sep=separator, index=indexcol)
        return True

    @staticmethod
    def rdshftget_sql(dbconndict, sql):
        if type(dbconndict) is dict:
            try:
                dbcon = psycopg2.connect(dbname=dbconndict['dbname'], host=dbconndict['host'], port=dbconndict['port'],
                                         user=dbconndict['user'], password=dbconndict['pwd'])
                db_results = pd.DataFrame()
                if 'call' in sql:
                    cur = dbcon.cursor()
                    cur.execute(sql)
                    dbcon.commit()
                    log.Aimtlog.aimtprintinfolog(curr_script_name, 'Procedure executed..')
                    return db_results
                else:
                    db_results = pd.read_sql_query(sql, dbcon)
                    log.Aimtlog.aimtprintinfolog(curr_script_name, 'SQL executed..')
                dbcon.close()
                return db_results
            except Exception as dbconnexep:
                log.Aimtlog.aimtprinterrorlog(curr_script_name, 'FAILED: DB Connection and data extraction...')
                log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Exception: ' + dbconnexep.__str__())
                return False
        else:
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Method expects a dictionary with db connection details')
            log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Failed: rdshftget_sql')
            return False
