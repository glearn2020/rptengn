# ************************************************
# Name   : aimtemailops.py
# Date   : 02/08/2021
# Author : Goutam Krishnamoorthy
# Desc   : Script to define classes for database in and out operations
# Usage  : import aimtemailops
# ***********************************************

import os
import aimtgenlib as log
import aimtfileops as flops
import smtplib
import ssl
# import ast
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup as bs

# global constants
curr_script_name = os.path.basename(__file__)

if __name__ == '__main__':
    print('''
# ***********************************************
# This script should not be executed from console. 
# This script contains library functions for AIMT processes
# ***********************************************
''')


def checkconn(smtpdetails):
    context = ssl.create_default_context()
    server = smtplib.SMTP(smtpdetails['smtpServer'], smtpdetails['smtpPort'])
    try:
        server.starttls(context=context)  # Secure the connection
        if not server.login(log.emailusnm, log.emailpass):
            return False
        else:
            return True
    except Exception as e:
        log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Email credential provided is not valid..')
        log.Aimtlog.aimtprinterrorlog(curr_script_name, smtpdetails['comOpsSender'])
        log.Aimtlog.aimtprinterrorlog(curr_script_name, log.emailusnm)
        log.Aimtlog.aimtprinterrorlog(curr_script_name, e.__str__())
        return False
    finally:
        server.quit()


class Aimtemailops:
    def __init__(self):
        self.emailmessage = MIMEMultipart()

    def reademailbody(self, emailbodyfile):
        resp = flops.Aimtfilevalid.fileorpathexists(emailbodyfile, 'file')
        body = MIMEMultipart('alternative')
        if not resp:
            return False
        with open(emailbodyfile) as dat:
            emailbody = dat.read()
        # Checking if file is HTML
        if bool(bs(emailbody, "html.parser").find()):
            body.attach(MIMEText(emailbody, "html"))
        else:
            body.attach(MIMEText(emailbody, "plain"))
        self.emailmessage.attach(body)
        return True

    def handleattachments(self, opfilenames):
        if not opfilenames:
            log.Aimtlog.aimtprintwarnlog(curr_script_name, "No attachment available..")
        else:
            for attachfile in opfilenames:
                part = MIMEBase("application", "octet-stream")
                if not flops.Aimtfilevalid.fileorpathexists(opfilenames[attachfile], 'file'):
                    log.Aimtlog.aimtprinterrorlog(curr_script_name, 'Issues with attachment...')
                    return False
                with open(opfilenames[attachfile], "rb") as attachment:
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition",
                                f"attachment; filename=" + os.path.basename(opfilenames[attachfile]))
                self.emailmessage.attach(part)
        # if all goes well
        return True

    def sendmail(self, connconfig, mainconfigsec, opfilenames, setuppath):
        log.Aimtlog.aimtprintbeginlog("blocklog", "Send email block", curr_script_name)

        # getting email connection details
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Prep for sending email..")
        emailsmtp = connconfig.smtpDetails

        emaildate = log.Aimtdate()

        log.Aimtlog.aimtprintinfolog(curr_script_name, "Report Name: " + mainconfigsec['reportName'])

        # log.Aimtlog.aimtprintinfolog((curr_script_name, "From        : ") + emailsmtp['comOpsSender'])
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Recipients-TO: " + mainconfigsec['emailtoAddress'])
        emailto = list(mainconfigsec['emailtoAddress'].split(";"))
        # emailto = ast.literal_eval(mainconfigsec['emailtoAddress'])
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Recipients-CC: " + mainconfigsec['emailCcAddress'])
        emailcc = list(mainconfigsec['emailCcAddress'].split(";"))
        # emailcc = ast.literal_eval(mainconfigsec['emailCcAddress'])
        # allemail = ', '.join([emailto, emailcc])
        allemail = emailto + emailcc

        # setting email subject
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Setting email headers..")
        self.emailmessage["Subject"] = mainconfigsec['emailSubject'] + ' - ' + emaildate.getdate()
        self.emailmessage["From"] = mainconfigsec['emailfromAddress']
        self.emailmessage["To"] = ",".join(emailto)
        self.emailmessage["Cc"] = ",".join(emailcc)

        # read email body
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Read email body..")
        emailbodyfile = os.path.join(setuppath, mainconfigsec['emailBody'])
        self.reademailbody(emailbodyfile)

        # handle attachments
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Checking for attachments..")
        self.handleattachments(opfilenames)

        # Block to send email
        print('\n')
        log.Aimtlog.aimtprintinfolog(curr_script_name, "Initialize email..")
        try:
            context = ssl.create_default_context()
            server = smtplib.SMTP(emailsmtp['smtpServer'], emailsmtp['smtpPort'])
            # Debug option should there be a need
            # server.set_debuglevel(1)
            server.ehlo()  # Can be omitted
            server.starttls(context=context)  # Secure the connection
            server.ehlo()  # Can be omitted
            log.Aimtlog.aimtprintinfolog(curr_script_name, "Logging in..")
            server.login(log.emailusnm, log.emailpass)
            log.Aimtlog.aimtprintinfolog(curr_script_name, "sending email..")
            resp = server.sendmail(mainconfigsec['emailfromAddress'], allemail, self.emailmessage.as_string())
            # as sendmail provides blank response when successful, if condition below is designed accordingly
            if not bool(resp):
                log.Aimtlog.aimtprintinfolog(curr_script_name, "email sent..")
            else:
                log.Aimtlog.aimtprinterrorlog(curr_script_name, "sending email failed..")
                log.Aimtlog.aimtprinterrorlog(curr_script_name, resp.__str__())
                raise Exception
        except Exception as e:
            # Print any error messages to stdout
            log.Aimtlog.aimtprinterrorlog(curr_script_name, "Send email exception..")
            log.Aimtlog.aimtprinterrorlog(curr_script_name, "Exception: " + e.__str__())
            return False
        finally:
            server.quit()
        return True
