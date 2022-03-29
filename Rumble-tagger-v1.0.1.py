#!/usr/bin/python

__version__='1.0.2'
__date__='2022/03/29'
from re import T
import sys
import os
import time
import argparse
import time
import logging
import requests
import json
import csv
import pandas as pd
from io import StringIO
import numpy
import warnings
import signal
from pathlib import Path
from requests.structures import CaseInsensitiveDict

# Parse Arguments
parser = argparse.ArgumentParser(description="Program to tag assets in Rumble.")
parser.add_argument('--version', action='version', version="Rumble-tagger version: " + __version__)
parser.add_argument('-c','--csvfile', action='store', required=True, dest='csvfile',
                    help="A list of assets and tag value names as a comma separated file. i.e. SevOne.csv or /home/user/SevOne.csv with IP, TagName as columns. No column header is required and at least entry one is required.")
parser.add_argument('-a','--apitoken', action='store', required=True, dest='authtoken', help='Your assigned API Token for your organization in Rumble.')
parser.add_argument('-k','--key', action='store', required=True, dest='keyvalue', help='The common key tag value to apply in Rumble to all assets proccesed by this run.')
parser.add_argument('-v','--verbose', action='store', dest='verbose', type=int, choices=[0, 1], default=0, help='Enter a value of 1 if you want the option to see more details of what is processing during the run.')
argresults = parser.parse_args()
apitoken = argresults.authtoken
csvlocation=argresults.csvfile
verbose_on=int(argresults.verbose)
tagkey=argresults.keyvalue
tag_key='"'+tagkey+'"'

#Environment variables
homedir = str(Path.home())
abspath = os.path.join(os.path.realpath(__file__))     
logpath = homedir + "/Rumble/logs/"
file_extension = ".log"
consfile = logpath + "console" + file_extension
logfilename = "runlog"
datetimestamp = time.strftime("%Y%m%d%H%M%S")  
logfile = logpath + logfilename + "-" + datetimestamp + file_extension
errlist = []
partial_success = 0
scheme = "https://"
host = "demo" #Change for your host. 
domain = ".rumble.run" #Chnage for your domain. 
base_path = "/api/v1.0"
base_url = scheme + host + domain + base_path
working_dir=os.getcwd()
working_dir=working_dir + "/"
errorcount = 0
warnings.filterwarnings("ignore")

def main():
    global errorlist
    global apitoken
    errorlist = []
    consoleinfo(cmsg="\n\n\n New Run" + datetimestamp)
    consoleinfo(cmsg="\n\n\n Arguments passed: " + str(argresults))
    consoleinfo(cmsg="\n\nLog Path: " + logpath + " exists")
    consoleinfo(cmsg="Console: " + consfile + " is open for writing")
    consoleinfo(cmsg="Absolute Path is: " + abspath)
    iswriteable = checkdir()
    if str(iswriteable) == "True":
        messageinfo(msg="Current Directory " + Currentdirectory + " is Writeable: " + str(iswriteable))
        messageinfo(msg="New Logfile is : " + logfile)
        open(logfile, "w")
        messageinfo(msg="Logfile: " + "is open for writing")
        messageinfo(msg="Users home directory is: " + homedir)
        #Begin the work
        print("The Rumble tagger is proccessing your request, please wait.")
        #Ask the user for their auth token
        #apitoken=input("Enter your bearer token: ")
        if verbose_on > 0:
            print("Abspath is: " + abspath)
            print("Working directory is: " + working_dir)
            print("Loging directory is: " + logpath)

        tokenlength=len(apitoken)
        if tokenlength == 30:
            print("Your token is of valid length")
            messageinfo(msg="Your token is of valid length")
        else:
            statrun = 1
            print("Error, your token is length: " + str(tokenlength) + " and needs to be at least 30 characters.")
            messagewarning(msg="Error, your token is length: " + str(tokenlength) + " and needs to be at least 30 characters.")
            runstate(statrun)
            
        #Check access to Rumble by getting the API key details
        getapikey()    

        #Ask the user for the location and name of the CSV file for processing.
        #csvlocation=input("Enter the CSV file name, i.e. SevOne.csv or /home/user/SevOne.csv: ")
        if os.path.exists(csvlocation):
            messageinfo(msg="CSV file exists: " + csvlocation)
            #Process the CSV file into dataframe
            csvheader=["address","TagName"]
            csvdf=pd.read_csv(csvlocation, names=csvheader)
            rows_csv=csvdf.shape[0]
            #csvfile=os.path.split(csvlocation)
            #filename=csvfile[1]
            #file=filename.split('.')
            #tag_key=file[0]
            #tag_key='"'+tag_key+'"'
        else:
            statrun = 1
            runstate(statrun)
            messagecritical(msg="CSV file does not exist: " + working_dir + csvlocation)
            print("CRITICAL: CSV file does not exist: " + working_dir + csvlocation)
            exit(runstatus)
                
        #Get the assests from Rumble
        headers = CaseInsensitiveDict()
        headers["Accept"] = "text/csv"
        headers["Authorization"] = "Bearer " + apitoken
        url_suffix = "/export/org/assets.csv"
        export_url=base_url + url_suffix
        if verbose_on > 0:
            print("Retrieving assets from url: " + export_url)
        messageinfo(msg="Retrieving assets from url: " + export_url)
        try:
            respcsv=requests.get(export_url, headers=headers, verify=True)
            respcsv.raise_for_status()
            consoleinfo(cmsg="Get export assests status: " + str(respcsv.status_code))
        except requests.exceptions.RequestException as err:
            print("OOps: Authorization or something else",err)
            messagecritical(msg="OOps: Authorization or something else" + str(err))
            statrun = 1
            runstate(statrun)
            exit(runstatus) 
        except requests.exceptions.HTTPError as errh:
            print("Http Error:",errh)
            messagecritical(msg="Http Error:" + str(errh))
            statrun = 1
            runstate(statrun)
            exit(runstatus) 
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:",errc)
            messagecritical(msg="Error Connecting:" + str(errc))
            statrun = 1
            runstate(statrun)
            exit(runstatus) 
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:",errt)
            messagecritical(msg="Timeout Error:" + str(errt))
            statrun = 1
            runstate(statrun)
            exit(runstatus) 

        #Write the assets results to a csv file
        messageinfo(msg="Processing assets from url: " + export_url)
        if verbose_on > 0:
            print("Processing assets from url: " + export_url)
        file=open(logpath + 'export_data.csv', 'w')
        file.write(respcsv.text)
        file.close()
        
        #Process the csv file into a dataframe
        exportcsvdf=pd.read_csv(logpath + 'export_data.csv')

        #Clean the dataframe
        exportcsvdf.drop(exportcsvdf.columns.difference(['id', 'address', 'names']), 1, inplace=True)
        nan_value=float("NaN")
        exportcsvdf.replace("", inplace=True)
        exportcsvdf.dropna(subset = ["address"], inplace=True)
        exportcsvdf=exportcsvdf.reset_index()
        rows_export=exportcsvdf.shape[0]
        
        #Remove the downloaded csv file
        os.remove(logpath + 'export_data.csv')
        if verbose_on > 0:
            print("Assets successfully processed from url: " + export_url)
        messageinfo(msg="Assets successfully processed from url: " + export_url)

        #Find the items in the CSV file dataframe from the assets dataframe
        if verbose_on > 0:
            print("Finding values from the CSV input file, assets: " + str(rows_csv) + " against the total assets to search: " + str(rows_export))
        messageinfo(msg="Finding values from the CSV input file, assets: " + str(rows_csv) + " against the total assets to search: " + str(rows_export))
        updatedf=pd.merge(exportcsvdf, csvdf, on='address')
        if verbose_on > 0:
            print("All assets have been matched as follows:")
        messageinfo(msg="All assets have been matched as follows:")
        if verbose_on > 0:
            print(updatedf.to_string(index=False))
        messageinfo(msg=updatedf.to_string(index=False))

        #Process each item in the found dataframe and apply the tags
        for index, row in updatedf.iterrows():
            assetid=row['id']
            #print(str(assetid))
            tagname=row['TagName']
            #print(str(tagname))
            tag_value='"'+tagname+'"'
            #Left Here - Create a loop to iterate over the data frame.
            headers = CaseInsensitiveDict()
            headers["Accept"] = "application/json"
            headers["Authorization"] = "Bearer " + apitoken
            url_suffix = "/org/assets/"
            Tag_id=assetid 
            url_action="/tags"
            tag_url=base_url + url_suffix + Tag_id + url_action
            pair='{' + tag_key + ":" +  tag_value + '}'
            tag='{"tags": {' +tag_key+ ': '+tag_value+'}}'
                        
            try:
                time.sleep(5)
                respatch=requests.patch(tag_url, headers=headers, data=tag, verify=True)
                respatch.raise_for_status()
                consoleinfo(cmsg="Tag Update status: " + "for asset id: " + assetid + str(respatch.status_code))
                messageinfo(msg="Asset id: " + Tag_id + "has been updated with tag: " +tag_key+ "=" +tag_value)
                if verbose_on > 0:
                    print("Asset id: " + Tag_id + "has been updated with tag: " +tag_key+ "=" +tag_value)
            except requests.exceptions.RequestException as err:
                print("OOps: Authorization or something else",err)
                messagecritical(msg="OOps: Authorization or something else" + str(err))
                statrun = 1
                runstate(statrun)
                exit(runstatus) 
            except requests.exceptions.HTTPError as errh:
                print("Http Error:",errh)
                messagecritical(msg="Http Error:" + str(errh))
                statrun = 1
                runstate(statrun)
                exit(runstatus) 
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:",errc)
                messagecritical(msg="Error Connecting:"+ str(errc))
                statrun = 1
                runstate(statrun)
                exit(runstatus) 
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:",errt)
                messagecritical(msg="Timeout Error:" + str(errt))
                statrun = 1
                runstate(statrun)
                exit(runstatus)
        print("\n The Rumble tagger has completed, exiting program.")
        #end the work
        if partial_success > 0:
            print("The program was partially successful. Please refer to the logfile: " + logfile + " for more information.")
            statrun = 2
            runstate(statrun)
            printerrors()
            messageinfo(msg="Program ended with status : " + str(statrun))
            exit(runstatus)
        else:
            statrun = 0
            runstate(statrun)
            printerrors()
            messageinfo(msg="Program ended with status : " + str(statrun))
            exit(runstatus)
    else:
        statrun = 1
        runstate(statrun)
        printerrors()
        consolecritical(cmsg="Directory" + Currentdirectory + " does not have write permission")
        print("Directory" + Currentdirectory + " does not have write permission")
        exit(runstatus)    
    #End main

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

def getapikey():
    url_suffix = "/org/key"
    apiurl=base_url + url_suffix
    headers = CaseInsensitiveDict()
    headers["Accept"] = "text/csv"
    headers["Authorization"] = "Bearer " + apitoken
    
    try:
        apiresp=requests.get(apiurl, headers=headers, verify=True)
        apiresp.raise_for_status()
        consoleinfo(cmsg="Get API Key status: " + str(apiresp.status_code))
    except requests.exceptions.RequestException as err:
        print("OOps: Authorization or something else",err)
        messagecritical(msg="OOps: Authorization or something else" + str(err))
        statrun = 1
        runstate(statrun)
        exit(runstatus) 
    except requests.exceptions.HTTPError as errh:
        print("Http Error:",errh)
        messagecritical(msg="Http Error:" + str(errh))
        statrun = 1
        runstate(statrun)
        exit(runstatus) 
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:",errc)
        messagecritical(msg="Error Connecting:"+ str(errc))
        statrun = 1
        runstate(statrun)
        exit(runstatus) 
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:",errt)
        messagecritical(msg="Timeout Error:" + str(errt))
        statrun = 1
        runstate(statrun)
        exit(runstatus) 

    apijson=json.loads(apiresp.text)
    org_id = apijson["organization_id"]
    consoleinfo(cmsg="Organization ID: " + org_id)
    api_user = apijson["created_by"]
    consoleinfo(cmsg="API User: " + api_user)
    last_used_ip  = apijson["last_used_ip"]
    consoleinfo(cmsg="Last Used IP: " + last_used_ip)
    #end getapikey

def printerrors():
    allerrors = errorlist + errlist
    if allerrors:
        messagewarning(msg="Run time errors and exceptions were detected.")
        for err in allerrors:
            messagewarning(msg=str(err))
    return

def getdate():
    return time.strftime("%Y%m%d%H%M%S")

def checkdir():
    global Currentdirectory 
    Currentdirectory = os.getcwd()
    iswriteable = os.access(Currentdirectory, os.W_OK)
    return iswriteable

def messageinfo(msg):
    consoledebug(dmsg="in Message Info Function")
    logging.basicConfig(level=logging.INFO, filename=consfile,
                        format='%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.info(msg)
    logger.removeHandler(handler)

def messagewarning(msg):
    consoledebug(dmsg="in Message Warning Function")
    logging.basicConfig(level=logging.WARNING, filename=consfile,
                        format='%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    # create a file handler
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.WARNING)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.warning(msg)
    logger.removeHandler(handler)

def messagecritical(msg):
    consoledebug(dmsg="in Message Critical Function")
    logging.basicConfig(level=logging.CRITICAL, filename=consfile,
                        format='%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.CRITICAL)
    # create a file handler
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.CRITICAL)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.critical(msg)
    logger.removeHandler(handler)

def consoleinfo(cmsg):
    consoledebug(dmsg="in Console Info Function")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # print("Effective loggging level is {}".format(logging.getLevelName(logger.getEffectiveLevel())))
    # create a file handler
    handler = logging.FileHandler(consfile)
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.info(cmsg)
    logger.removeHandler(handler)

def consolewarn(cmsg):
    consoledebug(dmsg="in Console Warn Function")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    # create a file handler
    handler = logging.FileHandler(consfile)
    handler.setLevel(logging.WARNING)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.warning(cmsg)
    logger.removeHandler(handler)


def consolecritical(cmsg):
    consoledebug(dmsg="in Console Critical Function")
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.CRITICAL)
    # create a file handler
    handler = logging.FileHandler(consfile)
    handler.setLevel(logging.CRITICAL)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.critical(cmsg)
    logger.removeHandler(handler)

def consoledebug(dmsg):
    logger = logging.getLogger(__name__)
    # create a file handler
    handler = logging.FileHandler(consfile)
    handler.setLevel(logging.DEBUG)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)
    logger.debug(dmsg)
    logger.removeHandler(handler)

def partialrun():
    global partial_success
    partial_success += 1
    return partial_success

def runstate(statrun):
    global runstatus
    runstatus = statrun
    return runstatus

def Exit_grace(sig, frame):
    print("\nExiting program.")
    messagewarning(msg="Keyboard interrupt exception caught")
    consolewarn(cmsg="Keyboard interrupt exception caught")        
    sys.exit(0)

if os.path.isdir(logpath):
    open(consfile, 'a')
else:
    print("Console path " +logpath + " does not exist")
    errlist.append(" : WARNING : Console path " +logpath + " does not exist.")
    errorcount += 1
    os.makedirs(logpath)
    print("The new directory " + logpath + " is created!")
    messageinfo(msg="The new directory " + logpath + " is created!")
    #exit(1)

signal.signal(signal.SIGINT, Exit_grace)
     
main()
if __name__ == '__main__':

    sys.exit(runstatus)
