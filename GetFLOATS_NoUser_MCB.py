#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sept. 2021 

@author: J.D. Goetz

Main Script to download new FLOATS TMs via SFTP from the CCMz and process the files
"""
import datetime as dt
import os
import glob
import pysftp
import gzip
import sys
import pandas as pd
import struct
import csv
import numpy as numpy
import re
import shutil
from datetime import datetime
import time
from numpy import log
from array import *
from os.path import exists

#from EFU_HKfileprocessor import *

#File names and directories
default_local_target_dir="/Users/martina/Strateole2/FLOATS/CCMZ_Mirror/" # directory where to store mirrored data on your local machine
FLOATS_csv_dir = "/Users/martina/Strateole2/FLOATS/FLOATS_C1_52/" # dir where to put processesed csv files 
FLOATS_MCB_csv_dir = "/Users/martina/Strateole2/FLOATS/FLOATS_C1_52/MCB/"  # dir where to put processesed MCB csv files 
Tempfile_dir = FLOATS_csv_dir+"/Recent_Download/"
singlescan_dir = FLOATS_csv_dir+"Single_FTR/"
FLOATS_log_file = FLOATS_csv_dir+"FLOATS_Log.txt" #file to save log of XML messages
ftr_file_name = FLOATS_csv_dir+"FLOATS_Raman_Master.csv"
HK_file_name = FLOATS_csv_dir+"FLOATS_HK_Master.csv"
EFU_file_name = FLOATS_csv_dir+"FLOATS_EFU_TSEN.csv"
EFU_HK_name = FLOATS_csv_dir+"FLOATS_EFU_HK.csv" 

gzfiles = sorted(os.listdir(default_local_target_dir))

#CCMz parameters
ccmz_url="sshstr2.ipsl.polytechnique.fr" # CCMz URL from where to download data
ccmz_user="mbramberger" # Your login on the CCMz
ccmz_pass="b%Tg~H42" # Your password on the CCMz

# ID of flights in which I'm interested in
my_flights=['ST2_C1_06_TTL5']#,'ST2_C0_05_TTL2'] # Adapt according to your needs

# ID of my instrument
my_instruments=['FLOATS'] # Adapt according to your needs
flight_or_test='Flight'
tm_or_tc='TM'
raw_or_processed='Processed'


def loop_over_flights_and_instruments():
    """
    Get all data from CCMz for the input list of flights/instruments
    """
    global new_files,sorted_files
    for flight in my_flights:
        for instrument in my_instruments:
            ccmz_folder=os.path.join(flight,instrument,flight_or_test,tm_or_tc,raw_or_processed)
            #mirror_ccmz_folder(ccmz_folder)
            new_files = mirror_ccmz_folder(instrument,ccmz_folder, show_individual_file=True)
            
            if new_files != None:
                # sort the files chronologically first
                # timestamp = [''] * len(new_files)
                unixtime = numpy.zeros([len(new_files)])           
                cnt =0
                for i in new_files:
                    helpfile = i
                    # cut out timestamp from filename
                    ia=helpfile.find('_202')
                    iz=helpfile.find('.FL')
                    timestamp = helpfile[ia+1:iz]
                    # create date_time object, so you can convert it to unixtime stamp
                    date_time_obj = datetime.strptime(timestamp, '%Y%m%d_%H_%M_%S')
                    unixtime[cnt] = time.mktime(date_time_obj.timetuple())
                    cnt = cnt+1
                # create pandas data frame to sort by unixtime
                df = pd.DataFrame(data=new_files,index=unixtime)
                # sort the list of files according to its unixtime
                sort_files = df.sort_index()
                sorted_files = sort_files.values
                #for f in gzfiles:
                for f1 in sorted_files:   
                    f = f1[0]   #clunky, but needed cause the values in the Pandas dataframe are array with one entry each and we need to get the string
                    simmatch = (re.search('.ready_tm', f))  ## use with OBC simulator unpacked data
                    gzmatch = (re.search('.gz', f))
                                     
                    if(simmatch): #if it is an unpacked binary file from the simulator                   
                        InputFile = f
                        fname = os.path.split(f)
                        ScanFile = singlescan_dir + fname[1] + '.csv'  
                        fname = os.path.split(f)
                        XMLmess = readXMLTHeader(InputFile, 1,FLOATS_log_file,HK_file_name) 
                        filetype = XMLmess[0]
                        
                        with open(InputFile, "rb") as binary_file:
                            #find binary section location in TM file
                            bindata = binary_file.read()
                            start = bindata.find(b'START') + 5  # Find the 'START' string that mark the start of the binary section
                            end = bindata.find(b'END') # Find the 'END' string
                            binPacket = (bindata[start:end-2]) # make a buffer containing only the binary section
                           
                            ## switch case to parse TM based on filetype
                            if filetype == 44:
                                parseFTRDatatoMasterCSV(binPacket,XMLmess,ftr_file_name)
                                parseSingleScanFTR(binPacket, XMLmess, ScanFile)
                                                  
                            elif filetype == 33:
                                parseEFUHKDatatoCSV(binPacket, EFU_HK_name)
                                                            
                            elif filetype == 22:
                                parseTSENDatatoCSV(binPacket, EFU_file_name)
                                
                    elif(gzmatch): #if it is an unpacked binary file from the simulator
                        InputFile = f
                        fname = os.path.split(f)
                        ScanFile = singlescan_dir + fname[1] + '.csv'  
                        XMLmess, mcbtype, unixtime = readXMLTHeader(InputFile, 2,FLOATS_log_file,HK_file_name) 
                        filetype = XMLmess[0]
                        
                        with gzip.open(InputFile, "rb") as binary_file:
                            #find binary section location in TM file
                            bindata = binary_file.read()
                            start = bindata.find(b'START') + 5  # Find the 'START' string that mark the start of the binary section
                            end = bindata.find(b'END') # Find the 'END' string
                            binPacket = (bindata[start:end-2]) # make a buffer containing only the binary section
                           
                            ## switch case to parse TM based on filetype
                            if filetype == 44:
                                parseFTRDatatoMasterCSV(binPacket,XMLmess,ftr_file_name)
                                parseSingleScanFTR(binPacket, XMLmess, ScanFile)
                                                   
                            elif filetype == 33:
                                parseEFUHKDatatoCSV(binPacket, EFU_HK_name)
                                                           
                            elif filetype == 22:
                                parseTSENDatatoCSV(binPacket, EFU_file_name)
                             
                            elif mcbtype == 'Finished commanded motion':
                                dt_time = datetime.fromtimestamp(int(unixtime))
                                mcbdate = dt_time.strftime("%m%d%Y_%H_%M_%S")
                                MCB_file_name = "FLOATS_MCB_TM_"+mcbdate+ ".csv"
                                parseMCBData(binPacket, MCB_file_name)

def mirror_ccmz_folder(instrument, ccmz_folder, local_target_dir=default_local_target_dir, show_individual_file=True):
   """
   Mirror one CCMz folder.
   Files are stored locally in local_target_dir/ccmz_path/to/ccmz_folder/
   Files already downloaded are not downloaded again.
   local_target_dir prescribes where CCMz files will be downloaded locally.
   show_individual_file controls whether the name of each downloaded file is displayed or not.
   """
   print('---------------------------------')
   print('Trying to mirror CCMz folder: \033[1m'+ccmz_folder+'\033[0m')

   downloaded_files = []
   # Create (if needed) the appropriate local directory
   local_folder=os.path.join(local_target_dir,ccmz_folder)  
   if not os.path.exists(local_folder):
      os.makedirs(local_folder)

   # Connect to CCMz
   try:
       with pysftp.Connection(host=ccmz_url, username=ccmz_user, password=ccmz_pass) as sftp:
          print("\033[1mConnection to CCMz succesfully established\033[0m...")
          # Switch to the remote directory
          try:
              sftp.cwd(ccmz_folder)
          except IOError:
              print('\033[1m\033[91mNo such directory on CCMz: '+ccmz_folder+'\033[0m')
              return

          # Get file list in current directory, i.e. those that have been already downloaded from CCMz
          local_files=glob.glob(os.path.join(local_folder,'*')) # filenames with relative path
          local_filenames=[os.path.basename(f) for f in local_files] # filenames without

          # Get file list from the CCMz directory with file attributes
          ccmz_file_list = sftp.listdir_attr()

          # check wether CCMz files need to be downloaded
          n_downloads=0
          for ccmz_file in ccmz_file_list:
              # Get rid of directories in CCMZ folder (if any)
              if ccmz_file.longname[0] == '-':
                 ccmz_filename=ccmz_file.filename

                 # Check whether the file has already been downloaded (so as to not download it again)
                 if not ccmz_filename in local_filenames:
                    # file has to be downloaded
                    if show_individual_file == True:
                       print('Downloading \033[92m'+ccmz_filename+'\033[0m...') # display file name
                       downloaded_files.append(os.path.join(local_folder,ccmz_filename))
                    sftp.get(ccmz_filename,os.path.join(local_folder,ccmz_filename),preserve_mtime=True)
                    
                    #print(ccmz_filename)
                    n_downloads=n_downloads+1

            # Create a ZipFile Object and load sample.zip in it
                    print (os.path.join(local_folder,ccmz_filename))

          # and print some statistics
          if n_downloads == 0:
              print('\nYour local repository \033[92m'+local_folder+'\033[0m looks\033[1m up do date\033[0m')

          else:
              print('\n\033[1m'+str(n_downloads)+ '\033[0m file(s) downloaded in \033[92m'+local_folder+'\033[0m')
              print('List of downloaded Files')
              return downloaded_files
   except:
          print('\033[1m\033[91mConnection to CCMz failed\033[0m: check your login/password')
          return

def readXMLTHeader(InputFile, filetype, logFile, hkFile): 
    if filetype == 1: #if it is a simulator file
        with open(InputFile, "rb") as binary_file:
            data = binary_file.read()

    elif filetype == 2: #if it is a CCMz gzip file
        with gzip.open(InputFile, "rb") as binary_file: 
            data = binary_file.read()

    ## Check State message one for FLOATS TM type
    start = data.find(b'<StateMess1>')
    end = data.find(b'</StateMess1>')
    XMLMsg = (data[start+12:end].decode())
    
    #Check if a state mess2 exists
    Mess2 = data.find(b'<StateMess2>')
    ## Get TM message ID
    start = data.find(b'<Msg>')
    end = data.find(b'</Msg>')

    MsgID = data[start+5:end].decode()   
    
    # get Unixtime to name MCB csv file
    start = data.find(b'<StateMess2>')
    end = data.find(b'</StateMess2>')
    HKMsg = data[start+12:end].decode()  
    HKstrlist = HKMsg.split(',')
    UnixTime = HKstrlist[0]
    
    # If State Mess 2 is a FTR mode TM type (i.e. not motor telemetry or error message)
    if XMLMsg.isnumeric() and XMLMsg.startswith('11') or XMLMsg.startswith('22') or XMLMsg.startswith('33') or XMLMsg.startswith('44'):
        start = data.find(b'<StateMess2>')
        end = data.find(b'</StateMess2>')
        
        HKMsg = data[start+12:end].decode()      
        ##example of statemess 2:  1632138266,29.12,25.42,971.69,12.32,11.98,0.76,-66
        HKstrlist = HKMsg.split(',')
        UnixTime = HKstrlist[0]
        Ref1T = HKstrlist[1]
        Ref2T = HKstrlist[2]
        Press = HKstrlist[3]
        ZephV = HKstrlist[4]
        FTRV = HKstrlist[5]
        ZephI = HKstrlist[6]
        RSSI = HKstrlist[7]

        if os.path.basename(InputFile).startswith('TM') or os.path.basename(InputFile).startswith('ST2') :
            #append to message log text file
            with open(logFile, "a") as log: 
                log.write(os.path.basename(InputFile) + ', ' + MsgID + ', ' + XMLMsg + ', ' + HKMsg + '\n')  
            
            #append to master csv of housekeeping data
            file_exists = exists(hkFile) #first check if the file and header exists
            headerexists = 0

            if file_exists: 
                sniff = csv.Sniffer() 
                headerexists = sniff.has_header(open(hkFile).read(2))

            with open(hkFile, mode='a+') as out_file:
                file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL) 

                if headerexists == 0: ##if there is no header
                    header = (['Filename','MessType','Time UNIX','Ref1_T [C]','Ref2_T [C]','Pressure [hpa]','Zephyr [V]', 'FTR [V]', 'FLOATS Current [A]', 'Last EFU RSSI'])
                    file_writer.writerow(header)

                header = [os.path.basename(InputFile),XMLMsg,UnixTime,Ref1T,Ref2T,Press,ZephV,FTRV,ZephI,RSSI]
                file_writer.writerow(header)
            

            return [int(XMLMsg), int(UnixTime), float(Ref1T), float(Ref2T), float(Press), float(ZephV), float(FTRV), float(ZephI), int(RSSI)], XMLMsg, UnixTime  #use [1,2,3] list to return all HK variables                         
    else : 
            if os.path.basename(InputFile).startswith('TM') or os.path.basename(InputFile).startswith('ST2') : ###change this for the flight data
                with open(logFile, "a") as log:
                    log.write(os.path.basename(InputFile) + ', ' + MsgID + ', ' + XMLMsg + '\n')  
            return [Mess2], XMLMsg, UnixTime
       
def parseEFUHKDatatoCSV(binData, OutFile):
    ###note that the csv.sniffer raises an error if the csv file is created but blank       
    # open CSV file to make sure the header exists
    file_exists = exists(EFU_HK_name)
    headerexists = 0

    if file_exists: 
        sniff = csv.Sniffer() 
        headerexists = sniff.has_header(open(OutFile).read(2))
    
    with open(OutFile, mode='a+') as out_file:
        file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL) 
        
        #write the header data to csv

        if headerexists == 0: ##if there is no header
            header = (['GPS', 'ADC', 'ADC','ADC', 'Heater', 'Temperature', 'Temperature'])
            file_writer.writerow(header)
            header = (['Time POSIX','Battery [V]','TSEN [V]','Teensy [V]','Status [1 = on]', 'Battery [C]', 'PCB [C]'])
            file_writer.writerow(header)

         #read the data

        for y in range(int((len(binData)/14))):
            index = y*14       
            GPSTime = (struct.unpack_from('>I',binData,0+index)[0])
            
            if GPSTime < 1627790400: #if the unix time is based on seconds counter and not GPS time.
                GPSTime /= 65535

            BattV = (struct.unpack_from('>H',binData,4+index)[0])/1000
            TSENV = (struct.unpack_from('>H',binData,6+index)[0])/1000
            HStat = (struct.unpack_from('B',binData,8+index)[0])
            TeenV = (struct.unpack_from('B',binData,9+index)[0])/10
            BattT = ((struct.unpack_from('>H',binData,10+index)[0])/10) - 273.15
            PCBT = ((struct.unpack_from('>H',binData,12+index)[0])/10) -273.15

            header = [GPSTime,BattV,TSENV,TeenV,HStat,BattT,PCBT]
            file_writer.writerow(header)

def parseTSENDatatoCSV(binData, OutFile): 

    ###note that the csv.sniffer raises an error if the csv file is created but blank   
    
    # open CSV file to make sure the header exists
    file_exists = exists(EFU_file_name)
    headerexists = 0

    if file_exists: 
        sniff = csv.Sniffer() 
        headerexists = sniff.has_header(open(EFU_file_name).read(2))   
    
    with open(OutFile, mode='a+') as out_file:
        file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        if headerexists == 0: ##if there is no header
            #write the header data to csv
            header = (['GPS', 'GPS', 'GPS','GPS', 'TSEN', 'TSEN', 'TSEN', 'TSEN'])
            file_writer.writerow(header)
            header = (['Time POSIX','Latitude [dd.ddddd]','Longitude [dd.ddddd]','Altitude [m]','Ambient Temperature [counts]',
                                    'Ambient Pressure [counts]', 'Psensor Temperature [counts]', 'V1 Cal Temperature [C]'])
            file_writer.writerow(header)

        #read the data

        for y in range(int((len(binData)/24))):
            index = y*24        
            GPSTime = (struct.unpack_from('>I',binData,0+index)[0])

            #if GPSTime < 1627790400: #if the unix time is based on seconds counter and not GPS time.
             #   GPSTime /= 65535
            GPSLat = (struct.unpack_from('<f',binData,4+index)[0])
            GPSLon = (struct.unpack_from('<f',binData,8+index)[0])
            GPSAlt = (struct.unpack_from('>H',binData,12+index)[0])
            TSEN_T = (struct.unpack_from('>H',binData,14+index)[0])
            TSEN_P = (struct.unpack_from('>I',binData,16+index)[0])
            TSEN_TP = (struct.unpack_from('>I',binData,20+index)[0])

            TSEN_CalT = TSENCalVal(TSEN_T)
            header = [GPSTime,GPSLat, GPSLon, GPSAlt, TSEN_T, TSEN_P, TSEN_TP, TSEN_CalT]
            file_writer.writerow(header)

def parseSingleScanFTR(binData, xml, OutFile):
    with open(OutFile, mode='w') as out_file: ##write a new csv
        file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
        ##read start of binary packet
        binlen = int(len(binData))
        #write the coadd values for the Raman averages
        OpticalLen = (struct.unpack_from('>H',binData,0)[0])
        OpticalRes = (struct.unpack_from('B',binData,2)[0])
        MeasurePeriod = (struct.unpack_from('>H',binData,3)[0])
        StokesCount = (struct.unpack_from('>H',binData,5)[0])
        AstokesCount = (struct.unpack_from('>H',binData,(OpticalLen*2+7))[0])

        StokesBuff = binData[7:5+(OpticalLen*2)]
        AStokesBuff = binData[7+(OpticalLen*2):(binlen)]
        
        #build header
        # header = (['FLOATS Scan Data:', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ','Raman Arrays:'])
        # file_writer.writerow(header)
            
        strArr = numpy.empty((OpticalLen*2+9), dtype='object')
        strArr[0] = "Unix Time"
        strArr[1] = "Scan Points"
        strArr[2] = "Spatial Resolution"
        strArr[3] = "Integration Period"
        strArr[4] = "Stokes Scans"
        strArr[5] = "Antistokes Scans"
        strArr[6] = "Ref 1 Temperature"
        strArr[7] = "Ref 2 Temperature"
        strArr[8] = "Pressure"

        for i in range(9,OpticalLen+9,1):
            strArr[i] = "Stokes Dist [m]"

        for i in range(OpticalLen+9,(OpticalLen*2)+9,1):
            strArr[i] = "Astokes Dist [m]"

        numpy.savetxt(out_file, [strArr], fmt="%s", delimiter=',')

        strArr = numpy.empty((OpticalLen*2+9), dtype='object')
        strArr[0] = "[seconds]"
        strArr[1] = "[count]"
        strArr[2] = "[meters]"
        strArr[3] = "[seconds]"
        strArr[4] = "[n]"
        strArr[5] = "[n]"
        strArr[6] = "[deg C]"
        strArr[7] = "[deg C]"
        strArr[8] = "[hpa]"

        for i in range(9,OpticalLen+9,1):
            strArr[i] = (i-8)*OpticalRes

        for i in range(OpticalLen+9,(OpticalLen*2)+9,1):
            strArr[i] = (i-(OpticalLen+8))*OpticalRes

        numpy.savetxt(out_file, [strArr], fmt="%s", delimiter=',')

        
        ParamArray = numpy.array([xml[1],OpticalLen,OpticalRes,MeasurePeriod,StokesCount,AstokesCount,xml[2],xml[3],xml[4]])
        StokesVal = []
        AStokesVal = []

        for i in range(0,(OpticalLen*2-2),2):
            StokesVal.append(struct.unpack_from('>H',StokesBuff,i)[0]) 

        for ii in range(2,(binlen//2)-4,2):
            AStokesVal.append(struct.unpack_from('>H',AStokesBuff,ii)[0])

        StokesArray = numpy.zeros(OpticalLen)
        AStokesArray = numpy.zeros(OpticalLen)
        counter = 0

        for i in range(0,OpticalLen-1,1):
            StokesArray[i] = StokesVal[counter]
            AStokesArray[i] = AStokesVal[counter]
            counter +=1
        
        Fullarray = numpy.concatenate((ParamArray,StokesArray, AStokesArray))

        numpy.savetxt(out_file, [Fullarray], fmt="%1.2f", delimiter=',')

def parseFTRDatatoMasterCSV(binData, xml, OutFile):    
     # open CSV file to make sure the header exists
    file_exists = exists(ftr_file_name)
    headerexists = 0

    if file_exists: 
        sniff = csv.Sniffer() 
        headerexists = sniff.has_header(open(OutFile).read(2))
    
    with open(OutFile, mode='a+') as out_file: ##append to csv file if it already exists
        file_writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if headerexists == 0:
            #build header
            header = (['FLOATS Scan Data:', ' ', ' ', ' ', ' ', ' ', ' ', ' ',' ','Raman Arrays:'])
            file_writer.writerow(header)
            
            strArr = numpy.empty(3709, dtype='object')
            strArr[0] = "Unix Time"
            strArr[1] = "Scan Points"
            strArr[2] = "Spatial Resolution"
            strArr[3] = "Integration Period"
            strArr[4] = "Stokes Scans"
            strArr[5] = "Antistokes Scans"
            strArr[6] = "Ref 1 Temperature"
            strArr[7] = "Ref 2 Temperature"
            strArr[8] = "Pressure"
            #strArr[9] = ""
            for i in range(9,1859,1):
                strArr[i] = "Stokes Dist [m]"

            for i in range(1859,3709,1):
                strArr[i] = "Astokes Dist [m]"

            numpy.savetxt(out_file, [strArr], fmt="%s", delimiter=',')

            strArr = numpy.empty(3709, dtype='object')
            strArr[0] = "[seconds]"
            strArr[1] = "[count]"
            strArr[2] = "[meters]"
            strArr[3] = "[seconds]"
            strArr[4] = "[n]"
            strArr[5] = "[n]"
            strArr[6] = "[deg C]"
            strArr[7] = "[deg C]"
            strArr[8] = "[hpa]"
            for i in range(9,1859,1):
                strArr[i] = i-8

            for i in range(1859,3709,1):
                strArr[i] = i-1858

            numpy.savetxt(out_file, [strArr], fmt="%s", delimiter=',')

        binlen = int(len(binData))
        #write the coadd values for the Raman averages
        OpticalLen = (struct.unpack_from('>H',binData,0)[0])
        OpticalRes = (struct.unpack_from('B',binData,2)[0])
        MeasurePeriod = (struct.unpack_from('>H',binData,3)[0])
        StokesCount = (struct.unpack_from('>H',binData,5)[0])
        AstokesCount = (struct.unpack_from('>H',binData,(OpticalLen*2+7))[0])

        StokesBuff = binData[7:5+(OpticalLen*2)]
        AStokesBuff = binData[7+(OpticalLen*2):(binlen)]
 
        ParamArray = numpy.array([xml[1],OpticalLen,OpticalRes,MeasurePeriod,StokesCount,AstokesCount,xml[2],xml[3],xml[4]])
        StokesVal = []
        AStokesVal = []

        for i in range(0,(OpticalLen*2-2),2):
            StokesVal.append(struct.unpack_from('>H',StokesBuff,i)[0]) 

        for ii in range(2,(binlen//2)-4,2):
            AStokesVal.append(struct.unpack_from('>H',AStokesBuff,ii)[0])

        StokesArray = numpy.zeros(1850)
        AStokesArray = numpy.zeros(1850)
        counter = 0

        for i in range(0,len(StokesVal)*OpticalRes,OpticalRes):
            StokesArray[i] = StokesVal[counter]
            AStokesArray[i] = AStokesVal[counter]
            counter +=1
        
        Fullarray = numpy.concatenate((ParamArray,StokesArray, AStokesArray))

        numpy.savetxt(out_file, [Fullarray], fmt="%1.2f", delimiter=',')
        
        
        # writing data netcdf file 

def TSENCalVal(T_counts):
    a = 4120.33771
    b = -114.69175
    k = 62941.9437
    a0 = -0.001508480319
    a1 = 0.001586963877
    a2 = -0.0002605665956
    a3 = 0.00002627958344
    a4 = -0.000001287017349
    a5 = 2.512381256e-08
  
    R = k*(T_counts-a)/(b-T_counts)
    T=(1.0/(a0+a1*log(R)+a2*(log(R)**2)+a3*(log(R)**3)+a4*(log(R)**4)+a5*(log(R)**5)))-273.15
    return T

# ----------------------------------------------------------------------------
# starting here are the functions to process MCB Data

def parseMCBData(data, file_name):
    SENSE_CURR_SLOPE = 11700
    MAX_ADC_READ = 4095
    VREF = 3.196
    I_OFFSET = 0.00018
    PULLDOWN_RESISTOR = 2000
    SUPPLY_VOLT_DIV = 0.102


    csv_name = FLOATS_MCB_csv_dir + file_name
    
    if (os.path.exists(csv_name)):
        print('Warning: already parsed, skipping')
        return

    data_length = len(data)
    # Make sure we have a valid number of packets in the data

    if ((data_length - 4) % 32 != 0):
        print("Invalid data length, contains " + str(int((data_length-4) / 32)) + " packets")
        return

    else:
        num_packets = int((data_length-4) / 32)

    # read the header (time in seconds since standard epoch)
    profile_start = struct.unpack_from('>I',data,0)[0]

    with open(csv_name, mode='w') as csv_file:
        # create a header for the CSV file
        header = ['Elapsed Time', 'Reel Torque Avg', 'Reel Torque Max', 'LW Torque Avg', 'LW Torque Max', 'Reel Curr Avg', 
                  'Reel Curr Max', 'LW Curr Avg', 'LW Curr Max', 'Differential Reel Speed', 'Reel Position', 
                  'LW Position', 'Enum', 'Reel Temp Avg', 'Reel Temp Max', 'LW Temp Avg', 'LW Temp Max', 'MC1 Temp Avg', 
                  'MC1 Temp Max', 'MC2 Temp Avg', 'MC2 Temp Max', 'Brake Curr Avg', 'Brake Curr Max', 'Supply Volt Avg', 
                  'Supply Volt Max']

        csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(['Start time (s)', profile_start])
        csv_writer.writerow(['Start time (UTC)', datetime.fromtimestamp(profile_start).strftime('%Y-%m-%d %H:%M:%S')])
        csv_writer.writerow(header)

        # parse all of the packets
        packet_start_index = 4
        for itr in range(num_packets):
            packet_data = ['-']*25
            packet = struct.unpack_from('>BHBHHHHHHHHHHff',data,packet_start_index+itr*32)
            
            # make sure the sync byte is valid
            if packet[0] != 0xA5:
                print("Bad sync, packet #" + itr+1)
                continue # skip to next packet

            # get the regular TM
            packet_data[0] = packet[1]/10.0 # ellapsed time
            packet_data[1] =  0 if (0 == packet[5]) else(packet[5] - 30000)/10.0 # reel torque avg
            packet_data[2] =  0 if (0 == packet[6]) else(packet[6] - 30000)/10.0 # reel torque max
            packet_data[3] =  0 if (0 == packet[7]) else(packet[7] - 30000)/10.0 # lw torque avg
            packet_data[4] =  0 if (0 == packet[8]) else(packet[8] - 30000)/10.0 # lw torque max
            packet_data[5] = SENSE_CURR_SLOPE * ((VREF/PULLDOWN_RESISTOR) * (packet[9]/MAX_ADC_READ) - I_OFFSET) # reel curr avg
            packet_data[6] = SENSE_CURR_SLOPE * ((VREF/PULLDOWN_RESISTOR) * (packet[10]/MAX_ADC_READ) - I_OFFSET) # reel curr max
            packet_data[7] = SENSE_CURR_SLOPE * ((VREF/PULLDOWN_RESISTOR) * (packet[11]/MAX_ADC_READ) - I_OFFSET) # lw curr avg
            packet_data[8] = SENSE_CURR_SLOPE * ((VREF/PULLDOWN_RESISTOR) * (packet[12]/MAX_ADC_READ) - I_OFFSET) # lw curr max

            if (itr > 0):
                packet_data[9] = (packet[13] - last_reel_pos) / (packet_data[0] - last_time) * 60

            packet_data[10] = packet[13] # reel position
            packet_data[11] = packet[14] # lw position

            # update tracking variables
            last_reel_pos = packet[13]
            last_time = packet_data[0]
            
            # get the rotating TM
            packet_data[12] = packet[2]
            if (0 == packet[2]): # reel temp
                packet_data[13] = 0 if (0 == packet[3]) else (packet[3] - 30000)/10.0
                packet_data[14] = 0 if (0 == packet[4]) else (packet[4] - 30000)/10.0

            elif (1 == packet[2]): # lw temp
                packet_data[15] = 0 if (0 == packet[3]) else (packet[3] - 30000)/10.0
                packet_data[16] = 0 if (0 == packet[4]) else (packet[4] - 30000)/10.0

            elif (2 == packet[2]): # mc1 temp
                packet_data[17] = 0 if (0 == packet[3]) else (packet[3] - 30000)/10.0
                packet_data[18] = 0 if (0 == packet[4]) else (packet[4] - 30000)/10.0

            elif (3 == packet[2]): # mc2 temp
                packet_data[19] = 0 if (0 == packet[3]) else (packet[3] - 30000)/10.0
                packet_data[20] = 0 if (0 == packet[4]) else (packet[4] - 30000)/10.0

            elif (4 == packet[2]): # brake curr
                packet_data[21] = packet[3]
                packet_data[22] = packet[4]

            elif (5 == packet[2]): # supply voltage
                packet_data[23] = VREF * (packet[3]/MAX_ADC_READ) / SUPPLY_VOLT_DIV
                packet_data[24] = VREF * (packet[4]/MAX_ADC_READ) / SUPPLY_VOLT_DIV

            csv_writer.writerow(packet_data)

    # print('Results in:   ' + csv_name)



# ---------------------------------------------------------------------------
def main():
    ##To add: mirror and download CCMz data
    loop_over_flights_and_instruments()

if __name__ == "__main__": 
  # calling main function 
  main() 