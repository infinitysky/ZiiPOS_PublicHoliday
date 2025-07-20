import sys, getopt
import pyodbc
import datetime
from decimal import Decimal

import os
import socket
import sys
import json
import wget
import pandas as pd
import numpy as np
import xlsxwriter
import xlrd
import requests

#===globalSettings ===
#filePath

filePath        ="C:\\Ziitech\\PHSync\\"
logFilePath     =filePath+"Log"
ConfigJsonFile  = filePath +"Config.json"
#fileServerURL   = "https://cdn.ziicloud.com/_misc/PublicHoliday/"
fileServerURL   = "http://mel.ziipos.com:8066/_misc/PublicHoliday/"
downloadFilePath = filePath + "DownloadFiles\\"
savedFile = downloadFilePath + "currentList.xlsx"


def downloadPublicHolidayExcelFromServer(fileName):
  downloadURL=fileServerURL+fileName
  downloadFile= downloadFilePath + fileName   
  # try:
  #     downloadURL=fileServerURL+fileName   
  #     wget.download(downloadURL, fileName)
  # except wget.Error as ex:
  #     print("Download Files error")
  #     writeErrorLog("An error occurred while downloading the public holiday file")  
  try:
        # send HTTP GET
        response = requests.get(downloadURL, stream=True, timeout=10)
        
        # check if the request was successful
        response.raise_for_status()
        
        os.makedirs(downloadFilePath, exist_ok=True)
        
        # 写入文件
        with open(downloadFile, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"downloadFile: {downloadFile}")
        return True
    
  except requests.exceptions.HTTPError as e:
      print(f"download failed: {e}")
      writeErrorLog(f"Download Failed, HTTP error: {e}")
  except requests.exceptions.ConnectionError:
      print("download failed, please check your network connection")
      writeErrorLog("Download Failed, Connection error, please check your network connection")
  except requests.exceptions.Timeout:
      print("Download failed, the request timed out")
      writeErrorLog("Download Failed, the request timed out")
  except requests.exceptions.RequestException as e:
      print(f"Donload Failed, error: {e}")
      writeErrorLog(f"Download Failed, error: {e}")
  except Exception as e:
      print(f"Download Failed, unknown issue: {e}")
      writeErrorLog(f"Download Failed, unknown issue: {e}")
      
  return False


 
 
def detailed_excel_comparison(file1, file2):
   
    df1 = pd.read_excel(file1, index_col=None,dtype = str)
    df2 = pd.read_excel(file2, index_col=None,dtype = str)

  
    comparison = df1 == df2
    if comparison.all().all():
        return True
    else:
        print("find difference:")
        print(comparison)
        return False

def writeLog(LogData):
  
  date = ''
  now = datetime.datetime.now()
  nDate = now.strftime('%Y%m%d')
  logTime= now.strftime('%Y%m%d %H:%M:%S')
  logFilename = nDate + '.txt'
  log_file = os.path.join(logFilePath, logFilename)
  if not os.path.exists(logFilePath):
        os.makedirs(logFilePath)
  if not os.path.isfile(log_file):
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(logTime+" Log File Created: "+nDate+"\n")
  try:
    with open(log_file, 'a', encoding='utf-8') as f:
      f.write(logTime+" "+LogData+"\n")  
  except Exception as e:
    print(f"An error occurred while writing to the log file: {e}")
    

    
def writeErrorLog(LogData):

  now = datetime.datetime.now()
  nDate = now.strftime('%Y%m%d')
  logTime= now.strftime('%Y%m%d %H:%M:%S')
  logFilename = nDate + '_Error.txt'
  errorLog_file = os.path.join(logFilePath, logFilename)
  
  if not os.path.exists(logFilePath):
        os.makedirs(logFilePath)
  if not os.path.isfile(errorLog_file):
        with open(errorLog_file, 'w', encoding='utf-8') as f:
            f.write(logTime+" Log File Created: "+nDate+"\n")
  
  try:
    with open(errorLog_file, 'a', encoding='utf-8') as f:
      f.write(logTime+" "+LogData+"\n")  
  except Exception as e:
    print(f"An error occurred while writing to the log file: {e}")
    
    




def createConfigFile():
    if not os.path.exists(filePath):
        os.makedirs(filePath)
        writeLog("Create Folder: "+filePath)
        
    if not os.path.isfile(ConfigJsonFile):
        json_data={
            
            "SourceSQLServer":".\\sqlexpress2008r2", 
            "SourceDatabase":"xxxxxxxxxx", 
            "Trusted_Connection":"YES",
            "SourceUsername":"sa",
            "SourcePassword":"0000",
            "PublicHolidaySourceFile":"PublicHolidayTemplate_1.xlsx"
           
            }
        
        with open(ConfigJsonFile, 'w', encoding='utf-8') as f:
            json.dump(json_data, f,indent=4)
            
       
        file_exists = os.path.exists(ConfigJsonFile)
        if (file_exists==True):
            print("Config Json File Ready")
            writeLog("Create ConfigFile: "+ConfigJsonFile)
        else:
            print("Config Json File Create Error") 
            writeErrorLog("Config Json File Create Error: "+ConfigJsonFile) 
  
def readConfigFile():
    if not os.path.exists(ConfigJsonFile):
        createConfigFile()
    try:
        with open(ConfigJsonFile) as file:
            configData = json.load(file)           
        
    except json.decoder.JSONDecodeError:
        print("There was a problem accessing the config data.")
        writeErrorLog("Accessing the config data Error. "+ConfigJsonFile)
          
    return configData

    

  
def writeConfigFile(configData):
    if not os.path.exists(ConfigJsonFile):
        createConfigFile()
    with open(ConfigJsonFile, 'w', encoding='utf-8') as f:
        json.dump(configData, f,indent=4)
        
    print("Config Json File Write Done")
    writeLog("update ConfigFile Done" )
    
    return 1
   
  
    
def ConnectionTest():
    connectionTestResult = 0
    config=readConfigFile()
    SourceSQLServer = config["SourceSQLServer"].replace("\\\\","\\")
    SourceDatabase = config["SourceDatabase"]
    SourceUsername = config["SourceUsername"]
    SourcePassword = config["SourcePassword"]
    trustConnection = config["Trusted_Connection"]
    if trustConnection=="YES":
      connect_string="DRIVER={SQL Server}; SERVER="+SourceSQLServer+"; DATABASE="+SourceDatabase+"; Trusted_Connection=yes;"
    else:
      connect_string="DRIVER={SQL Server}; SERVER="+SourceSQLServer+"; DATABASE="+SourceDatabase+"; UID="+SourceUsername+"; PWD="+SourcePassword
    try:
        PassSQLServerConnection = pyodbc.connect(connect_string)
        print("{c} is working".format(c=connect_string))
        PassSQLServerConnection.close()
        connectionTestResult = 1
    except pyodbc.Error as ex:
        print("{c} is not working".format(c=connect_string))
        writeErrorLog("DB Connection Error: "+connect_string)
    
        
    return connectionTestResult
  




def ProcessExcelToSQL(filePath,ProcessDate,ProcessTime):

    try:
        df = pd.read_excel(filePath)
        
        print(f"find {len(df)} records in the Excel file.")
    except Exception as e:
        print(f"fail to read the excel : {str(e)}")
       
    
    sqlStatements = []
    sqlStatements.append("Update Profile set AutoSurcharge =1;")
    sqlStatements.append("DELETE from ChargeScope;")
    # Change to your database name
    for _, row in df.iterrows():
        # escape single ' in string values
        def escape_str(value):
            if pd.isna(value):
                return "NULL"
            return str(value).replace("'", "''")
        
        
        values = {
            'ChargeRate': row['ChargeRate'],
            'Model': row['Model'],
            'Frequency': escape_str(row['Frequency']),
            'StartTime': escape_str(ProcessDate+" "+row['StartTime']),
            'EndTime': escape_str(ProcessDate+" "+row['EndTime']),
            'ApplyOnDineIn': row['ApplyOnDineIn'],
            'ApplyOnTakeaway': row['ApplyOnTakeaway'],
            'ApplyOnQuickSale': row['ApplyOnQuickSale'],
            'ApplyOnDelivery': row['ApplyOnDelivery'],
            'ApplyOnPickup': row['ApplyOnPickup'],
            # convert ProcessTime to string and escape single quotes
            'CreatedAt': escape_str(ProcessTime),
            'UpdatedAt': escape_str(ProcessTime)
        }
        
        # create SQL INSERT statement
        sql = f"""
INSERT INTO ChargeScope (
    ChargeRate, Model, Frequency, StartTime, EndTime,
    ApplyOnDineIn, ApplyOnTakeaway, ApplyOnQuickSale,
    ApplyOnDelivery, ApplyOnPickup, CreatedAt, UpdatedAt
) VALUES (
    {values['ChargeRate']}, 
    {values['Model']}, 
    '{values['Frequency']}', 
    '{values['StartTime']}', 
    '{values['EndTime']}',
    {values['ApplyOnDineIn']}, 
    {values['ApplyOnTakeaway']}, 
    {values['ApplyOnQuickSale']},
    {values['ApplyOnDelivery']}, 
    {values['ApplyOnPickup']},
    '{values['CreatedAt']}', 
    '{values['UpdatedAt']}'
);"""

    
        sqlStatements.append(sql)
    
    return sqlStatements




def execute(sqlStatements):
  config=readConfigFile()
  sourceSQLServer = config["SourceSQLServer"].replace("\\\\","\\")
  sourceDatabase = config["SourceDatabase"]
  sourceUsername = config["SourceUsername"]
  sourcePassword = config["SourcePassword"]
  trustConnection = config["Trusted_Connection"]
  if trustConnection=="YES":
    conn = pyodbc.connect('DRIVER={SQL Server}; SERVER='+sourceSQLServer+'; DATABASE='+sourceDatabase+'; Trusted_Connection=yes;')
    
  else:
    conn = pyodbc.connect('DRIVER={SQL Server}; SERVER='+sourceSQLServer+'; DATABASE='+sourceDatabase+'; UID='+sourceUsername+'; PWD='+ sourcePassword)
  
  cursor = conn.cursor()
  
  try:
    for statement in sqlStatements:
      print("processed SQL statement: ", statement)
      cursor.execute(statement)
      conn.commit()
      
      
  except Exception as e:
    conn.rollback() # rollback if any error occurs
    print("error:",e)
  finally:
    cursor.close()  
  
  print("All SQL statements executed successfully.")
  writeLog("All SQL statements executed successfully.")
  






def systemRun():
  #system Init setting

  now = datetime.datetime.now()
  nDate = now.strftime('%Y%m%d')
  pDate = now.strftime('%Y-%m-%d')

  
  time= datetime.datetime.now().strftime('%H%M%S')
  # logFilename = nDate + '_' + time + '.txt'
  # lines = []
  # hRecord = {}
  currentTime=now.strftime('%Y-%m-%d %H:%M:%S')
  
  config=readConfigFile()
  
  downloadPublicHolidayExcelFromServer(config["PublicHolidaySourceFile"])
  
  newExcelFile=downloadFilePath+config["PublicHolidaySourceFile"]
  
 
  # Start Main Process
  compaireFlag = False
  
  if os.path.exists(newExcelFile): # if the file exists run process
    print("New Public Holiday List File: ",newExcelFile)
    if os.path.exists(savedFile):
      compaireFlag=detailed_excel_comparison(savedFile, newExcelFile)
      
    else:
      compaireFlag=False
      
    if compaireFlag==False:
      
        SqlList=ProcessExcelToSQL(newExcelFile,pDate, currentTime)
        execute(SqlList)
        print("Public Holiday List has been updated, start to process the new list.")
        writeLog("Public Holiday List has been updated, start to process the new list.")
        #update saved file
        if os.path.exists(savedFile):
          os.remove(savedFile)
        os.rename(newExcelFile, savedFile)
        
    if compaireFlag==True:
       
        print("Public Holiday List is the same, no need to update.")
        os.remove(newExcelFile)
        
      
  else:
    
    print("Public Holiday List file not found, please check the file path."+newExcelFile)
    writeErrorLog("Public Holiday List file not found, please check the file path: "+newExcelFile)

    
  
  
  
  
  
  
def inforProcess():
    connectionTestResult=0

    connectionTestResult=ConnectionTest()
    if connectionTestResult==1:
      print("")
      systemRun()
    else:
      print("DB Connection Error, Please check the connection setting. This program will exit.")
      writeErrorLog("DB Connection Error, Please check the connection setting, program Exit.")
      exit


  

def main(argv):

  inforProcess()
  
if __name__ == "__main__":
    main(sys.argv[1:])

