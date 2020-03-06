
import os, sys
import shutil
from shutil import copytree, Error
import boto3
from botocore.exceptions import ClientError

#set variables
localFilePath = '\sampleSheetArchive' #archive folder - files are downloaded from AWS to here
awsFileList = [] #list of files in genomics sample sheet s3 bucket
localFileList = [] #list of files in archive folder

#get list of files from archive folder
def folder2(localFilePath):   
    #print(localFilePath) 
    if len(sys.argv) == 2:
            localFilePath = sys.argv[1]

    files = os.listdir(localFilePath)
    print(files)
    for name2 in files:
        localFileList.append(name2)

#compare the two lists and return a list of files that are different 
def Diff(awsFileList, localFileList):
        return(list(set(awsFileList) - set(localFileList)))

try:
    s3 = boto3.client('s3') #create service client

    resp = s3.list_objects_v2(Bucket = 'vai-diana-test') #list contents of s3 bucket

    #create a list of files in s3 bucket
    for obj in resp['Contents']:
        awsFileList.append(obj['Key'])   

except ClientError as e:   
    print(e.response) #print error if fails 
    sys.exit()
    
try:
    folder2(localFilePath) #create list of files in local archive folder

    listDifferences = Diff(faile, localFileList) #create a list of files that are not in both aws and archive folder
    
    print(Diff(awsFileList, localFileList))#print for testing purposes
except Error as e:
    print(e.response)
     #Would like to set up a notification email here

try:
    #loop through the files that are not in both aws and archive and download to local folders
    for allfiles in listDifferences:
        localDownloadPath =localFilePath + '\\' + allfiles + '.temp' #set the local file path to include file name
        folderName = allfiles.rsplit('.',1)[0] #get folder name to place sample sheet by removing all characters after and including the '.'
        print(folderName)
        pathForFile ='\\' + folderName + '\\samplesheet.csv' #set the file path for the sample sheet
        s3.download_file('vai-diana-test', allfiles, localDownloadPath) #download the file from aws to archive folder       
except ClientError as e:
    print(e.response) #print error if fails
    sys.exit()
try:
    for allfiles in listDifferences:
        localDownloadPath =localFilePath + '\\' + allfiles + '.temp' #set the local file path to include file name
        folderName = allfiles.rsplit('.',1)[0] #get folder name to place sample sheet by removing all characters after and including the '.'
        print(folderName)
        pathForFile ='\\' + folderName + '\\samplesheet.csv' #set the file path for the sample sheet
        dest = shutil.copy(localDownloadPath, pathForFile) # copy the file to the appropriate local folder (will change this to Marie's hpc node once I get this information)
except ClientError as e:
        print(e.response)
        sys.exit()

