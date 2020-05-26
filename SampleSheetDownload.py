
import os, sys
import shutil
from shutil import copytree, Error
import boto3
from botocore.exceptions import ClientError
from os import path

#set variables
localFilePath = '\\sampleSheetArchive' #archive folder - files are downloaded from AWS to here
awsFileList = [] #list of files in genomics sample sheet s3 bucket
localFileList = [] #list of files in archive folder
genomicsFolderList = []

AWS_SHARED_CREDENTIALS_FILE = '\\Development\\primary\\projects\\genomicscore\\.aws\\credentials'
os.chdir("\\Development")

#get list of files from archive folder
def folder2(localFilePath):   
    print(localFilePath) 
    # if len(sys.argv) == 2:
    #         localFilePath = sys.argv[1]
    #         print("local file path in Folder2 functoin: ", localFilePath)
    files = os.listdir(localFilePath)  
    for name2 in files:
        localFileList.append(name2)

def folder3(localFilePath):   
    files = os.listdir(localFilePath)  
    #print(files)
    for name2 in files:
        if name2.endswith('temp'):
            localFileList.append(name2)

def genomicsFoldersList():
    nextGenFolder = os.path.join('C:','Development', 'primary','instruments', 'sequencing', 'illumina', 'incoming')
    print("NextGen folder path",nextGenFolder)
    folderPath = os.path.abspath('C:\\Development\\primary\\instruments\\illumina\\incoming')
    print('folder path is ',folderPath)
    folderList1 = os.listdir(folderPath)   
    print("folder list is: ", folderList1)
    # print("not freaking out so far")
    # #folderList2 = os.listdir('\\instruments\\sequencing\\iSeq')
    # #folderList3 = os.listdir('\\instruments\\sequencing\\novaseq')
    # for folder1 in folderList1:
    #     genomicsFolderList.append('\\instruments\\sequencing\\illumina\\incoming\\' + folder1)
    # for folder2 in folderList2:
    #     genomicsFolderList.append('\\instruments\\sequencing\\iSeq\\' + folder2)
    # for folder3 in folderList3:
    #     genomicsFolderList.append('\\instruments\\sequencing\\novaseq\\' + folder3)
    
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
    print("working directory: ", os.getcwd())
    folder2(localFilePath) #create list of files in local archive folder
    genomicsFoldersList()
    print("AWS List: ", awsFileList)
    print("Local List: ", localFileList)
    print("Folder List: ",genomicsFolderList)
    listDifferences = Diff(awsFileList, localFileList) #create a list of files that are not in both aws and archive folder    
    print('This is the difference: ', Diff(awsFileList, localFileList))#print for testing purposes
except Error as e:
    print(e)

try:
    #loop through the files that are not in both aws and archive and download to local folders
    for allfiles in listDifferences:
        localDownloadPath =localFilePath + '\\' + allfiles + '.temp' #set the local file path to include file name
        #print(localDownloadPath)
        s3.download_file('vai-diana-test', allfiles, localDownloadPath) #download the file from aws to archive folder       
except ClientError as e:
    print(e.response) #print error if fails
    sys.exit()

try:
    print("transferring files to new folders")
    for allfiles in listDifferences:     
        localDownloadPath =localFilePath + '\\' + allfiles + '.temp' #set the local file path to include file name
        fileNameExtRemoved = allfiles.rsplit('.',1)[0] #get folder name to place sample sheet by removing all characters after and including the '.'
        flowCellName = fileNameExtRemoved[fileNameExtRemoved.rindex('_')+1:]
        #find folder it belongs to and copy samplesheet.csv to that folder
        for folderName in genomicsFolderList: #for each folder in genomics sequencing folders
            if flowCellName in folderName: #check if flowcell name is in the name of the folder               
                pathForFile = '\\' + folderName  + '\\samplesheet.csv' #set the file path for the sample sheet
                print("moving ", allfiles, "to ", pathForFile, "from ", localDownloadPath)
                # dest = shutil.copy(localDownloadPath, pathForFile) # copy the file to the appropriate local folder (will change this to Marie's hpc node once I get this information)
except Error as e:
    print(e)    
    sys.exit()

try:
    if listDifferences != []:
        #print("IM IN")
        localFileList = []
        folder2(localFilePath)
        #print("New local file list: ", localFileList)

        for files in localFileList:
            if "temp" in files:
                originalFilePath = localFilePath + '\\' + files 
                #print(files, 'is here')
                newFilePath = localFilePath + '\\' + (files.rsplit('.',1)[0])
                #print(newFilePath)
                os.rename(originalFilePath, newFilePath)
            else:
                print(files, "is not here")
    else:
        print("there are no differences")
except Error as e:
    print(e)