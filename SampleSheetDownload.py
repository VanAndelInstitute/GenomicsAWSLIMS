
import os, sys
import shutil
from shutil import copytree, Error
import boto3
from botocore.exceptions import ClientError
from os import path

#set variables
baseWorkingDir = "/home/zack/limsAwsSync"
archivePath =  baseWorkingDir + '/sampleSheetArchive' #archive folder - files are downloaded from AWS to here
searchPaths = ["/primary/instruments/sequencing/illumina/incoming", "/primary/instruments/sequencing/novaseq", "/primary/instruments/sequencing/novaseq","/primary/instruments/sequencing/iSeq"]
AWS_SHARED_CREDENTIALS_FILE = 'baseWorkingDir' + '/.aws/credentials'

#print log messages
def logIt(*logMessage):
    sys.stderr.write(" ".join(logMessage) + "\n")

#get list of files from folder
def listDir(myPath):
    logIt("listing" , myPath)
    fileNames = os.listdir(myPath)
    logIt(fileNames)
    return fileNames

#compare the two lists and return a list of files that are different
def Diff(right, left):
    logIt("comparing", left, "to", right)
    logIt(list(set(right) - set(left)))
    return(list(set(right) - set(left)))

try:
    s3 = boto3.client('s3') #create service client
    resp = s3.list_objects_v2(Bucket = 'vai-diana-test') #list contents of s3 bucket
    awsFileList = []
    #create a list of files in s3 bucket
    for obj in resp['Contents']:
        awsFileList.append(obj['Key'])  

    os.chdir(baseWorkingDir)
    listDifferences = Diff(awsFileList, listDir(archivePath)) #create a list of files that are not in both aws and archive folder

    # loop through the files that are not in both aws and archive and download to local folders
    # TODO: handle when sampleSheet already exists. What if the sample sheet is updated? (we may need to check timestamps)
    for diffFile in listDifferences:
        localDownloadPath = archivePath + '/' + diffFile  # set the local file path to include file name
        s3.download_file('vai-diana-test', diffFile, localDownloadPath)  # download the file from aws to archive folder
        fileNameExtRemoved = diffFile.rsplit('.', 1)[
            0]  # get folder name to place sample sheet by removing all characters after and including the '.'
        flowCellName = fileNameExtRemoved[fileNameExtRemoved.rindex('_') + 1:]
        for myDir in searchPaths:
            for myDirEntry in listDir(myDir):
                if flowCellName in myDirEntry and os.path.isdir(myDir + "/" + myDirEntry) and not os.path.exists(
                        myDir + "/" + myDirEntry + "/" + "SampleSheet.csv"):  # check if flowcell name is in the name of the folder
                    logIt("copying", archivePath + "/" + diffFile, "to ",
                          myDir + "/" + myDirEntry + "/" + "SampleSheet.csv")
                    # dest = shutil.copy(archivePath + "/" + diffFile, ,myDir + "/" + myDirEntry + "/" + diffFile ) # copy the file to the appropriate local folder (will change this to Marie's hpc node once I get this information)

except ClientError as ce:
    print(ce.response)  # print error if fails
    sys.exit()
except Error as e:
    print(e)

