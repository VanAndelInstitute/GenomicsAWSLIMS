
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

try:
    os.chdir(baseWorkingDir)
    s3 = boto3.client('s3') #create service client
    resp = s3.list_objects_v2(Bucket = 'vai-diana-test') #list contents of s3 bucket

    # TODO: handle when sampleSheet already exists. What if the sample sheet is updated? (we may need to check timestamps)
    for diffFileObject in  resp['Contents']:
        diffFile = diffFileObject['Key']
        if not os.path.exists(archivePath + "/" + diffFile): #or if age of diffFileObject['LastModified'] is newer than age of archivePath + "/" + diffFile
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

