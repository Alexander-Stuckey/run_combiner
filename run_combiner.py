import re
import os
import subprocess
import glob
from email.mime.text import MIMEText
import logging
from time import strftime, gmtime

'''
Check for the existence of files. It will check for the list of completed runs, and any gzipped, merged files that the script wants to make.
'''
def Check(dirPath, name):
    if (name == ".completed"):
        if not (os.path.isfile(dirPath + name)):
            subprocess.call(["touch", Inbox + ".completed"])
        else:
            pass
    elif (os.path.splitext(name)[1] == ".gz"):
        if (os.path.isfile(dirPath + "/" + name)):
            return True
        else:
            return False
    else:
        subject = "Script in odd location error"
        message = "The script has entered an area that it should never reach. Please investigate."
        logging.error(message)
        logging.error("dirPath is {}, name is {}".format(dirPath,name))
        SendMail(subject, message)

'''
Send email from python
'''
def SendMail(subject, message):
    msg = MIMEText(message)
    msg["From"] = "st-analysis-server@scilifelab.se"
    msg["To"] = "alexander.stuckey@scilifelab.se"
    msg["Subject"] = subject
    p = subprocess.Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=subprocess.PIPE)
    p.communicate(msg.as_string())

'''
Return all non-hidden folders (no dot folders)
'''
def ListDirNoHidden(path):
    allPaths = []
    for fname in os.listdir(Inbox):
        path = os.path.join(Inbox, fname)
        if os.path.isdir(path):
            allPaths.append(path)
    return allPaths

'''
Get the completed runs before we start so that we don't try to process any run twice.
'''
def GetCompleted():
    with open(Inbox + ".completed") as f:
        completed_runs = [ line.strip() for line in f]
    return completed_runs

'''
Update the completed runs list
'''
def UpdateCompleted(completed_dir):
    with open(Inbox+".completed", "a") as f:
        f.write(completed_dir + "\n")

'''
Check the md5 hash to make sure the files are fully transferred
'''
def Checkmd5(currentDir):
    md5Status = subprocess.Popen(["/usr/local/bin/md5sum", "-c", "md5sums.txt"], cwd=currentDir)
    md5Status.wait()
    return md5Status.returncode
    
'''
Return a list of all the files in a directory that are to be merged. This list is then passed into the GroupSamples method.
'''
def FilesToBeMerged(currentDir):
    return glob.glob1(currentDir[0], "*.gz")

'''
Return a list of files that are to be merged into one file. This method takes as input a list of all the .gz files in a directory
and returns a dictionary in the form {directory: [file list]} where [file list] is all the files to be merged into one file.
'''
def GroupSamples(currentDir, files):
    merge_list = {}
    read1 = []
    read2 = []
    samples = {}
    for one_file in files:
        sample = re.search("_(S\d+)_", one_file)
        samples.setdefault(sample.group(1), []).append(one_file)
        
    for key in samples:
        temp_read1=[]
        temp_read2 =[]
        for value in samples[key]:
            if re.search("_R1_", value):
                temp_read1.append(value)
            else:
                temp_read2.append(value)
        read1.append(temp_read1)
        read2.append(temp_read2)
    merge_list[currentDir[0]] = (read1,read2)    
    return merge_list

'''
Check that we have permissions in the directory
'''
def CheckPermissions(currentDir):
    if os.access(currentDir, os.R_OK) and os.access(currentDir, os.W_OK):
        return (currentDir, True)
    return (currentDir, False)

'''
Merges the files that are passed in to the function. If the output file already exists, and the script is in this function, then the 
output file is likely to be the result of a failed run of the script and can be safely removed (which the script does).
'''
def MergeFiles(currentDir, samples):

    exp_name = re.search("(.+?)_", samples[0])
    exp_sample = re.search("_(S\d+)_", samples[0])
    exp_read = re.search("_(R\d+)_", samples[0])
    merged_name = exp_name.group(1) + "_" + exp_sample.group(1) + "_" + exp_read.group(1) + ".fastq"
    outfolder = os.path.join(currentDir,"Merged")
    subprocess.Popen(["mkdir", "-p", outfolder], cwd=currentDir)
    
    try:
        file_exists = Check(outfolder, merged_name + ".gz")
        
        if (file_exists == True):
            subject = "File {}.gz exists, but an error previously stopped the script".format(merged_name)
            message = "The file {}.gz exists, but the script is trying to make it again. {}.gz will be removed before continuing.".format(merged_name,merged_name)
            logging.warning(message)
            SendMail(subject, message)
            subprocess.Popen(["rm", "-f", merged_name + ".gz"], cwd=outfolder)
        
        if merged_name + ".gz" in samples: #Probably depreciated now, with new merged folder.
            logging.warning("The sample {} is a remnant of a failed run, ignoring".format(merged_name + ".gz"))
        else:
            for sample in samples:
                subprocess.Popen(["touch", merged_name], cwd=outfolder)
                decompressed_file = subprocess.Popen(["/usr/bin/gzip", "-dc", sample], stdout=subprocess.PIPE ,cwd=currentDir)
                with open(os.path.join(outfolder, merged_name), "a") as f:
                    for line in decompressed_file.stdout.readlines():
                        f.write(line)
            
            subprocess.Popen(["gzip", merged_name], cwd=outfolder)
    
    except Exception as e: #The error catching code here catches all errors, as I don't know what errors the script might throw.
        logging.error("An error has occurred while running.")
        logging.error(e)


'''
Run the script
'''

Inbox = "/Users/alexanderstuckey/Projects/LiClipse_Workspace/Run_Combiner/"
Check(Inbox, ".completed")

logging.basicConfig(filename=os.path.join(Inbox, "Sample_merger.log"), level = logging.INFO)
logging.info("Merging script started at {}".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))

completed = GetCompleted()
directories = ListDirNoHidden(Inbox)
permissions = [CheckPermissions(directory) for directory in directories if directory not in completed]

writable_directories = [directory for directory in permissions if True in directory]
unwritable_directories = [directory for directory in permissions if (directory not in writable_directories and directory not in completed)]

for directory in unwritable_directories:
    subject = "Unwritable directories that are not completed"
    message = "The directory {} is unwritable. Please look into this and fix.".format(directory[0])
    SendMail(subject, message)
    logging.warning(message)

for directory in writable_directories:
    logging.info("Working on {}".format(directory[0]))
    md5status = Checkmd5(directory[0])

    if (md5status == 0):
        logging.info("md5sum for {} is good, proceeding".format(directory[0]))
        files_for_merging = FilesToBeMerged(directory)
        sample_groups = GroupSamples(directory, files_for_merging)
        for key in sample_groups:
            for value in sample_groups[key]:
                for item in value:
                    MergeFiles(key, item)
        UpdateCompleted(directory[0])
        logging.info("Merging completed on {}".format(directory[0]))
    else:
        subject = "Non matching md5 sums"
        message = "The md5 sum for {} is not correct, either files are still copying or an error has occurred during copying.".format(directory[0])
        SendMail(subject, message)
        logging.warning(message)

logging.info("Merging script finished at {}".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))