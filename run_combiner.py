"""
Script to automatically merge samples split across multiple lanes back into one
fastq file. It requires a configuration file in .yaml format as input.
The format of the config file should be as follows:

email:
    admin: email address
runs:
    in_folder: path/to/input_folder
    out_folder: path/to/output_folder
    keep_original_files: yes
logging:
    log_file_name: path/to/log_file
"""

import argparse
import re
import os
import subprocess
import glob
from email.mime.text import MIMEText
import logging
from time import strftime, gmtime
import yaml
from dask.compatibility import FileNotFoundError

def Check(dirPath, name):
    """
    Check for the existence of files. It will check for the list of completed runs, and any gzipped, merged files that the script wants to make.
    """
    if (name == ".completed"):
        if not (os.path.isfile(dirPath + name)):
            subprocess.call(["touch", dirPath + ".completed"])
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
        SendMail(subject, message, config_["email"]["admin"])

def SendMail(subject, message, address):
    """
    Send email from python.
    """
    msg = MIMEText(message)
    msg["From"] = "st-analysis-server@scilifelab.se"
    msg["To"] = address
    msg["Subject"] = subject
    p = subprocess.Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=subprocess.PIPE)
    p.communicate(msg.as_string())

def ListDirNoHidden(dirPath):
    """
    Return all non-hidden folders (no dot folders).
    """
    allPaths = []
    for fname in os.listdir(dirPath):
        path = os.path.join(dirPath, fname)
        if os.path.isdir(path):
            if not fname.startswith("."):
                allPaths.append(path)
    return allPaths

def GetCompleted(dirPath):
    """
    Get the completed runs before we start so that we don't try to process any run twice.
    """
    with open(os.path.join(dirPath,".completed"), "r") as f:
        completed_runs = [ line.strip() for line in f]
    return completed_runs


def UpdateCompleted(completed_dir, dirPath):
    """
    Update the completed runs list.
    """
    with open(os.path.join(dirPath,".completed"), "a") as f:
        f.write(completed_dir + "\n")


def Checkmd5(currentDir):
    """
    Check the md5 hash to make sure the files are fully transferred.
    """
    md5Status = subprocess.Popen(["/usr/local/bin/md5sum", "-c", "md5sums.txt"], cwd=currentDir)
    md5Status.wait()
    return md5Status.returncode
    
def FilesToBeMerged(currentDir):
    """
    Return a list of all the files in a directory that are to be merged. This list is then passed into the GroupSamples method.
    """
    return glob.glob1(currentDir[0], "*.gz")


def GroupSamples(currentDir, files):
    """
    Return a list of files that are to be merged into one file. This method takes as input a list of all the .gz files in a directory
    and returns a dictionary in the form {directory: [file list]} where [file list] is all the files to be merged into one file.
    """
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

def CheckPermissions(currentDir):
    """ Check that we have permission to read and write to the current directory
    """
    if os.access(currentDir, os.R_OK) and os.access(currentDir, os.W_OK):
        return (currentDir, True)
    return (currentDir, False)


def MergeFiles(currentDir, samples, keep_samples):
    """
    Merges the files that are passed in to the function. If the output file already exists, and the script is in this function, then the 
    output file is likely to be the result of a failed run of the script and can be safely removed (which the script does).
    """
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
            SendMail(subject, message, config_["email"]["admin"])
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
            
            delete_samples(currentDir, samples, keep_samples)
            
    except Exception as e: #The error catching code here catches all errors, as I don't know what errors the script might throw.
        logging.error("An error has occurred while running.")
        logging.error(e)

def delete_samples(currentDir, samples, keep_samples):
    if not keep_samples:
        pass

def default_logger(msg):
    """
    Create a default logging instance for when the config file is absent or malformed.
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(filename=os.path.join(current_dir, "run_combiner_config_error.log"), level = logging.INFO)
    logging.error(msg)
    print "A config file error has occurred. Please see the error message in {}.\n".format(os.path.join(current_dir,"run_combiner_config_error.log"))
    

def check_config(config_):
    """ Check that the config file is formatted properly"""
    
    if "email" not in config_:
        default_logger("Email field missing from config file. Please add a valid email field in the config file")
        raise KeyError
    else:
        if "admin" not in config_["email"]:
            default_logger("The email field is present, but missing the admin email. Please enter the admin email in the following format: admin: email@address")
            raise KeyError
    
    if "runs" not in config_:
        default_logger("Runs field missing from config file. Please add a valid runs field in the config file.")
        raise KeyError
    else:
        if "in_folder" not in config_["runs"]:
            default_logger("The input folder path is missing from the config file. Please enter the input folder path in the following format: in_folder: /path/to/input_folder")
            raise KeyError
        elif "out_folder" not in config_["runs"]:
            default_logger("The output folder path is missing from the config file. Please enter the output folder path in the following format: out_folder: /path/to/out_folder")
            raise KeyError
        elif "keep_original_files" not in config_["runs"]:
            default_logger("Please indicate if you would like to keep the original files. Valid entries are yes|no, True|False. Add it to the config file in he following format: keep_original_files: yes")
            raise KeyError
    
    if "logging" not in config_:
        default_logger("Logging field missing from config file. Please add a valid logging field in the config file.")
        raise KeyError
        if "log_file" not in config_["logging"]:
            default_logger("The log file name is missing from the config file. Please enter a valid log file name in the following format: log_file_name: file_name.log")
            raise KeyError 

def main(config_):
    if config_ is not None:
        try:
            check_config(config_)
        except KeyError as key_error:
            raise key_error
            
        Inbox = config_["runs"]["in_folder"]
        Check(Inbox, ".completed")
        
        logging.basicConfig(filename=os.path.join(config_["logging"]["log_file"]), level = logging.INFO)
        logging.info("Merging script started at {}".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        
        completed = GetCompleted(Inbox)
        directories = ListDirNoHidden(Inbox)
        permissions = [CheckPermissions(directory) for directory in directories if directory not in completed]
                       
        writable_directories = [directory for directory in permissions if True in directory]
        unwritable_directories = [directory for directory in permissions if (directory not in writable_directories and directory not in completed)]
        
        
        for directory in unwritable_directories:
            subject = "Unwritable directories that are not completed"
            message = "The directory {} is unwritable. Please look into this and fix.".format(directory[0])
            SendMail(subject, message, config_["email"]["admin"])
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
                            MergeFiles(key, item, config_["runs"]["keep_original_files"])
                    UpdateCompleted(directory[0], Inbox)
                    logging.info("Merging completed on {}".format(directory[0]))
            else:
                subject = "Non matching md5 sums"
                message = "The md5 sum for {} is not correct, either files are still copying or an error has occurred during copying.".format(directory[0])
                SendMail(subject, message, config_["email"]["admin"])
                logging.warning(message)

    logging.info("Merging script finished at {}".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config_file", help="Config file for the run combiner in .yaml format")
    args = parser.parse_args()
    
    try:
        with open(args.config_file, "r") as config_file_:
            config_ = yaml.load(config_file_)     
    except TypeError as missing_config:
        default_logger("Config file not supplied. Please supply one with the --config_file argument. {} UTC".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        raise missing_config
    except FileNotFoundError as file_not_found_error_:
        default_logger("The config file supplied cannot be found. Please check the path and / or that the name of the file is correct. {} UTC. The supplied file was {}".format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime()), args.config_file))
        raise file_not_found_error_
    
    main(config_)