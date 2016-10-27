"""
Script to automatically merge samples split across multiple lanes back into one
fastq file. It requires a configuration file in .yaml format as input.
The format of the config file should be as follows:

email:
    admin: email address
runs:
    runs_folder: /path/to/input_folders
    in_folder: input_folder_name
    out_folder: output_folder_name
    keep_original_files: yes
logging:
    log_file_name: path/to/log_file
verify_transfer:
    use_md5: yes
    md5_path: /usr/local/bin/md5sum
"""

import sys
import argparse
import re
import os
import subprocess
import glob
import logging
import yaml
from email.mime.text import MIMEText
from time import strftime, gmtime, time
from matplotlib.compat.subprocess import CalledProcessError
from check_config_file import check_config

def check(dirPath, name):
    """
    Check for the existence of files. It will check for the list of completed 
    runs, and any gzipped, merged files that the script wants to make.
    """
    logging.info("Checking for the existance of {}".format(
        os.path.join(dirPath,name)))
    if os.path.isfile(os.path.join(dirPath,name)):
        return True
    else:
        return False

def send_mail(subject, message, address):
    """
    Send email from python.
    """
    msg = MIMEText(message)
    msg["From"] = "st-analysis-server@scilifelab.se"
    msg["To"] = address
    msg["Subject"] = subject
    p = subprocess.Popen(["/usr/sbin/sendmail", "-t", "-oi"],
                         stdin=subprocess.PIPE)
    p.communicate(msg.as_string())
    logging.info("Email sent to {}".format(address))

def list_dir_no_hidden(dirPath):
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

def get_completed(dirPath):
    """
    Get the completed runs before we start so that 
    we don't try to process any run twice.
    """
    with open(os.path.join(dirPath,".completed"), "r") as f:
        completed_runs = [ line.strip() for line in f]
    return completed_runs


def update_completed(completed_dir, dirPath):
    """
    Update the completed runs list.
    """
    with open(os.path.join(dirPath,".completed"), "a") as f:
        f.write(completed_dir + "\n")

def check_md5(currentDir,md5_path):
    """
    Check the md5 hash to make sure the files are fully transferred.
    """
    logging.info("Checking the md5 status of files in {}".format(currentDir))
    md5Status = subprocess.Popen([md5_path, "-c", "md5sums.txt"],
                                 cwd=currentDir)
    md5Status.wait()
    return md5Status.returncode

def check_timestamps(current_dir,time_):
    file_list = files_to_be_merged(current_dir)
    fails_ = 0
    for file_ in file_list:
        mtime = os.path.getmtime(os.path.join(current_dir,file_))
        if (time_ - mtime) < 7200:
            fails_ += 1
    return fails_
    
def files_to_be_merged(currentDir):
    """
    Return a list of all the files in a directory that are to be merged.
    """
    return glob.glob1(currentDir, "*.gz")


def group_samples(currentDir, files):
    """
    Return a list of files that are to be merged into one file. This method
    takes as input a list of all the .gz files in a directory and returns a 
    dictionary in the form {directory: [file list]} where [file list] is all 
    the files to be merged into one file.
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

def check_permissions(currentDir):
    """
    Check that we have permission to read and write to the current directory
    """
    if os.access(currentDir, os.R_OK) and os.access(currentDir, os.W_OK):
        return (currentDir, True)
    return (currentDir, False)


def merge_files(currentDir, samples, keep_samples, email_address, in_folder,
                out_folder, ss_info):
    """
    Merges the files that are passed in to the function. If the output file 
    already exists, and the script is in this function, then the  output file 
    is likely to be the result of a failed run of the script and can be safely 
    removed.
    """
    logging.info("Merging files in {}".format(in_folder))
    if not os.path.isdir(os.path.join(currentDir, out_folder)):
        os.mkdir(os.path.join(currentDir, out_folder))
    
    exp_name = re.search("(.+?)_", samples[0])
    exp_sample = re.search("_S(\d+)_", samples[0])
    exp_read = re.search("_(R\d+)_", samples[0])
    merged_name = (exp_name.group(1) + "_" + ss_info[int(exp_sample.group(1))-1] +
                   "_" + exp_read.group(1) + ".fastq")
        
    infolder = os.path.join(currentDir,in_folder)
    outfolder = os.path.join(currentDir, out_folder)
    
    logging.info("Writing output to {}.".format(os.path.join(outfolder,
                                                             merged_name)))
    
    fastq_exists = check(outfolder, merged_name)
    gz_exists = check(outfolder, merged_name + ".gz")
    
    if fastq_exists:
        subject = ("File {} exists, but an error previously stopped the "
                   "script".format(merged_name))
        message = ("The file {} exists, but the script is trying to make"
        " it again. {} will be removed before continuing."
        .format(merged_name,merged_name))
        logging.warning(message)
        send_mail(subject, message, email_address)
        subprocess.Popen(["rm", "-f", merged_name], cwd=outfolder)
    elif gz_exists:
        subject = ("File {}.gz exists, but an error previously stopped the "
                   "script".format(merged_name))
        message = ("The file {}.gz exists, but the script is trying to make"
        " it again. {}.gz will be removed before continuing."
        .format(merged_name,merged_name))
        logging.warning(message)
        send_mail(subject, message, email_address)
        subprocess.Popen(["rm", "-f", merged_name + ".gz"], cwd=outfolder)
    else:
        logging.info("Beginning merging on {} at {}.".format(samples,
                                strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        for sample in samples:
            logging.info("Merging sample {}...".format(sample))
            with open(os.path.join(outfolder, merged_name), "a+") as outfile:
                decompress = subprocess.Popen(["/usr/bin/gzip", "-dc", sample],
                                 stdout=outfile, cwd=infolder)
                decompress.wait()
                logging.info("Merging for sample {} finished at {}.".format(
                        sample,strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        logging.info("Starting compression on {} at {}.".format(merged_name,
                                strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        compress = subprocess.Popen(["gzip", merged_name], cwd=outfolder)
        compress.wait()
        logging.info("Finished compression on {} at {}.".format(merged_name,
                                strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        delete_samples(infolder, samples, keep_samples)

def delete_samples(currentDir, samples, keep_samples):
    if not keep_samples:
        [subprocess.Popen(["rm", "-f", sample], cwd=currentDir) for sample
         in samples]
        
def parse_sample_sheet(currentDir,admin_email,use_ss_email):
    """
    Parse the sample sheet in the run directory to extract the sample names
    (to cover cases where the sample sheet has been incorrectly filled out and
    information is missing from the fastq file names). Additionally, the 
    investigator email can be obtained so that an email can be sent when the 
    merging finishes.
    """
    found_data = False
    samples = []
    email_ = admin_email
    try:
        with open(os.path.join(currentDir,"SampleSheet.csv"),"r") as sample_sheet:
            for line in sample_sheet:
                if use_ss_email:
                    email_ = re.search("^Investigator Name,(.+)", line)
                if not found_data:
                    if re.search("[Data]", line):
                        found_data = True
                if found_data:
                    if line.startswith("1"):
                        samples.append(line.split(",")[1] + "_" +
                                       line.split(",")[2])
    except IOError:
        logging.error("The samplesheet does not exist in {}, or an error "
                      "occurred while trying to open it. Defaulting to admin "
                      "email {}.".format(currentDir,admin_email)) 
    samples_and_email = (samples,email_)
    return samples_and_email

def default_logger(msg):
    """
    Create a default logging instance for when the config file is 
    absent or malformed.
    """
    current_dir = os.path.dirname(os.path.realpath(__file__))
    logging.basicConfig(filename=os.path.join(current_dir,
                                              "run_combiner_config_error.log"),
                                              level = logging.INFO)
    logging.error(msg)
    print ("A config file error has occurred. Please see the error message in "
    "{}.\n".format(os.path.join(current_dir,"run_combiner_config_error.log")))

def main(config_file):
    
    if not os.path.isfile(config_file):
        default_logger("Config file not supplied. Please supply one with the "
        "--config_file argument. {} UTC"
        .format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
        sys.exit(1)
    
    try:
        with open(config_file, "r") as config_file_:
            config_ = yaml.load(config_file_)     
    except Exception as e:
        default_logger("There was an error parsing the config file. {} at {}"
                       .format(e.message, strftime("%H:%M:%S, %A, %B %d, %Y",
                                                   gmtime())))
        sys.exit(1)
    try:
        check_config(config_)      
    except KeyError as e:
        print (e.message)
        sys.exit(1)
    
    logging.basicConfig(filename=os.path.join(config_["logging"]["log_file"]),
                        level = logging.INFO)
    logging.info("\n")
    logging.info("Merging script started at {}".
                 format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))

    logging.info("Config file loaded and parsed successfully at {}."
                       .format(strftime("%H:%M:%S, %A, %B %d, %Y",gmtime())))      
    Inbox = config_["runs"]["runs_folder"]
    logging.info("Base runs folder is {}".format(Inbox))
    runs_file = check(Inbox, ".completed")
    if not runs_file:
        open(os.path.join(config_["runs"]["runs_folder"],".completed"),
             "a+").close() 
    completed = get_completed(Inbox)
    directories = list_dir_no_hidden(Inbox)
    permissions = [check_permissions(directory) for directory in directories 
                   if directory not in completed]
                   
    writable_directories = [directory for directory in permissions 
                            if True in directory]
    unwritable_directories = [directory for directory in permissions 
                              if (directory not in writable_directories 
                                  and directory not in completed)]
    
    
    for directory in unwritable_directories:
        subject = "Unwritable directories that are not completed"
        message = "The directory {} is unwritable. ".format(directory[0])
        send_mail(subject, message, config_["email"]["admin"])
        logging.warning(message)
    
    logging.info("List of runs to merge: {}".format([dir_[0] for dir_ in 
                                                     writable_directories]))
    for directory in writable_directories:
        logging.info("Working on {}".format(directory[0]))
        
        md5status = 1
        timestamp = 1
        
        try:
            if config_["verify_transfer"]["use_md5"]:
                logging.info("Using md5sums to verify transfer.")
                md5status = check_md5(os.path.join(directory[0],
                                               config_["runs"]["in_folder"]),
                                      config_["verify_transfer"]["md5_path"])
            else:
                logging.info("Using timestamps to verify transfer.")
                timestamp = check_timestamps(os.path.join(directory[0],
                                              config_["runs"]["in_folder"]),
                                             time())
            
            if md5status == 0 or timestamp == 0:
                if md5status == 0:
                    logging.info("md5sum for {} is good, proceeding"
                             .format(directory[0]))
                else:
                    logging.info("Timestamps for {} are good, proceeding"
                                 .format(directory[0]))
                files_for_merging = files_to_be_merged(os.path.join(
                    directory[0],config_["runs"]["in_folder"]))
                ss_info = parse_sample_sheet(directory[0],
                                             config_["email"]["admin"],
                                             config_["email"]["use_ss_email"])
                sample_groups = group_samples(directory, files_for_merging)
                for key in sample_groups:
                    for value in sample_groups[key]:
                        for item in value:
                            merge_files(key, item,
                                        config_["runs"]["keep_original_files"],
                                        config_["email"]["admin"],
                                        config_["runs"]["in_folder"],
                                        config_["runs"]["out_folder"],
                                        ss_info[0])
                    update_completed(directory[0], Inbox)
                    logging.info("Merging completed on {}".format(directory[0]))
            else:
                subject = "Non matching md5 sums or timestamps too young"
                message = ("Either the md5 sum for {} is not correct, or the "
                           "timestamp is too recent. Files are still copying "
                           "or an error has occurred during copying."
                           .format(directory[0]))
                send_mail(subject, message, config_["email"]["admin"])
                logging.warning(message)
        except ValueError:
            logging.error("Popen has been called with invalid arguments. {} UTC"
                          .format(strftime("%H:%M:%S, %A, %B %d, %Y",gmtime())),
                          exc_info = True)
        except OSError:
            logging.error("An error has occurred. Working in directory {}"
                          .format(directory[0]), exc_info = True)
        except CalledProcessError:
            logging.error("The called system process has exited with an error ",
                          exc_info = True)
        except IOError:
            logging.error("The directory that the program is trying to write "
                          "to does not exist. {}."
                          .format(strftime("%H:%M:%S, %A, %B %d, %Y",
                                           gmtime())), exc_info = True)
        
        subject = "Run finished merging."
        msg = ("The run {} has finished merging. Feel free to start work on "
               "it at any time.".format(directory[0]))
        send_mail(subject, msg, ss_info[1])
        
    logging.info("Merging script finished at {}\n"
                 .format(strftime("%H:%M:%S, %A, %B %d, %Y", gmtime())))
      
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config_file", help=("Config file for the run "
                                               "combiner in .yaml format"))
    args = parser.parse_args()
    
    main(args.config_file)