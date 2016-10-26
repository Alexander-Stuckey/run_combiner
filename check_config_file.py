"""
Script to parse config files used in the run combiner. This script is not meant
 to be called as a standalone script, rather it is called from the run combiner
  script.
"""

import argparse

def check_config(config_):
    """ Check that the config file is formatted properly"""
    
    if "email" not in config_:
        raise KeyError("Email field missing from config file. Please add a "
        "valid email field in the config file")
    else:
        if config_["email"] is None:
            raise KeyError("The email section of the config file is blank. "
                           "Please fill in the section.")
        else:
            if "admin" not in config_["email"]:
                raise KeyError("The email field is present, but missing the "
                               "admin email. Please enter the admin email in "
                               "the following format: admin: email@address")
            if "parse_ss" not in config_["email"]:
                raise KeyError("Please indicate if you want to parse the "
                               "sample sheet for the investigator email. "
                               "Valid entries are yes|no, True|False.")
        
    if "runs" not in config_:
        raise KeyError("Runs field missing from config file. Please add a "
        "valid runs field in the config file.")
    else:
        if config_["runs"] is None:
            raise KeyError("The runs section of the config file is blank. "
            "Please fill in the section.")
        else:
            if "in_folder" not in config_["runs"]:
                raise KeyError("The input folder path is missing from the config "
                "file. Please enter the input folder path in the following format: "
                "in_folder: /path/to/input_folder")
            elif "out_folder" not in config_["runs"]:
                raise KeyError("The output folder path is missing from the config "
                "file. Please enter the output folder path in the following format:"
                " out_folder: /path/to/out_folder")
            elif "keep_original_files" not in config_["runs"]:
                raise KeyError("Please indicate if you would like to keep the "
                "original files. Valid entries are yes|no, True|False. Add it to the"
                " config file in he following format: keep_original_files: yes")
            elif "runs_folder" not in config_["runs"]:
                raise KeyError("The runs folder is missing from the config file. "
                               "Please enter the runs folder in the following "
                               "format: runs_folder: /path/to/runs_folder")
        
    if "logging" not in config_:
        raise KeyError("Logging field missing from config file. Please add a "
        "valid logging field in the config file.")
    else:
        if config_["logging"] is None:
            raise KeyError("The logging section of the config file is blank. " 
            "Please fill in the section")
        else:
            if "log_file" not in config_["logging"]:
                raise KeyError("The log file name is missing from the config "
                                "file. Please enter a valid log file name in "
                                "the following format: "
                               "log_file_name: file_name.log")
    if "verify_transfer" not in config_:
        raise KeyError("Verify_transfer field missing from config file. Please "
                       " add a valid verify_transfer field in the config file.")
    else:
        if config_["verify_transfer"] is None:
            raise KeyError("The verify_transfer field of the logging file is "
                           "blank. Please fill in the section")
        else:
            if "use_md5" not in config_["verify_transfer"]:
                raise KeyError("Please indicate if you want to use md5 sums to "
                               "verify file transfer. Valid entries are yes|no "
                               "True|False. Will use file modification time if "
                               "no")
            if "md5_path" not in config_["verify_transfer"]:
                raise KeyError("Please enter the path to the md5sum program. "
                               "On Linux this is usually /usr/bin/md5sum")
        
def main(config_file):
    
    print ("This is not meant to be called as a standalone script.")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    main()