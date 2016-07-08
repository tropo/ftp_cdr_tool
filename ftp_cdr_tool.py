import gzip
import json
import os
import sys
import time
from ftplib import FTP_TLS

class DLRTool(object):
    """
    This tool:
        -downloads all .txt and .gz log files from FTP site (ftp.tropo.com)
            *only if the file has updated/changed size since last download
            *downloads to local_working_dir
        -extracts the *.gz files, still in local_working_dir
        -parses the .txt files for CDRs (DLRs)
            *cleans the CDR for human readability 
        -writes the new parsed file to the local_dir
        -optionally create a CSV file if passed in at command line:
            *will create output.csv
                python ftp_cdr_tool.py output.csv
            *will create output.csv
                python ftp_cdr_tool.py output
            *will NOT create any csv
                python ftp_cdr_tool.py
    """
    
    #Used for translating the provider's returned SMS status code to be human readable in the parsed txt files and CSV
    statusCodes = {
        0:"Delivered",
        -1:"Bad or Unsupported Phone Number",
        -2:"Carrier Error",
        -3:"Gateway Error",
        -4:"Exceeded Rate Limit",
        -5:"Duplicate Message",
        -7:"Blocked",
        -99:"Unknown"
        }
    
    #Used for translating the provider's returned SMS status code to be human readable in the parsed txt files and CSV
    #This is not a complete HTTP response code list, can be updated.
    responseCodes = {
        200:"OK",
        400:"Bad Request",
        401:"Not Authorized",
        403:"Access Denied",
        404:"Not Found",
        405:"Method Not Allowed",
        415:"Unsupported Media Type",
        500:"Internal Server Error",
        503:"Service Unavailable",
        408:"User Unavailable",
        484:"Number Unsupported",
        487:"Request Terminated",
        -1 :"Delivered Successfully",
        }
        
    #Less frequently desired attributes commented out below.
    CDRAttributes = [
        "AccountID",
        "ApplicationId",
        #"ApplicationType",
        #"BrowserIP",
        "Called",
        "Caller",
        #"CallID",
        "Channel",
        "DateCreated", #DO NOT REMOVE OR COMMENT OUT, "DateCreated" is used for CSV append comparison
        "DateUpdated",
        "DeliveryStatus",
        "Duration",
        "EndTime",
        #"Flags",
        #"ID",
        "MessageBody",
        #"MessageCount",
        "Network",
        #"ParentCallID",
        #"ParentSessionID",
        #"PhoneNumberSid",
        #"PPID",
        #"ProviderID",
        #"ProviderName",
        #"RecordingDuration",
        "ResponseCode",
        #"ServiceId",
        "SessionID",
        #"SipSessionID",
        "StartTime",
        #"StartUrl",
        "Status",
        "StatusCode"]
        
    def __init__(self, host, username, password, output_csv=None, remote_dir="logs", local_working_dir="workinglogs", local_dir="parsedlogs", logging=True):
        self.hostname = host #FTP hostname to download log files from, like "ftp.tropo.com"
        self.username = username #FTP username
        self.password = password #FTP password
        
        self.remote_dir = remote_dir #FTP remote working dir
        self.local_working_dir = local_working_dir#local directory where log files stored prior to processing
        self.local_dir = local_dir #Final parsed log file destination
        self.needed_files = []
        
        if not os.path.exists(self.local_working_dir):
            os.makedirs(self.local_working_dir)#create the local directory if it does not exist
        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)#create the local directory if it does not exist
        if output_csv != None:#used to keep an ongoing csv file if the same filename and location are passed with each run
            self.csv = output_csv
            csv_path = os.path.split(output_csv)
            if len(csv_path[0]) > 0 and not os.path.exists(csv_path[0]):
                os.path.makedirs(csv_path[0])
        else:
            self.csv = None
            
        self.stored_files_list = os.listdir(self.local_working_dir)
        self.logging = logging
        self.date_field = "DateCreated"#Could potentially be DateUpdated, StartTime, EndTime
        if self.csv != None:
            self.date_compare_index = self.CDRAttributes.index(self.date_field)#let this fail for csvs if no date to compare
        else:
            self.date_compare_index = -1
    
    
    def get_file_attrs(self, ftp_file_str):
        """
        Determines the FTP file name and file size from a
        RETR command return line
        """
        file_attrs = ftp_file_str.split()
        name = file_attrs[len(file_attrs)-1] #return last item, which is the file name
        size = int(file_attrs[len(file_attrs)-5]) #return fifth to last item, which is the file size in bytes
        return name, size
    
    
    def ftp_list_callback(self, file_line):
        """
        determines the needed_files from an FTP RETR command
            -uses the FTP file size information
            -compares against local file size on disk
            -downloads and replaces file on disk if different.
        """
        file_name, file_size = self.get_file_attrs(file_line)
        if file_name not in self.stored_files_list:
            if self.logging:
                print "file {0} missing, adding to list".format(file_name)
            self.needed_files.append(file_name)
        else:
            file_size_on_disk = os.path.getsize(os.path.join(self.local_working_dir, file_name)) 
            if file_size != file_size_on_disk:
                if self.logging:
                    print "file {0} FTP size {1} does not equal {2} size on disk, adding to list".format(file_name, file_size, file_size_on_disk)
                self.needed_files.append(file_name)
                
    
    def sync(self):
        """
        downloads all needed_files from self.hostname (FTP)
        of the downloaded files, extracts .gz files to same local_working_dir
            -using self.extract function
        parses the .txt downloaded needed_files
            -using the self.parse function
        """
        ftps = FTP_TLS(self.hostname) # connect to host, default port
        ftps.login(self.username, self.password)
        ftps.prot_p()
        ftps.cwd(self.remote_dir) # change into "logs" directory
        ftps.retrlines('LIST *.gz *.txt', self.ftp_list_callback) # list directory contents
        for needed_file in self.needed_files:
            if self.logging:
                print "Writing {0} to {1}...".format(needed_file, self.local_working_dir)
            ftps.retrbinary("RETR " + needed_file, open(os.path.join(self.local_working_dir, needed_file), 'wb').write)
        if self.logging:
            print "done syncing files"
        for needed_file in self.needed_files:
            if needed_file.endswith(".gz"):
                self.extract(os.path.join(self.local_working_dir, needed_file))
            txt_file_name = needed_file.replace('.gz','')#if already a .txt file, this is unnceccessary but works.
            self.parse(txt_file_name)
        if self.logging:
            print "done extracting/parsing .gz files"
        ftps.quit()
    
    
    def extract(self, file_name):
        """
        Function extracts file_name with .gz extension into same directory as .gz file_name
        """
        out_file_name = file_name.replace('.gz','')
        if self.logging:
            print "Extracting {0} to {1}...".format(file_name, out_file_name)
        with gzip.open(file_name, 'rb') as infile:
            with open(out_file_name, 'w') as outfile:
                for line in infile:
                    outfile.write(line)
    
    
    def parse(self, file_name):
        """
        This function will parse all data in a specific prism log file entry and collect all CDRs for SMS messages and calls
        """
        results = []
        CDRs = []
        parse_file_name = os.path.join(self.local_working_dir, file_name)
        final_file_name = os.path.join(self.local_dir, file_name)
        with open(parse_file_name) as f:
            if self.logging:
                print 'Parsing {0} and saving to {1}"'.format(file_name, final_file_name)
            for line in f:
                if "Submitting CDR [text=" in line:
                    cdr_str = line.split("Submitting CDR [text=")[1].strip()#remove white space and new lines
                    cdr_str = cdr_str[:len(cdr_str)-1]#remove the last "]" char from the [text=, in order to make JSON
                    cdr_dict = json.loads(cdr_str)
                    CDRs.append(cdr_dict["call"])
        
        if self.csv != None and not os.path.exists(self.csv):
            #If the CSV does not exist, need to create it and write the headers
            with open(self.csv, "w") as csv:
                write_str = self.CDRAttributes[0]#this will break if no CDR attributes exist, but that seems silly to handle for
                for attr in self.CDRAttributes[1:]:
                    write_str += ","+attr   
                csv.write(write_str+"\n")
                
        with open(final_file_name, "w") as out_f:
            for cdr in CDRs:
                tempDictionary = {}
                for attr in self.CDRAttributes:
                    if cdr.get(attr) != None:
                        if attr == "StatusCode":
                            tempDictionary.update({attr: self.statusCodes.get(cdr[attr], cdr[attr])})#.get(cdr[attr]... will attempt to get pretty name, and use code if not found
                        elif attr == "ResponseCode":
                            tempDictionary.update({attr: self.responseCodes.get(cdr[attr], cdr[attr])})#.get(cdr[attr]... will attempt to get pretty name, and use code if not found
                        else:
                            tempDictionary.update({attr: cdr[attr]})
                write_str = tempDictionary[self.date_field] + " - "
                write_str += json.dumps(tempDictionary)+"\n"
                out_f.write(write_str)
                if self.csv != None:
                    with open(self.csv, "r") as csv_r:
                        lines = csv_r.readlines()
                        if len(lines) > 1:#if it's more than just headers
                            last_line = lines[-1]# read the last line to compare times.
                            last_line_list = last_line.split(',')
                            last_date = last_line_list[self.date_compare_index]
                            last_date_time = time.strptime(last_date, "%a  %d %b %Y %H:%M:%S +0000")
                        else:
                            last_date_time = 0
                    this_date_time = None
                    if tempDictionary.get(self.date_field, None) != None:
                        this_date_time = time.strptime(tempDictionary[self.date_field].replace(","," "), "%a  %d %b %Y %H:%M:%S +0000")
                    if this_date_time == None or this_date_time > last_date_time:
                        with open(self.csv, "a") as csv:
                            csv_write_str = ""
                            for attr in self.CDRAttributes: # this could probably be a little faster if handled in the previous loop
                                csv_write_str += str(tempDictionary.get(attr,"")).replace(","," ") + ","
                            csv_write_str = csv_write_str.rstrip(',') + "\n"#remove trailing comma
                            csv.write(csv_write_str)
                        


if __name__ == "__main__":
    hostname = 'ftp.tropo.com'
    username = raw_input("Please enter your tropo.com username: ")#Can change to a string and set TROPO USERNAME HERE
    password = raw_input("Please enter your tropo.com password: ")#Can change to a string and set TROPO PASSWORD HERE

    output_csv = None
    if len(sys.argv) > 1:#user can enter a CSV file to output new logs to
        output_csv = sys.argv[1]
        if not output_csv.endswith(".csv"):
            output_csv += ".csv"
    #print output_csv
    
    dlr_tool = DLRTool(hostname, username, password, output_csv)
    if username != "":
        if password != "":
            dlr_tool.sync()
        else:
            print "Failed: password has not been set (use tropo.com password)"
    else:
        print "Failed: username has not been set (use tropo.com username)"