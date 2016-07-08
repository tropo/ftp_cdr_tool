# ftp_cdr_tool
Download account log files from Tropo, and Parse out the CDRs into text files and a CSV


Call Detail Records (CDR) are useful for gleaning information about your account’s voice and SMS traffic.  They contain the callerID, the calledID, duration, channel/network, disposition and more.  Tropo provides the CDR information in your account logs, and they are available from the web portal or through FTP (ftp.tropo.com).  However, since these logs also contain lots of additional information about the application session, they can be a little overwhelming, and too verbose to easily search through manually.  We’ll show you how a simple program can solve that issue, and provide you with the full code to run it yourself.  Some modifications may be desired, but this walkthrough will provide a good starting point to get all of the CDRs, for every application in your Tropo account, for the last sixty days.

This tool requires knowledge of the command terminal, and Python (~2.7) needs to be installed in order to run it out of the box; no additional Python packages are required. It will use ftp.tropo.com (using your tropo.com username and password) to download your account log files.
