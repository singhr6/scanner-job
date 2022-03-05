'''
This script reads jobs name from config.yaml file and check status of last run of each job in the mongodb job status collection. if  job did not run last time,
then script send alert to alertmanager. Alertmanager sends the required notification (email/slack). This job will run on a separate server than the other jobs.
This is to take care of situation where a VM crashes and all jobs running on the VM fail. All jobs update status in mongodb and grafana. If job failed to do or
job did not run, then job scanner will identify the failure and send notification. If job scanner VM goes down for some reason, then other jobs will still run
, and update mongodb and grafana. The likelihood of both VMs going down at the same time  is very less.
'''
import yaml
from zbpackage import jobs,alertmanager
from zbpackage import email_utils
from zbpackage import argos_utils
from zbpackage import log
from datetime import datetime,timedelta
import logging
import os
from logging.handlers import RotatingFileHandler

def scan_jobs(job_list):
    try:
        message='Following jobs failed to run.\n-----------------------------------'
        flag=0
        job_name=''
        for job in job_list:
            job_status=jobs.get_jobstatus(job)
            for job_data in job_status:
                last_run=job_data['lastrun']
                interval=job_data['interval']

                # find the last run and using that to find if job ran last time or not.
                now = datetime.now()
                date_time_obj = datetime.strptime(last_run,'%Y-%m-%dT%H:%M:%SZ')
                job_next_run= date_time_obj + timedelta(minutes=interval)
                time_diff= now - job_next_run

                if(job_data['laststatus']=='failed' or time_diff.days >= 0):
                    job_name=job_name + "\n" + "- " + job_data['job']
                    flag=1

        if flag == 1:
            return message + job_name + "\n\n"
        else:
            return "OK"
    except Exception as e:
        raise

# Function to check size of the ems log file. If any log file is greater than 20 MB, then email is sent to AMS Team to rotate the log.
def scan_ems(files,ems_file_limit):

    try:
        message="Following ems files need attention as their size is more than " + str(ems_file_limit) + "MB\n--------------------------------------------------------------------------------------------"
        file_name=''
        flag=0
        for file in files:
            file_size=os.stat(file).st_size
            if ((file_size/1024)/1024) > ems_file_limit:
                file_name=file_name + "\n" + "- " + file
                flag=1

        if flag==1:
            return message + "\n" + file_name + "\n"
        else:
            return "OK"
    except Exception as e:
        raise


# main process starts here
try:
    email_body=''
    email_from=''
    email_to=''
    email_subject=''
    env=''
    jobs_name=''
    ems_files=''
    error_flag='N'
    ems_file_limit=20

    #initialize logger
    Log_Format = "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"
    logging.basicConfig(handlers=[RotatingFileHandler('./log/scanner.log', maxBytes=20480, backupCount=2)],
                        format=Log_Format,
                        level=logging.INFO)

    logging.info("------Initiating Scanner Job for TIBCO------.")


    logging.info("Reading the config file.")
    stream = open("config.yaml", 'r')
    docs = yaml.load_all(stream, Loader=yaml.FullLoader)

    logging.info("Setting the variables.")
    for doc in docs:
        for k,v in doc.items():
            if k == 'env':
                env=v
            if k == 'email_subject':
                email_subject=v
            if k == 'email-from':
                email_from=v
            if k == 'email-to':
                email_to=v
            if k == 'ems-files':
                ems_files=v
            if k == 'ems-file-limit':
                ems_file_limit=v
            if k == 'jobs':
                jobs_name=v


    # Performing several checks

    # Scan job status
    scan_job_msg=scan_jobs(jobs_name)
    logging.info("Job status scan completed")

    # Scan ems file size
    scan_ems_msg=scan_ems(ems_files,ems_file_limit)
    logging.info("Ems log scan completed")


    if scan_job_msg!="OK":
        email_body=scan_job_msg

    if scan_ems_msg!="OK":
        email_body=email_body + scan_ems_msg

    if email_body == '':
        email_utils.send_mail(email_from,email_to,email_subject + ' - All Looks good !!!',email_body)
    else:
        email_utils.send_mail(email_from,email_to,email_subject + '- Please check',email_body)

    logging.info("Email report sent")

except Exception as e:
    # we are using email notification instead of alertmanager as we don't to miss notification from this job as this job give identify failure case of
    #all other jobs. If we want to make sure we do get the notification all the time even if alertmanager is down. smtp is not supposed to be down anytime,
    #alertmanager can go down in some cases. This is just to increase resiliency of this job.
    logging.error("The scanner failed. Exception message : " + str(e))
    email_utils.send_mail(email_from,email_to,"scanner.py  scanner job failed to execute","Error Message :" + str(e))

