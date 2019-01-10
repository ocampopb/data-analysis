import os
from google.cloud import bigquery
import datetime
from pprint import pprint
from google.cloud.exceptions import NotFound
from google.auth import credentials
#from bigquery import JOB_WRITE_TRUNCATE
from bqcreatetable import CreateNewTable

loadFilePath = "gs://ocamposparkbucket/test.csv"

def loaddatafromfiletotable(bqclient, newdatasetname, newtablename):

    dataset_ref = newdatasetname
    table = newtablename.table_id
    table_ref = "{}.{}".format(dataset_ref, table)

    jobconfig = bigquery.LoadJobConfig()  # Initialize job configuration.
    jobconfig.source_format = 'CSV'
    jobconfig.skip_leading_rows = 1
    jobconfig.write_disposition = "JOB_WRITE_TRUNCATE"
    job = bqclient.load_table_from_uri(loadFilePath, table_ref, job_config=jobconfig)
    result = job.result()  # wait for job to complete

    print(" job-type = {}".format(job.job_type))
    print("Load job status")
    print(result.state)
    print("Load job statistics")


def main():
    bqclient = bigquery.Client.from_service_account_json('mytestproject-210009-8dda67e4a6d3.json')
    dataset = "wordcount_dataset"
    table = "bqimportfiletable2"
    newtable = CreateNewTable(bqclient, dataset, table)
    loaddatafromfiletotable(bqclient, dataset, newtable)


if __name__ == "__main__":
    main()







