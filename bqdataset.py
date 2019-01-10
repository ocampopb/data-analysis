import os
from google.cloud import bigquery
import datetime
from pprint import pprint
from google.cloud.exceptions import NotFound
from google.auth import credentials

def ListAllDatasetsAndTableInProject(y):

    for x in y.list_datasets() :
        print("---------------------------------------------------------------------")
        print("Dataset Name:  {}".format(x.dataset_id))
        print("Listing tables in {}".format(x.dataset_id))
        print('\nTable name: '.join ([str(table.table_id) for table in y.list_tables(x.dataset_id)]))
print("------------------------------------------------------------------------")

def main():
    bqclient = bigquery.Client.from_service_account_json('mytestproject-210009-8dda67e4a6d3.json')
    ListAllDatasetsAndTableInProject(bqclient)


if __name__=="__main__":
    main()

