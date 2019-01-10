import os
from google.cloud import bigquery
import datetime
from pprint import pprint
from google.cloud.exceptions import NotFound
from google.auth import credentials


def CreateNewDataset(x, newDatasetName):
       dataset_ref = x.dataset(newDatasetName)
       if not dataset_exists(x, dataset_ref):
                 dataset = x.create_dataset(
                                             bigquery.Dataset(dataset_ref)
                                             #fri='Current Month dataset',
                                             #description='Store current month data for all tables',
                                             #location = 'US'
                                            )
                 print('Created dataset {}'.format(dataset.dataset_id))
                 return dataset
       else:
                 print("Dataset already exists")
                 return dataset_ref

def dataset_exists(x,y):
    try:
        x.get_dataset(y)
        return True
    except NotFound:
        return False
    
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
    newDatasetName = datetime.datetime.today().strftime("AnotherNewSampleDataset_%Y%m")
    sampleDataset = CreateNewDataset(bqclient, newDatasetName)

if __name__ == "__main__":
    main()



