import os
from google.cloud import bigquery
import datetime
from pprint import pprint
from google.cloud.exceptions import NotFound
from google.auth import credentials

TableSchemaDefinition = ['PassengerId:STRING', 'Pclass:INTEGER', 'Name:STRING', 'Sex:STRING', 'Age:STRING', 'SibSp:INTEGER',
                         'Parch:INTEGER', 'Ticket:STRING', 'Fare:FLOAT', 'Cabin:STRING', 'Embarked:STRING']


def table_exists(client, table):
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False

def CreateNewTable(client, newDatasetName, newTableName):
    dataset_ref = client.dataset(newDatasetName)

    table_ref = dataset_ref.table(newTableName)
    table = bigquery.Table(table_ref)

    

    if not table_exists(client, table):

        for fieldDetails in TableSchemaDefinition:
                fieldDetail = fieldDetails.split(':')
                table.schema += (bigquery.SchemaField(fieldDetail[0], fieldDetail[1]),)

        table = client.create_table(table)

        print('Created table {} in dataset {}'.format(newTableName, newDatasetName))
        print("New table created sucessfully")
        return table
    else:
        print("Table already exist")
        return table

def main():
    bqclient = bigquery.Client.from_service_account_json('mytestproject-210009-8dda67e4a6d3.json')
    datasetname = 'wordcount_dataset'
    newTableName = datetime.datetime.today().strftime('justanothertable2_%Y_%m_%H%M%S%p')
    CreateNewTable(bqclient, datasetname, newTableName)
if  __name__  == "__main__":
    main()

