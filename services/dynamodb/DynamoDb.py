import boto3
import botocore
import json
import datetime
import math

from services.Service import Service
from utils.Config import Config
from services.dynamodb.drivers.DynamoDbCommon import DynamoDbCommon

class DynamoDb(Service):
    
    
    def __init__(self, region):
        super().__init__(region)
        self.dynamoDbClient = boto3.client('dynamodb')
        self.cloudWatchClient = boto3.client('cloudwatch')
        self.serviceQuotaClient = boto3.client('service-quotas')
        self.appScalingPolicyClient = boto3.client('application-autoscaling')
        self.backupClient = boto3.client('backup')
    
    
    #Get all tables descriptions [JSON]
    #Ref : https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/list_tables.html
    #Ref : https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_table.html
    def list_tables(self):
        tableArr = []
        try:
            tableNames = self.dynamoDbClient.list_tables()
            #append table name to array
            for tables in tableNames['TableNames']:
                tableDescription = self.dynamoDbClient.describe_table(TableName = tables)
                tableArr.append(tableDescription)
            
            #loop thru next page of results    
            while tableNames.get('LastEvaluatedTableName') is not None:
                tableNames = self.dynamoDbClient.list_tables(tableNames['LastEvaluatedTableName'],100)
                tableDescription = self.dynamoDbClient.describe_table(TableName = tableNames['TableNames'])
                tableArr.append(tableDescription)
                
            return tableArr    
            
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            
    def advise(self):
        objs = {}
        
        #retrieve all tables in DynamoDb
        listOfTables = self.list_tables()
        for eachTable in listOfTables:
            obj = DynamoDbCommon(eachTable, self.dynamoDbClient, self.cloudWatchClient, self.serviceQuotaClient, self.appScalingPolicyClient, self.backupClient)
            obj.run()
            
            objs['DynamoDb::' + eachTable['Table']['TableName']] = obj.getInfo()
            del obj
        
        return objs
        
            
           
if __name__ == "__main__":
    Config.init()
    o = DynamoDb('ap-southeast-2')
    out = o.advise()
    print(out)

    
