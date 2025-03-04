import boto3
import botocore
import json
import datetime
import math

from services.Service import Service
from utils.Config import Config
from services.dynamodb.drivers.DynamoDbCommon import DynamoDbCommon
from services.dynamodb.drivers.DynamoDbGeneric import DynamoDbGeneric

class DynamoDb(Service):
    
    
    def __init__(self, region):
        super().__init__(region)
        self.dynamoDbClient = boto3.client('dynamodb')
        self.cloudWatchClient = boto3.client('cloudwatch')
        self.serviceQuotaClient = boto3.client('service-quotas')
        self.appScalingPolicyClient = boto3.client('application-autoscaling')
        self.backupClient = boto3.client('backup')
        self.cloudTrailClient = boto3.client('cloudtrail')
    
    
    def list_tables(self):
        tableArr = []
        try:
            tableNames = self.dynamoDbClient.list_tables()
            
            #append table name to array
            for tables in tableNames['TableNames']:
                tableDescription = self.dynamoDbClient.describe_table(TableName = tables)
                tableArr.append(tableDescription)
            
            #loop thru next page of results    
            while 'LastEvaluatedTableName' in tableNames:
                tableNames = self.dynamoDbClient.list_tables(ExclusiveStartTableName = tableNames['LastEvaluatedTableName'],Limit = 100)
                for tables in tableNames['TableNames']:
                    tableDescription = self.dynamoDbClient.describe_table(TableName = tables)
                    tableArr.append(tableDescription)
            
            return tableArr 
            
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            
    def advise(self):
        
        objs = {}
        
        #Retrieve all tables with descriptions from DynamoDb
        listOfTables = self.list_tables()
        
        try:
            #Run generic checks
            obj = DynamoDbGeneric(listOfTables, self.dynamoDbClient, self.cloudWatchClient, self.serviceQuotaClient, self.appScalingPolicyClient, self.backupClient, self.cloudTrailClient)
            obj.run()
            objs['DynamoDb::Generic'] = obj.getInfo()
            del obj
        
            #Run table specific checks
            for eachTable in listOfTables:
                obj = DynamoDbCommon(eachTable, self.dynamoDbClient, self.cloudWatchClient, self.serviceQuotaClient, self.appScalingPolicyClient, self.backupClient, self.cloudTrailClient)
                obj.run()
                objs['DynamoDb::' + eachTable['Table']['TableName']] = obj.getInfo()
                del obj
            
            #Return objs
            return objs
            
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        

           
if __name__ == "__main__":
    Config.init()
    o = DynamoDb('ap-southeast-1')
    out = o.advise()
    out = json.dumps(out, indent=4)
    print(out)

    
