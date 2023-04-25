import boto3
import botocore
import datetime
import urllib.parse

from services.Service import Service
from utils.Config import Config
from utils.Policy import Policy
from services.Evaluator import Evaluator


class DynamoDbGeneric(Evaluator):
    
    def __init__(self, tables, dynamoDbClient, cloudWatchClient, serviceQuotaClient, appScalingPolicyClient, backupClient, cloudTrailClient):
        super().__init__()
        self.tables = tables
        self.dynamoDbClient = dynamoDbClient
        self.cloudWatchClient = cloudWatchClient
        self.serviceQuotaClient = serviceQuotaClient
        self.appScalingPolicyClient = appScalingPolicyClient
        self.backupClient = backupClient
        self.cloudTrailClient = cloudTrailClient
        
    # logic to check service limits Max table / region
    def DONE_check_service_limits_max_table_region(self):
        try:
            #Retrieve quota for DynamoDb = L-F98FE922
            serviceQuotasResutls = self.serviceQuotaClient.list_service_quotas(ServiceCode='dynamodb')
            for quotas in serviceQuotasResutls['Quotas']:
                #Check for max table / region
                if quotas['QuotaCode'] == 'L-F98FE922':
                    y = int(80 * quotas['Value'] / 100)
                    x = len(self.tables)
                    if x <= y:
                        self.results['serviceLimitMaxTablePerRegion'] = [-1, 'You have used ' + str(x) + ' tables from available limit of ' + str(int(quotas['Value']))]
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check trail of deleteBackup
    def DONE_check_trail_delete_backup(self):
        try:
            deleteBackupResults = self.cloudTrailClient.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey':'EventName',
                        'AttributeValue':'DeleteRecoveryPoint'
                    },
                ],
                StartTime = datetime.datetime.now() - datetime.timedelta(90),
                EndTime = datetime.datetime.now(),
                MaxResults = 50
                )
            numOfDeleteBackup = len(deleteBackupResults['Events'])
            
            
            if numOfDeleteBackup > 0:
                self.results['trailDeleteBackup'] = [-1, 'There was ' + str(numOfDeleteBackup) + ' backup deleted in the past 30 days']
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check trail of deleteTable
    def DONE_check_trail_delete_table(self):
        
        _startTime = datetime.datetime.now() - datetime.timedelta(30)
        _endTime = datetime.datetime.now()
        _deleteTableResultsArr = []
        try:
            deleteTableResults = self.cloudTrailClient.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey':'EventName',
                        'AttributeValue':'DeleteTable'
                    },
                ],
                StartTime = _startTime,
                EndTime = _endTime,
                MaxResults = 50,
            )
            _deleteTableResultsArr.extend(deleteTableResults['Events'])
            
            while 'NextToken' in deleteTableResults:
                deleteTableResults = self.cloudTrailClient.lookup_events(
                    LookupAttributes=[
                        {
                            'AttributeKey':'EventName',
                            'AttributeValue':'DeleteTable'
                        },
                    ],
                    StartTime = _startTime,
                    EndTime = _endTime,
                    MaxResults = 50,
                    NextToken = deleteTableResults['NextToken']
                )
                _deleteTableResultsArr.extend(deleteTableResults['Events'])
                
            numOfDeleteTable = len(_deleteTableResultsArr)

            ##SHOW ONLY 5 RESULTS
            if numOfDeleteTable > 0:
                self.results['trailDeleteTable'] = [-1, 'There was ' + str(numOfDeleteTable) + ' tables deleted in the past 30 days']

        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
        
                  