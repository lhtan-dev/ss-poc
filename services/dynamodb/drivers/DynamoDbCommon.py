import boto3
import botocore
import datetime
import urllib.parse
from statistics import mean

from services.Service import Service
from utils.Config import Config
from utils.Policy import Policy
from services.Evaluator import Evaluator


class DynamoDbCommon(Evaluator):

    def __init__(self, tables, dynamoDbClient, cloudWatchClient, serviceQuotaClient, appScalingPolicyClient, backupClient, cloudTrailClient):
        super().__init__()
        self.tables = tables
        self.dynamoDbClient = dynamoDbClient
        self.cloudWatchClient = cloudWatchClient
        self.serviceQuotaClient = serviceQuotaClient
        self.appScalingPolicyClient = appScalingPolicyClient
        self.backupClient = backupClient
        self.cloudTrailClient = cloudTrailClient


    # logic to check delete protection    
    def _check_delete_protection(self):
        #print('Checking ' + self.tables['Table']['TableName'] + ' delete protection started')
        try:
            if self.tables['Table']['DeletionProtectionEnabled'] == False:
                self.results['deleteTableProtection'] = [-1, 'Delete protection is disabled']
                
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check resources for tags
    def _check_resources_for_tags(self):
        #print('Checking ' + self.tables['Table']['TableName'] + ' for resource tag started')
        try:
            #retrieve tags for specific table by tableARN
            result = self.dynamoDbClient.list_tags_of_resource(ResourceArn = self.tables['Table']['TableArn'])
            #check tags
            if not result['Tags']:
                self.results['resourcesWithoutTags'] = [-1, 'No resource tag']
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check unused resources GSI - read
    def _check_unused_resources_gsi_read(self):
        
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConsumedReadCapacityUnits',
                Dimensions = [
                       {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                        {
                            'Name':'GlobalSecondaryIndexName',
                            'Value':self.tables['Table']['GlobalSecondaryIndexes'][0]['IndexName']
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(30),
                EndTime = datetime.datetime.now(),
                Period = 86400,
                Statistics = [
                    'Sum',
                ],
                Unit = 'Count'
            )
            
            #Calculate sum of all occurances within the 30 days period
            sumTotal = 0.0    
            for eachSum in result['Datapoints']:
               sumTotal += eachSum['Sum']
        
            #Flag as issue if > 0 issues in the 30 days period.
            if sumTotal == 0:
              self.results['unusedResourcesGSIRead'] = [-1, 'GSI ['+self.tables['Table']['GlobalSecondaryIndexes'][0]['IndexName']+'] with RCU of 0 over the past 30 days']
        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check unused resource GSI - write
    def _check_unused_resources_gsi_write(self):
        try:
            
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConsumedWriteCapacityUnits',
                Dimensions = [
                       {
                           'Name':'TableName',
                           'Value':self.tables['Table']['TableName']
                       },
                       {
                            'Name':'GlobalSecondaryIndexName',
                            'Value':self.tables['Table']['GlobalSecondaryIndexes'][0]['IndexName']
                       },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(30),
                EndTime = datetime.datetime.now(),
                Period = 86400,
                Statistics = [
                    'Sum',
                ],
                Unit = 'Count'
            )
            
            #Calculate sum of all occurances within the 30 days period
            sumTotal = 0.0    
            for eachSum in result['Datapoints']:
                sumTotal += eachSum['Sum']
        
            #Flag as issue if > 0 issues in the 30 days period.
            if sumTotal == 0:
                self.results['unusedResourcesGSIWrite'] = [-1, 'GSI ['+self.tables['Table']['GlobalSecondaryIndexes'][0]['IndexName']+'] with WCU of 0 over the past 30 days']
        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check attribute length > 15 and >8
    def _check_attribute_length(self):
        try:
            for tableAttributes in self.tables['Table']['AttributeDefinitions']:
                if len(tableAttributes['AttributeName']) > 15:
                    # error attributesNamesXL for length > 15
                    self.results['attributeNamesXL'] = [-1,  'Attribute name <' + tableAttributes['AttributeName'] + '> is longer than 15 characters.']
                elif len(tableAttributes['AttributeName']) > 8:
                    # error attributesNamesL for length > 8 <= 15
                    self.results['attributeNamesL'] = [-1, 'Attribute name : <' + tableAttributes['AttributeName'] + '> is longer than 8 characters.']
        
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check for TTL status
    def _check_time_to_live_status(self):
        try:
            result = self.dynamoDbClient.describe_time_to_live(TableName = self.tables['Table']['TableName'])
        
            #Check result for TimeToLiveStatus (ENABLED/DISABLED)
            if result['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
                self.results['disabledTTL'] = [-1, 'TTL is not enabled.']
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check Point In Time Recovery backup
    def _check_pitr_backup(self):
        try:
            result = self.dynamoDbClient.describe_continuous_backups(TableName = self.tables['Table']['TableName'])
            #Check results of ContinuousBackupStatus (ENABLED/DISABLED)
            if result['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus'] == 'DISABLED':                    
                self.results['disabledPointInTimeRecovery'] = [-1, 'Point In Time Recovery is disabled ']
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check capacity mode
    def _check_capacity_mode(self):
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConsumedWriteCapacityUnits',
                Dimensions = [
                       {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(7),
                EndTime = datetime.datetime.now(),
                Period = 3600,
                Statistics = [
                    'Average'
                ], 
                Unit = 'Count'
            )
        
            #Calculate % of write capacity in the given hour
            _percentageWrite = 0  
            for eachDatapoints in result['Datapoints']:
                _percentageWrite += eachDatapoints['Average']
        
            #Check if percentage <= 18 and billingmode is on-demand
            if _percentageWrite <= 0.018 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED' :
                #Recommended for On-demand capacity
                self.results['capacityModeOnDemand'] = [-1, 'Recommended for on-demand capacity']
            elif _percentageWrite > 0.018 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST' :
                #Recommended for Provisioned capacity
                self.results['capacityModeProvisioned'] = [-1, 'Recommended for provisioned capacity']
    
    
        except botocore.exception as e:
            ecode = e.response['Error']['Code']
            print(ecode)

    # logic to check provisoned capacity with autoscaling
    def _check_autoscaling_status(self):
        try:

            #Check for autoscaling policy in each table
            results = self.appScalingPolicyClient.describe_scaling_policies(
                ServiceNamespace = 'dynamodb',
                ResourceId = 'table/' + self.tables['Table']['TableName']
                )
            
            #If results comes back with record, autoscaling is enabled
            if len(results['ScalingPolicies']) == 0 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED':
                self.results['autoScalingStatus'] = [-1 , 'Autoscaling is disabled']
                
        except botocore.exceptions as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check for any existing backup available
    def _check_backup_status(self):
        try:
            results = self.backupClient.list_recovery_points_by_resource(ResourceArn = self.tables['Table']['TableArn'])

            if len(results['RecoveryPoints']) < 1:
                self.results['disabledBackup'] = [-1, 'No backup created for the table']
        except botocore.exception as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check service limits max GSI per table
    def _check_service_limits_max_gsi_table(self):
        try:
            #Retrieve quota for DynamoDb = L-F98FE922
            serviceQuotasResutls = self.serviceQuotaClient.list_service_quotas(ServiceCode='dynamodb')
            for quotas in serviceQuotasResutls['Quotas']:
                #Check for GSI limit / table
                if quotas['QuotaCode'] == 'L-F7858A77':
                    y = int(80 * quotas['Value'] / 100)
                    x = len(self.tables['Table']['GlobalSecondaryIndexes'])
                    
                    if x >= y:
                        self.results['serviceLimitMaxGSIPerTable'] = [-1, str(x) + '/' + str(quotas['Value']) + ' GSI. Exceed 80% recommended GSI in the table']
                        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check CW Sum ConditionalCheckFailedRequests > 0
    def _check_conditional_check_failed_requests(self):
        
        _sumOfConditionalCheckFailedRequest = 0;
        
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConditionalCheckFailedRequests',
                Dimensions = [
                        {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(7),
                EndTime = datetime.datetime.now(),
                Period = 900,
                Statistics = [
                    'SampleCount',
                ],
                Unit = 'Count'
            )
            
            for eachDatapoints in result['Datapoints']:
                _sumOfConditionalCheckFailedRequest += eachDatapoints['SampleCount']
                
            if _sumOfConditionalCheckFailedRequest >= 1.0:
                self.results['conditionalCheckFailedRequests'] = [-1, str(_sumOfConditionalCheckFailedRequest) + ' : ConditionalCheckFailedRequest error occured over the past 7 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check CW Sum UserErrors > 0
    def _check_user_errors(self):
        
        _sampleCount = 0;
        
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'UserErrors',
                StartTime = datetime.datetime.now() - datetime.timedelta(7),
                EndTime = datetime.datetime.now(),
                Period = 900,
                Statistics = [
                    'SampleCount',
                ],
                Unit = 'Count'
            )
            
            for eachDatapoints in result['Datapoints']:
                _sampleCount += eachDatapoints['SampleCount']
            
            if _sampleCount >= 1.0:
                self.results['userErrors'] = [-1, str(_sampleCount) + ' : SystemError resulting in HTTP500 error code over the past 7 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check service limit wcu and rcu
    def _check_service_limit_wcu_rcu(self):
        try:
            
            _startTime = datetime.datetime.now() - datetime.timedelta(7)
            _endTime = datetime.datetime.now()
            
            #Count the number active reads on the table RCU
            rcuResult = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConsumedReadCapacityUnits',
                Dimensions = [
                        {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                    ],
                StartTime = _startTime,
                EndTime = _endTime,
                Period = 60,
                Statistics = [
                    'Average',
                ],
                Unit = 'Count'
            )
            
            #Count the number active reads on the table WCU
            wcuResult = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ConsumedWriteCapacityUnits',
                Dimensions = [
                        {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                    ],
                StartTime = _startTime,
                EndTime = _endTime,
                Period = 60,
                Statistics = [
                    'Average',
                ],
                Unit = 'Count'
            )
            
            _rcuLimitCount = 0
            _wcuLimitCount = 0
            
            for eachRCUAverage in rcuResult['Datapoints']:
                if eachRCUAverage['Average'] > 0.8:
                    _rcuLimitCount += 1
            
            for eachWCUAverage in wcuResult['Datapoints']:
                if eachWCUAverage['Average'] > 0.8:
                    _wcuLimitCount += 1
                
            if _rcuLimitCount > 0:
                self.results['rcuServiceLimit'] = [-1, 'You have exceeded the recommended 80% RCU limit by ' + str(_rcuLimitCount) + ' count in the past 7 days.']
            
            if _wcuLimitCount > 0:
                self.results['wcuServiceLimit'] = [-1, 'You have exceeded the recommended 80% WCU limit by ' + str(_wcuLimitCount) + ' count in the past 7 days.']
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check autoscaling aggresiveness
    def _check_autoscaling_aggresiveness(self):
        try:
            results = self.appScalingPolicyClient.describe_scaling_policies(
                ServiceNamespace = 'dynamodb',
                ResourceId = 'table/' + self.tables['Table']['TableName']
            )
            
            
            if len(results['ScalingPolicies']) > 0:
            
                _wcuTarget = 0
                _rcuTarget = 0
                
                for eachScalingPolicies in results['ScalingPolicies']:
                    if eachScalingPolicies['ScalableDimension'] == 'dynamodb:table:WriteCapacityUnits':
                        _wcuTarget = eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue']
                    elif eachScalingPolicies['ScalableDimension'] == 'dynamodb:table:ReadCapacityUnits':
                        _rcuTarget = eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue']
                
                if _rcuTarget >= 80.0: 
                    self.results['autoScalingHighUtil'] = [-1, 'High utilization policy for RCU with value ' + str(_rcuTarget)]
            
                if _rcuTarget <= 50.0:
                    self.results['autoScalingLowUtil'] = [-1, 'Low utilization policy for RCU with value ' + str(_rcuTarget)]
            
                if _wcuTarget >= 80.0:
                    self.results['autoScalingHighUtil'] = [-1, 'High utilization policy for WCU with value ' + str(_wcuTarget)]
            
                if _wcuTarget <= 50.0:
                    self.results['autoScalingLowUtil'] = [-1, 'Low utilization policy for WCU with value ' + str(_wcuTarget)]
                   
        except botocore.exceptions as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check CW Sum SystemErrors > 0
    def _check_system_errors(self):
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'SystemErrors',
                Dimensions = [
                       {
                            'Name':'TableName',
                            'Value':self.tables['Table']['TableName']
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(30),
                EndTime = datetime.datetime.now(),
                Period = 3600,
                Statistics = [
                    'SampleCount',
                ],
                Unit = 'Count'
            )
            
            _systemErrorsCount = 0
            
            for eachDatapoints in result['Datapoints']:
                _systemErrorsCount += eachDatapoints['SampleCount']
            
            if _systemErrorsCount > 0:
                self.results['systemErrors'] = [-1,  '['+ str(_systemErrorsCount)+ '] SystemError resulting in HTTP500 error code over the past 7 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
     
    # logic to check CW Sum ThrottledRequest > 0
    def _check_throttled_request(self):
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ThrottledRequests',
                Dimensions = [
                       {
                            'Name':'TableName',
                            'Value': self.tables['Table']['TableName']
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(30),
                EndTime = datetime.datetime.now(),
                Period = 3600,
                Statistics = [
                    'SampleCount',
                ],
                Unit = 'Count'
            )
            
            _throttledRequestErrors = 0
            
            for eachDatapoints in result['Datapoints']:
                _throttledRequestErrors += eachDatapoints['SampleCount']
            
            if _throttledRequestErrors > 0:
                self.results['throttledRequest'] = [-1, '[' + str(_throttledRequestErrors) + '] request throttled in the past 30 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)

