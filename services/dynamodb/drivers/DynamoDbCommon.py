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
    def VALIDATED_check_delete_protection(self):
        #print('Checking ' + self.tables['Table']['TableName'] + ' delete protection started')
        try:
            if self.tables['Table']['DeletionProtectionEnabled'] == False:
                self.results['deleteTableProtection'] = [-1, 'Delete protection is disabled']
                
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check resources for tags
    def VALIDATED_check_resources_for_tags(self):
        #print('Checking ' + self.tables['Table']['TableName'] + ' for resource tag started')
        try:
            #retrieve tags for specific table by tableARN
            result = self.dynamoDbClient.list_tags_of_resource(ResourceArn = self.tables['Table']['TableArn'])
            #check tags
            if not result['Tags']:
                self.results['resourcesWithTags'] = [-1, 'No resource tag']
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check unused resources GSI - read
    def VALIDATED_check_unused_resources_gsi_read(self):
        
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
    def VALIDATED_check_unused_resources_gsi_write(self):
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
            print(result)
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
    def VALIDATED_check_attribute_length(self):
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
    def VALIDATED_check_time_to_live_status(self):
        try:
            result = self.dynamoDbClient.describe_time_to_live(TableName = self.tables['Table']['TableName'])
        
            #Check result for TimeToLiveStatus (ENABLED/DISABLED)
            if result['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
                self.results['enabledTTL'] = [-1, 'TTL is not enabled.']
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check Point In Time Recovery backup
    def VALIDATED_check_pitr_backup(self):
        try:
            result = self.dynamoDbClient.describe_continuous_backups(TableName = self.tables['Table']['TableName'])
            #Check results of ContinuousBackupStatus (ENABLED/DISABLED)
            if result['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus'] == 'DISABLED':                    
                self.results['enabledPointInTimeRecovery'] = [-1, 'Point In Time Recovery is disabled ']
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check autoscaling aggresiveness
    def NOTVALIDATED_check_autoscaling_aggresiveness(self):
        try:
            results = self.appScalingPolicyClient.describe_scaling_policies(
                ServiceNamespace = 'dynamodb',
                ResourceId = 'table/' + self.tables['Table']['TableName']
            )
                
            for eachScalingPolicies in results['ScalingPolicies']:
                if eachScalingPolicies is not None:
                    if eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue'] <= 50:
                        self.results['autoScalingLowUtil'] = [-1, self.tables['Table']['TableName'] + ' is on low utilization policy of value : ' + eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue']]
                    elif eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue'] >= 90:                         
                        self.results['autoScalingHighUtil'] = [-1, self.tables['Table']['TableName'] + ' is on high utilization policy of value : ' + eachScalingPolicies['TargetTrackingScalingPolicyConfiguration']['TargetValue']]

        except botocore.exceptions as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check capacity mode
    def VALIDATED_check_capacity_mode(self):
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
                self.results['capacityModeProvisioned'] = [-1, 'Recommended for on-demand capacity']
            elif _percentageWrite > 0.018 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST' :
                #Recommended for Provisioned capacity
                self.results['capacityModeOndemand'] = [-1, 'Recommended for provisioned capacity']
    
    
        except botocore.exception as e:
            ecode = e.response['Error']['Code']
            print(ecode)

    # logic to check provisoned capacity with autoscaling
    def NOTVALIDATED_check_autoscaling_status(self):
        try:

            #Check for autoscaling policy in each table
            results = self.appScalingPolicyClient.describe_scaling_policies(
                ServiceNamespace = 'dynamodb',
                ResourceId = 'table/' + self.tables['Table']['TableName']
                )
            
            #If results comes back with record, autoscaling is enabled
            if results is None and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED':
                self.results['autoScalingStatus'] = [-1 , self.tables['Table']['TableName'] + ' is on PROVISIONED capacity but autoscaling is disabled']
                
        except botocore.exceptions as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check for any existing backup available
    def VALIDATED_check_backup_status(self):
        try:
            results = self.backupClient.list_recovery_points_by_resource(ResourceArn = self.tables['Table']['TableArn'])

            if len(results['RecoveryPoints']) < 1:
                self.results['backupStatus'] = [-1, 'No backup created for the table']
        except botocore.exception as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check CW Sum SystemErrors > 0
    def NOTVALIDATED_check_system_errors(self):
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'SystemErrors',
                Dimensions = [
                       {
                            'Name':'Operation',
                            'Value':'GetRecords'
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(15),
                EndTime = datetime.datetime.now(),
                Period = 900,
                Statistics = [
                    'Sum',
                ],
                Unit = 'Count'
            )
            
            print(result)
            for eachDatapoints in result['Datapoints']:
                if eachDatapoints['Sum'] >= 1.0:
                    self.results['systemErrors'] = [-1, self.tables['Table']['TableName'] + ' : SystemError resulting in HTTP500 error code over the past 7 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
     
    # logic to check CW Sum ThrottledRequest > 0
    def NOTVALIDATED_check_throttled_request(self):
        try:
            #Count the number active reads on the table on the GSIs
            result = self.cloudWatchClient.get_metric_statistics(
                Namespace = 'AWS/DynamoDB',
                MetricName = 'ThrottledRequests',
                Dimensions = [
                       {
                            'Name':'Operation',
                            'Value':'BatchGetItem'
                        },
                    ],
                StartTime = datetime.datetime.now() - datetime.timedelta(7),
                EndTime = datetime.datetime.now(),
                Period = 900,
                Statistics = [
                    'Sum',
                ],
                Unit = 'Count'
            )
            
            for eachDatapoints in result['Datapoints']:
                if eachDatapoints['Sum'] >= 1.0:
                    self.results['throttledRequest'] = [-1, self.tables['Table']['TableName'] + ' : request throttled in the past 7 days']
                    
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check service limits max GSI per table
    def VALIDATED_check_service_limits_max_gsi_table(self):
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
    def VALIDATED_check_conditional_check_failed_requests(self):
        
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
    def VALIDATED_check_user_errors(self):
        
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
    def NOTVALIDATED_check_service_limit_wcu_rcu(self):
        try:
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
                StartTime = datetime.datetime.now() - datetime.timedelta(1),
                EndTime = datetime.datetime.now(),
                Period = 60,
                Statistics = [
                    'Average',
                ],
                Unit = 'Count'
            )
            
            arrRCUAverage = []
            
            for eachRCUAverage in rcuResult['Datapoints']:
                arrRCUAverage.append(eachRCUAverage['Average'])
                
            if arrRCUAverage >= 80.0:
                self.results['rcuServiceLimit'] = [-1, 'Your average RCU is ' + arrRCUAverage + ' over the past 1 day']
            
            arrWCUAverage = []
            
            for eachWCUAverage in rcuResult['Datapoints']:
                arrWCUAverage.append(eachWCUAverage['Average'])
                
            if arrRCUAverage >= 80.0:
                self.results['wcuServiceLimit'] = [-1, 'Your average WCU is ' + arrWCUAverage + ' over the past 1 day']
               
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check table with provisioned and autoscaling enable
    def NOTVALIDATED_check_provisioned_and_autoscaling(self):
        try:
            #retrieve provisioned values for a specific table
            if self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED':
                results = self.appScalingPolicyClient.describe_scalable_targets(
                    ServiceNamespace = 'dynamodb',
                    ResourceIds = ['table/'+self.tables['Table']['TableName']],
                    ScalableDimension = 'dynamodb:table:ReadCapacityUnits' and 'dynamodb:table:WriteCapacityUnits',
                    MaxResults = 100
                )
                for targets in results['ScalableTargets']:
                    if targets is None:
                        self.results['provisionedAutoscaling'] = [-1, 'Provisioned capacity but autoscaling is not enabled']
        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)

    # logic to check right provisioning
    def INCOMPLETE_check_right_provisioning(self):
        #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CostOptimization_RightSizedProvisioning.html#CostOptimization_RightSizedProvisioning_UnderProvisionedTables
        try:
            #WCU provisioned and consumed
            wcuResult = self.cloudWatchClient.get_metric_data(
                    MetricDataQueries = [
                        {
                            'Id':'ProvisionedWCU',
                            'MetricStat':{
                                'Metric':{
                                    'Namespace':'AWS/DynamoDb',
                                    'MetricName':'ProvisionedWriteCapacityUnits',
                                    'Dimensions':[
                                        {
                                            'Name':'TableName',
                                            'Value':self.tables['Table']['TableName']
                                        },
                                    ]
                                },
                                'Period':3600,
                                'Stat':'Average'
                            },
                            'Label':'Provisioned',
                            'ReturnData':False
                        },
                        {
                            'Id':'consumedWCU',
                            'MetricStat':{
                                'Metric':{
                                    'Namespace':'AWS/DynamoDb',
                                    'MetricName':'ConsumedWriteCapacityUnits',
                                    'Dimensions':[
                                        {
                                            'Name':'TableName',
                                            'Value':self.tables['Table']['TableName']
                                        },
                                    ]
                                },
                                'Period':3600,
                                'Stat':'Sum',
                            },
                            'Label':'',
                            'ReturnData':False
                        },
                        {
                            'Id':'m1',
                            'Expression':'consumedWCU/PERIOD(consumedWCU)',
                            'Label':'Consumed WCUs',
                            'ReturnData':False
                        },
                        {
                            'Id':'utilizationPercentage',
                            'Expression':'100*(m1/provisionedWCU)',
                            'Label':'Utilization Percentage',
                            'ReturnData':True
                        }
                    ],
                    StartTime = datetime.datetime.now() - datetime.timedelta(7),
                    EndTime = datetime.datetime.now(),
                    ScanBy = 'TimestampDescending',
                    MaxDataPoints = 24
                )
            print(wcuResult)
            
            #RCU provisioned and consumed
            rcuResult = self.cloudWatchClient.get_metric_data(
                    MetricDataQueries = [
                        {
                            'Id':'ProvisionedRCU',
                            'MetricStat':{
                                'Metric':{
                                    'Namespace':'AWS/DynamoDb',
                                    'MetricName':'ProvisionedReadCapacityUnits',
                                    'Dimensions':[
                                        {
                                            'Name':'TableName',
                                            'Value':self.tables['Table']['TableName']
                                        },
                                    ]
                                },
                                'Period':3600,
                                'Stat':'Average'
                            },
                            'Label':'Provisioned',
                            'ReturnData':False
                        },
                        {
                            'Id':'consumedRCU',
                            'MetricStat':{
                                'Metric':{
                                    'Namespace':'AWS/DynamoDb',
                                    'MetricName':'ConsumedReadCapacityUnits',
                                    'Dimensions':[
                                        {
                                            'Name':'TableName',
                                            'Value':self.tables['Table']['TableName']
                                        },
                                    ]
                                },
                                'Period':3600,
                                'Stat':'Sum',
                            },
                            'Label':'',
                            'ReturnData':False
                        },
                        {
                            'Id':'m1',
                            'Expression':'consumedRCU/PERIOD(consumedRCU)',
                            'Label':'Consumed WCUs',
                            'ReturnData':False
                        },
                        {
                            'Id':'utilizationPercentage',
                            'Expression':'100*(m1/provisionedRCU)',
                            'Label':'Utilization Percentage',
                            'ReturnData':True
                        }
                    ],
                    StartTime = datetime.datetime.now() - datetime.timedelta(365),
                    EndTime = datetime.datetime.now(),
                    ScanBy = 'TimestampDescending',
                    MaxDataPoints = 24
                )
                
            #TODO
            
            #Underprovision
            #1 - last 12 months ~ 90%
            #WCU
            arrWCUUtil = []
            for eachValue in wcuResult['MetricDataResults'][0]['Values']:
                arrWCUUtil.append(eachValue)
            
            averageWCUUtil = mean(arrWCUUtil)

                
            #2 - growing 8% monthly in 3 months
            #3 - growing 5% monthly in 4.5 months
            
            #Overprovision
            #1 - last 12 months ~ 20%
            #2 - low util to high throttle ration (Max(ThrottleEvents)/Min(ThrottleEvents) in the interval)
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
            
    # logic to check table class
    def INCOMPLETE_check_table_class(self):
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ce/client/get_cost_and_usage.html
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch/client/get_metric_statistics.html
        #https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CostOptimization_TableClass.html
        try:
            # TODO: write code...
            # Storage Cost vs RCU/WCU cost (IA: When storage cost is at least 50% > than IOPS). RI does not supports IA
            print('nothing')
        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)

