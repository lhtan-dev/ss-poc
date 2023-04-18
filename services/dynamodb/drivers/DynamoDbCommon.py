import boto3
import botocore
import datetime
import urllib.parse

from services.Service import Service
from utils.Config import Config
from utils.Policy import Policy
from services.Evaluator import Evaluator


class DynamoDbCommon(Evaluator):

    def __init__(self, tables, dynamoDbClient, cloudWatchClient, serviceQuotaClient, appScalingPolicyClient, backupClient):
        super().__init__()
        self.tables = tables
        self.dynamoDbClient = dynamoDbClient
        self.cloudWatchClient = cloudWatchClient
        self.serviceQuotaClient = serviceQuotaClient
        self.appScalingPolicyClient = appScalingPolicyClient
        self.backupClient = backupClient
        
        self.init()

    # logic to check delete protection    
    def _check_delete_protection(self):
        #print('Checking ' + self.tables['Table']['TableName'] + ' delete protection started')
        try:
            if self.tables['Table']['DeletionProtectionEnabled'] == False:
                self.results['deleteTableProtection'] = [-1, self.tables['Table']['TableName'] + ' delete protection is disabled']
                
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
            if result['Tags'] is None:
                self.results['resourcesWithTags'] = [-1, self.tables['Table']['TableName'] + ' does not have tag']
            #print('Checking ' + self.tables['Table']['TableName'] + ' for resource tag completed')
            
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
                            'Value':self.tables['Tables']['TableName']
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
               sumTotal = sumTotal + eachSum['Sum']
        
            #Flag as issue if > 0 issues in the 30 days period.
            if sumTotal > 0:
              self.results['unusedResourcesGSIRead'] = [-1, self.tables['Tables']['TableName'] + ' has RCU of ' + sumTotal + ' over the past 30 days']
        
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
                           'Value':self.tables['Tables']['TableName']
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
                sumTotal = sumTotal + eachSum['Sum']
        
            #Flag as issue if > 0 issues in the 30 days period.
            if sumTotal > 0:
                self.results['unusedResourcesGSIWrite'] = [-1, self.tables['Tables']['TableName'] + ' has WCU of ' + sumTotal + ' over the past 30 days']
        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check attribute length > 15 and >8
    def _check_attribute_length(self):
        try:
            for tableAttributes in self.tables['Table']['AttributeDefinitions']:
                if len(tableAttributes['TableName']) > 15:
                    # error attributesNamesXL for length > 15
                    self.results['attributeNamesXL'] = [-1, self.tables['Table']['TableName'] + ' - attribute name ' + tableAttributes['TableName']]
                elif len(tableAttributes['TableName']) > 8:
                    # error attributesNamesL for length > 8 <= 15
                    self.results['attributeNamesL'] = [-1, self.tables['Table']['TableName'] + ' - attribute name ' + tableAttributes['TableName']]
        
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
        
    # logic to check for TTL status
    def _check_time_to_live_status(self):
        try:
            result = self.dynamoDbClient.describe_time_to_live(TableName = self.tables['Table']['TableName'])
        
            #Check result for TimeToLiveStatus (ENABLED/DISABLED)
            if result['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
                self.results['enabledTTL'] = [-1, self.tables['Table']['TableName'] + ' TTL is disabled.']
            
        except botocore.exceptions.CLientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check continuous backup
    def _check_continuous_backup(self):
        try:
            result = self.dynamoDbClient.describe_continuous_backups(TableName = self.tables['Table']['TableName'])
            #Check results of ContinuousBackupStatus (ENABLED/DISABLED)
            if result['ContinuousBackupsDescription']['ContinuousBackupsStatus'] == 'DISABLED':                    
                self.results['enabledContinuousBackup'] = [-1, 'Continuous Backup Disabled for table ' + self.tables['Table']['TableName']]

        except botocore.exceptions.ClientError as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check autoscaling aggresiveness
    def _check_autoscaling_aggresiveness(self):
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
                StartTime = datetime.datetime.now() - datetime.timedelta(3),
                EndTime = datetime.datetime.now(),
                Period = 360,
                Statistics = [
                    'Average' and 'Maximum',
                ],
                Unit = 'Seconds'
            )
        
            #Calculate % of write capacity in the given hour
            percentageWrite = 0  
            for eachDatapoints in result['Datapoints']:
                if eachDatapoints is not None:
                    percentageWrite = round((eachDatapoints['Average'] / eachDatapoints['Maximum']) * 100, 2)
                    #Check if percentage <= 18 and billingmode is on-demand
                    if percentageWrite <= 18.00 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED' :
                        #Recommended for On-demand capacity
                        self.results['capacityModeProvisioned'] = [-1, self.tables['Table']['TableName'] + ' is recommended for on-demand capacity']
                    elif percentageWrite > 18 and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST' :
                        #Recommended for Provisioned capacity
                        self.results['capacityModeOndemand'] = [-1, self.tables['Table']['TableName'] + ' is recommended for provisioned capacity']
    
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
            if results is None and self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED':
                self.results['autoScalingStatus'] = [-1 , self.tables['Table']['TableName'] + ' is on PROVISIONED capacity but autoscaling is disabled']
                
        except botocore.exceptions as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    # logic to check table has PITR backup enabled
    def _check_backup_status(self):
        try:
            results = self.backupClient.list_recovery_points_by_resource(ResourceArn = self.tables['Table']['TableArn'])
            
            if results is None:
                self.results['backupStatus'] = [-1, self.tables['Table']['TableName'] + ' backup status is disabled']
                
        except botocore.exception as e:
            ecode = e.response['Error']['Code']
            print(ecode)
    
    
    
    
    
    
    #INCOMPLETE
    # logic to check max table / region
    def INCOMPLETE__check_service_quotas(self):
        accountQuota = []
        try:
            #QuotaCode = L-F98FE922
            serviceQuotasResutls = self.serviceQuotaClient.list_service_quotas(ServiceCode='dynamodb')
            for items in serviceQuotasResutls:
                if items['QuotaCode'] == 'L-F98FE922':
                    accountQuota.append(items)
                    
            #TODO
            
        except botocore.exceptions.CLientError as e:
            raise e
        
    #INCOMPLETE
    # logic to check table with provisioned and autoscaling enable
    def INCOMPLETE__check_provisioned_and_autoscaling(self):
        try:
            #retrieve provisioned values for a specific table
            if self.tables['Table']['BillingModeSummary']['BillingMode'] == 'PROVISIONED':
                print('empty')
                    #TO DO
                    #HOW to check autoscaliing
        except botocore.exceptions.CLientError as e:
            raise e
