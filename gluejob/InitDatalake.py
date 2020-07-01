import sys
import json
from urllib.parse import urlparse
import urllib
import datetime
import boto3
import time
from awsglue.utils import getResolvedOptions
import logging
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.context import GlueContext, DynamicFrame
from pyspark.sql.window import Window
import urllib.parse as urlparse
import urllib
from pyspark.sql.utils import AnalysisException

sparkContext = SparkContext.getOrCreate()
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session

job = Job(glueContext)
args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'prefix',
        'bucket',
        'datalake_bucket',
        'datalake_prefix',
        'region',
        'controller_table_name'
        ])
job.init(args['JOB_NAME'], args)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class InitialLoad():
    def __init__(self):
        self.prefix = args['prefix']
        self.bucket = args['bucket']
        self.datalake_bucket = args['datalake_bucket']
        self.datalake_prefix = args['datalake_prefix']
        self.index_prefix = 'index_prefix'
        region = args['region']
        self.ddbTableName = args['controller_table_name']
        self.s3conn = boto3.client('s3', region)
        self.ddbconn = boto3.client('dynamodb', region)

        # self.prefix = 'public/'
        # self.bucket = 'dms-rawdata'
        # self.datalake_bucket='marketboomer-datalake-table'
        # self.datalake_prefix='datalake/'
        # self.index_prefix = 'index/'
        # self.s3conn = boto3.client('s3', 'ap-southeast-1')
        # self.ddbconn = boto3.client('dynamodb', 'ap-southeast-1')
        # self.ddbTableName = 'DMSCDC_Controller'

    def load_index_file(self, input_partitioned, folder, partitionKeys, primaryKey):
        s3_index_path = 's3://' + self.datalake_bucket + '/' + self.index_prefix + folder
        input_partitioned.select(primaryKey, *partitionKeys).coalesce(1).write.mode('overwrite').parquet(s3_index_path)

    def load_initial_file(self, folder, partitionKey, primaryKey, needIndexFile):
        s3_inputpath = 's3://' + self.bucket + '/' + self.prefix + folder
        s3_data_outputpath = 's3://' + self.datalake_bucket + '/' + self.datalake_prefix + folder

        input = spark.read.parquet(s3_inputpath+"/LOAD*.parquet").withColumn("Op", lit("I"))

        if partitionKey != "null" :
            partitionKeys = partitionKey.split(",")
            input_partitioned = input 
            if 'created_year' in partitionKeys and 'created_month' in partitionKeys and 'created_day' not in partitionKeys:
                input_partitioned = input.withColumn('created_year', year('created_at')).withColumn('created_month', month('created_at'))
            elif 'created_year' in partitionKeys and 'created_month' in partitionKeys and 'created_day' in partitionKeys:
                input_partitioned = input.withColumn('created_year', year('created_at')).withColumn('created_month',month('created_at')).withColumn('created_day', dayofmonth('created_at'))

            input_partitioned_cache = input_partitioned.repartition(partitionKeys[0])
            
            if needIndexFile == 'true':
                input_partitioned_cache.cache()
                self.load_index_file(input_partitioned_cache, folder, partitionKeys, primaryKey)
            input_partitioned_cache.write.mode('overwrite').partitionBy(partitionKeys).parquet(s3_data_outputpath)
            
        else:
            input.write.mode('overwrite').parquet(s3_data_outputpath)

    def is_ddb_exist(self):
        try:
            self.ddbconn.describe_table(TableName=self.ddbTableName)
            return True
        except Exception:
            return False
    def start_load(self):
        if not self.is_ddb_exist():
            logger.info(self.ddbTableName + ' does not exist, please setup dynamodb table')
            return
        folders = self.s3conn.list_objects(Bucket=self.bucket, Prefix=self.prefix, Delimiter='/').get('CommonPrefixes')
        for f in folders:
            path = self.bucket + '/' + f['Prefix']
            full_folder = f['Prefix']
            folder = full_folder[len(self.prefix):]

            item = {
                'path': {'S':path},
                'bucket': {'S':self.bucket},
                'prefix': {'S':self.prefix},
                'folder': {'S':folder},
                'PrimaryKey': {'S': folder[:-1] + '_id'},
                'PartitionKey': {'S':'null'},
                'needIndexFile': {'S': 'null'},
                'LastFullLoadDate': {'S':'1900-01-01 00:00:00'},
                'LastIncrementalFile': {'S':path + '0.parquet'},
                'ActiveFlag': {'S':'true'}}
            try:
                response = self.ddbconn.get_item(
                    TableName=self.ddbTableName,
                    Key={'path': {'S':path}})
                if 'Item' in response:
                    item = response['Item']
                else:
                    self.ddbconn.put_item(
                        TableName=self.ddbTableName,
                        Item=item)
            except:
                self.ddbconn.put_item(
                    TableName=self.ddbTableName,
                    Item=item)

            partitionKey = item['PartitionKey']['S']
            activeFlag = item['ActiveFlag']['S']
            primaryKey = item['PrimaryKey']['S']
            needIndexFile = item['needIndexFile']['S']
            lastFullLoadDate = item['LastFullLoadDate']['S']
            if activeFlag == 'true':
                initialfiles = self.s3conn.list_objects(Bucket=self.bucket, Prefix=full_folder+'LOAD').get('Contents')
                loadInitial = False
                if initialfiles is not None :
                    s3FileTS = initialfiles[0]['LastModified'].replace(tzinfo=None)
                    ddbFileTS = datetime.datetime.strptime(lastFullLoadDate, '%Y-%m-%d %H:%M:%S')
                    if s3FileTS > ddbFileTS:
                        message='Starting to process Initial file.'
                        loadInitial = True
                        lastFullLoadDate = datetime.datetime.strftime(s3FileTS,'%Y-%m-%d %H:%M:%S')
                    else:
                        message='Intial files already processed.'
                else:
                    message='No initial files to process.'
                logger.info(message)
                if loadInitial:
                    logger.info('start load ' + folder)
                    self.load_initial_file(folder, partitionKey, primaryKey, needIndexFile)
                    self.ddbconn.update_item(
                        TableName= self.ddbTableName,
                        Key={"path": {"S":path}},
                        AttributeUpdates={"LastFullLoadDate": {"Value": {"S": lastFullLoadDate}}})

load = InitialLoad()
load.start_load()

