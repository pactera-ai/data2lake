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

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        'id_prefix',
        'controller_table_name',
        'region',
        'crawler_name'
        ])
job.init(args['JOB_NAME'], args)


class LoadIncremental():
    def __init__(self):
        # self.prefix = 'dms-rawdata/public/'
        # self.bucket = 'dms-rawdata'
        # self.datalake_bucket='marketboomer-datalake-table'
        # # Prod
        # self.datalake_prefix='datalake/'
        # self.id_prefix = 'index/'
        # self.ddbTableName = 'DMSCDC_Controller'
        self.prefix = args['prefix']
        self.bucket = args['bucket']
        self.datalake_bucket = args['datalake_bucket']
        self.datalake_prefix = args['datalake_prefix']
        self.id_prefix = args['id_prefix']
        self.ddbTableName = args['controller_table_name']
        region = args['region']
        # Dev
        # self.datalake_prefix='datalake-dev/'
        # self.id_prefix = 'index-dev/'
        # self.ddbTableName = 'DMSCDC_Controller-dev'
        self.s3conn = boto3.client('s3', region)
        self.ddbconn = boto3.client('dynamodb', region)

    def has_updated_at(self, df):
        try:
            df['updated_at']
            return True
        except AnalysisException:
            return False
    # Filter the updated files compared to last incremental file
    def get_updated_file_paths(self, incrementalFiles, lastIncrementalFile, newIncrementalFile):
        result = []
        for f in incrementalFiles:
            path = self.bucket + '/' + f['Key']
            if path > lastIncrementalFile and path <= newIncrementalFile:
                result.append('s3://' + path)
        return result
        
    def remove_files(self, filelist):
        for row in filelist:
            if row[0] != "null":
                o = urlparse.urlparse(row[0])
                self.s3conn.delete_object(Bucket=o.netloc, Key=urllib.parse.unquote(o.path)[1:])
    # Filter existing folders
    def existing_datalake_paths(self, target_paths):
        existing_paths = []
        for p in target_paths:
            res = self.s3conn.list_objects_v2(
                Bucket=self.datalake_bucket,
                MaxKeys=1,
                Prefix= p[len('s3://' + self.datalake_bucket + '/'):])
            if res['KeyCount'] == 1:
                existing_paths.append(p)
        return existing_paths
    
    
    # Get all objects from folder
    # updated files begin with 2
    # By default, S3 client only list 1000 objects, by checking IsTruncated property to init another request
    def get_all_objects(self, full_folder):
        result = []
        res = self.s3conn.list_objects_v2(Bucket=self.bucket, Delimiter='/',Prefix=full_folder+'2')
        if res['KeyCount'] > 0:
            result = result + res['Contents']
        
        while res['IsTruncated']:
            token = res['NextContinuationToken']
            res = self.s3conn.list_objects_v2(Bucket=self.bucket, Delimiter='/',Prefix=full_folder+'2', ContinuationToken = token)
            result = result + res['Contents']
        return result

    def get_output_no_pk(self, input):
        output = input.filter(input.Op=='I')
        return output

    def is_table_partitioned(self, partitionKey):
        return partitionKey != 'null'

    def get_updated_partitions(self, inputfile, indexfile, partitionKeys, primaryKey):
        keys = primaryKey.split(',')
        # All incoming inserted items exclude deleted keys
        new_inserted = inputfile.filter(inputfile.Op == 'I').join(inputfile.filter(inputfile.Op == 'D').select(*keys), keys, 'left_anti').select(*partitionKeys)
        # All incoming updated items exclude deleted keys
        new_updated = inputfile.filter(inputfile.Op == 'U').join(inputfile.filter(inputfile.Op == 'D').select(*keys), keys, 'left_anti').select(*keys, *partitionKeys)
        # The incoming update could update existing datas
        update_existing = indexfile.join(new_updated.select(*keys), keys, 'inner').select(*partitionKeys)
        deleted_ids = inputfile.filter(inputfile.Op == 'D').join(inputfile.filter(inputfile.Op == 'I'), keys, 'left_anti').select(*keys)
        # Existing deleted data
        delete_existing = indexfile.join(deleted_ids, keys, 'inner').select(*partitionKeys)
        
        result = new_inserted.union(new_updated.select(*partitionKeys)).union(update_existing).union(delete_existing)
        return result.distinct().collect()

    # Fore partitionKey 'created_year,created_month', add created_year, created_month column to inputfile
    def prepare_partition_columns(self, inputfile, partitionKeys):
        if 'created_year' in partitionKeys and 'created_month' in partitionKeys and 'created_day' in partitionKeys:
            return inputfile.withColumn('created_year', year('created_at')).withColumn('created_month', month('created_at')).withColumn('created_day', dayofmonth('created_at'))
        elif 'created_year' in partitionKeys and 'created_month' in partitionKeys and 'created_day' not in partitionKeys:
            return inputfile.withColumn('created_year', year('created_at')).withColumn('created_month', month('created_at'))
        else:
            return inputfile

    def convert_partitions_to_paths(self, all_partitions, folder, partitionKeys):
        target_paths = []
        for p in all_partitions:
            f = 's3://{}/{}{}'.format(self.datalake_bucket, self.datalake_prefix, folder)
            partition_part = ''
            for k in partitionKeys:
                partition_part = '{}{}={}/'.format(partition_part, k, p[k])
            path = f + partition_part
            target_paths.append(path)
        return target_paths
    def harmonize_schemas_and_combine(self, df_left, df_right):
        left_types = {f.name: f.dataType for f in df_left.schema}
        right_types = {f.name: f.dataType for f in df_right.schema}
        left_fields = set((f.name, f.dataType) for f in df_left.schema)
        right_fields = set((f.name, f.dataType) for f in df_right.schema)

        # First go over left-unique fields
        for l_name, l_type in left_fields.difference(right_fields):
            if l_name in right_types:
                r_type = right_types[l_name]
                if l_type != r_type:
                    raise TypeError("Union failed. Type conflict on field %s. left type %s, right type %s" % (l_name, l_type, r_type))
            df_right = df_right.withColumn(l_name, lit(None).cast(l_type))

        # Now go over right-unique fields
        for r_name, r_type in right_fields.difference(left_fields):
            if r_name in left_types:
                l_type = left_types[r_name]
                if r_type != l_type:
                    raise TypeError("Union failed. Type conflict on field %s. right type %s, left type %s" % (r_name, r_type, l_type))
            df_left = df_left.withColumn(r_name, lit(None).cast(r_type))    

        # Make sure columns are in the same order
        df_left = df_left.select(df_right.columns)

        return df_left.union(df_right)
    def get_output_has_partition(self, inputfile_partitioned, folder, primaryKey, partitionKeys, indexfile):
        primaryKeys = primaryKey.split(',')
        if self.has_updated_at(inputfile_partitioned): 
            windowRow = Window.partitionBy(primaryKeys).orderBy("updated_at")
        else:
            windowRow = Window.partitionBy(primaryKeys).orderBy("sortpath")
        files = spark.createDataFrame([['null']])
        deleted_files = files.collect()
        has_update = len(inputfile_partitioned.filter(inputfile_partitioned.Op == 'U').head(1)) > 0
        has_delete = len(inputfile_partitioned.filter(inputfile_partitioned.Op == 'D').head(1)) > 0
        if indexfile != None and (has_update or has_delete):
            all_partitions = self.get_updated_partitions(inputfile_partitioned, indexfile, partitionKeys, primaryKey)
        else:
            all_partitions = inputfile_partitioned.filter(inputfile_partitioned.Op != 'D').select(*partitionKeys).distinct().collect()

        
        target_file_paths = self.convert_partitions_to_paths(all_partitions, folder, partitionKeys)
        target_paths = self.existing_datalake_paths(target_file_paths)

        base_path = 's3://{}/{}{}'.format(self.datalake_bucket, self.datalake_prefix, folder)
        if len(target_paths) > 0:
            target = spark.read.option("basePath",base_path).option("mergeSchema", "true").parquet(*target_paths).withColumn("sortpath", lit("0")).withColumn("filepath",input_file_name()).withColumn("rownum", lit(0))
            target.cache()
            input = inputfile_partitioned.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))
            #files = target.join(inputfile_partitioned, primaryKeys, 'inner').select(col("filepath").alias("filepath1")).distinct()
            files = target.select('filepath').distinct()

            uniondata = self.harmonize_schemas_and_combine(input, target).join(input.filter(input.Op == 'D').select(*primaryKeys), primaryKeys, 'left_anti')

            #uniondata = input.select(target.columns).union(target).join(input.filter(input.Op == 'D').select(*primaryKeys), primaryKeys, 'left_anti')
            window = Window.partitionBy(primaryKeys).orderBy(desc("rownum"))
            output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile_partitioned.columns)
            deleted_files = files.collect()
        else:
            input = inputfile_partitioned.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))
            window = Window.partitionBy(primaryKeys).orderBy(desc("rownum"))
            output = input.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").join(input.filter(input.Op == 'D').select(*primaryKeys), primaryKeys, 'left_anti').coalesce(1).select(inputfile_partitioned.columns)
        
        return (output, deleted_files)

    # Data has primary key, but it's not partitioned
    def get_output_no_partition(self, inputfile, folder, primaryKey):
        s3_outputpath = 's3://' + self.datalake_bucket + '/' + self.datalake_prefix + folder

        primaryKeys = primaryKey.split(",")
        if self.has_updated_at(inputfile): 
            windowRow = Window.partitionBy(primaryKeys).orderBy("updated_at")
        else:
            windowRow = Window.partitionBy(primaryKeys).orderBy("sortpath")
    
        has_update = len(inputfile.filter(inputfile.Op == 'U').head(1)) > 0
        has_delete = len(inputfile.filter(inputfile.Op == 'D').head(1)) > 0
        if has_update or has_delete:
            target = spark.read.option("mergeSchema", "true").parquet(s3_outputpath).withColumn("sortpath", lit("0")).withColumn("filepath",input_file_name()).withColumn("rownum", lit(0))
            target.cache()
            input = inputfile.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))
    
            #determine impacted files
            files = target.join(inputfile, primaryKeys, 'inner').select(col("filepath").alias("filepath1")).distinct()
    
            #union new and existing data of impacted files
            uniondata = self.harmonize_schemas_and_combine(input, target.join(files,files.filepath1==target.filepath)).join(input.filter(input.Op == 'D').select(*primaryKeys), primaryKeys, 'left_anti')
            #uniondata = input.select(target.columns).union(target.join(files,files.filepath1==target.filepath).select(target.columns)).join(input.filter(input.Op == 'D').select(*primaryKeys), primaryKeys, 'left_anti')
            window = Window.partitionBy(primaryKeys).orderBy(desc("rownum"))
            output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)
            deleted_files = files.collect()
        else:
            input = inputfile.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))
            window = Window.partitionBy(primaryKeys).orderBy(desc("rownum"))
            output = input.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)
            files = spark.createDataFrame([['null']])
            deleted_files = files.collect()
        return (output, deleted_files)

    def get_updated_index_file(self, indexfile, inputfile_partitioned, partitionKeys, primaryKeys):
        keys = primaryKeys.split(',')
        if self.has_updated_at(inputfile_partitioned): 
            windowRow = Window.partitionBy(keys).orderBy("updated_at")
        else:
            windowRow = Window.partitionBy(keys).orderBy("sortpath")

        ori_file = indexfile.withColumn("sortpath", lit("0")).withColumn("rownum", lit(0)).withColumn('Op', lit('I'))
        
        input = inputfile_partitioned.withColumn("sortpath", input_file_name()).withColumn("rownum", row_number().over(windowRow))
        
        
        uniondata = input.select(ori_file.columns).union(ori_file).join(input.filter(input.Op == 'D').select(*keys), keys, 'left_anti').select(ori_file.columns)
        window = Window.partitionBy(keys).orderBy(desc("rownum"))
        updated_index_file = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(indexfile.columns)
        return updated_index_file
        

    def load_incremental_file(self, folder, updated_file_paths, primaryKey, partitionKey, needIndexFile):
        s3_outputpath = 's3://' + self.datalake_bucket + '/' + self.datalake_prefix + folder

        input_df = glueContext.create_dynamic_frame_from_options("s3", {'paths': updated_file_paths,  'groupFiles': 'inPartition' }, format="parquet")
        inputfile = input_df.toDF()
        inputfile.cache()
        partitionKeys = partitionKey.split(',')
        
        files = spark.createDataFrame([['null']])
        deleted_files = files.collect()

        has_index_file = False
        if primaryKey == 'null':
            output = self.get_output_no_pk(inputfile)
        else:
            if self.is_table_partitioned(partitionKey) and needIndexFile:
                id_file_path = 's3://{}/{}{}'.format(self.datalake_bucket, self.id_prefix, folder)
                indexfile_withpath = spark.read.parquet(id_file_path).withColumn('filepath', input_file_name())
                indexfile_withpath.cache()
                old_index_file_paths = indexfile_withpath.select('filepath').distinct().collect()
                
                indexfile = indexfile_withpath.drop('filepath')

                inputfile_partitioned = self.prepare_partition_columns(inputfile, partitionKeys)

                updated_index_file = self.get_updated_index_file(indexfile, inputfile_partitioned, partitionKeys, primaryKey)
                has_index_file = True
                output, deleted_files = self.get_output_has_partition(inputfile_partitioned, folder, primaryKey, partitionKeys, indexfile)
            elif self.is_table_partitioned(partitionKey) and not needIndexFile:
                inputfile_partitioned = self.prepare_partition_columns(inputfile, partitionKeys)
                output, deleted_files = self.get_output_has_partition(inputfile_partitioned, folder, primaryKey, partitionKeys, None)
            else:
                output, deleted_files = self.get_output_no_partition(inputfile, folder, primaryKey)


        if self.is_table_partitioned(partitionKey):
            output.repartition(partitionKeys[0]).write.mode('append').partitionBy(partitionKeys).parquet(s3_outputpath)
        else:
            output.coalesce(1).write.mode('append').parquet(s3_outputpath)

        if has_index_file:
            updated_index_file.coalesce(1).write.mode('append').parquet(id_file_path)
            self.remove_files(old_index_file_paths)
        
        self.remove_files(deleted_files)

    def prepare_ddb_table(self):
        try:
            self.ddbconn.describe_table(TableName=self.ddbTableName)
        except Exception:
            self.ddbconn.create_table(
                TableName=self.ddbTableName,
                KeySchema=[{'AttributeName': 'path','KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'path','AttributeType': 'S'}],
                ProvisionedThroughput={'ReadCapacityUnits': 1,'WriteCapacityUnits': 1})

    # Start to load incremental files
    def start(self):
        folders = self.s3conn.list_objects(Bucket=self.bucket, Prefix=self.prefix, Delimiter='/').get('CommonPrefixes')
        self.prepare_ddb_table()

        for f in folders:
            full_folder = f['Prefix']

            folder = full_folder[len(self.prefix):]
            path = self.bucket + '/' + full_folder

            item = {
                'path': {'S':path},
                'bucket': {'S':self.bucket},
                'prefix': {'S':self.prefix},
                'folder': {'S':folder},
                'PrimaryKey': {'S':'id'},
                'PartitionKey': {'S':'null'},
                'needIndexFile': {'S': 'null'},
                'LastFullLoadDate': {'S':'1900-01-01 00:00:00'},
                'LastIncrementalFile': {'S':path + '0.parquet'},
                'ActiveFlag': {'S':'false'}}


            #Put Item if not already present
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
            lastIncrementalFile = item['LastIncrementalFile']['S']
            activeFlag = item['ActiveFlag']['S']
            primaryKey = item['PrimaryKey']['S']
            needIndexFile = item['needIndexFile']['S'] == 'true'

            loadIncremental = False
            newIncrementalFile = path + '0.parquet'

            #determine if need to run incremental --> Run incremental --> Update DDB
            if activeFlag == 'true':
                #Get the latest incremental file
                incrementalFiles = self.get_all_objects(full_folder)
                if len(incrementalFiles) > 0:
                    filecount = len(incrementalFiles)
                    newIncrementalFile = self.bucket + '/' + incrementalFiles[filecount-1]['Key']
                    if newIncrementalFile != lastIncrementalFile:
                        updated_file_paths = self.get_updated_file_paths(incrementalFiles, lastIncrementalFile, newIncrementalFile)
                        loadIncremental = True
                        message = "Starting to process incremental files"
                    else:
                        message = "Incremental files already processed."
                else:
                    message = "No incremental files to process."
            else:
                message = "Load is not active.  Update dynamoDB."
            logger.info(message)

            if loadIncremental:
                self.load_incremental_file(folder, updated_file_paths, primaryKey, partitionKey, needIndexFile)
                logger.info('Complete load ' + newIncrementalFile)
                self.ddbconn.update_item(
                    TableName=self.ddbTableName,
                    Key={"path": {"S":path}},
                    AttributeUpdates={"LastIncrementalFile": {"Value": {"S": newIncrementalFile}}})

load = LoadIncremental()
load.start()
glue_client = boto3.client('glue', args['region'])
glue_client.start_crawler(Name=args['crawler_name'])






    
    