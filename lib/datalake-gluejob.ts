import { Construct, RemovalPolicy, Stack } from "@aws-cdk/core";
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import * as glue from '@aws-cdk/aws-glue'
import { CfnCrawler, CfnJob, CfnTrigger } from "@aws-cdk/aws-glue";
import * as iam from '@aws-cdk/aws-iam';
import { ServicePrincipal, PolicyStatement } from "@aws-cdk/aws-iam";
import { Bucket } from "@aws-cdk/aws-s3";
import * as asset from '@aws-cdk/aws-s3-assets'
import * as path from 'path';
import { LogGroup } from "@aws-cdk/aws-logs";
export interface GlueJobProps {
    rawBucket: Bucket;
    schemaName: string;
    logGroup: LogGroup;
}
export class GlueJob extends Construct {
    private readonly incrementalJobName: string = 'IncrementalDatalakeJob';
    constructor(scope: Construct, id: string, props: GlueJobProps) {
        super(scope, id);
        const controllerTable = new dynamodb.Table(this, 'ControllerTable', {
            partitionKey: { name: 'path', type: dynamodb.AttributeType.STRING },
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            removalPolicy: RemovalPolicy.DESTROY
        });
        const database = new glue.Database(this, 'Datalake-glue-database', {
            databaseName: 'datalake'
        });
        const datalakeBucket = new Bucket(this, 'Datalake-bucket', {
            removalPolicy: RemovalPolicy.DESTROY
        });
        const glueRole = new iam.Role(this, 'glueRole', {
            assumedBy: new ServicePrincipal('glue.amazonaws.com')
        });
        props.rawBucket.grantRead(glueRole);
        datalakeBucket.grantReadWrite(glueRole);
        glueRole.addManagedPolicy({
            managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        })
        const crawler = new CfnCrawler(this, 'Crawler', {
            role: glueRole.roleArn,
            configuration: "{\"Version\":1.0,\"CrawlerOutput\":{\"Partitions\":{\"AddOrUpdateBehavior\":\"InheritFromTable\"},\"Tables\":{\"AddOrUpdateBehavior\":\"MergeNewColumns\"}}}",
            databaseName: database.databaseName,
            targets: {
                s3Targets: [
                    {
                        path: 's3://' + datalakeBucket.bucketName + '/datalake/'
                    }
                ]
            },
            name: 'DatalakeCrawler'
        });

        const initDatalakeScript = new asset.Asset(this, 'initDatalakeScript', {
            path: path.join(__dirname, '..', 'gluejob', 'InitDatalake.py')
          });
        const incrementalJobScript = new asset.Asset(this, 'incrementalJobScript', {
            path: path.join(__dirname, '..', 'gluejob', 'LoadIncremental.py')
        });
        initDatalakeScript.grantRead(glueRole);
        incrementalJobScript.grantRead(glueRole);
        controllerTable.grantFullAccess(glueRole);

        const config = require('../config/config.json');
        const initGlueJob = new CfnJob(this, 'InitDatalakeJob', {
            role: glueRole.roleArn,
            allocatedCapacity: 2,
            executionProperty: {
                maxConcurrentRuns: 1
            },
            glueVersion: '1.0',
            defaultArguments: {
                '--prefix': props.schemaName + '/',
                '--bucket': props.rawBucket.bucketName,
                '--datalake_bucket': datalakeBucket.bucketName,
                '--datalake_prefix': 'datalake/',
                '--region': Stack.of(this).region,
                '--controller_table_name': controllerTable.tableName,
                '--primaryKey': config.primaryKey,
                '--partitionKey': config.partitionKey,
                '--enable-continuous-cloudwatch-log': true,
                '--continuous-log-logGroup': props.logGroup.logGroupName
            },
            command: {
                name: 'glueetl',
                scriptLocation: 's3://' + initDatalakeScript.s3BucketName + '/' + initDatalakeScript.s3ObjectKey,
                pythonVersion: '3'
            }
        });


        const incrementalGlueJob = new CfnJob(this, 'IncrementalDatalakeJob', {
            role: glueRole.roleArn,
            name: this.incrementalJobName,
            allocatedCapacity: 2,
            executionProperty: {
                maxConcurrentRuns: 1
            },
            glueVersion: '1.0',
            defaultArguments: {
                '--prefix': props.schemaName + '/',
                '--bucket': props.rawBucket.bucketName,
                '--datalake_bucket': datalakeBucket.bucketName,
                '--datalake_prefix': 'datalake/',
                '--id_prefix': 'index/',
                '--region': Stack.of(this).region,
                '--controller_table_name': controllerTable.tableName,
                '--crawler_name': crawler.name,
                '--enable-continuous-cloudwatch-log': true,
                '--continuous-log-logGroup': props.logGroup.logGroupName
            },
            command: {
                name: 'glueetl',
                scriptLocation: 's3://' + incrementalJobScript.s3BucketName + '/' + incrementalJobScript.s3ObjectKey,
                pythonVersion: '3'
            }
        });

        new CfnTrigger(this, 'DatalakeJobTrigger', {
            type: 'SCHEDULED',
            schedule: 'cron(0 6 * * ? *)',
            startOnCreation: false,
            actions: [
                {
                    jobName: this.incrementalJobName
                }
            ]
        });
    }
}