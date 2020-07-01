import { Construct, RemovalPolicy } from "@aws-cdk/core";
import {CfnEndpoint, CfnReplicationInstance, CfnReplicationSubnetGroup, CfnReplicationTask} from "@aws-cdk/aws-dms"
import { Bucket } from "@aws-cdk/aws-s3";
import * as iam from '@aws-cdk/aws-iam';
import { ServicePrincipal } from "@aws-cdk/aws-iam";
import { Vpc, SubnetType, SecurityGroup } from "@aws-cdk/aws-ec2";
import { SourceDB } from "./db-stack";
import { AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId } from "@aws-cdk/custom-resources";

export interface DmsProps {
    source: SourceProps;
    vpc: Vpc;
} 
export interface SourceProps {
    port: number,
    serverName: string,
    username: string,
    password: string,
    sourceDB: SourceDB 
}
export class DMS extends Construct {
    public readonly rawBucket: Bucket;
    constructor(scope: Construct, id: string, props: DmsProps) {
        super(scope, id);
        const sourceEndpoint: CfnEndpoint = new CfnEndpoint(this, 'Source', {
            endpointType: 'source',
            engineName: 'postgres',
            databaseName: 'dvdrental',
            password: props.source.password,
            port: props.source.port,
            serverName: props.source.serverName,
            username: props.source.username
        });
        this.rawBucket = new Bucket(this, 'RawBucket', {
            removalPolicy: RemovalPolicy.DESTROY
        });
        
        const dmsTargetRole = new iam.Role(this, 'dmsTargetRole', {
            assumedBy: new ServicePrincipal('dms.amazonaws.com')
        });
        
        this.rawBucket.grantReadWrite(dmsTargetRole);
        const targetEndpoint: CfnEndpoint = new CfnEndpoint(this, 'Target', {
            endpointType: 'target',
            engineName: 's3',
            extraConnectionAttributes: 'dataFormat=parquet;',
            s3Settings: {
                bucketName: this.rawBucket.bucketName,
                serviceAccessRoleArn: dmsTargetRole.roleArn
            }
        });
        const subnetGroup: CfnReplicationSubnetGroup = new CfnReplicationSubnetGroup(this, 'DmsSubnetGroup', {
            subnetIds: props.vpc.selectSubnets({
                subnetType: SubnetType.PRIVATE
            }).subnetIds,
            replicationSubnetGroupDescription: 'Replication Subnet group'
        });
        const dmsSecurityGroup = new SecurityGroup(this, 'DmsSecurityGroup', {
            vpc: props.vpc
        });
        props.source.sourceDB.sourceDB.connections.allowDefaultPortFrom(dmsSecurityGroup);
        const instance: CfnReplicationInstance = new CfnReplicationInstance(this, 'DmsInstance', {
            replicationInstanceClass: 'dms.t2.micro',
            allocatedStorage: 10,
            allowMajorVersionUpgrade: false,
            autoMinorVersionUpgrade: false,
            multiAz: false,
            publiclyAccessible: false,
            replicationSubnetGroupIdentifier: subnetGroup.ref,
            vpcSecurityGroupIds: [dmsSecurityGroup.securityGroupId]
        });
         
        const replicationTask: CfnReplicationTask = new CfnReplicationTask(this, 'ReplicationTask', {
            migrationType: 'full-load-and-cdc',
            replicationInstanceArn: instance.ref,
            sourceEndpointArn: sourceEndpoint.ref,
            tableMappings: JSON.stringify(this.getMappingRule()),
            targetEndpointArn: targetEndpoint.ref,
            replicationTaskSettings: JSON.stringify(this.getTaskSetting())
        });

        new AwsCustomResource(this, 'StartTask', {
            onCreate: {
                service: 'DMS',
                action: 'startReplicationTask',
                parameters: {
                    ReplicationTaskArn: replicationTask.ref,
                    StartReplicationTaskType: "start-replication"
                },
                physicalResourceId: PhysicalResourceId.fromResponse('ReplicationTask.ReplicationTaskIdentifier'),
                outputPath: 'ReplicationTask.ReplicationTaskIdentifier'
            },
            policy: AwsCustomResourcePolicy.fromSdkCalls({resources: AwsCustomResourcePolicy.ANY_RESOURCE})
        });
    }

    private getMappingRule(): Object {
        return {
            "rules": [
              {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                  "schema-name": "public",
                  "table-name": "%"
                },
                "rule-action": "include",
                "filters": []
              }
            ]
          };
          
    }
    private getTaskSetting(): Object {
        return {
            "TargetMetadata": {
                "TargetSchema": "",
                "SupportLobs": true,
                "FullLobMode": false,
                "LobChunkSize": 0,
                "LimitedSizeLobMode": true,
                "LobMaxSize": 32,
                "InlineLobMaxSize": 0,
                "LoadMaxFileSize": 0,
                "ParallelLoadThreads": 0,
                "ParallelLoadBufferSize": 0,
                "BatchApplyEnabled": false,
                "TaskRecoveryTableEnabled": false,
                "ParallelLoadQueuesPerThread": 0,
                "ParallelApplyThreads": 0,
                "ParallelApplyBufferSize": 0,
                "ParallelApplyQueuesPerThread": 0
            },
            "FullLoadSettings": {
                "TargetTablePrepMode": "DROP_AND_CREATE",
                "CreatePkAfterFullLoad": false,
                "StopTaskCachedChangesApplied": false,
                "StopTaskCachedChangesNotApplied": false,
                "MaxFullLoadSubTasks": 8,
                "TransactionConsistencyTimeout": 600,
                "CommitRate": 10000
            },
            "Logging": {
                "EnableLogging": false,
                "LogComponents": [
                    {
                        "Id": "DATA_STRUCTURE",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "COMMUNICATION",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "IO",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "COMMON",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "FILE_FACTORY",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "FILE_TRANSFER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "REST_SERVER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "ADDONS",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TARGET_LOAD",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TARGET_APPLY",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "SOURCE_UNLOAD",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "SOURCE_CAPTURE",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TRANSFORMATION",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "SORTER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TASK_MANAGER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TABLES_MANAGER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "METADATA_MANAGER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "PERFORMANCE",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "VALIDATOR_EXT",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    }
                ],
                "CloudWatchLogGroup": null,
                "CloudWatchLogStream": null
            },
            "ControlTablesSettings": {
                "historyTimeslotInMinutes": 5,
                "ControlSchema": "",
                "HistoryTimeslotInMinutes": 5,
                "HistoryTableEnabled": false,
                "SuspendedTablesTableEnabled": false,
                "StatusTableEnabled": false
            },
            "StreamBufferSettings": {
                "StreamBufferCount": 3,
                "StreamBufferSizeInMB": 8,
                "CtrlStreamBufferSizeInMB": 5
            },
            "ChangeProcessingDdlHandlingPolicy": {
                "HandleSourceTableDropped": true,
                "HandleSourceTableTruncated": true,
                "HandleSourceTableAltered": true
            },
            "ErrorBehavior": {
                "DataErrorPolicy": "LOG_ERROR",
                "DataTruncationErrorPolicy": "LOG_ERROR",
                "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                "DataErrorEscalationCount": 0,
                "TableErrorPolicy": "SUSPEND_TABLE",
                "TableErrorEscalationPolicy": "STOP_TASK",
                "TableErrorEscalationCount": 0,
                "RecoverableErrorCount": -1,
                "RecoverableErrorInterval": 5,
                "RecoverableErrorThrottling": true,
                "RecoverableErrorThrottlingMax": 1800,
                "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                "ApplyErrorInsertPolicy": "LOG_ERROR",
                "ApplyErrorUpdatePolicy": "LOG_ERROR",
                "ApplyErrorEscalationPolicy": "LOG_ERROR",
                "ApplyErrorEscalationCount": 0,
                "ApplyErrorFailOnTruncationDdl": false,
                "FullLoadIgnoreConflicts": true,
                "FailOnTransactionConsistencyBreached": false,
                "FailOnNoTablesCaptured": false
            },
            "ChangeProcessingTuning": {
                "BatchApplyPreserveTransaction": true,
                "BatchApplyTimeoutMin": 1,
                "BatchApplyTimeoutMax": 30,
                "BatchApplyMemoryLimit": 500,
                "BatchSplitSize": 0,
                "MinTransactionSize": 1000,
                "CommitTimeout": 1,
                "MemoryLimitTotal": 1024,
                "MemoryKeepTime": 60,
                "StatementCacheSize": 50
            },
            "PostProcessingRules": null,
            "CharacterSetSettings": null,
            "LoopbackPreventionSettings": null,
            "BeforeImageSettings": null
        };
    }
}