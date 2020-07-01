import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as secretmanager from '@aws-cdk/aws-secretsmanager';
import * as rds from '@aws-cdk/aws-rds';
import { Construct, RemovalPolicy } from '@aws-cdk/core';
import { Secret } from '@aws-cdk/aws-secretsmanager';
export interface SourceDBProps {
    vpc: ec2.Vpc;
}

export class SourceDB extends Construct {
    public readonly sourceDB: rds.DatabaseInstance;
    public readonly masterUsername: string = 'postgres';
    public readonly dbCredential: Secret;
    constructor(scope: Construct, id: string, props: SourceDBProps) {
        super(scope, id);
        const parameterGroup = new rds.ParameterGroup(this, 'ParameterGroup', {
            family: 'postgres11',
            parameters: {
              'rds.logical_replication': '1',
              'wal_sender_timeout': '0'
            }
          });
        const dbSg = new ec2.SecurityGroup(this, 'DBSecurityGroup', {
            vpc: props.vpc
        });
        dbSg.addIngressRule(dbSg, ec2.Port.allTraffic());
        this.dbCredential = new secretmanager.Secret(this, 'DBCredentialsSecret', {
            generateSecretString: {
              secretStringTemplate: JSON.stringify({
                username: this.masterUsername,
              }),
              excludePunctuation: true,
              includeSpace: false,
              excludeCharacters: '+%;:{}',
              generateStringKey: 'password'
            }
          });
        
        this.sourceDB = new rds.DatabaseInstance(this, 'datalake-db', {
            engine: rds.DatabaseInstanceEngine.POSTGRES,
            engineVersion: '11.5',
            parameterGroup,
            instanceClass: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE2, ec2.InstanceSize.MICRO),
            masterUsername: this.masterUsername,
            allocatedStorage: 20,
            databaseName: 'dvdrental',
            masterUserPassword: this.dbCredential.secretValueFromJson('password'),
            removalPolicy: RemovalPolicy.DESTROY,
            deleteAutomatedBackups: true,
            securityGroups: [dbSg],
            vpc: props.vpc,
            deletionProtection: false
          });
          this.dbCredential.attach(this.sourceDB);
    }
}