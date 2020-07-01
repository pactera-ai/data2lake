import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
import { SourceDB } from './db-stack';
import { InitDB } from './datalake-initdb';
import { DMS } from './datalake-dms';
import { GlueJob } from './datalake-gluejob';

export class DatalakeStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'VPC', {
      maxAzs: 2,
      natGateways: 1,
      cidr: '10.0.0.0/17'
    });

    const db = new SourceDB(this, 'SourceDB', {
      vpc
    });

    const buildProject = new InitDB(this, 'InitDBBuildProject', {
      vpc,
      database: db
    });
    db.sourceDB.connections.allowDefaultPortFrom(buildProject.buildProject)
    
    const dms = new DMS(this, 'DmsPart', {
      vpc,
      source: {
        port: db.sourceDB.instanceEndpoint.port,
        username: db.masterUsername,
        password: db.dbCredential.secretValueFromJson('password').toString(),
        serverName: db.sourceDB.instanceEndpoint.hostname,
        sourceDB: db
      }
    });
    dms.node.addDependency(buildProject);
    const rawBucket = dms.rawBucket;
    new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket
    });
  }
 
}
