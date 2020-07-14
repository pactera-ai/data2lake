import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
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

    const dms = new DMS(this, 'DmsPart', {
      vpc,
      source: {
        port: 5432,
        username: 'postgres',
        password: '12345678',
        serverName: 'cdk-datalake-sourcedb-1.ceh5weqzlis6.us-west-2.rds.amazonaws.com',
      }
    });

    const rawBucket = dms.rawBucket;
    new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket
    });
  }
 
}
