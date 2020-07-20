import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
import { DMS } from './datalake-dms';
import { GlueJob } from './datalake-gluejob';

const config = require("../config/config.json");

export class DatalakeStack extends cdk.Stack {
  constructor(scope: cdk.App, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'VPC', {
      maxAzs: 2,
      natGateways: 1,
      cidr: config.vpc
    });

    const dms = new DMS(this, 'DmsPart', {
      vpc,
      source: {
        port: config.port,
        username: config.username,
        password: config.password,
        serverName: config.serverName,
        engineName: config.engineName,
        databaseName: config.databaseName
      }
    });

    const rawBucket = dms.rawBucket;
    new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket
    });
  }
 
}
