import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
import { DMS } from './datalake-dms';
import { GlueJob } from './datalake-gluejob';
import { Subscription } from './datalake-subscription';
import { LogGroup } from '@aws-cdk/aws-logs';

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
      },
      s3LifecycleRule: parseLifecycleRule(config.s3LifecycleRule)
    });

    const rawBucket = dms.rawBucket;
    const glueJob = new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket: rawBucket,
      schemaName: config.schemaName
    });

    new Subscription(this, 'Subscription', {
      emailSubscriptionList: config.emailSubscriptionList,
      smsSubscriptionList: config.smsSubscriptionList,
      glueJob: glueJob
    });
  }
 
}


function parseLifecycleRule(json: Array<any>): any[] {
  let rules: any[] = [];

  json.forEach( (obj) => {
    let rule: any;
    rule = obj;
    if (obj.expiration) {
      rule.expiration = cdk.Duration.days(obj.expiration);
    }
    if (obj.abortIncompleteMultipartUploadAfter) {
      rule.abortIncompleteMultipartUploadAfter = cdk.Duration.days(obj.abortIncompleteMultipartUploadAfter);
    }
    rules.push(rule);
  })
  return rules;
}