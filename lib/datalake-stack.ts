import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
import { DMS } from './datalake-dms';
import { GlueJob } from './datalake-gluejob';
import { Subscription } from './datalake-subscription';
import { CfnDataLakeSettings } from '@aws-cdk/aws-lakeformation';
import { MyPrinciple } from './MyPrinciple';

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
      sources: config.connections,
      s3LifecycleRule: parseLifecycleRule(config.s3LifecycleRule)
    });

    const datalakeSettings = new CfnDataLakeSettings(this, 'DataLakeSetting', {
      admins: [ new MyPrinciple(this, 'MyPrinciple', {
        dataLakePrincipalIdentifier: config.executiveArn
      }) ]
    })

    const rawBucket = dms.rawBucket;
    const glueJob = new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket: rawBucket,
      schemaList: JSON.stringify(config.connections),
      dependsOn: datalakeSettings
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