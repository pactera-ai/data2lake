import * as cdk from '@aws-cdk/core';
import * as ec2 from '@aws-cdk/aws-ec2'
import { DMS } from './datalake-dms';
import { GlueJob } from './datalake-gluejob';
import * as sns from '@aws-cdk/aws-sns';
import * as subs from '@aws-cdk/aws-sns-subscriptions';
import * as lambda from '@aws-cdk/aws-lambda';
import { SubscriptionFilter, FilterPattern } from '@aws-cdk/aws-logs';
import { LambdaDestination } from '@aws-cdk/aws-logs-destinations';
import MyLogGroup from './myLogGroup';
import * as path from 'path';

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
    new GlueJob(this, 'DatalakeGlueJob', {
      rawBucket: rawBucket,
      schemaName: config.schemaName
    });

    const topic = new sns.Topic(this, 'datalake_topic', {
      displayName: 'DataLake Subscription Topic'
    });

    topic.addSubscription(new subs.EmailSubscription("steveguo1024@gmail.com"));
    
    const subscriptionFn = new lambda.Function(this, 'subscriptionFn', {
      runtime: lambda.Runtime.NODEJS_10_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'subscription')),
    });
    
    new SubscriptionFilter(this, 'subscriptionFilter', {
      logGroup: new MyLogGroup('arn:aws:logs:us-west-2:193793567275:log-group:/aws-glue/jobs/error:*', '/aws-glue/jobs/error'),
      destination: new LambdaDestination(subscriptionFn),
      filterPattern: FilterPattern.allTerms("Trackback")
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