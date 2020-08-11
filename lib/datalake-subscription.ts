import { Construct } from "@aws-cdk/core";
import * as sns from '@aws-cdk/aws-sns';
import * as subs from '@aws-cdk/aws-sns-subscriptions';
import { Rule } from '@aws-cdk/aws-events';
import MyPattern from './MyEventPattern';
import { GlueJob } from "./datalake-gluejob";
import {SnsTopic} from '@aws-cdk/aws-events-targets';

export interface SubscriptionProps {
    emailSubscriptionList: string[],
    smsSubscriptionList: string[],
    glueJob: GlueJob
} 
/**
 * Subscribe error log from DMS and Glue jobs 
 */
export class Subscription extends Construct {

    constructor(scope: Construct, id: string, props: SubscriptionProps ) {
        super(scope, id);
        const topic = new sns.Topic(this, 'datalakeTopic', {
            displayName: 'DataLake Subscription Topic'
        });
      
        for (let email of props.emailSubscriptionList) {
            topic.addSubscription(new subs.EmailSubscription(email));
        }

        for (let phone of props.smsSubscriptionList) {
            topic.addSubscription(new subs.SmsSubscription(phone));
        }

        const glueEvent = new Rule(this, 'GlueEvent', {
            eventPattern: new MyPattern(this, 'GlueEventPattern', {
                source: ["aws.glue"],
                detailType: ["Glue Job State Change"],
                detail: {
                    "jobName": [props.glueJob.initialJobName, props.glueJob.incrementalJobName],
                    "state": ["FAILED"]
                }
            }),
            targets: [new SnsTopic(topic)]
        })
    }
}