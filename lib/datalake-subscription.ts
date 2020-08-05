import { Construct } from "@aws-cdk/core";
import * as sns from '@aws-cdk/aws-sns';
import * as subs from '@aws-cdk/aws-sns-subscriptions';
import * as lambda from '@aws-cdk/aws-lambda';
import { SubscriptionFilter, FilterPattern } from '@aws-cdk/aws-logs';
import { LambdaDestination } from '@aws-cdk/aws-logs-destinations';
import * as iam from '@aws-cdk/aws-iam';
import { ServicePrincipal, ManagedPolicy } from "@aws-cdk/aws-iam";
import * as path from 'path';
import MyLogGroup from './myLogGroup';

export interface SubscriptionProps {
    emailSubscriptionList: string[],
    smsSubscriptionList: string[]
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

        const lambdaExecutionRole = new iam.Role(this, 'lambdaExecutionRole', {
            assumedBy: new ServicePrincipal('lambda.amazonaws.com'),    
            managedPolicies: [
                ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
                ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSFullAccess')
            ]
        });
        /**
         * Subscription Filter below will send the error log to this lambda function, and the function
         * will trigger SNS.
         */
        const subscriptionFn = new lambda.Function(this, 'subscriptionFn', {
            runtime: lambda.Runtime.NODEJS_10_X,
            handler: 'index.handler',
            code: lambda.Code.fromAsset(path.join(__dirname, '..', 'lambda', 'subscription')),
            role: lambdaExecutionRole,
            environment: {
                TOPIC_ARN: topic.topicArn
            }
        });
        
        //Monitoring logs in the given log group, when pattern detected, trigger destination function
        new SubscriptionFilter(this, 'subscriptionFilter', {
            logGroup: new MyLogGroup(
                'arn:aws:logs:us-west-2:193793567275:log-group:/aws-glue/jobs/error:*', 
                '/aws-glue/jobs/error'
            ),
            destination: new LambdaDestination(subscriptionFn),
            filterPattern: FilterPattern.allTerms("Traceback")
        });
    }
}