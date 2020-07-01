data2lake is a tool to enable you to connect to your specified data source and automatically create your data lake on AWS. 

It is based on AWS CDK to connect the datasources via AWS DMS and form your data lake on AWS S3 

The process goes something like this:

1. specify your data source
2. configure your refresh frequency
3. define your security control
4. click a button to form your data lake 

# Table of Contents
## Requirements
## Caveats
## Install
## Dependency Setup
## Usage
## Example
## Advanced Configuration
## Provider Specific Documentation

You should explore the contents of this project. It demonstrates a CDK app with an instance of a stack (`DatalakeStack`)
which contains an Amazon SQS queue that is subscribed to an Amazon SNS topic.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template
