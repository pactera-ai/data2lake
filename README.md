# data2lake
CDK to enable you to connect to your specified data source and automatically create your data lake on AWS. It includes a CDK with an instance of a stack (`DatalakeStack`) which contains an Amazon SQS queue that is subscribed to an Amazon SNS topic.

The process goes something like this:

1. specify your data source
2. configure your refresh frequency
3. define your security control
4. click a button to form your data lake 

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Requirements](#requirements)
- [Install](#install)
- [Dependency Setup](#dependency-setup)
- [Usage](#usage)
- [Example](#example)
- [Advanced Configuration](#advanced-configuration)
- [Building](#building)
- [Provider Specific Documentation](#provider-specific-documentation)

## requirements

## install

## dependency-setup

## usage

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## example

## advanced-configuration

## building
### Useful commands

 * `npm run build`   compile typescript to js
 * `npm run watch`   watch for changes and compile
 * `npm run test`    perform the jest unit tests
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk synth`       emits the synthesized CloudFormation template

## provider-specific-documentation
