import { Construct } from "@aws-cdk/core";
import * as ec2 from '@aws-cdk/aws-ec2';
import * as asset from '@aws-cdk/aws-s3-assets';
import * as path from 'path';
import * as codebuild from '@aws-cdk/aws-codebuild';
import { ComputeType, Source } from "@aws-cdk/aws-codebuild";
import * as rds from '@aws-cdk/aws-rds';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iam from '@aws-cdk/aws-iam';
import * as cdk from '@aws-cdk/core';
import * as cr from '@aws-cdk/custom-resources'
import * as cfn from '@aws-cdk/aws-cloudformation'
import { SourceDB } from "./db-stack";

export interface InitDBProps {
    vpc: ec2.Vpc,
    database: SourceDB
}

export class InitDB extends Construct {
    public readonly buildProject: codebuild.Project;
    public readonly initDbCodeBuild: cfn.CustomResource;
    constructor(scope: Construct, id: string, props: InitDBProps) {
        super(scope, id);
        const initDBAsset = new asset.Asset(this, 'InitDBAsset', {
            path: path.join(__dirname, '..', 'initDb', 'dvdrental.tar.zip')
          });
        const buildSg = new ec2.SecurityGroup(this, 'CodeBuildSecurityGroup', {
        vpc: props.vpc
        });
        const db = props.database.sourceDB;
        this.buildProject = new codebuild.Project(this, 'InitDBProject', {
            securityGroups: [buildSg],
            environment: {
              buildImage: codebuild.LinuxBuildImage.STANDARD_2_0,
              computeType: ComputeType.SMALL
            },
            vpc: props.vpc,
            source: codebuild.Source.s3({
              bucket: initDBAsset.bucket,
              path: initDBAsset.s3ObjectKey
            }),
            buildSpec: codebuild.BuildSpec.fromObject({
              version: '0.2',
              env: {
                "secrets-manager": {
                
                  PGPASSWORD: props.database.dbCredential.secretArn + ':password'
                },
                "variables": {
                    host: db.instanceEndpoint.hostname,
                    port: db.instanceEndpoint.port,
                    username: props.database.masterUsername
                }
              },
              phases: {
                install: {
                  "runtime-versions": {
                    python: '3.x'
                  },
                  commands: [
                    "apt-get update -y",
                    "apt-get install -y postgresql-client"
                  ]
                },
                build: {
                  commands: [
                    'pg_restore --host $host --port $port --username $username --dbname "dvdrental" --verbose "dvdrental.tar"'
                  ]
                }
              }
            })
        });
        props.database.dbCredential.grantRead(this.buildProject);
        initDBAsset.grantRead(this.buildProject);
        if (db.secret) {
          db.secret.grantRead(this.buildProject)
        }

        const restoreDBHandler = new lambda.Function(this, 'RestoreDBHandler', {
            runtime: lambda.Runtime.NODEJS_10_X,
            code: lambda.Code.fromAsset('lambda/startInitDB'),
            handler: 'restoreDB.onEvent',
        });
        restoreDBHandler.addToRolePolicy(new iam.PolicyStatement({
          actions: ["codebuild:BatchGetBuilds", "codebuild:StartBuild"],
          resources: [this.buildProject.projectArn]
        }));
        const checkInitDBHandler = new lambda.Function(this, 'CheckInitDBHandler', {
          runtime: lambda.Runtime.NODEJS_10_X,
          code: lambda.Code.fromAsset('lambda/checkInit'),
          handler: 'checkInit.onEvent'
        });
        checkInitDBHandler.addToRolePolicy(new iam.PolicyStatement({
          actions: ["codebuild:BatchGetBuilds", "codebuild:StartBuild"],
          resources: [this.buildProject.projectArn]
        }));
        const myProvider = new cr.Provider(this, 'MyProvider', {
          onEventHandler: restoreDBHandler,
          isCompleteHandler: checkInitDBHandler,
          queryInterval: cdk.Duration.seconds(30)
        });

        this.initDbCodeBuild = new cfn.CustomResource(this, 'RestoreDBResource', { provider: myProvider,
          properties: {
            ProjectName: this.buildProject.projectName
          }
        });

        
    }
}