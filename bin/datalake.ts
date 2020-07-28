#!/usr/bin/env node
import * as cdk from '@aws-cdk/core';
import { DatalakeStack } from '../lib/datalake-stack';

const app = new cdk.App();
new DatalakeStack(app, 'DatalakeStack2');
